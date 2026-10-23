package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// classifyHTTPError classifies one upstream error exactly as the Messages route
// does (classifyHTTPErrorForPath), recording the raw status like the handler.
func classifyHTTPError(resp *http.Response) failure {
	b, err := io.ReadAll(resp.Body)
	f := classifyHTTPErrorForPath(context.Background(), "/v1/messages", resp, b, err)
	f.origStatus = resp.StatusCode
	return f
}

// §2: a fully-framed event with broken JSON must become a retry, not reach the
// client as "JSON Parse error: Unexpected identifier".
func TestMalformedJSONEventConverts(t *testing.T) {
	s := "event: message_start\ndata: {\"type\":\"message_start\"}\n\n" +
		"event: content_block_delta\ndata: {oops not json\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	_, f := capture(t, s)
	if f == nil || !f.transient || f.code != "malformed_sse" {
		t.Fatalf("want transient malformed_sse, got %+v", f)
	}
}

// HTTP errors follow the Stainless SDK retry rule: 408/409/429/5xx are retried,
// every other 4xx is the upstream's final answer. Covers real Anthropic bodies,
// including the auth/billing/not-found/validation shapes once retried forever.
func TestClassifyRealMessages(t *testing.T) {
	terminal := []struct {
		status int
		body   string
		code   string
	}{
		{400, `{"error":{"type":"invalid_request_error","message":"Missing Tool Result Block"}}`, "request_shape"},
		{400, `{"error":{"type":"invalid_request_error","message":"duplicate tool_use ID in conversation history"}}`, "request_shape"},
		{400, `{"error":{"type":"invalid_request_error","message":"prompt is too long for the context window"}}`, "request_shape"},
		{400, `{"error":{"type":"invalid_request_error","message":"max_tokens: 64001 > 64000, which is the maximum allowed number of output tokens for claude-opus-4-1-20250805"}}`, "http_400"},
		{400, `{"error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API."}}`, "http_400"},
		{401, `{"error":{"type":"authentication_error","message":"invalid x-api-key"}}`, "http_401"},
		{401, `<html><body>401 Authorization Required</body></html>`, "http_401"},
		{402, `{"error":{"type":"billing_error","message":"Usage credits required for 1M context"}}`, "http_402"},
		{403, `{"error":{"type":"permission_error","message":"This organization has been disabled."}}`, "http_403"},
		{404, `{"error":{"type":"not_found_error","message":"model: claude-nonexistent"}}`, "http_404"},
		{413, `{"error":{"type":"request_too_large","message":"Request exceeds the maximum allowed number of bytes."}}`, "http_413"},
		{418, `{"error":{"type":"api_error","message":"teapot"}}`, "http_418"},
		{429, `{"error":{"type":"rate_limit_error","message":"prompt is too long for this tier"}}`, "http_429"},
	}
	for _, c := range terminal {
		f := classifyHTTPError(mkResp(c.status, c.body))
		if f.transient || f.status != c.status || f.code != c.code || f.origStatus != c.status {
			t.Errorf("status %d %q: want terminal %d/%s, got %+v", c.status, c.body, c.status, c.code, f)
		}
	}

	trans := []struct {
		status int
		body   string
	}{
		{529, `{"error":{"type":"overloaded_error","message":"Overloaded"}}`},
		{500, `{"error":{"type":"api_error","message":"Internal server error"}}`},
		{429, `{"error":{"type":"rate_limit_error","message":"Server is temporarily limiting requests"}}`},
		{503, `{"error":{"type":"api_error","message":"temporary capacity issue"}}`},
		{408, `{"error":{"type":"timeout_error","message":"request timeout"}}`},
		{409, `{"error":{"type":"api_error","message":"conflict"}}`},
		// A 4xx whose body explicitly names a transient fault rides it out.
		{400, `{"error":{"type":"overloaded_error","message":"gateway overloaded"}}`},
	}
	for _, c := range trans {
		f := classifyHTTPError(mkResp(c.status, c.body))
		if !f.transient {
			t.Errorf("status %d %q: want TRANSIENT (retry), got %+v", c.status, c.body, f)
		}
		// A retryable 4xx outside 408/409/429 MUST be normalized to 502 — the SDK
		// refuses to retry a raw 4xx by status, regardless of x-should-retry.
		if c.status == 400 && f.status != 502 {
			t.Errorf("status %d %q: retryable 4xx must normalize to 502, got %d", c.status, c.body, f.status)
		}
		// ...but the RAW upstream status is preserved for the logs.
		if got := origStatusOf(f); got != c.status {
			t.Errorf("status %d %q: origStatusOf=%d, want raw %d", c.status, c.body, got, c.status)
		}
	}
}

// Redirects are not followed, so a 3xx is a terminal configuration error whose
// message names the target without its (possibly credential-bearing) query.
func TestClassifyRedirectIsTerminal(t *testing.T) {
	resp := mkResp(308, "")
	resp.Header.Set("Location", "https://new.example.com/v1/messages?key=secret")
	f := classifyHTTPError(resp)
	if f.transient || f.code != "upstream_redirect_308" || f.status != http.StatusBadGateway {
		t.Fatalf("want terminal upstream_redirect_308/502, got %+v", f)
	}
	if !strings.Contains(f.message, "https://new.example.com/v1/messages") || strings.Contains(f.message, "secret") {
		t.Fatalf("redirect message must name the target without its query: %q", f.message)
	}
}

// 408/409 keep the upstream's Retry-After, which also suppresses the 250ms
// legacy fast retry.
func TestClassify408HonorsRetryAfter(t *testing.T) {
	resp := mkResp(409, `{"error":{"message":"busy"}}`)
	resp.Header.Set("Retry-After", "30")
	if f := classifyHTTPError(resp); !f.transient || f.retryAfter != 30 {
		t.Fatalf("want transient with retryAfter=30, got %+v", f)
	}
}

// In-band Anthropic errors use the same identities as HTTP: documented
// permanent types are final; transient and unknown types ride out.
func TestClassifySSEErrorTypes(t *testing.T) {
	for typ, status := range map[string]int{
		"authentication_error": 401, "billing_error": 402, "permission_error": 403,
		"not_found_error": 404, "request_too_large": 413, "invalid_request_error": 400,
	} {
		f := classifySSEError(`{"type":"error","error":{"type":"` + typ + `","message":"m"}}`)
		if f.transient || f.status != status || f.atype != typ {
			t.Errorf("%s: want terminal %d, got %+v", typ, status, f)
		}
	}
	for _, typ := range []string{"overloaded_error", "rate_limit_error", "api_error", "timeout_error", "something_new"} {
		if f := classifySSEError(`{"type":"error","error":{"type":"` + typ + `","message":"m"}}`); !f.transient {
			t.Errorf("%s: want transient, got %+v", typ, f)
		}
	}
	if f := classifySSEError(`{"type":"error","error":{"type":"context_length_exceeded","message":"prompt is too long"}}`); f.transient {
		t.Errorf("unknown type naming a request-shape problem must be terminal, got %+v", f)
	}
}

// statusField shows one number when nothing was masked, and orig->surfaced when a
// transient cause was collapsed to the generic 503. origStatusOf falls back to the
// classified status for faults that never had an HTTP status of their own.
func TestStatusFieldAndOrigStatus(t *testing.T) {
	if got := statusField(503, 503); got != "503" {
		t.Errorf("equal: want 503, got %s", got)
	}
	if got := statusField(529, 503); got != "529->503" {
		t.Errorf("masked: want 529->503, got %s", got)
	}
	if got := statusField(400, 0); got != "400" {
		t.Errorf("no surface: want 400, got %s", got)
	}
	// transport/SSE faults carry no raw HTTP status; origStatusOf uses the classified one.
	if got := origStatusOf(failure{status: 504, code: "deadline"}); got != 504 {
		t.Errorf("synthetic fault: want 504, got %d", got)
	}
}

// §3: every retryable failure must reach Claude Code as ONE generic shape (503 +
// api_error), never a recognizable identity like overloaded_error/529 or
// rate_limit_error/429 that CC routes into a status-specific give-up path (e.g.
// "Repeated 529 Overloaded errors"). The diagnostic cause stays in .code.
func TestSurfaceNormalizesTransient(t *testing.T) {
	transient := []failure{
		{transient: true, status: 529, atype: "overloaded_error", code: "sse_overloaded"},
		{transient: true, status: 429, atype: "rate_limit_error", code: "rate_limit"},
		{transient: true, status: 504, atype: "timeout_error", code: "deadline"},
		{transient: true, status: 502, atype: "api_error", code: "transport_error"},
		{transient: true, status: 500, atype: "api_error", code: "http_500"},
	}
	for _, f := range transient {
		if st, at := surface(f); st != 503 || at != "api_error" {
			t.Errorf("surface(%s/%d) = (%d,%q), want (503,\"api_error\")", f.code, f.status, st, at)
		}
	}
	// Request-shape (non-transient) keeps its real status + type so it surfaces.
	shape := failure{status: 400, atype: "invalid_request_error", code: "request_shape"}
	if st, at := surface(shape); st != 400 || at != "invalid_request_error" {
		t.Errorf("surface(shape) = (%d,%q), want (400,\"invalid_request_error\")", st, at)
	}
	// client_gone stays 499 (non-transient) so the access log keeps that signal.
	gone := failure{status: 499, atype: "api_error", code: "client_gone"}
	if st, _ := surface(gone); st != 499 {
		t.Errorf("surface(client_gone) status = %d, want 499", st)
	}
}

// gapReader yields chunks with a delay before each, and aborts (returns the
// context error) if the context is cancelled during a gap — mimicking the
// transport closing when the proxy's idle watchdog fires.
type gapReader struct {
	ctx    context.Context
	chunks []string
	gap    time.Duration
	i      int
}

func (g *gapReader) Read(p []byte) (int, error) {
	if g.i >= len(g.chunks) {
		return 0, io.EOF
	}
	if g.i > 0 && g.gap > 0 {
		select {
		case <-time.After(g.gap):
		case <-g.ctx.Done():
			return 0, g.ctx.Err()
		}
	}
	n := copy(p, g.chunks[g.i])
	g.i++
	return n, nil
}

func eventsOf(s string) []string {
	parts := strings.SplitAfter(s, "\n\n")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// §6: a slow but progressing stream (gaps < byte-idle) is held to completion.
func TestLongGenerationSlowDrip(t *testing.T) {
	useDefaultConfig(t)
	cfg.upstreamByteIdle = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	r := &gapReader{ctx: ctx, chunks: eventsOf(goodStream), gap: 50 * time.Millisecond}
	_, f := captureSSE(ctx, cancel, rec, http.Header{}, r, nil)
	if f != nil {
		t.Fatalf("slow-drip should complete, got failure %+v", *f)
	}
}

// §6: past the keepalive grace the proxy commits and streams live (so a >300s
// turn survives the client's no-bytes ceiling). Real >300s wall-clock is the
// live docker `long-gen` scenario; here we shrink the grace to prove the path.
func TestKeepaliveCommitThenLive(t *testing.T) {
	useDefaultConfig(t)
	cfg.keepaliveMs = 80 * time.Millisecond
	cfg.upstreamByteIdle = 3 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	r := &gapReader{ctx: ctx, chunks: eventsOf(goodStream), gap: 120 * time.Millisecond}
	wrote, f := captureSSE(ctx, cancel, rec, http.Header{}, r, nil)
	if f != nil || !wrote {
		t.Fatalf("want committed live success, got wrote=%v fail=%+v", wrote, f)
	}
	body := rec.Body.String()
	if !strings.Contains(body, ": keepalive") {
		t.Fatalf("expected a keepalive ping in live mode: %q", body)
	}
	if !strings.Contains(body, "message_stop") || !strings.Contains(body, "Hi") {
		t.Fatalf("missing streamed content: %q", body)
	}
	if rec.Header().Get("X-Steady-Proxy-Mode") != "live" {
		t.Fatalf("want live mode header, got %q", rec.Header().Get("X-Steady-Proxy-Mode"))
	}
}

// §6: once committed to live mode, a late mid-stream overloaded_error must NOT be
// forwarded raw (it would re-introduce the very identity CC gives up on). The
// proxy ends the stream as a DROP so CC's native truncated-stream retry kicks in,
// and the access log keeps the true cause.
func TestLiveModeErrorNotForwardedRaw(t *testing.T) {
	useDefaultConfig(t)
	cfg.keepaliveMs = 60 * time.Millisecond
	cfg.upstreamByteIdle = 3 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	// This is the ungated (main-session) path, so message_start commits on the
	// keepalive tick; the error arrives after, in live mode.
	r := &gapReader{ctx: ctx, chunks: []string{
		"event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
		"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n",
	}, gap: 120 * time.Millisecond}
	wrote, f := captureSSE(ctx, cancel, rec, http.Header{}, r, nil)
	if !wrote {
		t.Fatalf("expected commit (live) before the error; wrote=false fail=%+v", f)
	}
	if f == nil || f.code != "sse_overloaded" {
		t.Fatalf("want post-commit failure sse_overloaded, got %+v", f)
	}
	if strings.Contains(rec.Body.String(), "overloaded_error") {
		t.Fatalf("raw overloaded_error must not be forwarded in live mode: %q", rec.Body.String())
	}
}

// §6: a silent gap beyond byte-idle is a wedged upstream -> retryable.
func TestSilentGapAborts(t *testing.T) {
	useDefaultConfig(t)
	cfg.upstreamByteIdle = 150 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	// message_start, then a long gap before the next chunk.
	r := &gapReader{ctx: ctx, chunks: []string{
		"event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
	}, gap: 600 * time.Millisecond}
	_, f := captureSSE(ctx, cancel, rec, http.Header{}, r, nil)
	if f == nil || !f.transient || f.code != "upstream_idle" {
		t.Fatalf("want transient upstream_idle, got %+v", f)
	}
}
