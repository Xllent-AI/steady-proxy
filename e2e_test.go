package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testConfig is the binary's default configuration with a placeholder upstream.
// It reads no real environment, so a developer's exported PROXY_* variables
// cannot change what the suite tests.
func testConfig() config {
	c, err := parseConfig(func(k string) (string, bool) {
		if k == "PROXY_UPSTREAM_URL" {
			return "http://upstream.invalid", true
		}
		return "", false
	})
	if err != nil {
		panic(err)
	}
	return c
}

func setupForTest(upURL string) {
	cfg = testConfig()
	up, host, err := parseUpstream(upURL)
	if err != nil {
		panic(err)
	}
	cfg.upstream, cfg.upstreamHost = up, host
	cfg.upstreamByteIdle = 3 * time.Second
	client = &http.Client{CheckRedirect: stopRedirect, Transport: &http.Transport{DisableCompression: true, ResponseHeaderTimeout: 5 * time.Second}}
}

func useDefaultConfig(t *testing.T) {
	t.Helper()
	cfg = testConfig()
	t.Cleanup(func() { cfg = testConfig() })
}

func doStream(body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handle(rec, req)
	return rec
}

func captureLogs(t *testing.T, fn func()) (out string) {
	t.Helper()
	var buf bytes.Buffer
	prevOut, prevFlags := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	}()
	fn()
	return buf.String()
}

func firstLogLineContaining(logs, needle string) string {
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}
	return ""
}

func TestE2EStreamingSuccess(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)

	rec := doStream(`{"stream":true,"model":"m"}`)
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "message_stop") || !strings.Contains(rec.Body.String(), "Hi") {
		t.Fatalf("replayed body missing content: %q", rec.Body.String())
	}
}

func TestE2EPreStream500BecomesRetryable(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		io.WriteString(w, `{"error":{"type":"api_error","message":"boom"}}`)
	}))
	defer up.Close()
	setupForTest(up.URL)

	rec := doStream(`{"stream":true,"model":"m"}`)
	if got := rec.Header().Get("X-Should-Retry"); got != "true" {
		t.Fatalf("want x-should-retry true, got %q (code %d)", got, rec.Code)
	}
}

func TestE2ETruncatedStreamBecomesRetryable(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// send only the opening of a stream, then close — no message_stop
		io.WriteString(w, goodStream[:strings.Index(goodStream, "content_block_stop")])
	}))
	defer up.Close()
	setupForTest(up.URL)

	rec := doStream(`{"stream":true,"model":"m"}`)
	if rec.Code == 200 {
		t.Fatalf("truncated stream should not commit 200; body=%s", rec.Body.String())
	}
	if got := rec.Header().Get("X-Should-Retry"); got != "true" {
		t.Fatalf("want x-should-retry true on truncation, got %q (code %d)", got, rec.Code)
	}
}

// A terminal upstream error is the upstream's final answer: one attempt even
// with a local-retry budget, and the client receives its body and headers
// unchanged. Codex retries every status but 400/429, so the Responses route
// keeps the body but answers 400.
func TestE2ETerminalErrorRelayedVerbatim(t *testing.T) {
	const body = `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"},"request_id":"req_1"}`
	for _, c := range []struct {
		path string
		want int
	}{{"/v1/messages", 401}, {"/v1/responses", 400}} {
		t.Run(c.path, func(t *testing.T) {
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Request-Id", "req_1")
				w.WriteHeader(401)
				io.WriteString(w, body)
			}))
			defer up.Close()
			setupForTest(up.URL)
			t.Cleanup(func() { cfg = testConfig() })
			cfg.txLocalRetries = 3
			cfg.localBackoffCap = 0
			rec := httptest.NewRecorder()
			handle(rec, httptest.NewRequest(http.MethodPost, c.path, strings.NewReader(`{"stream":true,"model":"m"}`)))
			if hits.Load() != 1 || rec.Code != c.want || rec.Body.String() != body {
				t.Fatalf("want one attempt relayed as %d verbatim, got hits=%d status=%d body=%s", c.want, hits.Load(), rec.Code, rec.Body.String())
			}
			if rec.Header().Get("Request-Id") != "req_1" || rec.Header().Get("X-Should-Retry") != "false" || rec.Header().Get("X-Steady-Proxy-Reason") != "http_401" {
				t.Fatalf("relay must keep upstream headers and mark it final: %v", rec.Header())
			}
		})
	}
}

// A gateway may compress an error despite Accept-Encoding: identity. The relayed
// bytes stay encoded, so the encoding header must travel with them.
func TestE2EEncodedTerminalErrorKeepsContentEncoding(t *testing.T) {
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	io.WriteString(zw, `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`)
	zw.Close()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(401)
		w.Write(gz.Bytes())
	}))
	defer up.Close()
	setupForTest(up.URL)
	rec := doStream(`{"stream":true,"model":"m"}`)
	if rec.Code != 401 || rec.Header().Get("Content-Encoding") != "gzip" || !bytes.Equal(rec.Body.Bytes(), gz.Bytes()) {
		t.Fatalf("encoded error must be relayed with its encoding: status=%d headers=%v", rec.Code, rec.Header())
	}
}

// A streaming request answered with a complete non-SSE 2xx is still the
// upstream's answer: relayed once, unchanged, never re-sent.
func TestE2ENonSSESuccessRelayedOnce(t *testing.T) {
	var hits atomic.Int32
	const body = `{"id":"msg_1","type":"message","content":[{"type":"text","text":"<Hi & bye>"}]}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body)
	}))
	defer up.Close()
	setupForTest(up.URL)
	t.Cleanup(func() { cfg = testConfig() })
	cfg.txLocalRetries = 3
	rec := doStream(`{"stream":true,"model":"m"}`)
	if hits.Load() != 1 || rec.Code != 200 || rec.Body.String() != body || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("want one verbatim relay, got hits=%d status=%d headers=%v body=%s", hits.Load(), rec.Code, rec.Header(), rec.Body.String())
	}
}

// A non-SSE 2xx whose body breaks before its declared end is buffered, so it
// stays a clean pre-commit retry instead of a short success.
func TestE2ENonSSEBrokenBodyRetried(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", "100")
			io.WriteString(w, `{"id":"msg_1"`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	t.Cleanup(func() { cfg = testConfig() })
	cfg.txLocalRetries = 1
	cfg.localBackoffCap = 0
	rec := doStream(`{"stream":true,"model":"m"}`)
	if hits.Load() != 2 || rec.Code != 200 || !strings.HasPrefix(rec.Body.String(), "event: message_start") {
		t.Fatalf("want the broken body retried, got hits=%d status=%d body=%s", hits.Load(), rec.Code, rec.Body.String())
	}
}

// A non-SSE body that trickles past the no-output window is cut at that
// deadline like an error body, so the client is not held past its timeouts.
func TestE2ENonSSEBodyObeysBufferDeadline(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"msg_1",`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	setupForTest(up.URL)
	t.Cleanup(func() { cfg = testConfig() })
	cfg.keepaliveMs = 100 * time.Millisecond
	started := time.Now()
	rec := doStream(`{"stream":true,"model":"m"}`)
	if elapsed := time.Since(started); elapsed > time.Second || rec.Code != 503 || rec.Header().Get("X-Steady-Proxy-Reason") != "deadline" {
		t.Fatalf("want a prompt retryable deadline, got status=%d reason=%q after %s", rec.Code, rec.Header().Get("X-Steady-Proxy-Reason"), elapsed)
	}
}

func TestE2ESDKRetryCapStopsConverting(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		io.WriteString(w, `{"error":{"type":"api_error","message":"upstream unavailable"}}`)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.sdkRetryCap = 8 // pin the backstop so the test is independent of the default

	// Simulate the SDK already having retried past the cap.
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true,"model":"m"}`))
	req.Header.Set("X-Stainless-Retry-Count", "9") // > cfg.sdkRetryCap (8)
	rec := httptest.NewRecorder()
	handle(rec, req)
	if got := rec.Header().Get("X-Should-Retry"); got != "false" {
		t.Fatalf("past SDK retry cap must stop converting; want false, got %q", got)
	}
}

// The fix for "Repeated 529 Overloaded errors": a mid-stream overloaded_error
// (the most common shape) must NOT reach the client as 529/overloaded_error —
// Claude Code routes that into a dedicated give-up path that ignores
// x-should-retry. It must surface as a generic retryable 503 + api_error so the
// SDK retry loop drives it (bounded by the client's own maxRetries).
func TestE2EOverloadedNormalizedToGeneric(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\"}\n\n"+
			"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n")
	}))
	defer up.Close()
	setupForTest(up.URL)

	rec := doStream(`{"stream":true,"model":"m"}`)
	if got := rec.Header().Get("X-Should-Retry"); got != "true" {
		t.Fatalf("overloaded must be retryable; want x-should-retry true, got %q", got)
	}
	if rec.Code != 503 {
		t.Fatalf("overloaded must be masked as generic 503, got %d (529 trips CC's Repeated-Overloaded bail)", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "overloaded_error") {
		t.Fatalf("overloaded_error type must not leak to the client: %s", rec.Body.String())
	}
}

// Non-transactional routes (stream:false, count_tokens, model listing) go through
// proxyOnce. An upstream 529/overloaded_error there must ALSO be masked to a
// generic retryable 503, not copied through verbatim.
func TestE2ENonTransactional529Normalized(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(529)
		io.WriteString(w, `{"error":{"type":"overloaded_error","message":"Overloaded"}}`)
	}))
	defer up.Close()
	setupForTest(up.URL)

	rec := doStream(`{"stream":false,"model":"m"}`) // stream:false → proxyOnce
	if rec.Code != 503 {
		t.Fatalf("non-transactional 529 must normalize to 503, got %d", rec.Code)
	}
	if got := rec.Header().Get("X-Should-Retry"); got != "true" {
		t.Fatalf("want x-should-retry true on non-transactional route, got %q", got)
	}
	if strings.Contains(rec.Body.String(), "overloaded_error") {
		t.Fatalf("overloaded_error must not leak on the non-transactional route: %s", rec.Body.String())
	}
}

// Same masking for a pre-stream HTTP 529 (covers classifyHTTPError, not just the
// mid-stream SSE path).
func TestE2EPreStream529NormalizedToGeneric(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(529)
		io.WriteString(w, `{"error":{"type":"overloaded_error","message":"Overloaded"}}`)
	}))
	defer up.Close()
	setupForTest(up.URL)

	rec := doStream(`{"stream":true,"model":"m"}`)
	if rec.Code != 503 {
		t.Fatalf("pre-stream 529 must normalize to 503, got %d", rec.Code)
	}
	if got := rec.Header().Get("X-Should-Retry"); got != "true" {
		t.Fatalf("want x-should-retry true, got %q", got)
	}
	if strings.Contains(rec.Body.String(), "overloaded_error") {
		t.Fatalf("overloaded_error must not leak: %s", rec.Body.String())
	}
}

// A transient failure must be retried AND carry a Retry-After backoff so the
// client waits before re-sending. Covers a 5xx and a rate limit without its own
// Retry-After.
func TestE2ERetryableCarriesBackoff(t *testing.T) {
	cases := []struct {
		name, body string
		status     int
	}{
		{"503", `{"error":{"type":"api_error","message":"upstream unavailable"}}`, 503},
		{"429", `{"error":{"type":"rate_limit_error","message":"slow down"}}`, 429},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(c.status)
				io.WriteString(w, c.body)
			}))
			defer up.Close()
			setupForTest(up.URL)

			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true,"model":"m"}`))
			req.Header.Set("X-Stainless-Retry-Count", "1")
			rec := httptest.NewRecorder()
			handle(rec, req)

			if got := rec.Header().Get("X-Should-Retry"); got != "true" {
				t.Fatalf("want x-should-retry true, got %q (code %d body %s)", got, rec.Code, rec.Body.String())
			}
			if ra := atoiSafe(rec.Header().Get("Retry-After")); ra < 1 {
				t.Fatalf("want a Retry-After backoff >= 1s, got %q", rec.Header().Get("Retry-After"))
			}
		})
	}
}

func TestE2ETransactionalLocalRetryHTTPThenSuccess(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(529)
			io.WriteString(w, `{"error":{"type":"overloaded_error","message":"Overloaded"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.txLocalRetries = 1
	cfg.localBackoffCap = 0

	rec := doStream(`{"stream":true,"model":"m"}`)
	if rec.Code != 200 {
		t.Fatalf("want hidden retry to recover with 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("want exactly 2 upstream attempts, got %d", got)
	}
}

func TestE2ELocalRetryLogIncludesRequestIdentity(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(503)
			io.WriteString(w, `{"error":{"type":"api_error","message":"upstream unavailable"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	prevLocalRetries, prevBackoffCap, prevVerbose := cfg.txLocalRetries, cfg.localBackoffCap, cfg.verbose
	cfg.txLocalRetries = 1
	cfg.localBackoffCap = 0
	cfg.verbose = true
	t.Cleanup(func() {
		cfg.txLocalRetries = prevLocalRetries
		cfg.localBackoffCap = prevBackoffCap
		cfg.verbose = prevVerbose
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true,"model":"gpt-5.5"}`))
	req.Header.Set("X-Stainless-Retry-Count", "2")
	rec := httptest.NewRecorder()
	logs := captureLogs(t, func() { handle(rec, req) })

	if rec.Code != 200 {
		t.Fatalf("want hidden retry to recover with 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	line := firstLogLineContaining(logs, "[local-retry]")
	if line == "" {
		t.Fatalf("missing local retry log:\n%s", logs)
	}
	for _, want := range []string{"[local-retry] gpt-5.5/main", "http_503 503", "wait=0s", "retry=1/1", "attempt=2"} {
		if !strings.Contains(line, want) {
			t.Fatalf("local retry log missing %q:\n%s", want, line)
		}
	}
}

// cancelAtLogLine is a log sink that runs cancel on the first line containing
// marker, so a test can act at the exact point the proxy logs it.
type cancelAtLogLine struct {
	bytes.Buffer
	marker string
	cancel context.CancelFunc
}

func (c *cancelAtLogLine) Write(p []byte) (int, error) {
	if c.cancel != nil && bytes.Contains(p, []byte(c.marker)) {
		c.cancel()
		c.cancel = nil
	}
	return c.Buffer.Write(p)
}

// A client that leaves while the proxy waits to retry must be reported as gone,
// not as the upstream failure the retry was answering, on both primary retry paths.
func TestE2EClientGoneDuringLocalRetryWait(t *testing.T) {
	for _, tc := range []struct {
		name, model string
		retries     int
		retryAfter  string
		wantHits    int32
	}{
		{name: "local retry", model: "m", retries: 1, retryAfter: "30", wantHits: 1},
		{name: "legacy fast retry", model: "m", wantHits: 1}, // fixed 250ms pause; Retry-After must be absent
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				w.WriteHeader(503)
				io.WriteString(w, `{"error":{"type":"api_error","message":"upstream unavailable"}}`)
			}))
			defer up.Close()
			setupForTest(up.URL)
			t.Cleanup(func() { cfg = testConfig() })
			cfg.txLocalRetries = tc.retries
			cfg.localBackoffCap = 0
			cfg.verbose = true // the [local-retry] line is logged just before the wait starts

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sink := &cancelAtLogLine{marker: "[local-retry]", cancel: cancel}
			prevOut := log.Writer()
			log.SetOutput(sink)
			defer log.SetOutput(prevOut)

			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true,"model":"`+tc.model+`"}`)).WithContext(ctx)
			rec := httptest.NewRecorder()
			handle(rec, req)
			if hits.Load() != tc.wantHits || rec.Code != 499 || rec.Header().Get("X-Steady-Proxy-Reason") != "client_gone" {
				t.Fatalf("want the departed client reported, got hits=%d status=%d headers=%v body=%s", hits.Load(), rec.Code, rec.Header(), rec.Body.String())
			}
			if logs := sink.String(); !strings.Contains(logs, "client_gone 499") || strings.Contains(logs, "RETRY") {
				t.Fatalf("access log blames the upstream instead of the client:\n%s", logs)
			}
		})
	}
}

func TestE2EAccessLogShowsResolvedModelWhenDifferent(t *testing.T) {
	stream := strings.Replace(goodStream,
		`{"type":"message_start","message":{"id":"msg_1"}}`,
		`{"type":"message_start","message":{"id":"msg_1","model":"actual-model"}}`, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, stream)
	}))
	defer up.Close()
	setupForTest(up.URL)

	logs := captureLogs(t, func() {
		rec := doStream(`{"stream":true,"model":"alias-model"}`)
		if rec.Code != 200 {
			t.Fatalf("want 200, got %d body=%s", rec.Code, rec.Body.String())
		}
	})
	if !strings.Contains(logs, "OK    alias-model->actual-model/main") {
		t.Fatalf("access log missing resolved model:\n%s", logs)
	}
}

func TestE2EMessagesAccessLogShowsReasoningEffortWhenSet(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)

	logs := captureLogs(t, func() {
		rec := doStream(`{"stream":true,"model":"claude-opus-4-8","output_config":{"effort":"xhigh"}}`)
		if rec.Code != 200 {
			t.Fatalf("want 200, got %d body=%s", rec.Code, rec.Body.String())
		}
	})
	if !strings.Contains(logs, "OK    claude-opus-4-8/main  xhigh  ") {
		t.Fatalf("access log missing reasoning effort:\n%s", logs)
	}
}

func TestE2ETransactionalLocalRetryTruncateThenSuccess(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if hits.Add(1) == 1 {
			io.WriteString(w, goodStream[:strings.Index(goodStream, "content_block_stop")])
			return
		}
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.txLocalRetries = 1
	cfg.localBackoffCap = 0

	rec := doStream(`{"stream":true,"model":"m"}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "message_stop") {
		t.Fatalf("want hidden retry to recover full stream, code=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("want exactly 2 upstream attempts, got %d", got)
	}
}

func TestE2ESSEDisconnectRetryBudget(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		retries                   int
		recover                   bool
		backoff, window, deadline time.Duration
		attempts, status          int
	}{
		{name: "recovered", retries: 1, recover: true, attempts: 2, status: 200},
		{name: "local_disabled", recover: true, attempts: 1, status: 503},
		{name: "local_exhausted", retries: 1, attempts: 2, status: 503},
		{name: "deadline_insufficient", retries: 1, recover: true, backoff: time.Second, deadline: time.Second, attempts: 1, status: 503},
		{name: "window_insufficient", retries: 1, recover: true, backoff: time.Second, window: 500 * time.Millisecond, attempts: 1, status: 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			prefix := strings.Replace(goodStream[:strings.Index(goodStream, "event: content_block_stop")], "Hi", "discarded-attempt", 1)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				if hits.Add(1) > 1 && tc.recover {
					io.WriteString(w, goodStream)
					return
				}
				io.WriteString(w, prefix+mislabeledDisconnectStream)
			}))
			defer up.Close()
			setupForTest(up.URL)
			t.Cleanup(func() { cfg = testConfig() })
			cfg.txLocalRetries = tc.retries
			cfg.localBackoffCap = tc.backoff
			if tc.window > 0 {
				cfg.keepaliveMs = tc.window
			}
			if tc.deadline > 0 {
				cfg.maxRequestDur = tc.deadline
			}

			rec := doStream(`{"stream":true,"model":"m"}`)
			body := rec.Body.String()
			if rec.Code != tc.status || int(hits.Load()) != tc.attempts {
				t.Fatalf("want status=%d attempts=%d, got status=%d attempts=%d body=%s", tc.status, tc.attempts, rec.Code, hits.Load(), body)
			}
			if strings.Contains(body, "discarded-attempt") {
				t.Fatalf("failed attempt leaked output: %s", body)
			}
			if tc.status == 200 {
				if rec.Header().Get("X-Steady-Proxy-Mode") != "buffered" || !strings.Contains(body, "message_stop") || !strings.Contains(body, "Hi") || strings.Contains(body, "event: error") {
					t.Fatalf("recovery did not deliver a complete buffered response: headers=%v body=%s", rec.Header(), body)
				}
			} else if rec.Header().Get("X-Should-Retry") != "true" || rec.Header().Get("X-Steady-Proxy-Reason") != "sse_stream_disconnect" || !strings.Contains(body, `"type":"api_error"`) {
				t.Fatalf("uncommitted disconnect must retain the client retry signal: headers=%v body=%s", rec.Header(), body)
			}
		})
	}
}

func TestE2ESSEDisconnectAfterCommitDoesNotReplay(t *testing.T) {
	var hits atomic.Int32
	releaseError := make(chan struct{}, 1)
	prefix := strings.Replace(goodStream[:strings.Index(goodStream, "event: content_block_stop")], "Hi", "committed-attempt", 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if hits.Add(1) > 1 {
			io.WriteString(w, goodStream)
			return
		}
		io.WriteString(w, prefix)
		w.(http.Flusher).Flush()
		select {
		case <-releaseError:
			io.WriteString(w, mislabeledDisconnectStream)
		case <-r.Context().Done():
		}
	}))
	defer up.Close()
	setupForTest(up.URL)
	t.Cleanup(func() { cfg = testConfig() })
	cfg.keepaliveMs = 20 * time.Millisecond
	cfg.txLocalRetries = 2
	cfg.localBackoffCap = 0
	proxy := httptest.NewServer(http.HandlerFunc(handle))
	defer proxy.Close()
	downstream := &http.Client{Timeout: 3 * time.Second}
	resp, err := downstream.Post(proxy.URL+"/v1/messages", "application/json", strings.NewReader(`{"stream":true,"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("X-Steady-Proxy-Mode") != "live" {
		t.Fatalf("want committed live response, got status=%d headers=%v", resp.StatusCode, resp.Header)
	}
	reader := bufio.NewReader(resp.Body)
	var received strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("reading committed content: %v", err)
		}
		received.WriteString(line)
		if strings.Contains(line, "committed-attempt") {
			break
		}
	}
	// Release the error only after the client has observed actual response content.
	releaseError <- struct{}{}
	rest, err := io.ReadAll(reader)
	if err == nil {
		t.Fatal("a committed failure must abort the response, not end it cleanly")
	}
	received.Write(rest)
	body := received.String()
	if hits.Load() != 1 {
		t.Fatalf("committed failure replayed upstream: attempts=%d body=%s", hits.Load(), body)
	}
	if strings.Contains(body, "message_stop") || strings.Contains(body, "event: error") || strings.Contains(body, streamDisconnectMessage) || strings.Contains(body, `"text":"Hi"`) {
		t.Fatalf("committed failure must end the partial stream without an error or replacement response: %s", body)
	}
}

func TestE2ETransactionalLocalRetryDoesNotRetryPermanentShape(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(400)
		io.WriteString(w, `{"error":{"type":"invalid_request_error","message":"prompt is too long for context"}}`)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.txLocalRetries = 3
	cfg.localBackoffCap = 0

	rec := doStream(`{"stream":true,"model":"m"}`)
	if got := rec.Header().Get("X-Should-Retry"); got != "false" {
		t.Fatalf("permanent shape must not retry; want x-should-retry=false, got %q", got)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("want exactly 1 upstream attempt, got %d", got)
	}
}

func TestE2ETransactionalLocalRetryExhaustsThenReturnsRetryable(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(503)
		io.WriteString(w, `{"error":{"type":"api_error","message":"upstream unavailable"}}`)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.txLocalRetries = 2
	cfg.localBackoffCap = 0

	rec := doStream(`{"stream":true,"model":"m"}`)
	if rec.Code != 503 {
		t.Fatalf("want final retryable generic 503, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Should-Retry"); got != "true" {
		t.Fatalf("want x-should-retry=true after local retries exhaust, got %q", got)
	}
	if got := hits.Load(); got != 3 {
		t.Fatalf("want initial + 2 local retries = 3 upstream attempts, got %d", got)
	}
}

func TestE2ELocalRetryDoesNotApplyToNonTransactional(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(503)
		io.WriteString(w, `{"error":{"type":"api_error","message":"upstream unavailable"}}`)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.txLocalRetries = 3
	cfg.localBackoffCap = 0

	rec := doStream(`{"stream":false,"model":"m"}`)
	if rec.Code != 503 {
		t.Fatalf("want non-transactional route to surface retryable 503, got %d", rec.Code)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("non-transactional route must not use local retries; got %d attempts", got)
	}
}

func TestLocalRetryDelayAddsRetryAfterAndCappedExtraBackoff(t *testing.T) {
	prev := cfg.localBackoffCap
	cfg.localBackoffCap = 10 * time.Second
	t.Cleanup(func() { cfg.localBackoffCap = prev })

	if got := localRetryDelay(7, 0); got != 8*time.Second {
		t.Fatalf("first local retry delay = %s, want 8s", got)
	}
	if got := localRetryDelay(7, 4); got != 17*time.Second {
		t.Fatalf("capped local retry delay = %s, want 17s", got)
	}
}

func TestE2EStainlessTimeoutDoesNotLimitStreams(t *testing.T) {
	const advertisedTimeout = "1.0"
	for _, tc := range []struct {
		name, path, stream, terminal string
		buffered                     bool
	}{
		{"messages-live", "/v1/messages", goodStream, "message_stop", false},
		{"messages-buffered", "/v1/messages", goodStream, "message_stop", true},
		{"responses-live", "/v1/responses", goodResponsesStream, "response.completed", false},
		{"gemini-live", geminiStreamPath,
			strings.Repeat("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hi\"}]}}]}\n\n", 5) +
				"data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n", "\"finishReason\":\"STOP\"", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("X-Stainless-Timeout"); got != advertisedTimeout {
					t.Errorf("client timeout fingerprint changed: got %q, want %q", got, advertisedTimeout)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				for i, event := range eventsOf(tc.stream) {
					if i > 0 {
						select {
						case <-time.After(300 * time.Millisecond):
						case <-r.Context().Done():
							return
						}
					}
					if _, err := io.WriteString(w, event); err != nil {
						return
					}
					w.(http.Flusher).Flush()
				}
			}))
			defer up.Close()
			setupForTest(up.URL)
			t.Cleanup(func() { cfg = testConfig() })
			cfg.maxRequestDur = 6 * time.Second
			// The buffer window also bounds upstream connection/header setup.
			cfg.keepaliveMs = 250 * time.Millisecond
			cfg.geminiBufferMs = 250 * time.Millisecond
			if tc.buffered {
				cfg.keepaliveMs = 3 * time.Second
			}
			proxy := httptest.NewServer(http.HandlerFunc(handle))
			defer proxy.Close()
			req, err := http.NewRequest(http.MethodPost, proxy.URL+tc.path, strings.NewReader(`{"stream":true,"model":"test","contents":[]}`))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("X-Stainless-Timeout", advertisedTimeout)
			downstream := &http.Client{Timeout: 5 * time.Second}
			started := time.Now()
			resp, err := downstream.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(body), tc.terminal) || !strings.Contains(string(body), "Hi") {
				t.Fatalf("progressing stream did not complete: status=%d err=%v body=%s", resp.StatusCode, err, body)
			}
			if elapsed := time.Since(started); elapsed <= time.Second {
				t.Fatalf("stream did not cross the advertised timeout: %s", elapsed)
			}
			mode := "live"
			if tc.buffered {
				mode = "buffered"
			}
			if got := resp.Header.Get("X-Steady-Proxy-Mode"); got != mode {
				t.Fatalf("stream used %q mode, want %q", got, mode)
			}
		})
	}
}

func TestE2ELiveStreamRetainsRequestBounds(t *testing.T) {
	for _, mode := range []string{"client-cancel", "client-deadline", "upstream-idle", "request-limit"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			upstreamDone := make(chan struct{})
			prefix := goodStream[:strings.Index(goodStream, "event: content_block_stop")]
			delta := eventsOf(goodStream)[2]
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					defer close(upstreamDone)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, prefix)
				w.(http.Flusher).Flush()
				if mode == "upstream-idle" {
					<-r.Context().Done()
					return
				}
				ticker := time.NewTicker(20 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-r.Context().Done():
						return
					case <-ticker.C:
						if _, err := io.WriteString(w, delta); err != nil {
							return
						}
						w.(http.Flusher).Flush()
					}
				}
			}))
			defer up.Close()
			setupForTest(up.URL)
			t.Cleanup(func() { cfg = testConfig() })
			cfg.keepaliveMs = 250 * time.Millisecond
			cfg.maxRequestDur = 6 * time.Second
			cfg.upstreamByteIdle = 5 * time.Second
			cfg.txLocalRetries = 3
			if mode == "request-limit" {
				cfg.maxRequestDur = time.Second
			}
			if mode == "upstream-idle" {
				cfg.upstreamByteIdle = time.Second
			}
			proxyDone := make(chan struct{})
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(proxyDone)
				handle(w, r)
			}))
			defer proxy.Close()
			clientTimeout := 6 * time.Second
			if mode == "client-deadline" {
				clientTimeout = time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, proxy.URL+"/v1/messages", strings.NewReader(`{"stream":true,"model":"test"}`))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("X-Stainless-Timeout", "600")
			started := time.Now()
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Steady-Proxy-Mode") != "live" {
				t.Fatalf("stream did not commit live: status=%d headers=%v", resp.StatusCode, resp.Header)
			}
			reader := bufio.NewReader(resp.Body)
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					t.Fatalf("stream ended before content arrived: %v", err)
				}
				if strings.Contains(line, `"text":"Hi"`) {
					break
				}
			}
			if mode == "client-cancel" {
				cancel()
			}
			body, err := io.ReadAll(reader)
			if err == nil || strings.Contains(string(body), "message_stop") {
				t.Fatalf("bounded stream ended as a complete response: err=%v body=%s", err, body)
			}
			if elapsed := time.Since(started); elapsed > 3*time.Second {
				t.Fatalf("stream limit did not interrupt promptly: %s", elapsed)
			}
			var wantContextErr error
			switch mode {
			case "client-cancel":
				wantContextErr = context.Canceled
			case "client-deadline":
				wantContextErr = context.DeadlineExceeded
			}
			if ctx.Err() != wantContextErr {
				t.Fatalf("wrong cancellation owner: client context=%v, want %v", ctx.Err(), wantContextErr)
			}
			for _, done := range []<-chan struct{}{upstreamDone, proxyDone} {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("stream work continued after termination")
				}
			}
			if calls.Load() != 1 {
				t.Fatalf("committed stream was retried: calls=%d", calls.Load())
			}
		})
	}
}

func TestE2ELiveToolInputLimitAbortsWithoutRetry(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, toolHead("tool_use", nil))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		var body strings.Builder
		input := `{"value":"` + strings.Repeat("x", 8192) + `"}`
		for _, fragment := range splitEvery(input, 128) {
			writeInputJSONDelta(&body, fragment)
		}
		body.WriteString(contentBlockStopFrame(0))
		body.WriteString(messageDeltaFrame("tool_use"))
		body.WriteString(messageStopFrame)
		io.WriteString(w, body.String())
	}))
	defer up.Close()
	setupForTest(up.URL)
	t.Cleanup(func() { cfg = testConfig() })
	cfg.keepaliveMs = 250 * time.Millisecond
	cfg.maxRequestDur = 5 * time.Second
	cfg.maxResponseBytes = 1024
	cfg.txLocalRetries = 3
	finished := make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		handle(w, r)
	}))
	defer proxy.Close()
	downstream := &http.Client{Timeout: 6 * time.Second}
	resp, err := downstream.Post(proxy.URL+"/v1/messages", "application/json", strings.NewReader(`{"stream":true,"model":"test"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want committed stream", resp.StatusCode)
	}
	close(release)
	body, err := io.ReadAll(resp.Body)
	if err == nil || strings.Contains(string(body), "message_stop") {
		t.Fatalf("oversized tool input must abort the committed stream: err=%v body=%q", err, body)
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("oversized tool input left the handler active")
	}
	if calls.Load() != 1 {
		t.Fatalf("committed stream was retried: calls=%d", calls.Load())
	}
}

func TestE2EPreservesEscapedRequestTargets(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, r.RequestURI)
	}))
	defer up.Close()
	setupForTest(up.URL + "/api%2Fprefix")
	t.Cleanup(func() { cfg = testConfig() })
	proxy := httptest.NewServer(http.HandlerFunc(handle))
	defer proxy.Close()
	for _, target := range []string{
		"/v1/files/a%2Fb/content?download=1", "/v1/files/a%23b/content?download=1",
		"/v1/files/a%3Fb/content?download=1", "/v1/files/a%25b/content?download=1",
		"/v1/models?", "/v1/files/%E2%98%83/content",
	} {
		t.Run(target, func(t *testing.T) {
			resp, err := proxy.Client().Get(proxy.URL + target)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			got, err := io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != http.StatusOK || string(got) != "/api%2Fprefix"+target {
				t.Fatalf("target changed: status=%d body=%q err=%v", resp.StatusCode, got, err)
			}
		})
	}
}

func TestE2EPassthroughSSEFlushesBeforeCompletion(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer up.Close()
	defer finish()
	setupForTest(up.URL)
	t.Cleanup(func() { cfg = testConfig() })
	proxy := httptest.NewServer(http.HandlerFunc(handle))
	defer proxy.Close()
	downstream := &http.Client{Timeout: 2 * time.Second}
	resp, err := downstream.Post(proxy.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil || line != "data: first\n" {
		t.Fatalf("first event unavailable: %q %v", line, err)
	}
	finish()
	rest, err := io.ReadAll(reader)
	if err != nil || !strings.Contains(string(rest), "data: [DONE]") {
		t.Fatalf("terminal lost: %q %v", rest, err)
	}
}

func TestE2EPreservesConditionalAndHeadMetadata(t *testing.T) {
	for _, tc := range []struct {
		method string
		status int
	}{
		{http.MethodGet, http.StatusNotModified}, {http.MethodHead, http.StatusOK},
		{http.MethodHead, http.StatusNotModified}, {http.MethodHead, http.StatusUnauthorized},
	} {
		t.Run(tc.method+itoa(tc.status), func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "12345")
				w.Header().Set("ETag", `"v1"`)
				w.Header().Set("Cache-Control", "max-age=60")
				w.WriteHeader(tc.status)
			}))
			defer up.Close()
			setupForTest(up.URL)
			t.Cleanup(func() { cfg = testConfig() })
			proxy := httptest.NewServer(http.HandlerFunc(handle))
			defer proxy.Close()
			req, _ := http.NewRequest(tc.method, proxy.URL+"/v1/models", nil)
			req.Header.Set("If-None-Match", `"v1"`)
			resp, err := proxy.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil || len(body) != 0 || resp.StatusCode != tc.status || resp.Header.Get("ETag") != `"v1"` || resp.Header.Get("Cache-Control") != "max-age=60" {
				t.Fatalf("metadata response changed: status=%d headers=%v body=%q err=%v", resp.StatusCode, resp.Header, body, err)
			}
			// net/http strips Content-Length from a 304 at the upstream server.
			if tc.status != http.StatusNotModified && resp.Header.Get("Content-Length") != "12345" {
				t.Fatalf("HEAD lost representation length: %v", resp.Header)
			}
		})
	}
}

func TestE2ERejectsUnfinishedOversizedUploads(t *testing.T) {
	for _, tc := range []struct{ name, headers, body string }{
		{"declared", "Content-Length: 1000\r\n", ""},
		{"chunked", "Transfer-Encoding: chunked\r\n", "41\r\n" + strings.Repeat("x", 65) + "\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
			defer up.Close()
			setupForTest(up.URL)
			t.Cleanup(func() { cfg = testConfig() })
			cfg.maxRequestBytes = 64
			cfg.maxRequestDur = 5 * time.Second
			proxy := httptest.NewServer(http.HandlerFunc(handle))
			defer proxy.Close()
			conn, err := net.Dial("tcp", strings.TrimPrefix(proxy.URL, "http://"))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(time.Second))
			io.WriteString(conn, "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\n"+tc.headers+"\r\n"+tc.body)
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatalf("oversize response waited for body completion: %v", err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != http.StatusRequestEntityTooLarge || !resp.Close || hits.Load() != 0 || !strings.Contains(string(body), "request body exceeds limit") {
				t.Fatalf("invalid early rejection: status=%d close=%v hits=%d body=%q err=%v", resp.StatusCode, resp.Close, hits.Load(), body, err)
			}
		})
	}
}

func TestE2ERequestLimitBoundariesPreserveBody(t *testing.T) {
	const body = `{"model":"test","stream":false}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(w, r.Body) }))
	defer up.Close()
	setupForTest(up.URL)
	t.Cleanup(func() { cfg = testConfig() })
	for _, limit := range []int64{int64(len(body)), math.MaxInt64} {
		cfg.maxRequestBytes = limit
		rec := httptest.NewRecorder()
		handle(rec, httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body)))
		if rec.Code != http.StatusOK || rec.Body.String() != body {
			t.Fatalf("limit %d changed accepted body: status=%d body=%q", limit, rec.Code, rec.Body.String())
		}
	}
}

func TestE2EAcceptedUploadsReuseHTTP1Connection(t *testing.T) {
	const body = `{"model":"test","stream":false}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(w, r.Body) }))
	defer up.Close()
	setupForTest(up.URL)
	t.Cleanup(func() { cfg = testConfig() })
	cfg.maxRequestBytes = int64(len(body))
	var connections atomic.Int32
	proxy := httptest.NewUnstartedServer(http.HandlerFunc(handle))
	proxy.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	proxy.Start()
	defer proxy.Close()
	for range 2 {
		resp, err := proxy.Client().Post(proxy.URL+"/v1/messages", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK || string(got) != body || resp.Close {
			t.Fatalf("accepted upload: status=%d body=%q close=%v err=%v", resp.StatusCode, got, resp.Close, err)
		}
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("accepted uploads used %d connections, want one", got)
	}
}

func TestE2EOversizedHTTP2UploadPreservesConnection(t *testing.T) {
	for _, length := range []int64{-1, 1000} {
		t.Run(itoa(int(length)), func(t *testing.T) {
			var hits, connections atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				io.Copy(w, r.Body)
			}))
			defer up.Close()
			setupForTest(up.URL)
			t.Cleanup(func() { cfg = testConfig() })
			cfg.maxRequestBytes = 64
			proxy := httptest.NewUnstartedServer(http.HandlerFunc(handle))
			proxy.EnableHTTP2 = true
			proxy.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
			}
			proxy.StartTLS()
			defer proxy.Close()
			pc := proxy.Client()
			pc.Timeout = time.Second
			body, writer := io.Pipe()
			defer writer.Close()
			req, _ := http.NewRequest(http.MethodPost, proxy.URL+"/v1/messages", body)
			req.ContentLength = length
			if length < 0 {
				go func() { io.WriteString(writer, strings.Repeat("x", 65)) }()
			}
			resp, err := pc.Do(req)
			if err != nil {
				t.Fatalf("unfinished HTTP/2 upload was not rejected promptly: %v", err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusRequestEntityTooLarge || hits.Load() != 0 {
				t.Fatalf("oversize response: proto=%s status=%d hits=%d", resp.Proto, resp.StatusCode, hits.Load())
			}
			resp, err = pc.Post(proxy.URL+"/v1/messages", "application/json", strings.NewReader(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != http.StatusOK || string(got) != `{}` || hits.Load() != 1 || connections.Load() != 1 {
				t.Fatalf("follow-up response: status=%d body=%q hits=%d connections=%d err=%v", resp.StatusCode, got, hits.Load(), connections.Load(), err)
			}
		})
	}
}
