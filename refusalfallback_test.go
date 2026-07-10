package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// refusalStream is goodStream with its terminal stop_reason flipped to "refusal"
// — a complete, valid stream the model declined to answer.
var refusalStream = strings.Replace(goodStream, `"stop_reason":"end_turn"`, `"stop_reason":"refusal"`, 1)

func TestSwapModelPreservesOtherFields(t *testing.T) {
	in := []byte(`{"model":"claude-fable-5","max_tokens":256,"stream":true,"metadata":{"user_id":"u1"}}`)
	out, ok := swapModel(in, "claude-opus-4-8")
	if !ok {
		t.Fatalf("swapModel returned ok=false")
	}
	if modelOf(out) != "claude-opus-4-8" {
		t.Fatalf("model not swapped: %s", out)
	}
	// Other fields must survive byte-for-byte (no number reformatting, no drops).
	for _, want := range []string{`"max_tokens":256`, `"stream":true`, `"metadata":{"user_id":"u1"}`} {
		if !bytes.Contains(out, []byte(want)) {
			t.Fatalf("swapped body lost %s: %s", want, out)
		}
	}
}

func TestSwapModelNoModelField(t *testing.T) {
	if _, ok := swapModel([]byte(`{"max_tokens":256}`), "claude-opus-4-8"); ok {
		t.Fatalf("swapModel must report ok=false when body has no model field")
	}
	if _, ok := swapModel([]byte(`not json`), "claude-opus-4-8"); ok {
		t.Fatalf("swapModel must report ok=false on non-JSON body")
	}
}

func TestLoadRefusalFallback(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		orig, had := os.LookupEnv("PROXY_REFUSAL_FALLBACK_MODEL")
		os.Unsetenv("PROXY_REFUSAL_FALLBACK_MODEL")
		t.Cleanup(func() {
			if had {
				os.Setenv("PROXY_REFUSAL_FALLBACK_MODEL", orig)
			} else {
				os.Unsetenv("PROXY_REFUSAL_FALLBACK_MODEL")
			}
		})
		if got := loadRefusalFallback(); got != "claude-opus-4-8" {
			t.Fatalf("default = %q, want claude-opus-4-8", got)
		}
	})
	for _, off := range []string{"", "off", "None", "disabled", "  "} {
		t.Run("disabled="+off, func(t *testing.T) {
			t.Setenv("PROXY_REFUSAL_FALLBACK_MODEL", off)
			if got := loadRefusalFallback(); got != "" {
				t.Fatalf("%q should disable the feature, got %q", off, got)
			}
		})
	}
	t.Run("custom", func(t *testing.T) {
		t.Setenv("PROXY_REFUSAL_FALLBACK_MODEL", "claude-sonnet-5")
		if got := loadRefusalFallback(); got != "claude-sonnet-5" {
			t.Fatalf("custom = %q, want claude-sonnet-5", got)
		}
	})
}

// When interception is armed, a refusal must be withheld from the client and
// reported as the refusalFallbackCode sentinel so the handler can re-issue.
func TestCaptureRefusalInterceptedWhenArmed(t *testing.T) {
	useDefaultConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	var st captureStats
	wrote, f := captureSSEWindow(ctx, cancel, rec, http.Header{}, strings.NewReader(refusalStream), &st, time.Hour, false, true)
	if wrote {
		t.Fatalf("armed refusal must not commit downstream (wrote=true)")
	}
	if f == nil || f.code != refusalFallbackCode {
		t.Fatalf("want refusal_fallback sentinel, got %+v", f)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("no bytes may reach the client on interception, got %q", rec.Body.String())
	}
	if st.stop != "refusal" {
		t.Fatalf("stop=%q, want refusal", st.stop)
	}
}

// With interception disarmed the refusal is a normal successful stream and is
// delivered to the client unchanged.
func TestCaptureRefusalDeliveredWhenDisarmed(t *testing.T) {
	useDefaultConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	var st captureStats
	wrote, f := captureSSEWindow(ctx, cancel, rec, http.Header{}, strings.NewReader(refusalStream), &st, time.Hour, false, false)
	if f != nil {
		t.Fatalf("disarmed refusal must succeed, got failure %+v", *f)
	}
	if !wrote || !strings.Contains(rec.Body.String(), "message_stop") {
		t.Fatalf("disarmed refusal must be delivered, wrote=%v body=%q", wrote, rec.Body.String())
	}
}

// End to end: a refusal from the requested model transparently re-issues with the
// configured fallback model, the client receives only the fallback's answer, and a
// WARN line is logged. The fallback request must carry the swapped model while
// preserving every other field of the original body.
func TestE2ERefusalFallsBackToConfiguredModel(t *testing.T) {
	var hits atomic.Int32
	var mu sync.Mutex
	var bodies [][]byte
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if hits.Add(1) == 1 {
			io.WriteString(w, refusalStream)
			return
		}
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.refusalFallback = "claude-opus-4-8"

	var rec *httptest.ResponseRecorder
	logs := captureLogs(t, func() {
		rec = doStream(`{"stream":true,"model":"claude-fable-5","max_tokens":256}`)
	})

	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "Hi") {
		t.Fatalf("want fallback success 200 with content, got %d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "refusal") {
		t.Fatalf("the refusal stream must not be delivered to the client: %s", rec.Body.String())
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("want 2 upstream attempts (refusal + fallback), got %d", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("want 2 upstream bodies, got %d", len(bodies))
	}
	if modelOf(bodies[0]) != "claude-fable-5" {
		t.Fatalf("first attempt model = %q, want claude-fable-5", modelOf(bodies[0]))
	}
	if modelOf(bodies[1]) != "claude-opus-4-8" {
		t.Fatalf("fallback attempt model = %q, want claude-opus-4-8", modelOf(bodies[1]))
	}
	if !bytes.Contains(bodies[1], []byte(`"max_tokens":256`)) {
		t.Fatalf("fallback request lost other fields: %s", bodies[1])
	}
	if !strings.Contains(logs, "WARN") || !strings.Contains(logs, "refusal -> retry with claude-opus-4-8") {
		t.Fatalf("missing refusal fallback WARN log:\n%s", logs)
	}
}

// Refusal interception scrapes stop_reason itself when armed, so it must keep
// working with PROXY_VALIDATE_JSON=0 (which disables the usage/stop scrape that
// full validation would otherwise provide).
func TestE2ERefusalFallbackWorksWithValidationDisabled(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		if modelOf(b) == "claude-opus-4-8" {
			io.WriteString(w, goodStream)
			return
		}
		hits.Add(1)
		io.WriteString(w, refusalStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.refusalFallback = "claude-opus-4-8"
	cfg.validateJSON = false
	t.Cleanup(func() { cfg.validateJSON = true })

	rec := doStream(`{"stream":true,"model":"claude-fable-5"}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "Hi") {
		t.Fatalf("want fallback success with validation disabled, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("want exactly one refusing attempt before fallback, got %d", got)
	}
}

// Fable's safeguard block can arrive as a pre-stream HTTP invalid_request error
// rather than a normal SSE message_delta.stop_reason="refusal". It must still use
// the configured fallback immediately instead of retrying/surfacing the same model.
func TestE2EHTTPSafeguardFallsBackToConfiguredModel(t *testing.T) {
	var hits atomic.Int32
	var mu sync.Mutex
	var bodies [][]byte
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		if hits.Add(1) == 1 {
			w.Header().Set("X-Should-Retry", "false")
			w.WriteHeader(400)
			io.WriteString(w, `{"error":{"type":"invalid_request_error","message":"Fable 5's safeguards flagged this message (https://www.anthropic.com/legal/aup). This sometimes happens with safe, normal conversations."}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.refusalFallback = "claude-opus-4-8"
	cfg.txLocalRetries = 3
	cfg.localBackoffCap = 0

	var rec *httptest.ResponseRecorder
	logs := captureLogs(t, func() {
		rec = doStream(`{"stream":true,"model":"claude-fable-5","max_tokens":256}`)
	})

	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "Hi") {
		t.Fatalf("want fallback success 200 with content, got %d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "safeguards flagged") {
		t.Fatalf("the safeguard error must not be delivered to the client: %s", rec.Body.String())
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("want 2 upstream attempts (safeguard + fallback), got %d", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("want 2 upstream bodies, got %d", len(bodies))
	}
	if modelOf(bodies[0]) != "claude-fable-5" {
		t.Fatalf("first attempt model = %q, want claude-fable-5", modelOf(bodies[0]))
	}
	if modelOf(bodies[1]) != "claude-opus-4-8" {
		t.Fatalf("fallback attempt model = %q, want claude-opus-4-8", modelOf(bodies[1]))
	}
	if !strings.Contains(logs, "WARN") || !strings.Contains(logs, "safeguard -> retry with claude-opus-4-8") {
		t.Fatalf("missing safeguard fallback WARN log:\n%s", logs)
	}
}

// Regression: the fallback attempt is a fresh request and must earn its own free
// legacy fast retry (PROXY_TRANSACTIONAL_LOCAL_RETRIES=0). Here the fallback's
// first hit is a fast 503; the proxy must fast-retry it locally and recover with
// 200 — NOT surface a 503 that makes the SDK resend the original refusing body.
func TestE2ERefusalFallbackKeepsItsOwnFastRetry(t *testing.T) {
	var hits atomic.Int32
	var mu sync.Mutex
	var models []string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		models = append(models, modelOf(b))
		mu.Unlock()
		switch hits.Add(1) {
		case 1: // requested model refuses
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, refusalStream)
		case 2: // fallback's first attempt: a fast, retryable 503 (no Retry-After)
			w.WriteHeader(503)
			io.WriteString(w, `{"error":{"type":"api_error","message":"upstream unavailable"}}`)
		default: // fallback's fast local retry succeeds
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, goodStream)
		}
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.refusalFallback = "claude-opus-4-8"
	cfg.txLocalRetries = 0 // exercise the legacy fast-retry path specifically

	rec := doStream(`{"stream":true,"model":"claude-fable-5"}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "Hi") {
		t.Fatalf("fallback must recover via its own fast retry to 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := hits.Load(); got != 3 {
		t.Fatalf("want 3 attempts (refusal + fallback 503 + fallback retry), got %d", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(models) != 3 || models[0] != "claude-fable-5" || models[1] != "claude-opus-4-8" || models[2] != "claude-opus-4-8" {
		t.Fatalf("want [fable, opus, opus], got %v", models)
	}
}

// Regression: with the explicit hidden local-retry budget enabled
// (PROXY_TRANSACTIONAL_LOCAL_RETRIES>0), the refusing model may spend that budget
// before it refuses. The fallback request must start with a FRESH budget, not the
// exhausted one — otherwise a transient fallback fault surfaces a 503 and the SDK
// resends the original refusing body.
func TestE2ERefusalFallbackResetsLocalRetryBudget(t *testing.T) {
	var hits atomic.Int32
	var mu sync.Mutex
	seen := map[string]int{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		m := modelOf(b)
		mu.Lock()
		seen[m]++
		n := seen[m]
		mu.Unlock()
		hits.Add(1)
		// Each model: first hit is a fast, retryable 503 (spends a local retry);
		// second hit is the real payload (refusal for the requested model, success
		// for the fallback).
		if n == 1 {
			w.WriteHeader(503)
			io.WriteString(w, `{"error":{"type":"api_error","message":"upstream unavailable"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if m == "claude-opus-4-8" {
			io.WriteString(w, goodStream)
		} else {
			io.WriteString(w, refusalStream)
		}
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.refusalFallback = "claude-opus-4-8"
	cfg.txLocalRetries = 1  // one hidden local retry per logical request
	cfg.localBackoffCap = 0 // no backoff wait in the test

	rec := doStream(`{"stream":true,"model":"claude-fable-5"}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "Hi") {
		t.Fatalf("fallback must recover with its own local retry budget, got %d body=%s", rec.Code, rec.Body.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if seen["claude-fable-5"] != 2 || seen["claude-opus-4-8"] != 2 {
		t.Fatalf("want fable used its retry then refused, opus used its own retry then succeeded; got %v", seen)
	}
}

// A refusal from the fallback model itself is terminal: it is delivered to the
// client (no infinite re-issue loop).
func TestE2ERefusalFromFallbackModelIsDelivered(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, refusalStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.refusalFallback = "claude-opus-4-8"

	rec := doStream(`{"stream":true,"model":"claude-opus-4-8"}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "message_stop") {
		t.Fatalf("refusal from the fallback model must be delivered, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("must not re-issue when the request already targets the fallback; got %d attempts", got)
	}
}

// With the feature disabled, a refusal is delivered unchanged and never re-issued.
func TestE2ERefusalDeliveredWhenFeatureOff(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, refusalStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.refusalFallback = "" // disabled

	rec := doStream(`{"stream":true,"model":"claude-fable-5"}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "message_stop") {
		t.Fatalf("disabled feature must deliver the refusal, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("disabled feature must not re-issue; got %d attempts", got)
	}
}
