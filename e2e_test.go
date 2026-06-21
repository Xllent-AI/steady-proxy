package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func setupForTest(upURL string) {
	cfg = loadConfig()
	cfg.upstream = strings.TrimRight(upURL, "/")
	h := cfg.upstream
	if i := strings.Index(h, "://"); i >= 0 {
		h = h[i+3:]
	}
	cfg.upstreamHost = h
	cfg.upstreamByteIdle = 3 * time.Second
	client = &http.Client{Transport: &http.Transport{DisableCompression: true, ResponseHeaderTimeout: 5 * time.Second}}
}

func doStream(body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handle(rec, req)
	return rec
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

func TestE2EPermanent400PassesThrough(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		io.WriteString(w, `{"error":{"type":"invalid_request_error","message":"prompt is too long for context"}}`)
	}))
	defer up.Close()
	setupForTest(up.URL)

	rec := doStream(`{"stream":true,"model":"m"}`)
	if got := rec.Header().Get("X-Should-Retry"); got != "false" {
		t.Fatalf("permanent 400 must NOT be retried; want false, got %q", got)
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

// Blind-stabilizer policy: a transient failure must be retried AND carry a
// Retry-After backoff so the client waits before re-sending. Covers a 5xx and an
// auth 4xx (which now retries instead of surfacing).
func TestE2ERetryableCarriesBackoff(t *testing.T) {
	cases := []struct {
		name, body string
		status     int
	}{
		{"503", `{"error":{"type":"api_error","message":"upstream unavailable"}}`, 503},
		{"401", `{"error":{"type":"authentication_error","message":"Invalid API key"}}`, 401},
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
