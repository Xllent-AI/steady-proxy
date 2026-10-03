package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
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
	if err != nil {
		t.Fatal(err)
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
