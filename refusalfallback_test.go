package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The terminal delta matches the observed gateway refusal shape, including
// stop_details; detection must depend on stop_reason, not explanation wording.
var refusalStream = strings.Replace(goodStream, `"stop_reason":"end_turn"`, `"stop_reason":"refusal","stop_sequence":null,"stop_details":{"type":"refusal","category":"cyber","explanation":"This request triggered restrictions on violative cyber content and was blocked under Anthropic's Usage Policy. To learn more, see https://platform.claude.com/docs/en/build-with-claude/refusals-and-fallback."},"container":null`, 1)

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
	cfg.claudeRefusalFallbacks = []string{"claude-opus-4-8"}

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
	cfg.claudeRefusalFallbacks = []string{"claude-opus-4-8"}
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
	cfg.claudeRefusalFallbacks = []string{"claude-opus-4-8"}
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
	cfg.claudeRefusalFallbacks = []string{"claude-opus-4-8"}

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
	cfg.claudeRefusalFallbacks = nil // disabled

	rec := doStream(`{"stream":true,"model":"claude-fable-5"}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "message_stop") {
		t.Fatalf("disabled feature must deliver the refusal, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("disabled feature must not re-issue; got %d attempts", got)
	}
}

// The fallback is a Claude model, so only Claude requests are re-routed; another
// vendor's refusal is that model's answer.
func TestE2ERefusalFromNonClaudeModelIsDelivered(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, refusalStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.claudeRefusalFallbacks = []string{"claude-opus-4-8"}

	rec := doStream(`{"stream":true,"model":"Qwen/Qwen3.8-Flash-Next"}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"stop_reason":"refusal"`) || hits.Load() != 1 {
		t.Fatalf("non-Claude refusal must be delivered without re-routing, got %d hits=%d body=%s", rec.Code, hits.Load(), rec.Body.String())
	}
}

// The fallback is best-effort: when it cannot answer, the client gets the
// requested model's own answer (refusal stream or safeguard error), never the
// fallback's unrelated error.
func TestE2EFailedFallbackDeliversOriginalAnswer(t *testing.T) {
	const safeguard = `{"type":"error","error":{"type":"invalid_request_error","message":"Fable 5's safeguards flagged this message (https://www.anthropic.com/legal/aup)."}}`
	for _, c := range []struct {
		name     string
		original func(http.ResponseWriter)
		status   int
		body     string
	}{
		{"refusal", func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, refusalStream)
		}, 200, refusalStream},
		{"safeguard", func(w http.ResponseWriter) {
			w.WriteHeader(400)
			io.WriteString(w, safeguard)
		}, 400, safeguard},
	} {
		t.Run(c.name, func(t *testing.T) {
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				hits.Add(1)
				if modelOf(b) == "claude-opus-4-8" {
					w.WriteHeader(404)
					io.WriteString(w, `{"type":"error","error":{"type":"not_found_error","message":"model: claude-opus-4-8"}}`)
					return
				}
				c.original(w)
			}))
			defer up.Close()
			setupForTest(up.URL)
			cfg.claudeRefusalFallbacks = []string{"claude-opus-4-8"}

			rec := doStream(`{"stream":true,"model":"claude-fable-5"}`)
			if hits.Load() != 2 || rec.Code != c.status || rec.Body.String() != c.body {
				t.Fatalf("want the original %d answer after a failed fallback, got hits=%d status=%d body=%s", c.status, hits.Load(), rec.Code, rec.Body.String())
			}
		})
	}
}

func TestE2EClaudeRefusalOrderedChain(t *testing.T) {
	const body = `{"stream":true,"model":"claude-original","max_tokens":256,"temperature":0.25,"messages":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"metadata":{"user_id":"u1"},"extra":{"large":9007199254740993}}`
	var bodies [][]byte
	var mu sync.Mutex
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		switch modelOf(b) {
		case "claude-original", "claude-first":
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, refusalStream)
		case "claude-missing":
			w.WriteHeader(404)
			io.WriteString(w, `{"error":{"type":"not_found_error","message":"model unavailable"}}`)
		case "claude-last":
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, goodStream)
		default:
			w.WriteHeader(500)
		}
	}))
	defer up.Close()
	setupForTest(up.URL)
	parsed, err := parseConfig(envOf(map[string]string{
		"PROXY_UPSTREAM_URL":                   up.URL,
		"PROXY_CLAUDE_REFUSAL_FALLBACK_MODELS": "claude-original, claude-first,claude-missing,claude-first,claude-last,claude-original",
	}))
	if err != nil {
		t.Fatal(err)
	}
	cfg.claudeRefusalFallbacks = parsed.claudeRefusalFallbacks
	rec := doStream(body)
	mu.Lock()
	defer mu.Unlock()
	if rec.Code != 200 || rec.Body.String() != goodStream {
		t.Fatalf("want final candidate answer, got %d %s", rec.Code, rec.Body.String())
	}
	want := []string{"claude-original", "claude-first", "claude-missing", "claude-last"}
	var models []string
	var original map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &original); err != nil {
		t.Fatal(err)
	}
	delete(original, "model")
	for _, b := range bodies {
		models = append(models, modelOf(b))
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(b, &fields); err != nil {
			t.Fatal(err)
		}
		delete(fields, "model")
		if !reflect.DeepEqual(fields, original) {
			t.Fatalf("non-model JSON changed: %s", b)
		}
	}
	if !reflect.DeepEqual(models, want) {
		t.Fatalf("models = %v, want %v", models, want)
	}
}

func TestE2EClaudeRefusalExhaustionKeepsFirstAnswer(t *testing.T) {
	const safeguard = `{"type":"error","error":{"type":"invalid_request_error","message":"Fable 5's safeguards flagged this message (https://www.anthropic.com/legal/aup)."}}`
	for _, first := range []string{"stream", "http"} {
		t.Run(first, func(t *testing.T) {
			var models []string
			var mu sync.Mutex
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				m := modelOf(b)
				mu.Lock()
				models = append(models, m)
				mu.Unlock()
				switch m {
				case "claude-original":
					if first == "http" {
						w.WriteHeader(400)
						io.WriteString(w, safeguard)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, refusalStream)
				case "claude-safeguard":
					w.WriteHeader(400)
					io.WriteString(w, strings.ReplaceAll(safeguard, "Fable 5", "Later model"))
				case "claude-refusal":
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, strings.ReplaceAll(refusalStream, "Hi", "Different refusal"))
				case "claude-broken":
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"truncated\"}}\n\n")
				default:
					w.WriteHeader(503)
					io.WriteString(w, `{"error":{"type":"api_error","message":"unavailable"}}`)
				}
			}))
			defer up.Close()
			setupForTest(up.URL)
			cfg.claudeRefusalFallbacks = []string{"claude-safeguard", "claude-refusal", "claude-broken", "claude-unavailable"}
			cfg.txLocalRetries = 5
			cfg.localBackoffCap = 0
			rec := doStream(`{"stream":true,"model":"claude-original"}`)
			mu.Lock()
			defer mu.Unlock()
			status, body := 200, refusalStream
			if first == "http" {
				status, body = 400, safeguard
			}
			if rec.Code != status || rec.Body.String() != body {
				t.Fatalf("must preserve first answer, not retryable 503: got %d %s", rec.Code, rec.Body.String())
			}
			want := []string{"claude-original", "claude-safeguard", "claude-refusal", "claude-broken", "claude-unavailable"}
			if !reflect.DeepEqual(models, want) {
				t.Fatalf("models = %v, want %v", models, want)
			}
		})
	}
}

type fallbackRoundTripper func(*http.Request) (*http.Response, error)

func (f fallbackRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestE2EClaudeFallbackSharesTransportRetry(t *testing.T) {
	setupForTest("http://upstream.invalid")
	cfg.claudeRefusalFallbacks = []string{"claude-a", "claude-b", "claude-c"}
	cfg.txLocalRetries = 5
	var models []string
	client.Transport = fallbackRoundTripper(func(r *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(r.Body)
		m := modelOf(b)
		models = append(models, m)
		if m == "claude-original" {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(refusalStream))}, nil
		}
		return nil, errors.New("synthetic transport failure")
	})
	rec := doStream(`{"stream":true,"model":"claude-original"}`)
	want := []string{"claude-original", "claude-a", "claude-a", "claude-b", "claude-c"}
	if !reflect.DeepEqual(models, want) || rec.Code != 200 || rec.Body.String() != refusalStream {
		t.Fatalf("shared retry must be bounded and retain original: models=%v status=%d body=%s", models, rec.Code, rec.Body.String())
	}
}

func TestE2EClaudeFallbackStopsOnGlobalErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"invalid", 400, `{"error":{"type":"invalid_request_error","message":"max_tokens must be positive"}}`},
		{"auth", 401, `{"error":{"type":"authentication_error","message":"invalid API key"}}`},
		{"permission", 403, `{"error":{"type":"permission_error","message":"access denied"}}`},
		{"billing", 400, `{"error":{"type":"invalid_request_error","message":"credit balance too low"}}`},
		{"rate", 429, `{"error":{"type":"rate_limit_error","message":"rate limit exceeded"}}`},
	} {
		for _, armed := range []bool{false, true} {
			t.Run(tc.name+"/armed="+strconv.FormatBool(armed), func(t *testing.T) {
				var hits atomic.Int32
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if hits.Add(1) == 1 && armed {
						w.Header().Set("Content-Type", "text/event-stream")
						io.WriteString(w, refusalStream)
						return
					}
					w.Header().Set("X-Should-Retry", "false")
					w.WriteHeader(tc.status)
					io.WriteString(w, tc.body)
				}))
				defer up.Close()
				setupForTest(up.URL)
				cfg.claudeRefusalFallbacks = []string{"claude-a", "claude-b"}
				rec := doStream(`{"stream":true,"model":"claude-original"}`)
				wantHits := int32(1)
				status, body := tc.status, tc.body
				if armed {
					wantHits = 2
				}
				if tc.name == "rate" {
					if !armed {
						if hits.Load() != wantHits {
							t.Fatalf("primary rate limit armed chain: hits=%d", hits.Load())
						}
						return // primary rate-limit masking is a separate policy
					}
					status, body = 200, refusalStream
				}
				if hits.Load() != wantHits || rec.Code != status || rec.Body.String() != body {
					t.Fatalf("global error must stop chain: hits=%d status=%d body=%s", hits.Load(), rec.Code, rec.Body.String())
				}
			})
		}
	}
}

func TestE2EClaudeFallbackTerminalErrorsOverrideRetryHints(t *testing.T) {
	for _, tc := range []struct {
		name, atype string
		status      int
	}{
		{"auth", "authentication_error", http.StatusUnauthorized},
		{"permission", "permission_error", http.StatusForbidden},
		{"invalid", "invalid_request_error", http.StatusBadRequest},
	} {
		for _, hint := range []string{"X-Should-Retry", "X-Gateway-Retryable"} {
			for _, fallback := range []bool{false, true} {
				t.Run(tc.name+"/"+hint+"/fallback="+strconv.FormatBool(fallback), func(t *testing.T) {
					body := `{"type":"error","error":{"type":"` + tc.atype + `","message":"request rejected"},"request_id":"req_terminal"}`
					var hits atomic.Int32
					up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if hits.Add(1) == 1 && fallback {
							w.Header().Set("Content-Type", "text/event-stream")
							io.WriteString(w, refusalStream)
							return
						}
						w.Header().Set(hint, "true")
						w.Header().Set("WWW-Authenticate", `Bearer realm="upstream"`)
						w.WriteHeader(tc.status)
						io.WriteString(w, body)
					}))
					defer up.Close()
					setupForTest(up.URL)
					cfg.claudeRefusalFallbacks = []string{"claude-a", "claude-b"}
					rec := doStream(`{"stream":true,"model":"claude-original"}`)
					if !fallback {
						if hits.Load() != 1 || rec.Code != 503 || rec.Header().Get("X-Should-Retry") != "true" {
							t.Fatalf("primary retry hint changed: hits=%d status=%d headers=%v", hits.Load(), rec.Code, rec.Header())
						}
						return
					}
					if hits.Load() != 2 || rec.Code != tc.status || rec.Body.String() != body || rec.Header().Get("X-Should-Retry") != "false" || rec.Header().Get("WWW-Authenticate") != `Bearer realm="upstream"` {
						t.Fatalf("terminal fallback error was masked: hits=%d status=%d headers=%v body=%s", hits.Load(), rec.Code, rec.Header(), rec.Body.String())
					}
				})
			}
		}
	}
}

func TestE2EClaudeFallbackUsesUpstreamErrorIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		terminal   bool
		wantHits   int32
		wantStatus int
		wantBody   string
	}{
		{"typed_rate", `{"error":{"type":"rate_limit_error","message":"shared account rate limit"}}`, 400, false, 2, 200, refusalStream},
		{"terminal_rate", `{"error":{"type":"api_error","message":"shared quota exhausted"}}`, 429, true, 2, 200, refusalStream},
		{"billing", `{"error":{"type":"billing_error","message":"account payment required"}}`, 500, false, 2, 500, ""},
		{"invalid", `{"error":{"type":"invalid_request_error","message":"max_tokens must be positive"}}`, 500, false, 2, 500, ""},
		{"http_model_missing", `{"error":{"type":"invalid_request_error","code":"model_not_found","message":"The requested model does not exist"}}`, 400, false, 3, 200, goodStream},
		{"sse_model_missing", `{"error":{"type":"invalid_request_error","code":"model_not_found","message":"The requested model does not exist"}}`, 200, false, 3, 200, goodStream},
		{"sse_disconnect", `{"error":{"type":"invalid_request_error","message":"stream error: stream disconnected before completion: stream closed before response.completed"}}`, 200, false, 3, 200, goodStream},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch hits.Add(1) {
				case 1:
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, refusalStream)
				case 2:
					if tc.terminal {
						w.Header().Set("X-Should-Retry", "false")
					}
					if tc.status == 200 {
						w.Header().Set("Content-Type", "text/event-stream")
						io.WriteString(w, "event: error\ndata: "+tc.body+"\n\n")
						return
					}
					w.WriteHeader(tc.status)
					io.WriteString(w, tc.body)
				default:
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, goodStream)
				}
			}))
			defer up.Close()
			setupForTest(up.URL)
			cfg.claudeRefusalFallbacks = []string{"claude-a", "claude-b"}
			rec := doStream(`{"stream":true,"model":"claude-original"}`)
			wantBody := tc.wantBody
			if wantBody == "" {
				wantBody = tc.body
			}
			if hits.Load() != tc.wantHits || rec.Code != tc.wantStatus || rec.Body.String() != wantBody || rec.Header().Get("X-Should-Retry") == "true" {
				t.Fatalf("upstream error identity lost: hits=%d status=%d headers=%v body=%s", hits.Load(), rec.Code, rec.Header(), rec.Body.String())
			}
		})
	}
}

func TestE2EClaudeFallbackHeaderTimeoutAdvancesWithinSharedWindow(t *testing.T) {
	setupForTest("http://upstream.invalid")
	cfg.claudeRefusalFallbacks = []string{"claude-a", "claude-b"}
	var models []string
	client.Transport = fallbackRoundTripper(func(r *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(r.Body)
		model := modelOf(b)
		models = append(models, model)
		if model == "claude-a" {
			// A header timeout matches DeadlineExceeded without expiring the
			// request context. Do not depend on scheduler timing to reproduce it.
			if err := r.Context().Err(); err != nil {
				t.Fatalf("shared request context already ended: %v", err)
			}
			return nil, context.DeadlineExceeded
		}
		answer := goodStream
		if model == "claude-original" {
			answer = refusalStream
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(answer))}, nil
	})
	rec := doStream(`{"stream":true,"model":"claude-original"}`)
	want := []string{"claude-original", "claude-a", "claude-b"}
	if !reflect.DeepEqual(models, want) || rec.Code != 200 || rec.Body.String() != goodStream {
		t.Fatalf("attempt timeout exhausted healthy chain: models=%v status=%d body=%s", models, rec.Code, rec.Body.String())
	}
}

func TestE2EClaudeFallbackOnlyStreamingMessages(t *testing.T) {
	const safeguard = `{"error":{"type":"invalid_request_error","message":"Fable 5's safeguards flagged this message (https://www.anthropic.com/legal/aup)."}}`
	for _, tc := range []struct{ path, body string }{
		{"/v1/messages", `{"stream":false,"model":"claude-original"}`},
		{"/v1/chat/completions", `{"stream":true,"model":"claude-original"}`},
		{"/v1/responses", `{"stream":true,"model":"claude-original"}`},
		{"/v1/messages", `{"stream":true,"model":"qwen"}`},
	} {
		t.Run(tc.path+tc.body, func(t *testing.T) {
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				w.Header().Set("X-Should-Retry", "false")
				w.WriteHeader(400)
				io.WriteString(w, safeguard)
			}))
			defer up.Close()
			setupForTest(up.URL)
			cfg.claudeRefusalFallbacks = []string{"claude-a", "claude-b"}
			rec := httptest.NewRecorder()
			handle(rec, httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body)))
			if hits.Load() != 1 || rec.Code != 400 || rec.Body.String() != safeguard {
				t.Fatalf("ineligible request substituted: hits=%d status=%d body=%s", hits.Load(), rec.Code, rec.Body.String())
			}
		})
	}
}

func TestE2EClaudeFallbackCancellationStopsChain(t *testing.T) {
	setupForTest("http://upstream.invalid")
	cfg.claudeRefusalFallbacks = []string{"claude-a", "claude-b"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var models []string
	client.Transport = fallbackRoundTripper(func(r *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(r.Body)
		m := modelOf(b)
		models = append(models, m)
		if m == "claude-original" {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(refusalStream))}, nil
		}
		cancel()
		return nil, context.Canceled
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true,"model":"claude-original"}`)).WithContext(ctx)
	handle(rec, req)
	if !reflect.DeepEqual(models, []string{"claude-original", "claude-a"}) || rec.Code != 499 || strings.Contains(rec.Body.String(), "message_stop") {
		t.Fatalf("canceled chain continued or emitted held answer: models=%v status=%d body=%s", models, rec.Code, rec.Body.String())
	}
}

func TestE2EClaudeRefusalAfterCommitDoesNotSubstitute(t *testing.T) {
	var hits atomic.Int32
	release := make(chan struct{})
	cut := strings.Index(refusalStream, "event: content_block_stop")
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, refusalStream[:cut])
		w.(http.Flusher).Flush()
		select {
		case <-release:
			io.WriteString(w, refusalStream[cut:])
		case <-r.Context().Done():
		}
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.claudeRefusalFallbacks = []string{"claude-a", "claude-b"}
	cfg.keepaliveMs = 20 * time.Millisecond
	proxy := httptest.NewServer(http.HandlerFunc(handle))
	defer proxy.Close()
	downstream := &http.Client{Timeout: 3 * time.Second}
	resp, err := downstream.Post(proxy.URL+"/v1/messages", "application/json", strings.NewReader(`{"stream":true,"model":"claude-original"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	var received strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		received.WriteString(line)
		if strings.Contains(line, "Hi") {
			break
		}
	}
	close(release)
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	received.Write(rest)
	if hits.Load() != 1 || resp.StatusCode != 200 || !strings.Contains(received.String(), `"stop_reason":"refusal"`) || !strings.Contains(received.String(), "message_stop") {
		t.Fatalf("committed refusal substituted: hits=%d status=%d body=%s", hits.Load(), resp.StatusCode, received.String())
	}
}

func TestE2EClaudeFallbackPreservesPrimaryRetryBudget(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch hits.Add(1) {
		case 1:
			w.WriteHeader(503)
			io.WriteString(w, `{"error":{"type":"api_error","message":"unavailable"}}`)
		case 2:
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, refusalStream)
		default:
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, goodStream)
		}
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.claudeRefusalFallbacks = []string{"claude-a"}
	cfg.txLocalRetries = 1
	cfg.localBackoffCap = 0
	rec := doStream(`{"stream":true,"model":"claude-original"}`)
	if hits.Load() != 3 || rec.Code != 200 || rec.Body.String() != goodStream {
		t.Fatalf("primary transient should retry before arming fallback: hits=%d status=%d body=%s", hits.Load(), rec.Code, rec.Body.String())
	}
}
