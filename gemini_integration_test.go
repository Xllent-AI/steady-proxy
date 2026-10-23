package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const geminiStreamPath = "/v1beta/models/gemini-test:streamGenerateContent?alt=sse"

func TestGeminiHiddenRetryDiscardsFailedOutput(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if calls.Add(1) == 1 {
			io.WriteString(w, "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"discard me\"}]}}]}\n\n")
			io.WriteString(w, "data: {\"error\":{\"code\":503,\"status\":\"UNAVAILABLE\",\"message\":\"busy\"}}\n\n")
			return
		}
		io.WriteString(w, "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"recovered\"}]},\"finishReason\":\"STOP\"}]}\n\n")
	}))
	defer up.Close()
	setupForTest(up.URL)
	dir := t.TempDir()
	cfg.requestLogDir = dir
	t.Cleanup(func() { cfg.requestLogDir = "" })
	cfg.txLocalRetries = 1
	cfg.localBackoffCap = time.Millisecond
	// These Anthropic-only signals must not change Gemini's window or model.
	cfg.wfKeepaliveMs = time.Millisecond
	body := `{"model":"not-a-gemini-body-field","system":"workflow orchestration script","contents":[{"parts":[{"text":"hi"}]}]}`
	rec := httptest.NewRecorder()
	handle(rec, httptest.NewRequest(http.MethodPost, geminiStreamPath, strings.NewReader(body)))
	if rec.Code != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("status=%d calls=%d body=%s", rec.Code, calls.Load(), rec.Body)
	}
	if strings.Contains(rec.Body.String(), "discard me") || strings.Contains(rec.Body.String(), "busy") || !strings.Contains(rec.Body.String(), "recovered") {
		t.Fatalf("failed attempt leaked or recovery missing: %s", rec.Body)
	}
	if rec.Header().Get("X-Steady-Proxy-Mode") != "buffered" {
		t.Fatalf("Anthropic marker changed Gemini buffering: %v", rec.Header())
	}
	files := archiveFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("want one request archive, got %d", len(files))
	}
	archive, _ := readArchive(t, files[0])
	if len(archive.Attempts) != 2 {
		t.Fatalf("archive lost retry history: %+v", archive.Attempts)
	}
	for _, attempt := range archive.Attempts {
		if attempt.Model != "gemini-test" {
			t.Fatalf("attempt model must come from URL, not JSON body: %+v", attempt)
		}
	}
}

func TestGeminiInvalidArgumentDoesNotRetry(t *testing.T) {
	for _, path := range []string{
		geminiStreamPath,
		"/v1/models/gemini-test:generateContent",
		"/v1beta1/projects/p/locations/global/publishers/google/models/gemini-test:streamGenerateContent?alt=sse",
	} {
		t.Run(path, func(t *testing.T) {
			var calls atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, `{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"bad tool schema"}}`)
			}))
			defer up.Close()
			setupForTest(up.URL)
			cfg.txLocalRetries = 3
			rec := httptest.NewRecorder()
			handle(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"contents":[]}`)))
			var result struct {
				Error struct {
					Code   int    `json:"code"`
					Status string `json:"status"`
				} `json:"error"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if rec.Code != 400 || calls.Load() != 1 || rec.Header().Get("X-Should-Retry") != "false" || result.Error.Code != 400 || result.Error.Status != "INVALID_ARGUMENT" {
				t.Fatalf("status=%d calls=%d headers=%v error=%+v", rec.Code, calls.Load(), rec.Header(), result.Error)
			}
		})
	}
}

func TestGeminiDisabledRetryConversionIsTerminal(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"quota"}}`)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.sdkRetryCap = 0
	rec := httptest.NewRecorder()
	handle(rec, httptest.NewRequest(http.MethodPost, geminiStreamPath, strings.NewReader(`{"contents":[]}`)))
	if rec.Code != 400 || rec.Header().Get("X-Should-Retry") != "false" || rec.Header().Get("Retry-After") != "" {
		t.Fatalf("disabled conversion still retryable: status=%d headers=%v body=%s", rec.Code, rec.Header(), rec.Body)
	}
	// The HTTP status is forced to 400 so clients that retry on 429 stop, but
	// the synthesized envelope still names the real condition.
	if got := decodeGeminiError(t, rec.Body.Bytes()); got.Error.Code != 400 || got.Error.Status != "RESOURCE_EXHAUSTED" {
		t.Fatalf("forced status replaced the canonical status: %+v", got)
	}
}

func TestGeminiRetryOverrideIsTerminalToSDKs(t *testing.T) {
	for _, status := range []int{408, 429, 503} {
		for _, header := range []string{"X-Gateway-Retryable", "X-Should-Retry"} {
			for _, path := range []string{geminiStreamPath, "/v1/models/gemini-test:generateContent"} {
				t.Run(itoa(status)+"/"+header+"/"+path, func(t *testing.T) {
					var calls atomic.Int32
					up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						w.Header().Set(header, "false")
						w.Header().Set("Retry-After", "7")
						w.WriteHeader(status)
						io.WriteString(w, `{"error":{"message":"terminal upstream failure"}}`)
					}))
					defer up.Close()
					setupForTest(up.URL)
					cfg.txLocalRetries = 3
					rec := httptest.NewRecorder()
					handle(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"contents":[]}`)))
					// The forced 400 and X-Should-Retry: false stop SDK retries; the
					// upstream's Retry-After must not contradict them.
					if rec.Code != 400 || calls.Load() != 1 || rec.Header().Get("X-Should-Retry") != "false" || rec.Header().Get("Retry-After") != "" {
						t.Fatalf("permanent upstream error still retryable: status=%d calls=%d headers=%v", rec.Code, calls.Load(), rec.Header())
					}
				})
			}
		}
	}
}

func TestGeminiCommittedTruncationAbortsHTTP(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"partial\"}]}}]}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.geminiBufferMs = 5 * time.Millisecond
	cfg.upstreamByteIdle = 40 * time.Millisecond
	cfg.txLocalRetries = 3
	proxy := httptest.NewServer(http.HandlerFunc(handle))
	defer proxy.Close()
	resp, err := http.Post(proxy.URL+geminiStreamPath, "application/json", strings.NewReader(`{"contents":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err == nil || !strings.Contains(string(body), "partial") || calls.Load() != 1 {
		t.Fatalf("committed truncation must fail transport without replay: body=%q err=%v calls=%d", body, err, calls.Load())
	}
}

func TestGeminiTruncatedHTTPErrorCanRecover(t *testing.T) {
	for _, archived := range []bool{false, true} {
		t.Run(strconv.FormatBool(archived), func(t *testing.T) {
			var calls atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.Header().Set("Content-Length", "1024")
					w.WriteHeader(400)
					io.WriteString(w, `{"error":{"code":400,`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, goodGeminiStream)
			}))
			defer up.Close()
			setupForTest(up.URL)
			cfg.txLocalRetries = 1
			cfg.localBackoffCap = 0
			if archived {
				cfg.requestLogDir = t.TempDir()
				t.Cleanup(func() { cfg.requestLogDir = "" })
			}
			rec := httptest.NewRecorder()
			handle(rec, httptest.NewRequest(http.MethodPost, geminiStreamPath, strings.NewReader(`{}`)))
			if rec.Code != 200 || calls.Load() != 2 || !strings.Contains(rec.Body.String(), `"text":"Hello"`) || strings.Contains(rec.Body.String(), `"error"`) {
				t.Fatalf("truncated error suppressed recovery: status=%d calls=%d body=%s", rec.Code, calls.Load(), rec.Body)
			}
		})
	}
}

func TestGeminiTruncatedUnaryErrorHonorsRetryHints(t *testing.T) {
	for _, hint := range []string{"", "false", "true"} {
		t.Run("hint="+hint, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Gateway-Retryable", hint)
				w.Header().Set("Retry-After", "17")
				w.Header().Set("Content-Length", "1024")
				w.WriteHeader(400)
				io.WriteString(w, `{"error":{"code":400,`)
			}))
			defer up.Close()
			setupForTest(up.URL)
			rec := httptest.NewRecorder()
			handle(rec, httptest.NewRequest(http.MethodPost, "/v1/models/gemini-test:generateContent", strings.NewReader(`{}`)))
			if hint == "false" {
				if rec.Code != 400 || rec.Header().Get("X-Should-Retry") != "false" || rec.Header().Get("Retry-After") != "" {
					t.Fatalf("explicit permanent hint lost: status=%d headers=%v", rec.Code, rec.Header())
				}
			} else if rec.Code != 503 || rec.Header().Get("X-Should-Retry") != "true" || rec.Header().Get("Retry-After") != "17" {
				t.Fatalf("read failure lost retry eligibility or cooldown: status=%d headers=%v", rec.Code, rec.Header())
			}
		})
	}
}

func TestGeminiRetryInfoPreventsPrematureLocalRetry(t *testing.T) {
	for _, sse := range []bool{false, true} {
		for _, delay := range []string{"23", "315576000000"} {
			t.Run(strconv.FormatBool(sse)+"/"+delay, func(t *testing.T) {
				var calls atomic.Int32
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					body := `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"` + delay + `s"}]}}`
					if sse {
						w.Header().Set("Content-Type", "text/event-stream")
						io.WriteString(w, "data: "+body+"\n\n")
					} else {
						w.WriteHeader(429)
						io.WriteString(w, body)
					}
				}))
				defer up.Close()
				setupForTest(up.URL)
				cfg.txLocalRetries = 1
				cfg.localBackoffCap = 0
				cfg.geminiBufferMs = time.Second
				rec := httptest.NewRecorder()
				handle(rec, httptest.NewRequest(http.MethodPost, geminiStreamPath, strings.NewReader(`{}`)))
				wantDelay := delay
				if strconv.IntSize == 32 && delay == "315576000000" {
					wantDelay = "2147483647"
				}
				if rec.Code != 503 || calls.Load() != 1 || rec.Header().Get("Retry-After") != wantDelay {
					t.Fatalf("native cooldown was shortened or bypassed: status=%d calls=%d headers=%v", rec.Code, calls.Load(), rec.Header())
				}
			})
		}
	}
}

type geminiErrorBody struct {
	Error struct {
		Code    int    `json:"code"`
		Status  string `json:"status"`
		Message string `json:"message"`
	} `json:"error"`
}

func decodeGeminiError(t *testing.T, body []byte) geminiErrorBody {
	t.Helper()
	var got geminiErrorBody
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("not a Google error envelope: %v: %s", err, body)
	}
	return got
}

// Every model-scoped route shape: SSE and unary generation, Vertex express, and
// methods that are not generation at all.
var geminiRoutePaths = []string{
	geminiStreamPath,
	"/v1/publishers/google/models/gemini-test:streamGenerateContent?alt=sse",
	"/v1/models/gemini-test:generateContent",
	"/v1/publishers/google/models/gemini-test:generateContent",
	"/v1beta/models/text-embedding-004:embedContent",
	"/v1beta/models/text-embedding-004:batchEmbedContents",
	"/v1beta1/projects/p/locations/global/publishers/google/models/text-embedding-004:embedContent",
}

// An error no retry can fix is the upstream's own final answer: one upstream
// call, and the client receives its status and bytes untouched, so it sees the
// real diagnosis instead of a masked retryable 503.
func TestGeminiTerminalUpstreamErrorsAreDeliveredVerbatim(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"unauthenticated", 401, `{"error":{"code":401,"message":"Request had invalid authentication credentials.","status":"UNAUTHENTICATED"}}`},
		{"permission-denied", 403, `{"error":{"code":403,"message":"Method doesn't allow unregistered callers.","status":"PERMISSION_DENIED"}}`},
		{"failed-precondition", 400, `{"error":{"code":400,"message":"User location is not supported for the API use.","status":"FAILED_PRECONDITION"}}`},
		{"api-key-invalid", 400, `{"error":{"code":400,"message":"API key not valid. Please pass a valid API key.","status":"INVALID_ARGUMENT","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"API_KEY_INVALID","domain":"googleapis.com","metadata":{"service":"generativelanguage.googleapis.com"}}]}}`},
		{"out-of-range", 400, `{"error":{"code":400,"message":"Offset is out of range.","status":"OUT_OF_RANGE"}}`},
		{"not-found", 404, `{"error":{"code":404,"message":"models/gemini-test is not found.","status":"NOT_FOUND"}}`},
	} {
		for _, path := range geminiRoutePaths {
			t.Run(tc.name+path, func(t *testing.T) {
				var calls atomic.Int32
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tc.status)
					io.WriteString(w, tc.body)
				}))
				defer up.Close()
				setupForTest(up.URL)
				cfg.txLocalRetries = 3
				rec := httptest.NewRecorder()
				handle(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"contents":[]}`)))
				if rec.Code != tc.status || calls.Load() != 1 || rec.Body.String() != tc.body {
					t.Fatalf("want one call and the upstream's %d verbatim, got status=%d calls=%d body=%s", tc.status, rec.Code, calls.Load(), rec.Body)
				}
				if rec.Header().Get("X-Should-Retry") != "false" || rec.Header().Get("Retry-After") != "" || rec.Header().Get("X-Steady-Proxy-Reason") != "request_shape" {
					t.Fatalf("terminal error still signals a retry: %v", rec.Header())
				}
			})
		}
	}
}

// A per-day quota is exhausted until the day rolls over: retrying (locally or
// through the client) cannot succeed. The route forces HTTP 400 so clients that
// retry on 429 stop, while the body keeps the upstream's own diagnosis.
func TestGeminiDailyQuotaIsTerminal(t *testing.T) {
	envelope := `{"error":{"code":429,"message":"You exceeded your current quota.","status":"RESOURCE_EXHAUSTED","details":` + geminiDailyQuotaDetails + `}}`
	for _, path := range []string{geminiStreamPath, "/v1/models/gemini-test:generateContent", "/v1beta/models/text-embedding-004:embedContent"} {
		t.Run("http"+path, func(t *testing.T) {
			var calls atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				io.WriteString(w, envelope)
			}))
			defer up.Close()
			setupForTest(up.URL)
			cfg.txLocalRetries = 3
			rec := httptest.NewRecorder()
			handle(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"contents":[]}`)))
			if rec.Code != 400 || calls.Load() != 1 || rec.Body.String() != envelope {
				t.Fatalf("want one call and the verbatim body under a forced 400, got status=%d calls=%d body=%s", rec.Code, calls.Load(), rec.Body)
			}
			if rec.Header().Get("X-Should-Retry") != "false" || rec.Header().Get("Retry-After") != "" {
				t.Fatalf("daily quota still signals a retry (its RetryInfo is not a reset time): %v", rec.Header())
			}
		})
	}
	t.Run("in-band", func(t *testing.T) {
		var calls atomic.Int32
		up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: "+envelope+"\n\n")
		}))
		defer up.Close()
		setupForTest(up.URL)
		cfg.txLocalRetries = 3
		cfg.localBackoffCap = 0
		rec := httptest.NewRecorder()
		handle(rec, httptest.NewRequest(http.MethodPost, geminiStreamPath, strings.NewReader(`{"contents":[]}`)))
		got := decodeGeminiError(t, rec.Body.Bytes())
		if rec.Code != 400 || calls.Load() != 1 || got.Error.Code != 400 || got.Error.Status != "RESOURCE_EXHAUSTED" {
			t.Fatalf("want one call and a forced 400 that keeps RESOURCE_EXHAUSTED, got status=%d calls=%d body=%s", rec.Code, calls.Load(), rec.Body)
		}
		if rec.Header().Get("X-Should-Retry") != "false" || rec.Header().Get("Retry-After") != "" {
			t.Fatalf("daily quota still signals a retry: %v", rec.Header())
		}
	})
}

// A per-minute 429 clears on its own, so it keeps the retry path, and the
// upstream's RetryInfo still sets the cooldown.
func TestGeminiPerMinuteQuotaIsStillRetried(t *testing.T) {
	const withoutHint = `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[{"quotaId":"GenerateRequestsPerMinutePerProjectPerModel-FreeTier"}]}]}}`
	withHint := `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","details":` + geminiMinuteQuotaDetails + `}}`
	serve := func(calls *atomic.Int32, sse bool, firstBody string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) > 1 {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, goodGeminiStream)
				return
			}
			if sse {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: "+firstBody+"\n\n")
				return
			}
			w.WriteHeader(http.StatusTooManyRequests)
			io.WriteString(w, firstBody)
		}))
	}
	for _, sse := range []bool{false, true} {
		wantReason := "rate_limit"
		if sse {
			wantReason = "gemini_sse_rate_limit"
		}
		t.Run("cooldown/sse="+strconv.FormatBool(sse), func(t *testing.T) {
			var calls atomic.Int32
			up := serve(&calls, sse, withHint)
			defer up.Close()
			setupForTest(up.URL)
			cfg.txLocalRetries = 1
			cfg.localBackoffCap = 0
			cfg.geminiBufferMs = time.Second // the 29s RetryInfo cannot be waited out locally
			rec := httptest.NewRecorder()
			logs := captureLogs(t, func() {
				handle(rec, httptest.NewRequest(http.MethodPost, geminiStreamPath, strings.NewReader(`{"contents":[]}`)))
			})
			if rec.Code != 503 || calls.Load() != 1 || rec.Header().Get("Retry-After") != "29" || rec.Header().Get("X-Should-Retry") != "true" {
				t.Fatalf("want the RetryInfo cooldown handed to the client: status=%d calls=%d headers=%v", rec.Code, calls.Load(), rec.Header())
			}
			// The retry surface must not leak the rate-limit identity to the client.
			if got := decodeGeminiError(t, rec.Body.Bytes()); got.Error.Code != 503 || got.Error.Status != "UNAVAILABLE" {
				t.Fatalf("retry surface exposes the upstream status: %+v", got)
			}
			// The log keeps the real upstream status next to the surfaced one.
			if !strings.Contains(logs, wantReason+" 429->503") {
				t.Fatalf("log lost the upstream status: %q", logs)
			}
		})
		t.Run("local-retry/sse="+strconv.FormatBool(sse), func(t *testing.T) {
			var calls atomic.Int32
			up := serve(&calls, sse, withoutHint)
			defer up.Close()
			setupForTest(up.URL)
			cfg.txLocalRetries = 1
			cfg.localBackoffCap = 0
			rec := httptest.NewRecorder()
			handle(rec, httptest.NewRequest(http.MethodPost, geminiStreamPath, strings.NewReader(`{"contents":[]}`)))
			if rec.Code != 200 || calls.Load() != 2 || !strings.Contains(rec.Body.String(), `"text":"Hello"`) {
				t.Fatalf("per-minute 429 was not retried: status=%d calls=%d body=%s", rec.Code, calls.Load(), rec.Body)
			}
		})
	}
}

// A mixed list is still exhausted for the day: the per-minute violation
// alongside it does not make the request retryable.
func TestGeminiDailyQuotaAmongOtherViolationsIsTerminal(t *testing.T) {
	envelope := `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[{"quotaId":"GenerateRequestsPerMinutePerProjectPerModel-FreeTier"},{"quotaId":"GenerateRequestsPerDayPerProjectPerModel-FreeTier"}]}]}}`
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, envelope)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.txLocalRetries = 3
	rec := httptest.NewRecorder()
	handle(rec, httptest.NewRequest(http.MethodPost, geminiStreamPath, strings.NewReader(`{"contents":[]}`)))
	if rec.Code != 400 || calls.Load() != 1 || rec.Body.String() != envelope || rec.Header().Get("X-Should-Retry") != "false" {
		t.Fatalf("status=%d calls=%d headers=%v body=%s", rec.Code, calls.Load(), rec.Header(), rec.Body)
	}
}

// Methods beyond generation (embeddings) and Vertex express paths are Gemini
// routes, so errors the proxy synthesizes carry Google's envelope, not
// Anthropic's.
func TestGeminiModelMethodErrorsUseGoogleEnvelope(t *testing.T) {
	for _, path := range geminiRoutePaths {
		t.Run(path, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				io.WriteString(w, "upstream overloaded")
			}))
			defer up.Close()
			setupForTest(up.URL)
			cfg.txLocalRetries = 1 // a stream route retries once without the legacy 250ms pause
			cfg.localBackoffCap = 0
			rec := httptest.NewRecorder()
			handle(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"contents":[]}`)))
			got := decodeGeminiError(t, rec.Body.Bytes())
			if rec.Code != 503 || got.Error.Code != 503 || got.Error.Status != "UNAVAILABLE" || got.Error.Message == "" || rec.Header().Get("X-Should-Retry") != "true" {
				t.Fatalf("want a retryable Google-shaped 503: status=%d headers=%v body=%s", rec.Code, rec.Header(), rec.Body)
			}
		})
	}
}

// Only streamGenerateContent?alt=sse is buffered; every other model method is
// forwarded as it arrives.
func TestGeminiBufferingIsLimitedToSSEStreaming(t *testing.T) {
	for _, tc := range []struct {
		path     string
		response string
		buffered bool
	}{
		{geminiStreamPath, goodGeminiStream, true},
		{"/v1/publishers/google/models/gemini-test:streamGenerateContent?alt=sse", goodGeminiStream, true},
		{"/v1beta/models/gemini-test:streamGenerateContent", `[{"candidates":[{"finishReason":"STOP"}]}]`, false},
		{"/v1/publishers/google/models/gemini-test:generateContent", `{"candidates":[{"finishReason":"STOP"}]}`, false},
		{"/v1beta/models/text-embedding-004:embedContent", `{"embedding":{"values":[0.1,0.2]}}`, false},
		{"/v1beta/models/text-embedding-004:batchEmbedContents", `{"embeddings":[{"values":[0.1]}]}`, false},
	} {
		t.Run(tc.path, func(t *testing.T) {
			var calls atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if tc.buffered {
					w.Header().Set("Content-Type", "text/event-stream")
				}
				io.WriteString(w, tc.response)
			}))
			defer up.Close()
			setupForTest(up.URL)
			rec := httptest.NewRecorder()
			handle(rec, httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(`{"contents":[]}`)))
			if rec.Code != 200 || calls.Load() != 1 || rec.Body.String() != tc.response {
				t.Fatalf("response changed: status=%d calls=%d body=%s", rec.Code, calls.Load(), rec.Body)
			}
			if buffered := rec.Header().Get("X-Steady-Proxy-Mode") == "buffered"; buffered != tc.buffered {
				t.Fatalf("buffered=%v, want %v: %v", buffered, tc.buffered, rec.Header())
			}
		})
	}
}

// A data chunk that spells out "error": null is still a data chunk, so the
// stream completes instead of failing as an in-band error.
func TestGeminiNullErrorChunksComplete(t *testing.T) {
	stream := strings.ReplaceAll(goodGeminiStream, "}\n\n", ",\"error\":null}\n\n")
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, stream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.txLocalRetries = 3
	rec := httptest.NewRecorder()
	handle(rec, httptest.NewRequest(http.MethodPost, geminiStreamPath, strings.NewReader(`{"contents":[]}`)))
	if rec.Code != 200 || calls.Load() != 1 || rec.Body.String() != stream || rec.Header().Get("X-Steady-Proxy-Mode") != "buffered" {
		t.Fatalf("null error field failed the stream: status=%d calls=%d headers=%v body=%s", rec.Code, calls.Load(), rec.Header(), rec.Body)
	}
}
