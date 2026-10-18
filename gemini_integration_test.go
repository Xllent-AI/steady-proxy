package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
