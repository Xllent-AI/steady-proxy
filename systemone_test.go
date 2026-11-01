package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func setupSystemOneTest(t *testing.T, upstream string) {
	t.Helper()
	previousConfig, previousClient := cfg, client
	setupForTest(upstream)
	testClient := client
	t.Cleanup(func() {
		testClient.CloseIdleConnections()
		cfg, client = previousConfig, previousClient
	})
}

func requestSystemOne(t *testing.T, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"xllent/decider-latest","stream":true,"input":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	handle(rec, req)
	return rec
}

func requireSystemOneDetail(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("native error must be JSON: %v; body=%q", err, rec.Body.String())
	}
	var detail string
	if err := json.Unmarshal(envelope["detail"], &detail); err != nil || detail == "" {
		t.Fatalf("native proxy error must have a nonempty string detail: %s", rec.Body)
	}
	if _, present := envelope["error"]; present {
		t.Fatalf("native proxy error contains another protocol's error envelope: %s", rec.Body)
	}
	if mediaType, _, _ := strings.Cut(rec.Header().Get("Content-Type"), ";"); mediaType != "application/json" {
		t.Fatalf("native error content type=%q", rec.Header().Get("Content-Type"))
	}
	return detail
}

type systemOneErrorTransport struct {
	calls int
}

func (tr *systemOneErrorTransport) RoundTrip(*http.Request) (*http.Response, error) {
	tr.calls++
	return nil, errors.New("test upstream connection failure")
}

func TestSystemOneNativeProxyErrors(t *testing.T) {
	t.Run("early size rejection", func(t *testing.T) {
		setupSystemOneTest(t, "http://upstream.invalid")
		cfg.maxRequestBytes = 4
		tr := &systemOneErrorTransport{}
		client.Transport = tr
		rec := requestSystemOne(t, "/v1/systemone")
		if rec.Code != http.StatusRequestEntityTooLarge || tr.calls != 0 || rec.Header().Get("X-Should-Retry") != "false" {
			t.Fatalf("oversized request was not rejected before upstream: status=%d calls=%d headers=%v", rec.Code, tr.calls, rec.Header())
		}
		requireSystemOneDetail(t, rec)
	})

	t.Run("transport failure", func(t *testing.T) {
		setupSystemOneTest(t, "http://upstream.invalid")
		cfg.txLocalRetries = 3
		tr := &systemOneErrorTransport{}
		client.Transport = tr
		rec := requestSystemOne(t, "/v1/systemone")
		if rec.Code != http.StatusServiceUnavailable || tr.calls != 1 || rec.Header().Get("X-Should-Retry") != "true" {
			t.Fatalf("single-shot transport fault lost native retry behavior: status=%d calls=%d headers=%v", rec.Code, tr.calls, rec.Header())
		}
		requireSystemOneDetail(t, rec)
	})
}

func TestSystemOneHTTPStatusAndDetailSemantics(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		body       string
		retry      bool
		diagnostic string
	}{
		{"conflict is final", 409, `{"detail":"conflicting decision"}`, false, ""},
		{"request timeout", 408, `{"detail":"decision timed out"}`, true, "decision timed out"},
		{"rate limit", 429, `{"detail":"capacity temporarily unavailable"}`, true, "capacity temporarily unavailable"},
		{"overload", 529, `{"detail":{"type":"capacity","message":"decision workers busy"}}`, true, "decision workers busy"},
		{"internal error", 500, `{"detail":"decision failed"}`, true, "decision failed"},
		{"bad gateway", 502, `{"detail":"decision gateway failed"}`, true, "decision gateway failed"},
		{"service unavailable", 503, `{"detail":"decision service unavailable"}`, true, "decision service unavailable"},
		{"gateway timeout", 504, `{"detail":"decision gateway timed out"}`, true, "decision gateway timed out"},
		{"validation array is final", 422, `{"detail":[{"loc":["body","input"],"msg":"field required","type":"missing"}]}`, false, ""},
		{"Anthropic identity cannot promote a final status", 400, `{"detail":"overloaded_error rate_limit_error timeout_error","error":{"type":"overloaded_error","message":"server busy"}}`, false, ""},
		{"Anthropic request-shape text cannot veto rate limit", 429, `{"detail":"prompt is too long; tool_use context_length","error":{"type":"invalid_request_error","message":"prompt is too long"}}`, true, "prompt is too long"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Request-Id", "decision-request")
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer up.Close()
			setupSystemOneTest(t, up.URL)
			cfg.txLocalRetries = 3
			rec := requestSystemOne(t, "/v1/systemone")
			wantStatus := tc.status
			if tc.retry {
				wantStatus = http.StatusServiceUnavailable
			}
			if rec.Code != wantStatus || calls.Load() != 1 || rec.Header().Get("X-Should-Retry") != strconv.FormatBool(tc.retry) {
				t.Fatalf("status=%d want=%d calls=%d headers=%v body=%s", rec.Code, wantStatus, calls.Load(), rec.Header(), rec.Body)
			}
			if tc.retry {
				detail := requireSystemOneDetail(t, rec)
				if !strings.Contains(detail, tc.diagnostic) || atoiSafe(rec.Header().Get("Retry-After")) < 1 {
					t.Fatalf("native diagnostic or retry backoff missing: detail=%q headers=%v", detail, rec.Header())
				}
			} else if rec.Body.String() != tc.body || rec.Header().Get("Request-Id") != "decision-request" {
				t.Fatalf("terminal native reply was replaced: headers=%v body=%s", rec.Header(), rec.Body)
			}
		})
	}
}

func TestSystemOneRetryHintsAndSDKBackstop(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        int
		gateway, hint string
		capDisabled   bool
		retry         bool
		wantStatus    int
	}{
		{"gateway true overrides should-retry false", 409, "true", "false", false, true, 503},
		{"gateway false overrides should-retry true", 503, "false", "true", false, false, 400},
		{"should-retry true overrides conflict", 409, "", "true", false, true, 503},
		{"should-retry false overrides timeout", 408, "", "false", false, false, 400},
		{"invalid gateway hint defers to should-retry", 429, "unknown", "false", false, false, 400},
		{"disabled SDK retries stop status-based clients", 529, "", "", true, false, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const body = `{"detail":{"message":"capacity unavailable","code":"busy"}}`
			var calls atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "17")
				w.Header().Set("retry-after-ms", "1501")
				if tc.gateway != "" {
					w.Header().Set("X-Gateway-Retryable", tc.gateway)
				}
				if tc.hint != "" {
					w.Header().Set("X-Should-Retry", tc.hint)
				}
				w.WriteHeader(tc.status)
				io.WriteString(w, body)
			}))
			defer up.Close()
			setupSystemOneTest(t, up.URL)
			cfg.txLocalRetries = 3
			if tc.capDisabled {
				cfg.sdkRetryCap = 0
			}
			rec := requestSystemOne(t, "/v1/systemone")
			if rec.Code != tc.wantStatus || calls.Load() != 1 || rec.Header().Get("X-Should-Retry") != strconv.FormatBool(tc.retry) {
				t.Fatalf("retry authority lost: status=%d calls=%d headers=%v body=%s", rec.Code, calls.Load(), rec.Header(), rec.Body)
			}
			if tc.retry {
				if rec.Header().Get("Retry-After") != "2" {
					t.Fatalf("millisecond delay must win and round up: headers=%v", rec.Header())
				}
				if detail := requireSystemOneDetail(t, rec); !strings.Contains(detail, "capacity unavailable") {
					t.Fatalf("native object detail diagnostic lost: %q", detail)
				}
			} else if rec.Header().Get("Retry-After") != "" || rec.Header().Get("retry-after-ms") != "" {
				t.Fatalf("terminal status retains contradictory retry delay: %v", rec.Header())
			} else if !tc.capDisabled && rec.Body.String() != body {
				t.Fatalf("explicit terminal native body changed: %s", rec.Body)
			} else if tc.capDisabled {
				requireSystemOneDetail(t, rec)
			}
		})
	}
}

func TestSystemOneRouteBoundaries(t *testing.T) {
	for _, tc := range []struct {
		path   string
		native bool
	}{
		{"/v1/systemone", true},
		{"/v1/systemone/", true},
		{"/v1/systemone-lookalike", false},
		{"/v1/systemone2", false},
		{"/v1/models", false},
	} {
		t.Run(tc.path, func(t *testing.T) {
			const body = `{"detail":"decision conflict"}`
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				io.WriteString(w, body)
			}))
			defer up.Close()
			setupSystemOneTest(t, up.URL)
			rec := requestSystemOne(t, tc.path)
			if tc.native {
				if rec.Code != 409 || rec.Header().Get("X-Should-Retry") != "false" || rec.Body.String() != body {
					t.Fatalf("native conflict was retried: status=%d headers=%v body=%s", rec.Code, rec.Header(), rec.Body)
				}
			} else {
				var envelope map[string]json.RawMessage
				if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
					t.Fatal(err)
				}
				if rec.Code != 503 || rec.Header().Get("X-Should-Retry") != "true" || envelope["error"] == nil || envelope["detail"] != nil {
					t.Fatalf("generic route acquired System One semantics: status=%d headers=%v body=%s", rec.Code, rec.Header(), rec.Body)
				}
			}
		})
	}
}

func TestSystemOneSuccessMetadataLogsAndArchives(t *testing.T) {
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err := io.WriteString(zw, `{"model":"must-not-resolve","usage":{"input_tokens":42,"output_tokens":42}}`); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, body, contentType, encoding string
		resolved, input, output           string
		stats                             bool
	}{
		{"resolved model and zero output", `{"model":"decider-resolved","usage":{"input_tokens":17,"output_tokens":0}}`, "application/json; charset=utf-8", "", "decider-resolved", "17", "0", true},
		{"zero input", `{"usage":{"input_tokens":0,"output_tokens":9}}`, "application/json", "", "", "0", "9", true},
		{"model without usage", `{"model":"decider-resolved"}`, "application/json", "", "decider-resolved", "?", "?", false},
		{"missing counters", `{"usage":{}}`, "application/json", "", "", "?", "?", false},
		{"null input", `{"usage":{"input_tokens":null,"output_tokens":0}}`, "application/json", "", "", "?", "0", false},
		{"negative input", `{"usage":{"input_tokens":-1,"output_tokens":8}}`, "application/json", "", "", "?", "8", false},
		{"string output", `{"usage":{"input_tokens":12,"output_tokens":"0"}}`, "application/json", "", "", "12", "?", false},
		{"fractional input", `{"usage":{"input_tokens":1.5,"output_tokens":0}}`, "application/json", "", "", "?", "0", false},
		{"overflow input", `{"usage":{"input_tokens":1e100,"output_tokens":0}}`, "application/json", "", "", "?", "0", false},
		{"wrong-case keys", `{"Model":"must-not-resolve","Usage":{"Input_Tokens":42,"Output_Tokens":42}}`, "application/json", "", "", "?", "?", false},
		{"malformed JSON", `{"model":"must-not-resolve","usage":{"input_tokens":42,"output_tokens":42}`, "application/json", "", "", "?", "?", false},
		{"non-JSON media type", `{"model":"must-not-resolve","usage":{"input_tokens":42,"output_tokens":42}}`, "text/plain", "", "", "?", "?", false},
		{"encoded response", compressed.String(), "application/json", "gzip", "", "?", "?", false},
		{"oversized metadata", `{"padding":"` + strings.Repeat("x", 1<<20) + `","model":"must-not-resolve","usage":{"input_tokens":42,"output_tokens":42}}`, "application/json", "", "", "?", "?", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				if tc.encoding != "" {
					w.Header().Set("Content-Encoding", tc.encoding)
				}
				io.WriteString(w, tc.body)
			}))
			defer up.Close()
			setupSystemOneTest(t, up.URL)
			cfg.requestLogDir = t.TempDir()
			var rec *httptest.ResponseRecorder
			logs := captureLogs(t, func() { rec = requestSystemOne(t, "/v1/systemone") })
			if rec.Code != http.StatusOK {
				t.Fatalf("metadata must never decide response success: status=%d", rec.Code)
			}
			line := firstLogLineContaining(logs, "OK ")
			wantWho := "xllent/decider-latest"
			if tc.resolved != "" {
				wantWho += "->" + tc.resolved
			}
			wantWho += "/main"
			if !strings.Contains(line, wantWho) || !strings.Contains(line, "in="+tc.input+" out="+tc.output+" tok") || strings.Contains(line, "must-not-resolve") {
				t.Fatalf("model/token diagnostics are inaccurate: %q", line)
			}
			files := archiveFiles(t, cfg.requestLogDir)
			if len(files) != 1 {
				t.Fatalf("want one request archive, got %d", len(files))
			}
			archive, _ := readArchive(t, files[0])
			if len(archive.Attempts) != 1 {
				t.Fatalf("want one archived attempt, got %d", len(archive.Attempts))
			}
			if !tc.stats {
				if archive.Outcome.Stats != nil || archive.Attempts[0].Stats != nil {
					t.Fatalf("unknown counters acquired fabricated archive counts: outcome=%+v attempt=%+v", archive.Outcome.Stats, archive.Attempts[0].Stats)
				}
				return
			}
			for _, stats := range []*archiveStats{archive.Outcome.Stats, archive.Attempts[0].Stats} {
				if stats == nil || stats.InputTokens != atoiSafe(tc.input) || stats.TotalInputTokens != atoiSafe(tc.input) || stats.OutputTokens != atoiSafe(tc.output) || stats.Model != tc.resolved {
					t.Fatalf("archive lost observed native usage: %+v", stats)
				}
			}
		})
	}
}

func TestSystemOneInterruptedErrorBodyKeepsMillisecondCooldown(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "1024")
		w.Header().Set("Retry-After", "17")
		w.Header().Set("retry-after-ms", "1501")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"detail":"unfinished`)
	}))
	defer up.Close()
	setupSystemOneTest(t, up.URL)
	cfg.txLocalRetries = 3
	rec := requestSystemOne(t, "/v1/systemone")
	if rec.Code != http.StatusServiceUnavailable || calls.Load() != 1 || rec.Header().Get("X-Should-Retry") != "true" || rec.Header().Get("Retry-After") != "2" {
		t.Fatalf("interrupted native error lost retry eligibility or cooldown: status=%d calls=%d headers=%v body=%s", rec.Code, calls.Load(), rec.Header(), rec.Body)
	}
	requireSystemOneDetail(t, rec)
}
