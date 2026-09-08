package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestResponsesSpecificCodesOwnRetryability(t *testing.T) {
	for _, tc := range []struct {
		code, typ string
		retry     bool
	}{
		{"rate_limit_exceeded", "invalid_request_error", true},
		{"server_is_overloaded", "invalid_request_error", true},
		{"overloaded", "invalid_request_error", true},
		{"request_timeout", "invalid_request_error", true},
		{"context_length_exceeded", "overloaded_error", false},
		{"unknown_parameter", "rate_limit_error", false},
	} {
		t.Run(tc.code, func(t *testing.T) {
			// Generic message signatures must not override an explicit transient code.
			const message = "tool_use temporarily unavailable"
			f := classifyResponsesError(tc.typ, tc.code, message)
			body, _ := json.Marshal(map[string]any{"error": map[string]string{"type": tc.typ, "code": tc.code, "message": message}})
			httpFailure := classifyResponsesHTTPErrorBytes(&http.Response{StatusCode: 400, Header: http.Header{}}, body)
			if f.transient != tc.retry || httpFailure.transient != tc.retry {
				t.Fatalf("SSE=%+v HTTP=%+v; want retry=%v", f, httpFailure, tc.retry)
			}
		})
	}
}

func TestStainlessTimeoutSecondsAndMargin(t *testing.T) {
	useDefaultConfig(t)
	for _, tc := range []struct {
		header string
		want   time.Duration
	}{
		{"720", 695 * time.Second},
		{"720.5", 695*time.Second + 500*time.Millisecond},
		{"10", 5 * time.Second},
		{"", cfg.maxRequestDur},
		{"invalid", cfg.maxRequestDur},
		{"NaN", cfg.maxRequestDur},
		{"+Inf", cfg.maxRequestDur},
		{"1e30", cfg.maxRequestDur},
	} {
		if got := deriveDuration(tc.header); got != tc.want {
			t.Errorf("timeout %q => %s, want %s", tc.header, got, tc.want)
		}
	}
}

func TestSpoolsClosedWithoutGarbageCollection(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("uses Linux fd inspection")
	}
	useDefaultConfig(t)
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)
	cfg.maxBufferMem = 1
	cfg.spoolDir = t.TempDir()
	for _, tc := range []struct {
		name, stream   string
		early, success bool
	}{
		{"EOF", "data: {\"type\":\"response.created\"}\n\n", false, false},
		{"buffered-success", goodResponsesStream, false, true},
		{"live-success", goodResponsesStream, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, f := captureResp(t, iotest1byte(tc.stream), nil, time.Hour, tc.early)
			if (f == nil) != tc.success {
				t.Fatalf("unexpected outcome: %+v", f)
			}
			fds, err := os.ReadDir("/proc/self/fd")
			if err != nil {
				t.Fatal(err)
			}
			for _, fd := range fds {
				target, _ := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
				if strings.HasPrefix(target, cfg.spoolDir+"/") {
					t.Errorf("spool still open: %s", target)
				}
			}
		})
	}
}

type countedStream struct {
	r     io.Reader
	bytes atomic.Int64
}

func (r *countedStream) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.bytes.Add(int64(n))
	return n, err
}

func TestSSEBoundsUnfinishedEvents(t *testing.T) {
	useDefaultConfig(t)
	cfg.maxResponseBytes = 1024
	for _, input := range []string{
		"data: " + strings.Repeat("x", 2<<20),
		strings.Repeat("data: x\n", 1<<18),
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"" + strings.Repeat("x", 2<<20) + "\"}}\n\n",
	} {
		r := &countedStream{r: strings.NewReader(input)}
		rec, wrote, f := captureResp(t, r, nil, time.Hour, false)
		if wrote || rec.Body.Len() != 0 || f == nil || f.transient || f.code != "response_too_large" {
			t.Fatalf("oversized event must fail before commit: wrote=%v failure=%+v", wrote, f)
		}
		// Allow transport read-ahead, but never consume the entire oversized frame.
		if r.bytes.Load() > 128<<10 {
			t.Fatalf("read past event limit: %d", r.bytes.Load())
		}
	}
	parser := &sseParser{maxEventBytes: 64}
	frames := parser.feed([]byte(strings.Repeat("data: {}\n\n", 2000)))
	if parser.err != nil || len(frames) != 2000 {
		t.Fatalf("small events in a large chunk rejected: count=%d err=%v", len(frames), parser.err)
	}
}

func TestSpoolStorageFailureCanRecover(t *testing.T) {
	var hits atomic.Int32
	dir := filepath.Join(t.TempDir(), "spool")
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 2 {
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Error(err)
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodResponsesStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	t.Cleanup(func() { cfg = loadConfig() })
	cfg.maxBufferMem = 1
	cfg.spoolDir = dir
	cfg.responsesEarlyCommit = false
	cfg.txLocalRetries = 1
	cfg.localBackoffCap = 0
	logs := captureLogs(t, func() {
		rec := doStreamResponses(`{"stream":true,"model":"test"}`)
		if rec.Code != 200 || !strings.Contains(rec.Body.String(), "response.completed") || hits.Load() != 2 {
			t.Fatalf("storage failure did not recover: status=%d attempts=%d body=%s", rec.Code, hits.Load(), rec.Body.String())
		}
	})
	if strings.Contains(logs, "response_too_large") {
		t.Fatal("storage failure mislabeled as size limit", logs)
	}
}

func TestBufferWindowIncludesHeadersAndLocalRetries(t *testing.T) {
	for _, mode := range []string{"late-headers", "local-retry", "backoff"} {
		t.Run(mode, func(t *testing.T) {
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := hits.Add(1)
				pause := func(d time.Duration) bool {
					select {
					case <-time.After(d):
						return true
					case <-r.Context().Done():
						return false
					}
				}
				if mode == "late-headers" && !pause(400*time.Millisecond) {
					return
				}
				if mode == "backoff" {
					w.WriteHeader(503)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.(http.Flusher).Flush()
				if n == 1 && mode == "local-retry" {
					if pause(60 * time.Millisecond) {
						io.WriteString(w, "data: {\"type\":\"error\",\"code\":\"request_timeout\"}\n\n")
					}
					return
				}
				if pause(300 * time.Millisecond) {
					io.WriteString(w, goodResponsesStream)
				}
			}))
			defer up.Close()
			setupForTest(up.URL)
			t.Cleanup(func() { cfg = loadConfig() })
			cfg.responsesEarlyCommit = false
			cfg.responsesBufferMs = 120 * time.Millisecond
			cfg.txLocalRetries = 1
			cfg.localBackoffCap = 0
			if mode == "backoff" {
				cfg.localBackoffCap = time.Second
			}
			proxy := httptest.NewServer(http.HandlerFunc(handle))
			defer proxy.Close()
			downstream := &http.Client{Timeout: time.Second}
			started := time.Now()
			resp, err := downstream.Post(proxy.URL+"/v1/responses", "application/json", strings.NewReader(`{"stream":true,"model":"test"}`))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if elapsed := time.Since(started); elapsed > 280*time.Millisecond {
				t.Fatalf("no-output window restarted: headers took %s", elapsed)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "local-retry" {
				if resp.StatusCode != 200 || hits.Load() != 2 || !strings.Contains(string(body), "response.completed") {
					t.Fatalf("retry should go live within the original window: status=%d hits=%d body=%s", resp.StatusCode, hits.Load(), body)
				}
			} else if resp.StatusCode != 503 || resp.Header.Get("X-Should-Retry") != "true" || hits.Load() != 1 {
				t.Fatalf("uncommitted timeout/backoff must surface promptly: status=%d hits=%d", resp.StatusCode, hits.Load())
			}
		})
	}
}

func TestHTTP2HeaderDeadlineRemainsRetryable(t *testing.T) {
	var protocol atomic.Int32
	up := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protocol.Store(int32(r.ProtoMajor))
		<-r.Context().Done()
	}))
	up.EnableHTTP2 = true
	up.StartTLS()
	defer up.Close()
	setupForTest(up.URL)
	t.Cleanup(func() { cfg = loadConfig() })
	client = up.Client()
	client.CheckRedirect = stopRedirect
	cfg.responsesBufferMs = 100 * time.Millisecond
	cfg.responsesEarlyCommit = false
	rec := doStreamResponses(`{"stream":true,"model":"test"}`)
	if protocol.Load() != 2 {
		t.Fatalf("test did not negotiate HTTP/2: %d", protocol.Load())
	}
	if rec.Code != 503 || rec.Header().Get("X-Should-Retry") != "true" || rec.Header().Get("X-CC-Retry-Proxy-Reason") != "deadline" {
		t.Fatalf("HTTP/2 deadline became a terminal cancellation: status=%d headers=%v body=%s", rec.Code, rec.Header(), rec.Body.String())
	}
}

func TestProxyOnceAbortsTruncatedBody(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "32768")
		io.WriteString(w, strings.Repeat("x", 8192))
	}))
	defer up.Close()
	setupForTest(up.URL)
	t.Cleanup(func() { cfg = loadConfig() })
	proxy := httptest.NewServer(http.HandlerFunc(handle))
	defer proxy.Close()
	logs := captureLogs(t, func() {
		resp, err := http.Post(proxy.URL+"/v1/responses", "application/json", strings.NewReader(`{"stream":false}`))
		if err == nil {
			defer resp.Body.Close()
			_, err = io.ReadAll(resp.Body)
		}
		if err == nil {
			t.Fatal("truncated body completed cleanly")
		}
	})
	if !strings.Contains(logs, "DROP") || strings.Contains(logs, "OK    ") {
		t.Fatal("truncation must be logged as failure", logs)
	}
}

func TestResponsesMalformedTerminalsStayRetryable(t *testing.T) {
	useDefaultConfig(t)
	for _, payload := range []string{
		`{"type":"response.completed","response":{"id":"first","id":"second"}}`,
		`{"type":"response.completed","response":{"id":"\ud800"}}`,
		`{"type":"response.completed","response":{"id":"r","usage":{"input_tokens":1,"input_tokens":2,"output_tokens":0,"total_tokens":2}}}`,
	} {
		rec, wrote, f := captureResp(t, strings.NewReader("data: "+payload+"\n\n"), nil, time.Hour, false)
		if wrote || rec.Body.Len() != 0 || f == nil || !f.transient {
			t.Fatalf("malformed terminal accepted: %s failure=%+v", payload, f)
		}
	}
}

func TestUpstreamRedirectIsNotFollowed(t *testing.T) {
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetHits.Add(1) }))
	defer target.Close()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, strings.Replace(target.URL, "127.0.0.1", "localhost", 1), 302)
	}))
	defer up.Close()
	setupForTest(up.URL)
	t.Cleanup(func() { cfg = loadConfig() })
	r := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	r.Header.Set("X-Api-Key", "test-sentinel")
	resp, _, err := roundTrip(r.Context(), r, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 302 || targetHits.Load() != 0 {
		t.Fatalf("redirect followed: status=%d target_hits=%d", resp.StatusCode, targetHits.Load())
	}
}

func TestCodexDisabledRetriesAreTerminal(t *testing.T) {
	for _, stream := range []string{"true", "false"} {
		t.Run(stream, func(t *testing.T) {
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				w.Header().Set("Retry-After", "5")
				w.WriteHeader(529)
			}))
			defer up.Close()
			setupForTest(up.URL)
			t.Cleanup(func() { cfg = loadConfig() })
			cfg.sdkRetryCap = 0
			cfg.txLocalRetries = 3
			rec := doStreamResponses(`{"stream":` + stream + `,"model":"test"}`)
			if rec.Code != 400 || rec.Header().Get("X-Should-Retry") != "false" || rec.Header().Get("Retry-After") != "" || hits.Load() != 1 {
				t.Fatalf("disabled retries must stop Codex: status=%d headers=%v hits=%d", rec.Code, rec.Header(), hits.Load())
			}
		})
	}
}

func TestErrorArchiveObeysByteIdle(t *testing.T) {
	for _, status := range []int{200, 503} {
		t.Run(itoa(status), func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				io.WriteString(w, `{"error":{"message":"upstream unavailable"}}`)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer up.Close()
			setupForTest(up.URL)
			t.Cleanup(func() { cfg = loadConfig() })
			cfg.requestLogDir = t.TempDir()
			cfg.txLocalRetries = 0
			cfg.upstreamByteIdle = 30 * time.Millisecond
			cfg.maxRequestDur = time.Second
			started := time.Now()
			rec := doStreamResponses(`{"stream":true,"model":"test"}`)
			if time.Since(started) > 500*time.Millisecond || rec.Code != 503 {
				t.Fatalf("archive delayed recovery: elapsed=%s status=%d", time.Since(started), rec.Code)
			}
			files, err := filepath.Glob(filepath.Join(cfg.requestLogDir, "*.json"))
			if err != nil || len(files) != 1 {
				t.Fatalf("missing archive: files=%v err=%v", files, err)
			}
			b, err := os.ReadFile(files[0])
			if err != nil {
				t.Fatal(err)
			}
			var archive requestArchive
			if err := json.Unmarshal(b, &archive); err != nil {
				t.Fatal(err)
			}
			if archive.Attempts[0].Response.Body.CaptureError == "" {
				t.Fatal("incomplete archive must carry its read error")
			}
		})
	}
}
