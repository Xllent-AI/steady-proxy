package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// logFiles returns the .log files written into dir.
func logFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return matches
}

// doStreamWith issues a streaming request carrying extra headers.
func doStreamWith(body string, hdr http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	for k, vv := range hdr {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	handle(rec, req)
	return rec
}

func TestRequestLogWritesRequestAndResponse(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	dir := t.TempDir()
	cfg.requestLogDir = dir
	t.Cleanup(func() { cfg.requestLogDir = "" })

	hdr := http.Header{"Authorization": {"Bearer sk-secret-abcd1234"}}
	rec := doStreamWith(`{"stream":true,"model":"m"}`, hdr)
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d", rec.Code)
	}

	files := logFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("want exactly 1 log file, got %d (%v)", len(files), files)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	got := string(data)

	for _, want := range []string{"=== REQUEST ===", "=== RESPONSE ===", `"model":"m"`, "message_stop", "Outcome: OK"} {
		if !strings.Contains(got, want) {
			t.Errorf("log missing %q\n---\n%s", want, got)
		}
	}
	// The gateway secret must be redacted, not written verbatim.
	if strings.Contains(got, "sk-secret-abcd1234") {
		t.Errorf("Authorization secret leaked into log:\n%s", got)
	}
	if !strings.Contains(got, "***redacted") {
		t.Errorf("expected redaction marker for Authorization:\n%s", got)
	}
	// Filename should be derived from the route path.
	if base := filepath.Base(files[0]); !strings.HasPrefix(base, "v1-messages-") {
		t.Errorf("unexpected log filename %q", base)
	}
}

func TestRequestLogDisabledWritesNothing(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL) // loadConfig leaves requestLogDir == ""

	rec := doStream(`{"stream":true,"model":"m"}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "message_stop") {
		t.Fatalf("disabled request-log must not alter behavior; code=%d", rec.Code)
	}
	if requestLogEnabled() {
		t.Fatalf("request-log should be disabled by default")
	}
}

func TestRequestLogTruncatesOversizedBodies(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	dir := t.TempDir()
	prevMax := cfg.requestLogMax
	cfg.requestLogDir = dir
	cfg.requestLogMax = 40 // tiny: both request and response exceed this
	t.Cleanup(func() { cfg.requestLogDir = ""; cfg.requestLogMax = prevMax })

	rec := doStream(`{"stream":true,"model":"m","extra":"` + strings.Repeat("x", 200) + `"}`)
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	files := logFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("want 1 log file, got %d", len(files))
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	got := string(data)
	if !strings.Contains(got, "[truncated") {
		t.Errorf("expected a truncation marker with a tiny cap:\n%s", got)
	}
	// The captured response must not exceed the cap (plus the small marker text).
	if strings.Count(got, "message_start") > 0 && len(got) > 4096 {
		t.Errorf("capped log unexpectedly large: %d bytes", len(got))
	}
}

// A pathological negative cap must never panic (the file write runs in the
// handler's deferred path), and a credential in the query string must be masked.
func TestRequestLogHostileInputs(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	dir := t.TempDir()
	prevMax := cfg.requestLogMax
	cfg.requestLogDir = dir
	cfg.requestLogMax = -1 // negative: must clamp, not panic on p[:max]
	t.Cleanup(func() { cfg.requestLogDir = ""; cfg.requestLogMax = prevMax })

	req := httptest.NewRequest(http.MethodPost,
		"/v1/messages?beta=true&api_key=sk-secret-9999", strings.NewReader(`{"stream":true,"model":"m"}`))
	rec := httptest.NewRecorder()
	handle(rec, req) // would panic before the fix
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d", rec.Code)
	}

	files := logFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("want 1 log file, got %d", len(files))
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	got := string(data)
	if strings.Contains(got, "sk-secret-9999") {
		t.Errorf("query credential leaked into log:\n%s", got)
	}
	if !strings.Contains(got, "REDACTED") {
		t.Errorf("expected the api_key query param to be REDACTED:\n%s", got)
	}
	// Cap clamped to 0 → bodies fully dropped but recorded as truncated, not <empty>.
	if !strings.Contains(got, "[truncated") {
		t.Errorf("expected truncation marker at zero cap:\n%s", got)
	}
}
