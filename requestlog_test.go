package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// archiveFiles returns the JSON archive files written into dir.
func archiveFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return matches
}

func readArchive(t *testing.T, path string) (requestArchive, []byte) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	var ar requestArchive
	if err := json.Unmarshal(data, &ar); err != nil {
		t.Fatalf("unmarshal archive: %v\n%s", err, data)
	}
	return ar, data
}

func requireArchiveBody(t *testing.T, b archiveBody, want []byte) {
	t.Helper()
	if b.Encoding != "base64" {
		t.Fatalf("encoding = %q, want base64", b.Encoding)
	}
	if b.Size != int64(len(want)) {
		t.Fatalf("size = %d, want %d", b.Size, len(want))
	}
	if b.SHA256 != bodySHA256(want) {
		t.Fatalf("sha256 = %s, want %s", b.SHA256, bodySHA256(want))
	}
	if !bytes.Equal(b.Data, want) {
		t.Fatalf("body mismatch\nwant: %q\n got: %q", want, b.Data)
	}
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
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

func TestRequestLogWritesRestorableRequestAndResponse(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	dir := t.TempDir()
	prevMem := cfg.maxBufferMem
	cfg.requestLogDir = dir
	cfg.maxBufferMem = 4 // force the response archive body through the spool path
	t.Cleanup(func() {
		cfg.requestLogDir = ""
		cfg.maxBufferMem = prevMem
	})

	reqBody := []byte(`{"stream":true,"model":"m"}`)
	hdr := http.Header{"Authorization": {"Bearer sk-secret-abcd1234"}}
	rec := doStreamWith(string(reqBody), hdr)
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d", rec.Code)
	}

	files := archiveFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("want exactly 1 archive file, got %d (%v)", len(files), files)
	}
	ar, raw := readArchive(t, files[0])
	if ar.Schema != requestArchiveSchema {
		t.Fatalf("schema = %q, want %q", ar.Schema, requestArchiveSchema)
	}
	if ar.ID == "" {
		t.Fatalf("archive id is empty")
	}
	base := filepath.Base(files[0])
	if ok := regexp.MustCompile(`^v1-messages-\d{8}t\d{6}-[0-9a-f]{8}\.json$`).MatchString(base); !ok {
		t.Fatalf("unexpected archive filename %q", base)
	}
	if !strings.Contains(base, ar.ID) {
		t.Fatalf("archive filename %q does not contain id %q", base, ar.ID)
	}

	requireArchiveBody(t, ar.ClientRequest.Body, reqBody)
	if ar.ClientRequest.BodyReference != "client_request.body" {
		t.Fatalf("body reference = %q", ar.ClientRequest.BodyReference)
	}
	if len(ar.Attempts) != 1 {
		t.Fatalf("want 1 attempt, got %d", len(ar.Attempts))
	}
	a := ar.Attempts[0]
	if a.RequestBodyRef != "client_request.body" || a.RequestBody != nil {
		t.Fatalf("attempt should reference client body without duplicating it: %+v", a)
	}
	if a.Response == nil {
		t.Fatalf("attempt response missing")
	}
	requireArchiveBody(t, a.Response.Body, []byte(goodStream))
	if ar.Outcome.Result != "OK" || ar.Outcome.Status != 200 {
		t.Fatalf("unexpected outcome: %+v", ar.Outcome)
	}
	if bytes.Contains(raw, []byte("sk-secret-abcd1234")) {
		t.Fatalf("Authorization secret leaked into archive:\n%s", raw)
	}
	if got := ar.ClientRequest.Headers.Get("Authorization"); !strings.Contains(got, "redacted") {
		t.Fatalf("expected redacted Authorization header, got %q", got)
	}
	if !containsString(ar.Redactions.HeaderNames, "Authorization") {
		t.Fatalf("redactions missing Authorization: %+v", ar.Redactions)
	}
}

// TestRequestLogIDCorrelatesLogLineToDump is the whole point of the correlation
// id: the exact id printed in the one-line access log must also be in the
// archive's filename and body, so a bad log line pins straight to the payload.
func TestRequestLogIDCorrelatesLogLineToDump(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	dir := t.TempDir()
	cfg.requestLogDir = dir
	t.Cleanup(func() { cfg.requestLogDir = "" })

	var logbuf bytes.Buffer
	prevOut := log.Writer()
	log.SetOutput(&logbuf)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	if rec := doStream(`{"stream":true,"model":"m"}`); rec.Code != 200 {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	files := archiveFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("want 1 archive file, got %d", len(files))
	}
	ar, raw := readArchive(t, files[0])
	if ar.ID == "" {
		t.Fatalf("archive has no id:\n%s", raw)
	}
	if base := filepath.Base(files[0]); !strings.Contains(base, ar.ID) {
		t.Fatalf("archive filename %q does not contain id %q", base, ar.ID)
	}
	if al := logbuf.String(); !strings.Contains(al, "id="+ar.ID) {
		t.Fatalf("access log line missing id=%s:\n%s", ar.ID, al)
	}
}

func TestRequestLogShowsResolvedModelWhenDifferent(t *testing.T) {
	stream := strings.Replace(goodStream,
		`{"type":"message_start","message":{"id":"msg_1"}}`,
		`{"type":"message_start","message":{"id":"msg_1","model":"actual-model"}}`, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, stream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	dir := t.TempDir()
	cfg.requestLogDir = dir
	t.Cleanup(func() { cfg.requestLogDir = "" })

	rec := doStream(`{"stream":true,"model":"alias-model"}`)
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d", rec.Code)
	}

	files := archiveFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("want exactly 1 archive file, got %d (%v)", len(files), files)
	}
	ar, _ := readArchive(t, files[0])
	if ar.Outcome.Who != "alias-model->actual-model/main" {
		t.Fatalf("outcome who = %q, want resolved model", ar.Outcome.Who)
	}
	if ar.Outcome.Stats == nil || ar.Outcome.Stats.Model != "actual-model" {
		t.Fatalf("archive missing resolved stats model: %+v", ar.Outcome.Stats)
	}
}

func TestRequestLogRecordsModelSwapWithAttemptDetails(t *testing.T) {
	var hits atomic.Int32
	errBody := []byte(`{"error":{"type":"invalid_request_error","message":"Fable 5's safeguards flagged this message"}}`)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(400)
			w.Write(errBody)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	dir := t.TempDir()
	cfg.requestLogDir = dir
	cfg.refusalFallback = "claude-opus-4-8"
	t.Cleanup(func() {
		cfg.requestLogDir = ""
		cfg.refusalFallback = ""
	})

	original := []byte(`{"stream":true,"model":"claude-fable-5","max_tokens":256}`)
	rec := doStream(string(original))
	if rec.Code != 200 {
		t.Fatalf("want 200 after fallback, got %d body=%s", rec.Code, rec.Body.String())
	}
	files := archiveFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("want exactly 1 archive file, got %d (%v)", len(files), files)
	}
	ar, _ := readArchive(t, files[0])

	requireArchiveBody(t, ar.ClientRequest.Body, original)
	if ar.Outcome.ModelSwap == nil {
		t.Fatalf("outcome missing model swap: %+v", ar.Outcome)
	}
	if *ar.Outcome.ModelSwap != (archiveModelSwap{Reason: "safeguard", From: "claude-fable-5", To: "claude-opus-4-8"}) {
		t.Fatalf("unexpected outcome model swap: %+v", ar.Outcome.ModelSwap)
	}
	if len(ar.Attempts) != 2 {
		t.Fatalf("want 2 attempts, got %d", len(ar.Attempts))
	}
	first := ar.Attempts[0]
	if first.ModelSwap == nil || first.ModelSwap.Reason != "safeguard" || first.Result != "model_swap" {
		t.Fatalf("first attempt missing model swap details: %+v", first)
	}
	if first.Response == nil {
		t.Fatalf("first attempt missing error response")
	}
	requireArchiveBody(t, first.Response.Body, errBody)
	if first.Failure == nil || first.Failure.Message == "" {
		t.Fatalf("first attempt missing failure: %+v", first)
	}
	second := ar.Attempts[1]
	if second.RequestBody == nil {
		t.Fatalf("second attempt should archive the swapped request body: %+v", second)
	}
	if !bytes.Contains(second.RequestBody.Data, []byte(`"claude-opus-4-8"`)) {
		t.Fatalf("swapped request body missing fallback model: %s", second.RequestBody.Data)
	}
	requireArchiveBody(t, second.Response.Body, []byte(goodStream))
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

func TestRequestLogRestoresBinaryNonStreamingPayload(t *testing.T) {
	reqBody := []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n', 0x00, 0xff}
	respBody := []byte{0x00, 0x01, 0x02, 0xfe, 0xff}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		if !bytes.Equal(got, reqBody) {
			t.Fatalf("upstream request body mismatch: %q", got)
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(respBody)
	}))
	defer up.Close()
	setupForTest(up.URL)
	dir := t.TempDir()
	cfg.requestLogDir = dir
	t.Cleanup(func() { cfg.requestLogDir = "" })

	req := httptest.NewRequest(http.MethodPost, "/v1/files?z=1&x=a%2Bb", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "image/png")
	req.Header.Set("Authorization", "Bearer sk-binary-secret")
	rec := httptest.NewRecorder()
	handle(rec, req)
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	if !bytes.Equal(rec.Body.Bytes(), respBody) {
		t.Fatalf("downstream response mismatch: %q", rec.Body.Bytes())
	}

	files := archiveFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("want 1 archive file, got %d", len(files))
	}
	ar, raw := readArchive(t, files[0])
	requireArchiveBody(t, ar.ClientRequest.Body, reqBody)
	if ar.ClientRequest.Query != "z=1&x=a%2Bb" {
		t.Fatalf("query was normalized: %q", ar.ClientRequest.Query)
	}
	if len(ar.Attempts) != 1 || ar.Attempts[0].Response == nil {
		t.Fatalf("archive missing response attempt: %+v", ar.Attempts)
	}
	requireArchiveBody(t, ar.Attempts[0].Response.Body, respBody)
	if bytes.Contains(raw, []byte("sk-binary-secret")) {
		t.Fatalf("Authorization secret leaked into archive:\n%s", raw)
	}
}

func TestRequestLogHostileInputs(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	dir := t.TempDir()
	cfg.requestLogDir = dir
	t.Cleanup(func() { cfg.requestLogDir = "" })

	reqBody := []byte(`{"stream":true,"model":"m"}`)
	req := httptest.NewRequest(http.MethodPost,
		"/v1/messages?beta=true&api_key=sk-secret-9999&plain=a%2Bb", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()
	handle(rec, req)
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d", rec.Code)
	}

	files := archiveFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("want 1 archive file, got %d", len(files))
	}
	ar, raw := readArchive(t, files[0])
	if bytes.Contains(raw, []byte("sk-secret-9999")) {
		t.Fatalf("query credential leaked into archive:\n%s", raw)
	}
	if ar.ClientRequest.Query != "beta=true&api_key=REDACTED&plain=a%2Bb" {
		t.Fatalf("unexpected redacted query: %q", ar.ClientRequest.Query)
	}
	if !containsString(ar.Redactions.QueryParams, "api_key") {
		t.Fatalf("query redaction metadata missing api_key: %+v", ar.Redactions)
	}
	requireArchiveBody(t, ar.ClientRequest.Body, reqBody)
}

func TestRequestLogLocalRetryCapturesAllAttemptResponses(t *testing.T) {
	var hits atomic.Int32
	firstBody := []byte("event: message_start\ndata: {\"type\":\"message_start\"}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"FIRST_BAD\"}}\n\n")
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if hits.Add(1) == 1 {
			w.Write(firstBody)
			return
		}
		io.WriteString(w, goodStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	dir := t.TempDir()
	cfg.requestLogDir = dir
	cfg.txLocalRetries = 1
	cfg.localBackoffCap = 0
	t.Cleanup(func() {
		cfg.requestLogDir = ""
		cfg.txLocalRetries = 0
	})

	rec := doStream(`{"stream":true,"model":"m"}`)
	if rec.Code != 200 {
		t.Fatalf("want 200 after hidden retry, got %d", rec.Code)
	}
	files := archiveFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("want 1 archive file, got %d", len(files))
	}
	ar, _ := readArchive(t, files[0])
	if ar.Outcome.ProxyRetries != 1 {
		t.Fatalf("proxy retries = %d, want 1", ar.Outcome.ProxyRetries)
	}
	if len(ar.Attempts) != 2 {
		t.Fatalf("want 2 attempts, got %d", len(ar.Attempts))
	}
	if ar.Attempts[0].Result != "local_retry" || ar.Attempts[0].Failure == nil {
		t.Fatalf("first attempt should be retained as a local retry failure: %+v", ar.Attempts[0])
	}
	requireArchiveBody(t, ar.Attempts[0].Response.Body, firstBody)
	requireArchiveBody(t, ar.Attempts[1].Response.Body, []byte(goodStream))
}

func TestRequestLogOutcomeStatsResetForFinalAttempt(t *testing.T) {
	setupForTest("http://127.0.0.1:1")
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true,"model":"claude-fable-5"}`))
	rec := newReqRecorder(time.Now(), req, []byte(`{"stream":true,"model":"claude-fable-5"}`))

	rec.beginAttempt([]byte(`{"stream":true,"model":"claude-fable-5"}`), "claude-fable-5/main", 0)
	rec.noteAttemptStats(&captureStats{model: "claude-fable-5", stop: "refusal", inTok: 100, outTok: 2})
	rec.noteAttemptFailure(failure{code: refusalFallbackCode, status: 502, atype: "api_error"}, "model_swap")

	rec.beginAttempt([]byte(`{"stream":true,"model":"claude-opus-4-8"}`), "claude-opus-4-8/main", 0)
	rec.noteAttemptFailure(failure{code: "transport_error", status: 502, atype: "api_error", transient: true}, "transport_error")
	rec.note("RETRY", 502, 503, "transport_error")
	rec.finishCurrentAttempt()

	ar := rec.archive()
	if ar.ClientRequest.Who != "claude-fable-5/main" {
		t.Fatalf("client request who changed: %q", ar.ClientRequest.Who)
	}
	if ar.Outcome.Who != "claude-opus-4-8/main" {
		t.Fatalf("outcome who = %q, want final attempt", ar.Outcome.Who)
	}
	if ar.Outcome.Stats != nil {
		t.Fatalf("outcome stats should not reuse abandoned attempt stats: %+v", ar.Outcome.Stats)
	}
}
