package main

// Optional request/response archive. When PROXY_REQUEST_LOG_DIR is set, the
// proxy writes one JSON file per proxied request into that directory. Bodies are
// encoded as base64 JSON []byte fields with size+SHA256 metadata so request and
// response payload bytes can be restored exactly. Credential-bearing headers and
// query params are redacted before writing.

import (
	"bytes"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

const requestArchiveSchema = "cc-retry-proxy.payload.v2"

func requestLogEnabled() bool { return cfg.requestLogDir != "" }

type bodyCapture struct {
	buf  bytes.Buffer
	file *os.File
	hash hash.Hash
	size int64
	err  error
}

func newBodyCapture() *bodyCapture {
	return &bodyCapture{hash: sha256.New()}
}

func requestArchiveMemoryLimit() int64 {
	if cfg.maxBufferMem > 0 {
		return cfg.maxBufferMem
	}
	return 1 << 20
}

func (c *bodyCapture) Write(p []byte) (int, error) {
	if c == nil {
		return len(p), nil
	}
	c.hash.Write(p)
	c.size += int64(len(p))
	if c.err != nil {
		return len(p), nil
	}
	if c.file != nil {
		if _, err := c.file.Write(p); err != nil {
			c.err = err
		}
		return len(p), nil
	}
	if int64(c.buf.Len()+len(p)) <= requestArchiveMemoryLimit() {
		c.buf.Write(p)
		return len(p), nil
	}
	f, err := os.CreateTemp(cfg.spoolDir, "ccrp-archive-*.body")
	if err != nil {
		c.err = err
		return len(p), nil
	}
	os.Remove(f.Name()) // the open fd keeps the data; no extra request file remains.
	if _, err := f.Write(c.buf.Bytes()); err != nil {
		c.err = err
		f.Close()
		return len(p), nil
	}
	c.buf.Reset()
	c.file = f
	if _, err := c.file.Write(p); err != nil {
		c.err = err
	}
	return len(p), nil
}

func (c *bodyCapture) archiveBody() archiveBody {
	if c == nil {
		return makeArchiveBody(nil)
	}
	out := archiveBody{
		Encoding: "base64",
		Size:     c.size,
		SHA256:   hex.EncodeToString(c.hash.Sum(nil)),
		file:     c.file,
	}
	if c.file == nil {
		out.Data = append([]byte(nil), c.buf.Bytes()...)
	}
	if c.err != nil {
		out.CaptureError = c.err.Error()
	}
	return out
}

func (c *bodyCapture) cleanup() {
	if c != nil && c.file != nil {
		c.file.Close()
		c.file = nil
	}
}

type archiveBody struct {
	Encoding     string `json:"encoding"`
	Size         int64  `json:"size"`
	SHA256       string `json:"sha256"`
	Data         []byte `json:"data_base64"`
	CaptureError string `json:"capture_error,omitempty"`

	file *os.File
}

type archiveClientRequest struct {
	Method           string      `json:"method"`
	RequestURI       string      `json:"request_uri,omitempty"`
	Path             string      `json:"path"`
	Query            string      `json:"query,omitempty"`
	Host             string      `json:"host,omitempty"`
	Proto            string      `json:"proto,omitempty"`
	ContentLength    int64       `json:"content_length"`
	TransferEncoding []string    `json:"transfer_encoding,omitempty"`
	Who              string      `json:"who"`
	Headers          http.Header `json:"headers"`
	Body             archiveBody `json:"body"`
	BodyReference    string      `json:"body_reference"`
}

type archiveResponse struct {
	Status  int         `json:"status,omitempty"`
	Headers http.Header `json:"headers,omitempty"`
	Body    archiveBody `json:"body"`
}

type archiveFailure struct {
	Code       string `json:"code,omitempty"`
	Type       string `json:"type,omitempty"`
	Message    string `json:"message,omitempty"`
	Transient  bool   `json:"transient"`
	FastRetry  bool   `json:"fast_retry,omitempty"`
	Status     int    `json:"status,omitempty"`
	OrigStatus int    `json:"orig_status,omitempty"`
	RetryAfter int    `json:"retry_after,omitempty"`
}

type archiveStats struct {
	Mode                string `json:"mode,omitempty"`
	Bytes               int64  `json:"bytes"`
	InputTokens         int    `json:"input_tokens"`
	CacheCreationTokens int    `json:"cache_creation_input_tokens"`
	CacheReadTokens     int    `json:"cache_read_input_tokens"`
	TotalInputTokens    int    `json:"total_input_tokens"`
	OutputTokens        int    `json:"output_tokens"`
	StopReason          string `json:"stop_reason,omitempty"`
	Model               string `json:"model,omitempty"`
	MaxPingRun          int    `json:"max_ping_run"`
}

type archiveModelSwap struct {
	Reason string `json:"reason"`
	From   string `json:"from"`
	To     string `json:"to"`
}

type archiveAttempt struct {
	Index             int               `json:"index"`
	StartedAt         string            `json:"started_at"`
	DurationMS        int64             `json:"duration_ms"`
	Who               string            `json:"who"`
	Model             string            `json:"model"`
	ProxyRetry        int               `json:"proxy_retry"`
	RequestBodyRef    string            `json:"request_body_ref"`
	RequestBodySize   int               `json:"request_body_size"`
	RequestBodySHA256 string            `json:"request_body_sha256"`
	RequestBody       *archiveBody      `json:"request_body,omitempty"`
	Response          *archiveResponse  `json:"response,omitempty"`
	Failure           *archiveFailure   `json:"failure,omitempty"`
	Stats             *archiveStats     `json:"stats,omitempty"`
	ModelSwap         *archiveModelSwap `json:"model_swap,omitempty"`
	Result            string            `json:"result,omitempty"`
	RetryWaitMS       int64             `json:"retry_wait_ms,omitempty"`

	started         time.Time
	responseCapture *bodyCapture
}

type archiveOutcome struct {
	Result       string            `json:"result"`
	Who          string            `json:"who,omitempty"`
	OrigStatus   int               `json:"orig_status,omitempty"`
	Status       int               `json:"status,omitempty"`
	Code         string            `json:"code,omitempty"`
	ProxyRetries int               `json:"proxy_retries"`
	ModelSwap    *archiveModelSwap `json:"model_swap,omitempty"`
	Stats        *archiveStats     `json:"stats,omitempty"`
	DurationMS   int64             `json:"duration_ms"`
}

type archiveRedactions struct {
	HeaderNames []string `json:"header_names,omitempty"`
	QueryParams []string `json:"query_params,omitempty"`
}

type requestArchive struct {
	Schema        string               `json:"schema"`
	ID            string               `json:"id"`
	StartedAt     string               `json:"started_at"`
	FinishedAt    string               `json:"finished_at"`
	DurationMS    int64                `json:"duration_ms"`
	ProxyVersion  versionInfo          `json:"proxy_version"`
	UpstreamURL   string               `json:"upstream_url,omitempty"`
	ClientRequest archiveClientRequest `json:"client_request"`
	Attempts      []*archiveAttempt    `json:"attempts"`
	Outcome       archiveOutcome       `json:"outcome"`
	Redactions    archiveRedactions    `json:"redactions,omitempty"`
}

// reqRecorder accumulates one request and all upstream attempts for finish().
type reqRecorder struct {
	id          string // short correlation id: printed in access logs and filename
	when        time.Time
	method      string
	requestURI  string
	path        string
	query       string
	host        string
	proto       string
	contentLen  int64
	transferEnc []string
	who         string
	outcomeWho  string
	reqHeaders  http.Header
	reqBody     []byte

	attempts []*archiveAttempt
	cur      *archiveAttempt
	bodyRefs map[string]string

	stats        *captureStats
	proxyRetries int
	swapReason   string
	swapFrom     string
	swapTo       string

	outcome    string
	origStatus int
	status     int
	code       string
}

func newReqRecorder(start time.Time, r *http.Request, body []byte) *reqRecorder {
	key := bodyKey(body)
	return &reqRecorder{
		id:          newReqID(),
		when:        start,
		method:      r.Method,
		requestURI:  redactedRequestURI(r.RequestURI, r.URL.Path, r.URL.RawQuery),
		path:        r.URL.Path,
		query:       r.URL.RawQuery,
		host:        r.Host,
		proto:       r.Proto,
		contentLen:  r.ContentLength,
		transferEnc: append([]string(nil), r.TransferEncoding...),
		who:         who(r, body),
		outcomeWho:  who(r, body),
		reqHeaders:  r.Header.Clone(),
		reqBody:     append([]byte(nil), body...),
		bodyRefs:    map[string]string{key: "client_request.body"},
	}
}

func (rec *reqRecorder) beginAttempt(body []byte, reqWho string, proxyRetries int) {
	if rec == nil {
		return
	}
	rec.finishCurrentAttempt()
	rec.stats = nil
	rec.outcomeWho = reqWho
	started := time.Now()
	idx := len(rec.attempts) + 1
	ref, archived := rec.bodyReference(body, fmt.Sprintf("attempts.%d.request_body", idx))
	a := &archiveAttempt{
		Index:             idx,
		StartedAt:         rec.whenString(started),
		started:           started,
		Who:               reqWho,
		Model:             modelOf(body),
		ProxyRetry:        proxyRetries,
		RequestBodyRef:    ref,
		RequestBodySize:   len(body),
		RequestBodySHA256: bodySHA256(body),
		RequestBody:       archived,
	}
	rec.attempts = append(rec.attempts, a)
	rec.cur = a
}

func (rec *reqRecorder) bodyReference(body []byte, newRef string) (string, *archiveBody) {
	if rec.bodyRefs == nil {
		rec.bodyRefs = map[string]string{}
	}
	key := bodyKey(body)
	if ref := rec.bodyRefs[key]; ref != "" {
		return ref, nil
	}
	rec.bodyRefs[key] = newRef
	b := makeArchiveBody(body)
	return newRef, &b
}

func (rec *reqRecorder) finishCurrentAttempt() {
	if rec == nil || rec.cur == nil {
		return
	}
	if rec.cur.DurationMS == 0 {
		rec.cur.DurationMS = time.Since(rec.cur.started).Milliseconds()
	}
	if rec.cur.Response != nil && rec.cur.Response.Body.Encoding == "" {
		rec.cur.Response.Body = rec.cur.responseCapture.archiveBody()
	}
}

// respWriter returns the current attempt's exact response-body sink.
func (rec *reqRecorder) respWriter() io.Writer {
	if rec == nil || rec.cur == nil {
		return nil
	}
	rec.ensureAttemptResponse(0, nil)
	if rec.cur.responseCapture == nil {
		rec.cur.responseCapture = newBodyCapture()
	}
	return rec.cur.responseCapture
}

func (rec *reqRecorder) noteAttemptResponseHeaders(status int, h http.Header) {
	if rec == nil || rec.cur == nil {
		return
	}
	rec.ensureAttemptResponse(status, h)
}

func (rec *reqRecorder) noteAttemptResponse(status int, h http.Header, body []byte) {
	if rec == nil || rec.cur == nil {
		return
	}
	rec.ensureAttemptResponse(status, h)
	rec.cur.responseCapture = newBodyCapture()
	rec.cur.responseCapture.Write(body)
	rec.cur.Response.Body = rec.cur.responseCapture.archiveBody()
}

func (rec *reqRecorder) ensureAttemptResponse(status int, h http.Header) {
	if rec.cur.Response == nil {
		rec.cur.Response = &archiveResponse{}
	}
	if status != 0 {
		rec.cur.Response.Status = status
	}
	if h != nil {
		rec.cur.Response.Headers = maskHeaders(h)
	}
}

func (rec *reqRecorder) noteAttemptFailure(f failure, result string) {
	if rec == nil || rec.cur == nil {
		return
	}
	rec.cur.Failure = &archiveFailure{
		Code:       f.code,
		Type:       f.atype,
		Message:    f.message,
		Transient:  f.transient,
		FastRetry:  f.fastRetry,
		Status:     f.status,
		OrigStatus: f.origStatus,
		RetryAfter: f.retryAfter,
	}
	if result != "" {
		rec.cur.Result = result
	}
}

func (rec *reqRecorder) noteAttemptResult(result string) {
	if rec == nil || rec.cur == nil {
		return
	}
	rec.cur.Result = result
}

func (rec *reqRecorder) noteAttemptRetry(result string, wait time.Duration) {
	if rec == nil || rec.cur == nil {
		return
	}
	rec.cur.Result = result
	rec.cur.RetryWaitMS = wait.Milliseconds()
}

func (rec *reqRecorder) noteAttemptStats(st *captureStats) {
	if rec == nil || rec.cur == nil || st == nil {
		return
	}
	rec.cur.Stats = statsArchive(st)
	rec.stats = st
}

func (rec *reqRecorder) noteProxyRetries(n int) {
	if rec == nil {
		return
	}
	rec.proxyRetries = n
}

func (rec *reqRecorder) noteModelSwap(reason, from, to string) {
	if rec == nil {
		return
	}
	rec.swapReason = reason
	rec.swapFrom = from
	rec.swapTo = to
	if rec.cur != nil {
		rec.cur.ModelSwap = &archiveModelSwap{Reason: reason, From: from, To: to}
		rec.cur.Result = "model_swap"
	}
}

// note records the final outcome (mirrors the one-line access log). orig is the
// true upstream status; surfaced is what the client received.
func (rec *reqRecorder) note(outcome string, orig, surfaced int, code string) {
	if rec == nil {
		return
	}
	rec.outcome, rec.origStatus, rec.status, rec.code = outcome, orig, surfaced, code
}

// finish writes the captured request/attempt archive to one file. Best-effort:
// any error is logged (verbose) and swallowed.
func (rec *reqRecorder) finish() {
	if rec == nil {
		return
	}
	defer rec.cleanup()
	defer func() {
		if p := recover(); p != nil {
			vlog("[request-log] recovered: %v", p)
		}
	}()
	rec.finishCurrentAttempt()
	if err := os.MkdirAll(cfg.requestLogDir, 0o700); err != nil {
		vlog("[request-log] mkdir %s: %v", cfg.requestLogDir, err)
		return
	}
	path := filepath.Join(cfg.requestLogDir, requestLogName(rec.path, rec.id, rec.when))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		vlog("[request-log] create %s: %v", path, err)
		return
	}
	writeErr := rec.writeArchive(f)
	closeErr := f.Close()
	if writeErr != nil {
		vlog("[request-log] write %s: %v", path, writeErr)
	} else if closeErr != nil {
		vlog("[request-log] close %s: %v", path, closeErr)
	}
}

func (rec *reqRecorder) cleanup() {
	for _, a := range rec.attempts {
		if a.responseCapture != nil {
			a.responseCapture.cleanup()
		}
	}
}

func (rec *reqRecorder) writeArchive(w io.Writer) error {
	ar := rec.archive()
	o := newJSONObject(w)
	if err := o.field("schema", ar.Schema); err != nil {
		return err
	}
	if err := o.field("id", ar.ID); err != nil {
		return err
	}
	if err := o.field("started_at", ar.StartedAt); err != nil {
		return err
	}
	if err := o.field("finished_at", ar.FinishedAt); err != nil {
		return err
	}
	if err := o.field("duration_ms", ar.DurationMS); err != nil {
		return err
	}
	if err := o.field("proxy_version", ar.ProxyVersion); err != nil {
		return err
	}
	if ar.UpstreamURL != "" {
		if err := o.field("upstream_url", ar.UpstreamURL); err != nil {
			return err
		}
	}
	if err := o.rawField("client_request", func(w io.Writer) error {
		return writeArchiveClientRequest(w, ar.ClientRequest)
	}); err != nil {
		return err
	}
	if err := o.rawField("attempts", func(w io.Writer) error {
		return writeArchiveAttempts(w, ar.Attempts)
	}); err != nil {
		return err
	}
	if err := o.field("outcome", ar.Outcome); err != nil {
		return err
	}
	if err := o.field("redactions", ar.Redactions); err != nil {
		return err
	}
	if err := o.end(); err != nil {
		return err
	}
	_, err := io.WriteString(w, "\n")
	return err
}

func writeArchiveClientRequest(w io.Writer, cr archiveClientRequest) error {
	o := newJSONObject(w)
	if err := o.field("method", cr.Method); err != nil {
		return err
	}
	if cr.RequestURI != "" {
		if err := o.field("request_uri", cr.RequestURI); err != nil {
			return err
		}
	}
	if err := o.field("path", cr.Path); err != nil {
		return err
	}
	if cr.Query != "" {
		if err := o.field("query", cr.Query); err != nil {
			return err
		}
	}
	if cr.Host != "" {
		if err := o.field("host", cr.Host); err != nil {
			return err
		}
	}
	if cr.Proto != "" {
		if err := o.field("proto", cr.Proto); err != nil {
			return err
		}
	}
	if err := o.field("content_length", cr.ContentLength); err != nil {
		return err
	}
	if len(cr.TransferEncoding) > 0 {
		if err := o.field("transfer_encoding", cr.TransferEncoding); err != nil {
			return err
		}
	}
	if err := o.field("who", cr.Who); err != nil {
		return err
	}
	if err := o.field("headers", cr.Headers); err != nil {
		return err
	}
	if err := o.rawField("body", func(w io.Writer) error {
		return writeArchiveBody(w, cr.Body)
	}); err != nil {
		return err
	}
	if err := o.field("body_reference", cr.BodyReference); err != nil {
		return err
	}
	return o.end()
}

func writeArchiveAttempts(w io.Writer, attempts []*archiveAttempt) error {
	if _, err := io.WriteString(w, "["); err != nil {
		return err
	}
	for i, a := range attempts {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		if err := writeArchiveAttempt(w, a); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "]")
	return err
}

func writeArchiveAttempt(w io.Writer, a *archiveAttempt) error {
	o := newJSONObject(w)
	if err := o.field("index", a.Index); err != nil {
		return err
	}
	if err := o.field("started_at", a.StartedAt); err != nil {
		return err
	}
	if err := o.field("duration_ms", a.DurationMS); err != nil {
		return err
	}
	if err := o.field("who", a.Who); err != nil {
		return err
	}
	if err := o.field("model", a.Model); err != nil {
		return err
	}
	if err := o.field("proxy_retry", a.ProxyRetry); err != nil {
		return err
	}
	if err := o.field("request_body_ref", a.RequestBodyRef); err != nil {
		return err
	}
	if err := o.field("request_body_size", a.RequestBodySize); err != nil {
		return err
	}
	if err := o.field("request_body_sha256", a.RequestBodySHA256); err != nil {
		return err
	}
	if a.RequestBody != nil {
		if err := o.rawField("request_body", func(w io.Writer) error {
			return writeArchiveBody(w, *a.RequestBody)
		}); err != nil {
			return err
		}
	}
	if a.Response != nil {
		if err := o.rawField("response", func(w io.Writer) error {
			return writeArchiveResponse(w, *a.Response)
		}); err != nil {
			return err
		}
	}
	if a.Failure != nil {
		if err := o.field("failure", a.Failure); err != nil {
			return err
		}
	}
	if a.Stats != nil {
		if err := o.field("stats", a.Stats); err != nil {
			return err
		}
	}
	if a.ModelSwap != nil {
		if err := o.field("model_swap", a.ModelSwap); err != nil {
			return err
		}
	}
	if a.Result != "" {
		if err := o.field("result", a.Result); err != nil {
			return err
		}
	}
	if a.RetryWaitMS != 0 {
		if err := o.field("retry_wait_ms", a.RetryWaitMS); err != nil {
			return err
		}
	}
	return o.end()
}

func writeArchiveResponse(w io.Writer, resp archiveResponse) error {
	o := newJSONObject(w)
	if resp.Status != 0 {
		if err := o.field("status", resp.Status); err != nil {
			return err
		}
	}
	if len(resp.Headers) > 0 {
		if err := o.field("headers", resp.Headers); err != nil {
			return err
		}
	}
	if err := o.rawField("body", func(w io.Writer) error {
		return writeArchiveBody(w, resp.Body)
	}); err != nil {
		return err
	}
	return o.end()
}

func writeArchiveBody(w io.Writer, body archiveBody) error {
	o := newJSONObject(w)
	if err := o.field("encoding", defaultString(body.Encoding, "base64")); err != nil {
		return err
	}
	if err := o.field("size", body.Size); err != nil {
		return err
	}
	if err := o.field("sha256", body.SHA256); err != nil {
		return err
	}
	if body.CaptureError != "" {
		if err := o.field("capture_error", body.CaptureError); err != nil {
			return err
		}
	}
	if err := o.rawField("data_base64", func(w io.Writer) error {
		if _, err := io.WriteString(w, "\""); err != nil {
			return err
		}
		enc := base64.NewEncoder(base64.StdEncoding, w)
		var copyErr error
		if body.file != nil {
			if _, err := body.file.Seek(0, io.SeekStart); err != nil {
				copyErr = err
			} else {
				_, copyErr = io.Copy(enc, body.file)
			}
		} else if len(body.Data) > 0 {
			_, copyErr = enc.Write(body.Data)
		}
		closeErr := enc.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		_, err := io.WriteString(w, "\"")
		return err
	}); err != nil {
		return err
	}
	return o.end()
}

type jsonObjectWriter struct {
	w     io.Writer
	first bool
	err   error
}

func newJSONObject(w io.Writer) *jsonObjectWriter {
	_, err := io.WriteString(w, "{")
	return &jsonObjectWriter{w: w, first: true, err: err}
}

func (o *jsonObjectWriter) field(name string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return o.rawField(name, func(w io.Writer) error {
		_, err := w.Write(b)
		return err
	})
}

func (o *jsonObjectWriter) rawField(name string, write func(io.Writer) error) error {
	if o.err != nil {
		return o.err
	}
	if !o.first {
		if _, err := io.WriteString(o.w, ","); err != nil {
			return err
		}
	}
	o.first = false
	nameJSON, err := json.Marshal(name)
	if err != nil {
		return err
	}
	if _, err := o.w.Write(nameJSON); err != nil {
		return err
	}
	if _, err := io.WriteString(o.w, ":"); err != nil {
		return err
	}
	return write(o.w)
}

func (o *jsonObjectWriter) end() error {
	if o.err != nil {
		return o.err
	}
	_, err := io.WriteString(o.w, "}")
	return err
}

func (rec *reqRecorder) archive() requestArchive {
	finished := time.Now()
	headers, redactedHeaderNames := maskHeadersWithNames(rec.reqHeaders)
	query, redactedQueryNames := redactQueryWithNames(rec.query)
	sort.Strings(redactedHeaderNames)
	sort.Strings(redactedQueryNames)
	out := requestArchive{
		Schema:       requestArchiveSchema,
		ID:           rec.id,
		StartedAt:    rec.whenString(rec.when),
		FinishedAt:   rec.whenString(finished),
		DurationMS:   finished.Sub(rec.when).Milliseconds(),
		ProxyVersion: currentVersion(),
		UpstreamURL:  redactedUpstreamURL(rec.path, rec.query),
		ClientRequest: archiveClientRequest{
			Method:           rec.method,
			RequestURI:       rec.requestURI,
			Path:             rec.path,
			Query:            query,
			Host:             rec.host,
			Proto:            rec.proto,
			ContentLength:    rec.contentLen,
			TransferEncoding: append([]string(nil), rec.transferEnc...),
			Who:              rec.who,
			Headers:          headers,
			Body:             makeArchiveBody(rec.reqBody),
			BodyReference:    "client_request.body",
		},
		Attempts: rec.attempts,
		Outcome: archiveOutcome{
			Result:       dash(rec.outcome),
			Who:          rec.displayWho(),
			OrigStatus:   rec.origStatus,
			Status:       rec.status,
			Code:         rec.code,
			ProxyRetries: rec.proxyRetries,
			ModelSwap:    rec.modelSwap(),
			Stats:        statsArchive(rec.stats),
			DurationMS:   finished.Sub(rec.when).Milliseconds(),
		},
		Redactions: archiveRedactions{
			HeaderNames: redactedHeaderNames,
			QueryParams: redactedQueryNames,
		},
	}
	return out
}

func (rec *reqRecorder) displayWho() string {
	w := rec.outcomeWho
	if w == "" {
		w = rec.who
	}
	if rec.stats == nil {
		return w
	}
	return whoWithResolvedModel(w, rec.stats.model)
}

func (rec *reqRecorder) modelSwap() *archiveModelSwap {
	if rec.swapTo == "" {
		return nil
	}
	return &archiveModelSwap{Reason: defaultString(rec.swapReason, "unknown"), From: defaultString(rec.swapFrom, "?"), To: rec.swapTo}
}

func (rec *reqRecorder) whenString(t time.Time) string {
	return t.Format("2006-01-02T15:04:05.000Z07:00")
}

func statsArchive(st *captureStats) *archiveStats {
	if st == nil {
		return nil
	}
	return &archiveStats{
		Mode:                st.mode,
		Bytes:               st.bytes,
		InputTokens:         st.inputTok,
		CacheCreationTokens: st.cacheCreationTok,
		CacheReadTokens:     st.cacheReadTok,
		TotalInputTokens:    st.inTok,
		OutputTokens:        st.outTok,
		StopReason:          st.stop,
		Model:               st.model,
		MaxPingRun:          st.maxPingRun,
	}
}

func makeArchiveBody(p []byte) archiveBody {
	cp := append([]byte(nil), p...)
	return archiveBody{
		Encoding: "base64",
		Size:     int64(len(cp)),
		SHA256:   bodySHA256(cp),
		Data:     cp,
	}
}

func bodySHA256(p []byte) string {
	sum := sha256.Sum256(p)
	return hex.EncodeToString(sum[:])
}

func bodyKey(p []byte) string {
	return fmt.Sprintf("%d:%s", len(p), bodySHA256(p))
}

// secretHeaders are redacted in the archive so an API key never lands on disk.
var secretHeaders = map[string]bool{
	"authorization": true, "x-api-key": true, "api-key": true,
	"cookie": true, "set-cookie": true, "proxy-authorization": true,
	"x-stainless-api-key": true,
}

func maskHeaders(h http.Header) http.Header {
	out, _ := maskHeadersWithNames(h)
	return out
}

func maskHeadersWithNames(h http.Header) (http.Header, []string) {
	out := make(http.Header, len(h))
	var redacted []string
	for k, vv := range h {
		if secretHeaders[strings.ToLower(k)] {
			redacted = append(redacted, k)
			for _, v := range vv {
				out.Add(k, maskHeader(k, v))
			}
			continue
		}
		out[k] = append([]string(nil), vv...)
	}
	return out, redacted
}

// maskHeader redacts secret-bearing values, keeping a short tail as a hint.
func maskHeader(name, value string) string {
	if !secretHeaders[strings.ToLower(name)] {
		return value
	}
	if len(value) >= 8 {
		return "***redacted (..." + value[len(value)-4:] + ")***"
	}
	return "***redacted***"
}

// secretParamSigs mark query-parameter names whose value may be a credential.
var secretParamSigs = []string{
	"key", "token", "secret", "password", "passwd", "pwd",
	"sig", "auth", "bearer", "credential", "session", "jwt",
}

func redactQuery(raw string) string {
	q, _ := redactQueryWithNames(raw)
	return q
}

func redactQueryWithNames(raw string) (string, []string) {
	if raw == "" {
		return "", nil
	}
	var redacted []string
	parts := strings.Split(raw, "&")
	for i, part := range parts {
		namePart := part
		valueStart := -1
		if eq := strings.IndexByte(part, '='); eq >= 0 {
			namePart = part[:eq]
			valueStart = eq + 1
		}
		name, err := url.QueryUnescape(namePart)
		if err != nil {
			return "[redacted: unparseable query]", []string{"*"}
		}
		lk := strings.ToLower(name)
		for _, sig := range secretParamSigs {
			if strings.Contains(lk, sig) {
				redacted = append(redacted, name)
				if valueStart >= 0 {
					parts[i] = part[:valueStart] + "REDACTED"
				} else {
					parts[i] = part + "=REDACTED"
				}
				break
			}
		}
	}
	if len(redacted) == 0 {
		return raw, nil
	}
	return strings.Join(parts, "&"), redacted
}

func redactedUpstreamURL(path, rawQuery string) string {
	target := strings.TrimRight(cfg.upstream, "/") + path
	if rawQuery != "" {
		query, _ := redactQueryWithNames(rawQuery)
		target += "?" + query
	}
	u, err := url.Parse(target)
	if err != nil || u.User == nil {
		return target
	}
	u.User = url.User("***redacted***")
	return u.String()
}

func redactedRequestURI(rawURI, path, rawQuery string) string {
	if rawURI == "" {
		if rawQuery == "" {
			return path
		}
		query, _ := redactQueryWithNames(rawQuery)
		return path + "?" + query
	}
	if rawQuery == "" {
		return rawURI
	}
	query, redacted := redactQueryWithNames(rawQuery)
	if len(redacted) == 0 {
		return rawURI
	}
	if i := strings.IndexByte(rawURI, '?'); i >= 0 {
		return rawURI[:i+1] + query
	}
	return path + "?" + query
}

// requestIDSeq is a monotonic fallback used only if crypto/rand ever fails.
var requestIDSeq atomic.Uint64

// newReqID returns a short, greppable correlation id (8 hex chars).
func newReqID() string {
	var b [4]byte
	if _, err := crand.Read(b[:]); err != nil {
		return fmt.Sprintf("%08x", requestIDSeq.Add(1))
	}
	return hex.EncodeToString(b[:])
}

// idField renders the correlation id for one-line access logs; nil-safe and
// empty when request logging is off.
func (rec *reqRecorder) idField() string {
	if rec == nil || rec.id == "" {
		return ""
	}
	return "  id=" + rec.id
}

// requestLogName builds a sortable filename with route, request start datetime,
// and correlation id, e.g. v1-messages-20260706t143005-a1b2c3d4.json.
func requestLogName(urlPath, id string, when time.Time) string {
	return fmt.Sprintf("%s-%s-%s.json",
		sanitizeForFilename(urlPath), when.Format("20060102t150405"), id)
}

// sanitizeForFilename reduces a URL path to a safe, bounded filename stem.
func sanitizeForFilename(p string) string {
	var b strings.Builder
	for _, r := range strings.TrimPrefix(p, "/") {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "root"
	}
	if len(out) > 64 {
		out = strings.Trim(out[:64], "-")
	}
	return out
}
