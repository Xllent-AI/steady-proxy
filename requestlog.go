package main

// Optional request/response capture. When PROXY_REQUEST_LOG_DIR is set, the proxy
// writes one human-readable file per proxied request into that directory: the
// outbound request (method/path/headers/body) plus the captured response
// (outcome/stats/headers/body). It is a debugging aid — every operation here is
// best-effort and must NEVER affect the proxied response. Secret-bearing headers
// are redacted, and both bodies are capped (PROXY_REQUEST_LOG_MAX_BYTES) so a
// huge stream can't blow up RAM or disk.

import (
	"bufio"
	"fmt"
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

func requestLogEnabled() bool { return cfg.requestLogDir != "" }

// requestLogCap is the per-section byte cap, clamped non-negative so a stray
// negative env value can never produce a negative slice bound.
func requestLogCap() int64 {
	if cfg.requestLogMax < 0 {
		return 0
	}
	return cfg.requestLogMax
}

// cappedBuffer is an io.Writer that retains at most max bytes and counts the rest
// as dropped. Write never errors — a tee on the live stream must not break it.
type cappedBuffer struct {
	buf     []byte
	max     int64
	dropped int64
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if room := c.max - int64(len(c.buf)); room > 0 {
		if int64(len(p)) <= room {
			c.buf = append(c.buf, p...)
		} else {
			c.buf = append(c.buf, p[:room]...)
			c.dropped += int64(len(p)) - room
		}
	} else {
		c.dropped += int64(len(p))
	}
	return len(p), nil
}

// reqRecorder accumulates one request/response for later writing by finish().
type reqRecorder struct {
	when         time.Time
	method       string
	path         string
	query        string
	who          string
	reqHeaders   http.Header
	reqBody      []byte
	respHeaders  http.Header
	resp         cappedBuffer
	stats        *captureStats // optional: token/stop/mode summary for the SSE path
	proxyRetries int

	// outcome, set once at the terminal log site via note().
	outcome    string
	origStatus int // true upstream status (e.g. 529)
	status     int // status surfaced to the client (e.g. masked 503)
	code       string
}

func newReqRecorder(start time.Time, r *http.Request, body []byte) *reqRecorder {
	return &reqRecorder{
		when:       start,
		method:     r.Method,
		path:       r.URL.Path,
		query:      r.URL.RawQuery,
		who:        who(r, body),
		reqHeaders: r.Header,
		reqBody:    body,
		resp:       cappedBuffer{max: requestLogCap()},
	}
}

// respWriter returns the capped response sink; tee upstream bytes into it.
func (rec *reqRecorder) respWriter() io.Writer {
	if rec == nil {
		return nil
	}
	return &rec.resp
}

func (rec *reqRecorder) resetResponse() {
	if rec == nil {
		return
	}
	rec.respHeaders = nil
	rec.resp = cappedBuffer{max: requestLogCap()}
	rec.stats = nil
}

func (rec *reqRecorder) noteProxyRetries(n int) {
	if rec == nil {
		return
	}
	rec.proxyRetries = n
}

// note records the final outcome (mirrors the one-line access log). orig is the
// true upstream status; surfaced is what the client received (the two differ when
// a transient cause was masked to a generic 503).
func (rec *reqRecorder) note(outcome string, orig, surfaced int, code string) {
	if rec == nil {
		return
	}
	rec.outcome, rec.origStatus, rec.status, rec.code = outcome, orig, surfaced, code
}

// finish writes the captured request/response to a file. Best-effort: any error
// is logged (verbose) and swallowed.
func (rec *reqRecorder) finish() {
	if rec == nil {
		return
	}
	// Belt-and-suspenders: a logging bug must never crash the request goroutine
	// (this runs in the handler's deferred path).
	defer func() {
		if p := recover(); p != nil {
			vlog("[request-log] recovered: %v", p)
		}
	}()
	if err := os.MkdirAll(cfg.requestLogDir, 0o700); err != nil {
		vlog("[request-log] mkdir %s: %v", cfg.requestLogDir, err)
		return
	}
	path := filepath.Join(cfg.requestLogDir, requestLogName(rec.path))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		vlog("[request-log] create %s: %v", path, err)
		return
	}
	bw := bufio.NewWriter(f)
	rec.writeTo(bw)
	flushErr := bw.Flush()
	closeErr := f.Close()
	if flushErr != nil {
		vlog("[request-log] write %s: %v", path, flushErr)
	} else if closeErr != nil {
		vlog("[request-log] close %s: %v", path, closeErr)
	}
}

func (rec *reqRecorder) writeTo(w io.Writer) {
	fmt.Fprintln(w, "=== REQUEST ===")
	fmt.Fprintf(w, "Timestamp: %s\n", rec.when.Format("2006-01-02T15:04:05.000Z07:00"))
	fmt.Fprintf(w, "Method: %s   Path: %s   Who: %s\n", rec.method, rec.path, rec.who)
	if q := redactQuery(rec.query); q != "" {
		fmt.Fprintf(w, "Query: %s\n", q)
	}
	fmt.Fprintln(w, "Headers:")
	writeHeaders(w, rec.reqHeaders)
	fmt.Fprintln(w, "Body:")
	writeCapped(w, rec.reqBody, requestLogCap())

	fmt.Fprintln(w, "\n=== RESPONSE ===")
	fmt.Fprintf(w, "Outcome: %s   status=%s   code=%s%s\n",
		dash(rec.outcome), statusField(rec.origStatus, rec.status), dash(rec.code), proxyRetryField(rec.proxyRetries))
	if st := rec.stats; st != nil {
		fmt.Fprintf(w, "Stats: in=%s out=%s tok   stop=%s   mode=%s   dur=%s\n",
			htok(st.inTok), htok(st.outTok), dash(st.stop), dash(st.mode), since(rec.when))
	} else {
		fmt.Fprintf(w, "Stats: dur=%s\n", since(rec.when))
	}
	fmt.Fprintln(w, "Headers:")
	writeHeaders(w, rec.respHeaders)
	fmt.Fprintln(w, "Body:")
	if len(rec.resp.buf) == 0 && rec.resp.dropped == 0 {
		fmt.Fprintln(w, "<empty>")
		return
	}
	if len(rec.resp.buf) > 0 {
		w.Write(rec.resp.buf)
		if rec.resp.buf[len(rec.resp.buf)-1] != '\n' {
			fmt.Fprintln(w)
		}
	}
	if rec.resp.dropped > 0 {
		fmt.Fprintf(w, "… [truncated %d bytes]\n", rec.resp.dropped)
	}
}

// writeCapped writes at most max bytes of p, annotating any truncation.
func writeCapped(w io.Writer, p []byte, max int64) {
	if max < 0 {
		max = 0
	}
	if len(p) == 0 {
		fmt.Fprintln(w, "<empty>")
		return
	}
	if int64(len(p)) > max {
		w.Write(p[:max])
		fmt.Fprintf(w, "\n… [truncated %d bytes]\n", int64(len(p))-max)
		return
	}
	w.Write(p)
	if p[len(p)-1] != '\n' {
		fmt.Fprintln(w)
	}
}

// secretHeaders are redacted in the dump so an API key never lands on disk.
var secretHeaders = map[string]bool{
	"authorization": true, "x-api-key": true, "api-key": true,
	"cookie": true, "set-cookie": true, "proxy-authorization": true,
	"x-stainless-api-key": true,
}

func writeHeaders(w io.Writer, h http.Header) {
	if len(h) == 0 {
		fmt.Fprintln(w, "  <none>")
		return
	}
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		for _, v := range h[k] {
			fmt.Fprintf(w, "  %s: %s\n", k, maskHeader(k, v))
		}
	}
}

// maskHeader redacts secret-bearing values, keeping a short tail as a hint.
func maskHeader(name, value string) string {
	if !secretHeaders[strings.ToLower(name)] {
		return value
	}
	if len(value) >= 8 {
		return "***redacted (…" + value[len(value)-4:] + ")***"
	}
	return "***redacted***"
}

// secretParamSigs mark query-parameter names whose value may be a credential.
var secretParamSigs = []string{
	"key", "token", "secret", "password", "passwd", "pwd",
	"sig", "auth", "bearer", "credential", "session", "jwt",
}

// redactQuery masks the values of credential-looking query parameters. Some
// gateways carry auth in the URL; the request body is the user's prompt, so we
// never want a key landing on disk via the query string either.
func redactQuery(raw string) string {
	if raw == "" {
		return ""
	}
	vals, err := url.ParseQuery(raw)
	if err != nil {
		return "[redacted: unparseable query]"
	}
	for k, vv := range vals {
		lk := strings.ToLower(k)
		for _, sig := range secretParamSigs {
			if strings.Contains(lk, sig) {
				for i := range vv {
					vv[i] = "REDACTED"
				}
				break
			}
		}
	}
	return vals.Encode()
}

var requestLogSeq atomic.Uint64

// requestLogName builds a collision-free, sortable filename for one request,
// e.g. v1-messages-20260621t143005-000017.log.
func requestLogName(urlPath string) string {
	return fmt.Sprintf("%s-%s-%06d.log",
		sanitizeForFilename(urlPath), time.Now().Format("20060102t150405"), requestLogSeq.Add(1))
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
