// cc-retry-proxy — a transactional, self-healing reverse proxy for Claude Code.
//
//	Claude Code (+ subagents) ──HTTP──▶ this proxy (loopback) ──HTTPS──▶ your gateway
//
// It makes the agent's API calls survive transient gateway failures WITHOUT the
// user ever typing "continue", and without any terminal automation.
//
// How it works (transactional mode):
//   - For POST /v1/messages it withholds ALL downstream bytes until it has
//     captured a complete, valid Anthropic SSE stream ending in `message_stop`.
//     Only then does it write `200 OK` and replay the buffered stream.
//   - Any failure that happens before that commit point (connection error,
//     5xx, stalled/truncated stream, mid-stream `error` event) is turned into a
//     *retryable* HTTP response: it stamps `x-should-retry: true` so Claude
//     Code's own SDK retry loop transparently re-sends the request.
//   - Genuinely permanent errors (bad request, context-length, missing
//     tool_result, auth) are passed through unchanged with `x-should-retry:
//     false`, so they surface instead of looping forever.
//   - A retry budget (driven by the SDK's own X-Stainless-Retry-Count) and a
//     per-route circuit breaker bound cost during a real outage.
//
// Re-issuing /v1/messages is safe here because the client never saw a partial
// response, and Claude Code executes tools only after a COMPLETE response — so a
// re-send cannot double-execute client-local tools. (Do NOT enable auto-retry if
// you use server-side / remote MCP tools without their own idempotency keys.)
//
// Stdlib only. Build: `go build -o cc-retry-proxy .`
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	listenAddr       string
	upstream         string // scheme://host[:port], no trailing slash
	upstreamHost     string
	maxBufferMem     int64
	maxResponseBytes int64
	maxRequestBytes  int64
	respHeaderTO     time.Duration
	upstreamByteIdle time.Duration
	keepaliveMs      time.Duration
	deadlineMargin   time.Duration
	maxRequestDur    time.Duration
	sdkRetryCap      int
	episodeWindow    time.Duration
	spoolDir         string
	requestLogDir    string // when non-empty, save each request/response to a file here
	requestLogMax    int64  // per-section cap (request body, response body) written per file
	validateJSON     bool
	verbose          bool
}

func loadConfig() config {
	up := strings.TrimRight(env("PROXY_UPSTREAM_URL", "https://your-gateway.example.com"), "/")
	host := up
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[i+3:]
	}
	return config{
		listenAddr:       env("PROXY_LISTEN_ADDR", "127.0.0.1:8789"),
		upstream:         up,
		upstreamHost:     host,
		maxBufferMem:     envInt64("PROXY_MAX_BUFFER_MEM_BYTES", 1<<20),    // 1 MiB in RAM, then temp file
		maxResponseBytes: envInt64("PROXY_MAX_RESPONSE_BYTES", 128<<20),    // 128 MiB hard cap
		maxRequestBytes:  envInt64("PROXY_MAX_REQUEST_BYTES", 64<<20),      // 64 MiB request cap
		respHeaderTO:     envDur("PROXY_RESP_HEADER_TIMEOUT_MS", 60000),    // wait for upstream status line
		upstreamByteIdle: envDur("PROXY_UPSTREAM_BYTE_IDLE_MS", 600000),    // abort+retry a wedged silent upstream (covers a sparse turn within the 600s window)
		keepaliveMs:      envDur("PROXY_KEEPALIVE_MS", 600000),             // stay fully transactional up to this long, then commit + stream live. REQUIRES *both* client abort timers to exceed it: CLAUDE_CODE_CONNECT_TIMEOUT_MS (~660000) and API_TIMEOUT_MS (~720000). 0 = pure transactional.
		deadlineMargin:   envDur("PROXY_DEADLINE_MARGIN_MS", 25000),        // finish before the client's own timeout
		maxRequestDur:    envDur("PROXY_MAX_REQUEST_DURATION_MS", 1500000), // absolute ceiling per attempt (25m)
		sdkRetryCap:      int(envInt64("PROXY_SDK_RETRY_CAP", 8)),          // stop converting past this many SDK retries
		episodeWindow:    envDur("PROXY_EPISODE_WINDOW_MS", 900000),        // 15m logical-failure window
		spoolDir:         env("PROXY_SPOOL_DIR", os.TempDir()),
		requestLogDir:    env("PROXY_REQUEST_LOG_DIR", ""),                // "" = disabled; set a dir to save each request/response
		requestLogMax:    envInt64("PROXY_REQUEST_LOG_MAX_BYTES", 10<<20), // 10 MiB per section, then truncate (bounds RAM/disk)
		validateJSON:     os.Getenv("PROXY_VALIDATE_JSON") != "0",         // default on
		verbose:          os.Getenv("PROXY_VERBOSE") == "1",
	}
}

var (
	cfg     config
	breaker *circuitBreaker
	ledger  *episodeLedger
	hmacKey []byte
	client  *http.Client
)

func main() {
	log.SetFlags(log.LstdFlags) // timestamp every line at second resolution: date + HH:MM:SS
	cfg = loadConfig()
	breaker = newCircuitBreaker()
	ledger = newEpisodeLedger(cfg.episodeWindow)
	hmacKey = make([]byte, 32)
	if _, err := rand.Read(hmacKey); err != nil {
		log.Fatalf("rand: %v", err)
	}

	client = &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   32, // many concurrent subagents
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: cfg.respHeaderTO,
			ExpectContinueTimeout: 1 * time.Second,
			DisableCompression:    true, // we need raw SSE bytes
		},
		// No Client.Timeout: per-request contexts own the deadlines.
	}

	srv := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           http.HandlerFunc(handle),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
		// WriteTimeout intentionally 0: long-lived holds; ctx deadlines bound work.
		MaxHeaderBytes: 1 << 20,
	}
	log.Printf("cc-retry-proxy listening on http://%s -> %s  (transactional, keepalive=%s, sdkRetryCap=%d; one log line per request)",
		cfg.listenAddr, cfg.upstream, cfg.keepaliveMs, cfg.sdkRetryCap)
	log.Fatal(srv.ListenAndServe())
}

func handle(w http.ResponseWriter, r *http.Request) {
	reqStart := time.Now()
	// Only POST /v1/messages gets the full transactional treatment. Everything
	// else (e.g. /v1/messages/count_tokens, model listing) is forwarded with a
	// single attempt; transport errors there are still converted to retryable.
	body, tooBig, err := readBody(r)
	if tooBig {
		writeAnthropicError(w, false, http.StatusRequestEntityTooLarge, "invalid_request_error",
			"cc-retry-proxy: request body exceeds limit", 0, "request_too_large")
		return
	}
	if err != nil {
		writeAnthropicError(w, false, http.StatusBadGateway, "api_error", "proxy: reading request body", 0, "read_body")
		return
	}

	// Optional: capture this request/response to a file (best-effort, never fatal).
	var rec *reqRecorder
	if requestLogEnabled() {
		rec = newReqRecorder(reqStart, r, body)
		defer rec.finish()
	}

	transactional := r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/messages") &&
		!strings.Contains(r.URL.Path, "count_tokens") && requestWantsStream(body)
	if !transactional {
		proxyOnce(w, r, body, rec)
		return
	}

	retryCount := atoiSafe(r.Header.Get("X-Stainless-Retry-Count"))
	key := logicalKey(r, body)
	route := routeKey(r, body)
	budgetLeft := func() bool { return retryCount < cfg.sdkRetryCap }

	if remaining, open := breaker.isOpen(route); open {
		n := ledger.bump(key, "circuit_open")
		canRetry := budgetLeft() && n.convertedCount <= cfg.sdkRetryCap
		decision := "surfaced"
		if canRetry {
			decision = "auto-retry"
		}
		log.Printf("BLOCK %s  circuit open (%s left) -> %s%s",
			who(r, body), remaining.Round(time.Second), decision, att(retryCount))
		rec.note("BLOCK", http.StatusServiceUnavailable, "circuit_open")
		writeAnthropicError(w, canRetry, http.StatusServiceUnavailable, "overloaded_error",
			"cc-retry-proxy: upstream circuit open", int(remaining.Seconds())+1, "circuit_open")
		return
	}

	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(deriveDuration(r.Header.Get("X-Stainless-Timeout"))))
	defer cancel()

	var last failure
	// Initial attempt + at most one cheap local retry for pre-header faults
	// (only while the retry budget allows it).
	for localAttempt := 0; localAttempt < 2; localAttempt++ {
		resp, started, rtErr := roundTrip(ctx, r, body)
		if rtErr != nil {
			last = classifyTransport(rtErr, ctx)
			if localAttempt == 0 && last.fastRetry && budgetLeft() && time.Since(started) < 3*time.Second {
				vlog("[local-retry] transport fault, retrying once: %v", rtErr)
				time.Sleep(250 * time.Millisecond)
				continue
			}
			break
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			last = classifyHTTPError(resp)
			resp.Body.Close()
			if localAttempt == 0 && last.fastRetry && last.retryAfter == 0 && budgetLeft() && time.Since(started) < 3*time.Second {
				vlog("[local-retry] http %d, retrying once", last.status)
				time.Sleep(250 * time.Millisecond)
				continue
			}
			break
		}

		if !isEventStream(resp.Header.Get("Content-Type")) {
			resp.Body.Close()
			last = failure{transient: true, status: http.StatusBadGateway, atype: "api_error",
				code: "unexpected_content_type", message: "upstream returned non-SSE to a streaming request"}
			break
		}

		// CAPTURE: buffer+validate (transactional), or commit+stream live past the
		// keepalive grace. captureSSE writes the downstream response itself.
		var st captureStats
		st.respTee = rec.respWriter() // nil when request-log disabled
		if rec != nil {
			rec.respHeaders = resp.Header
			rec.stats = &st
		}
		wrote, fail := captureSSE(ctx, cancel, w, resp.Header, resp.Body, &st)
		resp.Body.Close()
		if fail == nil {
			breaker.recordSuccess(route)
			ledger.clear(key)
			log.Printf("OK    %s  in=%s out=%s tok  %s  %s  %s%s",
				who(r, body), htok(st.inTok), htok(st.outTok), dash(st.stop), st.mode, since(reqStart), att(retryCount))
			rec.note("OK", http.StatusOK, "")
			return
		}
		if wrote { // failed AFTER committing — can't convert, response already streaming
			if fail.transient {
				breaker.recordFailure(route)
			}
			log.Printf("DROP  %s  %s -> committed, Claude retries natively  out=%s tok  %s%s",
				who(r, body), fail.code, htok(st.outTok), since(reqStart), att(retryCount))
			rec.note("DROP", http.StatusOK, fail.code)
			return
		}
		last = *fail
		break // do not nest local retries around an expensive capture
	}

	// FAILED before committing: decide whether to hand back to the SDK retry loop.
	if last.transient {
		breaker.recordFailure(route)
	}
	n := ledger.bump(key, last.code)
	canRetry := last.transient && budgetLeft() &&
		n.convertedCount <= cfg.sdkRetryCap && n.sameFaultCount <= 3
	tag := "RETRY" // converted to x-should-retry=true; the SDK will re-send
	if !canRetry {
		tag = "FAIL" // surfaced to the user (permanent, or retry budget exhausted)
	}
	log.Printf("%-5s %s  %s (%d)  [transient=%v episode=%d]  %s%s",
		tag, who(r, body), last.code, statusFor(last), last.transient, n.convertedCount, since(reqStart), att(retryCount))
	rec.note(tag, statusFor(last), last.code)
	writeAnthropicError(w, canRetry, statusFor(last), last.atype, msgFor(last), last.retryAfter, last.code)
}

// readBody reads the request body with a hard cap, reporting overflow rather
// than silently truncating an oversized request.
func readBody(r *http.Request) (body []byte, tooBig bool, err error) {
	body, err = io.ReadAll(io.LimitReader(r.Body, cfg.maxRequestBytes+1))
	r.Body.Close()
	if int64(len(body)) > cfg.maxRequestBytes {
		return nil, true, nil
	}
	return body, false, err
}

// proxyOnce is a plain single-shot reverse proxy for non-transactional routes.
func proxyOnce(w http.ResponseWriter, r *http.Request, body []byte, rec *reqRecorder) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), cfg.maxRequestDur)
	defer cancel()
	resp, _, err := roundTrip(ctx, r, body)
	if err != nil {
		f := classifyTransport(err, ctx)
		log.Printf("FAIL  %s  %d (%s)  %s", r.URL.Path, statusFor(f), f.code, since(start))
		rec.note("FAIL", statusFor(f), f.code)
		writeAnthropicError(w, f.transient, statusFor(f), f.atype, msgFor(f), 0, f.code)
		return
	}
	defer resp.Body.Close()
	for k, v := range safeResponseHeaders(resp.Header) {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	// Tee the body into the recorder when request-log is on (capped sink).
	var dst io.Writer = w
	if sink := rec.respWriter(); sink != nil {
		rec.respHeaders = resp.Header
		dst = io.MultiWriter(w, sink)
	}
	n, _ := io.Copy(dst, resp.Body)
	log.Printf("OK    %s  %d  %s  %s", r.URL.Path, resp.StatusCode, hbytes(n), since(start))
	rec.note("OK", resp.StatusCode, "")
}

// roundTrip issues one upstream request from the exact buffered body.
func roundTrip(ctx context.Context, r *http.Request, body []byte) (*http.Response, time.Time, error) {
	started := time.Now()
	u := cfg.upstream + r.URL.Path
	if r.URL.RawQuery != "" {
		u += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(ctx, r.Method, u, bytes.NewReader(body))
	if err != nil {
		return nil, started, err
	}
	copyForwardHeaders(req.Header, r.Header)
	req.Host = cfg.upstreamHost
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	return resp, started, err
}

// ----------------------------------------------------------------- headers ---

// Hop-by-hop headers must not be forwarded (RFC 7230 §6.1) + internal trust headers.
var hopByHop = map[string]bool{
	"connection": true, "proxy-connection": true, "keep-alive": true,
	"transfer-encoding": true, "te": true, "trailer": true, "upgrade": true,
	"content-length": true, "proxy-authorization": true, "proxy-authenticate": true,
}
var stripFromClient = map[string]bool{
	// Don't let a client assert provenance our shim is meant to assert.
	"x-gateway-error-stage": true, "x-gateway-error-code": true, "x-gateway-retryable": true,
}

// connectionTokens returns the lowercased header names listed in the source's
// Connection header — these are hop-by-hop for this message and must be dropped.
func connectionTokens(src http.Header) map[string]bool {
	out := map[string]bool{}
	for _, v := range src.Values("Connection") {
		for _, tok := range strings.Split(v, ",") {
			if t := strings.ToLower(strings.TrimSpace(tok)); t != "" {
				out[t] = true
			}
		}
	}
	return out
}

func copyForwardHeaders(dst, src http.Header) {
	drop := connectionTokens(src)
	for k, vv := range src {
		lk := strings.ToLower(k)
		if hopByHop[lk] || stripFromClient[lk] || drop[lk] || lk == "host" {
			continue
		}
		dst[k] = append([]string(nil), vv...)
	}
}

func safeResponseHeaders(src http.Header) http.Header {
	drop := connectionTokens(src)
	out := http.Header{}
	for k, vv := range src {
		lk := strings.ToLower(k)
		if hopByHop[lk] || drop[lk] || lk == "content-encoding" {
			continue
		}
		out[k] = append([]string(nil), vv...)
	}
	return out
}

// ------------------------------------------------------------- small utils ---

func requestWantsStream(body []byte) bool {
	var b struct {
		Stream bool `json:"stream"`
	}
	_ = json.Unmarshal(body, &b)
	return b.Stream
}

func routeKey(r *http.Request, body []byte) string {
	var b struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(body, &b)
	return cfg.upstreamHost + "|" + b.Model
}

func logicalKey(r *http.Request, body []byte) string {
	mac := hmac.New(sha256.New, hmacKey)
	io.WriteString(mac, r.Method+"\n"+r.URL.Path+"\n")
	io.WriteString(mac, r.Header.Get("Authorization")+"\n")
	io.WriteString(mac, r.Header.Get("X-Claude-Code-Session-Id")+"\n")
	io.WriteString(mac, r.Header.Get("X-Claude-Code-Agent-Id")+"\n")
	io.WriteString(mac, r.Header.Get("X-Claude-Code-Parent-Agent-Id")+"\n")
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))[:24]
}

// deriveDuration picks an attempt deadline below the client's own timeout.
func deriveDuration(stainlessTimeout string) time.Duration {
	d := cfg.maxRequestDur
	if ms := atoiSafe(stainlessTimeout); ms > 0 {
		clientDur := time.Duration(ms) * time.Millisecond
		if clientDur-cfg.deadlineMargin > 0 && clientDur-cfg.deadlineMargin < d {
			d = clientDur - cfg.deadlineMargin
		}
	}
	return d
}

func isEventStream(ct string) bool { return strings.Contains(strings.ToLower(ct), "text/event-stream") }

// ---------------------------------------------------- friendly access log ---
// One line per request, optimized for a live `docker logs -f` viewer: a 5-char
// outcome tag, then who it's for (model/agent), then only the fields that vary.
// The method/path/gateway are static for the transactional route, so omitted.

func who(r *http.Request, body []byte) string {
	return modelOf(body) + "/" + agentKind(r)
}

// att renders the SDK attempt number only when it's non-zero (i.e. a retry), so
// the happy path stays uncluttered.
func att(n int) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("  attempt=%d", n)
}

func modelOf(body []byte) string {
	var b struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(body, &b)
	if b.Model == "" {
		return "?"
	}
	return b.Model
}

// agentKind reports "sub" when the request carries a parent-agent header (i.e. it
// came from a spawned subagent) and "main" otherwise.
func agentKind(r *http.Request) string {
	if r.Header.Get("X-Claude-Code-Parent-Agent-Id") != "" {
		return "sub"
	}
	return "main"
}

func htok(n int) string {
	if n >= 1000 {
		return fmt.Sprintf("%.1fk", float64(n)/1000)
	}
	return strconv.Itoa(n)
}

func hbytes(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func since(t time.Time) string { return time.Since(t).Round(time.Millisecond).String() }

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt64(k string, d int64) int64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return d
}
func envDur(k string, dms int64) time.Duration {
	return time.Duration(envInt64(k, dms)) * time.Millisecond
}
func atoiSafe(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}
func vlog(f string, a ...any) {
	if cfg.verbose {
		log.Printf(f, a...)
	}
}
