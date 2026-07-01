// cc-retry-proxy — a transactional, self-healing reverse proxy for Claude Code.
//
//	Claude Code (+ subagents) ──HTTP──▶ this proxy (loopback) ──HTTPS──▶ your gateway
//
// It makes the agent's API calls survive transient gateway failures WITHOUT the
// user ever typing "continue", and without any terminal automation.
//
// ── DESIGN PRINCIPLE: a blind stabilizer ─────────────────────────────────────
//
// This proxy is intentionally NOT a smart retry brain — your upstream gateway
// already owns retry intelligence (routing, provider selection, backoff). The
// proxy's only job is to keep the client alive through outages. So the whole
// policy is one rule:
//
//	Buffer the full response. On ANY failure, tell the client to wait and retry.
//	Only give up on a request that can never succeed as written.
//
// "Can never succeed" = a deterministic request-shape error (context too long,
// malformed tool blocks, schema/validation, model-not-found — see
// requestShapeSigs in classify.go). EVERYTHING else is retried, on purpose:
// network outages, 5xx, rate limits, capacity ("no available providers"), auth
// blocks, billing, and unknown 4xx. A temporary block is ridden out, not
// surfaced. The cost: a genuinely bad key/credential surfaces late (after the
// retry budget) rather than fast — an accepted trade for steadiness.
//
// How it works (transactional mode):
//   - For POST /v1/messages it withholds ALL downstream bytes until it has
//     captured a complete, valid Anthropic SSE stream ending in `message_stop`.
//     Only then does it write `200 OK` and replay the buffered stream.
//   - Any failure before that commit point (connection error, 5xx, stalled or
//     truncated stream, mid-stream `error` event, or a retryable status) is
//     turned into a *retryable* response: it stamps `x-should-retry: true` plus a
//     `Retry-After` backoff, so Claude Code's own SDK retry loop waits and
//     transparently re-sends. Claude Code 2.1.191 clamps
//     CLAUDE_CODE_MAX_RETRIES to 15; PROXY_TRANSACTIONAL_LOCAL_RETRIES can
//     opt-in to extra uncommitted upstream attempts inside each client attempt,
//     and PROXY_SDK_RETRY_CAP remains only a backstop.
//   - Every retryable failure is surfaced as ONE generic shape — a plain `503`
//     with `api_error` (see surface() in classify.go) — never its real identity
//     like `overloaded_error`/529 or `rate_limit_error`/429. Claude Code handles
//     those specific shapes on dedicated paths that ignore `x-should-retry` and
//     give up after ~3 tries (e.g. "Repeated 529 Overloaded errors"); masking
//     them as a generic 503 keeps every retry inside the SDK loop above.
//   - Request-shape errors are passed through with `x-should-retry: false`, so
//     they surface instead of looping forever.
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
	listenAddr        string
	upstream          string // scheme://host[:port], no trailing slash
	upstreamHost      string
	maxBufferMem      int64
	maxResponseBytes  int64
	maxRequestBytes   int64
	respHeaderTO      time.Duration
	upstreamByteIdle  time.Duration
	keepaliveMs       time.Duration
	deadlineMargin    time.Duration
	maxRequestDur     time.Duration
	sdkRetryCap       int
	txLocalRetries    int
	localBackoffCap   time.Duration
	spoolDir          string
	requestLogDir     string // when non-empty, save each request/response to a file here
	requestLogMax     int64  // per-section cap (request body, response body) written per file
	validateJSON      bool
	normalizeToolJSON bool
	verbose           bool
}

func loadConfig() config {
	up := strings.TrimRight(env("PROXY_UPSTREAM_URL", "https://your-gateway.example.com"), "/")
	host := up
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[i+3:]
	}
	return config{
		listenAddr:        env("PROXY_LISTEN_ADDR", "127.0.0.1:8789"),
		upstream:          up,
		upstreamHost:      host,
		maxBufferMem:      envInt64("PROXY_MAX_BUFFER_MEM_BYTES", 1<<20),    // 1 MiB in RAM, then temp file
		maxResponseBytes:  envInt64("PROXY_MAX_RESPONSE_BYTES", 128<<20),    // 128 MiB hard cap
		maxRequestBytes:   envInt64("PROXY_MAX_REQUEST_BYTES", 64<<20),      // 64 MiB request cap
		respHeaderTO:      envDur("PROXY_RESP_HEADER_TIMEOUT_MS", 60000),    // wait for upstream status line
		upstreamByteIdle:  envDur("PROXY_UPSTREAM_BYTE_IDLE_MS", 600000),    // abort+retry a wedged silent upstream (covers a sparse turn within the 600s window)
		keepaliveMs:       envDur("PROXY_KEEPALIVE_MS", 600000),             // stay fully transactional up to this long, then commit + stream live. REQUIRES *both* client abort timers to exceed it: CLAUDE_CODE_CONNECT_TIMEOUT_MS (~660000) and API_TIMEOUT_MS (~720000). 0 = pure transactional.
		deadlineMargin:    envDur("PROXY_DEADLINE_MARGIN_MS", 25000),        // finish before the client's own timeout
		maxRequestDur:     envDur("PROXY_MAX_REQUEST_DURATION_MS", 1500000), // absolute ceiling per attempt (25m)
		sdkRetryCap:       int(envInt64("PROXY_SDK_RETRY_CAP", 100)),        // backstop only; Claude Code's own retry cap still applies
		txLocalRetries:    envNonNegInt("PROXY_TRANSACTIONAL_LOCAL_RETRIES", 0),
		localBackoffCap:   envDur("PROXY_LOCAL_RETRY_EXTRA_BACKOFF_CAP_MS", 10000),
		spoolDir:          env("PROXY_SPOOL_DIR", os.TempDir()),
		requestLogDir:     env("PROXY_REQUEST_LOG_DIR", ""),                // "" = disabled; set a dir to save each request/response
		requestLogMax:     envInt64("PROXY_REQUEST_LOG_MAX_BYTES", 10<<20), // 10 MiB per section, then truncate (bounds RAM/disk)
		validateJSON:      os.Getenv("PROXY_VALIDATE_JSON") != "0",         // default on
		normalizeToolJSON: os.Getenv("PROXY_NORMALIZE_TOOL_JSON") != "0",   // default on: coalesce tool_use input_json_delta chunks before downstream forwarding
		verbose:           os.Getenv("PROXY_VERBOSE") == "1",
	}
}

var (
	cfg    config
	client *http.Client
)

func main() {
	log.SetFlags(log.LstdFlags) // timestamp every line at second resolution: date + HH:MM:SS
	cfg = loadConfig()

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
	log.Printf("cc-retry-proxy listening on http://%s -> %s  (transactional, keepalive=%s, sdkRetryCap=%d, txLocalRetries=%d; one log line per request)",
		cfg.listenAddr, cfg.upstream, cfg.keepaliveMs, cfg.sdkRetryCap, cfg.txLocalRetries)
	log.Fatal(srv.ListenAndServe())
}

func handle(w http.ResponseWriter, r *http.Request) {
	reqStart := time.Now()
	// Only POST /v1/messages gets the full transactional treatment. Everything
	// else (e.g. /v1/messages/count_tokens, model listing) is forwarded with a
	// single attempt; transport errors AND upstream HTTP errors there are still
	// converted to retryable and masked to a generic 503 (see proxyOnce).
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
	budgetLeft := func() bool { return retryCount < cfg.sdkRetryCap }

	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(deriveDuration(r.Header.Get("X-Stainless-Timeout"))))
	defer cancel()

	var last failure
	proxyRetries := 0
	reqWho := who(r, body)
	tryLocalRetry := func(f failure) bool {
		if cfg.txLocalRetries <= 0 || proxyRetries >= cfg.txLocalRetries || !f.transient || !budgetLeft() {
			return false
		}
		wait := localRetryDelay(f.retryAfter, proxyRetries)
		if !canWaitForLocalRetry(ctx, wait) {
			logLocalRetrySkip(reqWho, f, wait, retryCount, "not-enough-time")
			return false
		}
		logLocalRetry(reqWho, f, wait, proxyRetries+1, cfg.txLocalRetries, retryCount, "")
		if !sleepWithContext(ctx, wait) {
			return false
		}
		proxyRetries++
		return true
	}

	// With PROXY_TRANSACTIONAL_LOCAL_RETRIES=0, preserve the old behavior:
	// initial attempt + at most one cheap local retry for fast pre-header faults.
	// When enabled, use the explicit local retry budget for any transient
	// uncommitted transactional failure.
	for localAttempt := 0; ; localAttempt++ {
		if localAttempt > 0 {
			rec.resetResponse()
		}
		resp, started, rtErr := roundTrip(ctx, r, body)
		if rtErr != nil {
			last = classifyTransport(rtErr, ctx)
			if tryLocalRetry(last) {
				continue
			}
			if cfg.txLocalRetries == 0 && localAttempt == 0 && last.fastRetry && budgetLeft() && time.Since(started) < 3*time.Second {
				wait := 250 * time.Millisecond
				logLocalRetry(reqWho, last, wait, 1, 1, retryCount, "legacy-fast")
				if !sleepWithContext(ctx, wait) {
					break
				}
				continue
			}
			break
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			last = classifyHTTPError(resp)
			resp.Body.Close()
			if tryLocalRetry(last) {
				continue
			}
			if cfg.txLocalRetries == 0 && localAttempt == 0 && last.fastRetry && last.retryAfter == 0 && budgetLeft() && time.Since(started) < 3*time.Second {
				wait := 250 * time.Millisecond
				logLocalRetry(reqWho, last, wait, 1, 1, retryCount, "legacy-fast")
				if !sleepWithContext(ctx, wait) {
					break
				}
				continue
			}
			break
		}

		if !isEventStream(resp.Header.Get("Content-Type")) {
			resp.Body.Close()
			last = failure{transient: true, status: http.StatusBadGateway, atype: "api_error",
				code: "unexpected_content_type", message: "upstream returned non-SSE to a streaming request"}
			if tryLocalRetry(last) {
				continue
			}
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
			log.Printf("OK    %s  in=%s out=%s tok  %s  %s  %s%s%s",
				whoWithResolvedModel(reqWho, st.model), htok(st.inTok), htok(st.outTok), dash(st.stop), st.mode, since(reqStart), att(retryCount), proxyRetryField(proxyRetries))
			rec.noteProxyRetries(proxyRetries)
			rec.note("OK", http.StatusOK, http.StatusOK, "")
			return
		}
		if wrote { // failed AFTER committing — can't convert, response already streaming
			log.Printf("DROP  %s  %s -> committed, Claude retries natively  out=%s tok  %s%s%s",
				whoWithResolvedModel(reqWho, st.model), fail.code, htok(st.outTok), since(reqStart), att(retryCount), proxyRetryField(proxyRetries))
			rec.noteProxyRetries(proxyRetries)
			rec.note("DROP", http.StatusOK, http.StatusOK, fail.code)
			return
		}
		last = *fail
		if tryLocalRetry(last) {
			continue
		}
		break
	}

	// FAILED before committing. One rule: retry unless it's a request-shape error
	// (or we've hit the backstop). On retry, supply a Retry-After backoff so the
	// client waits before re-sending.
	canRetry := last.transient && budgetLeft()
	retryAfter := last.retryAfter
	if canRetry && retryAfter == 0 {
		retryAfter = retryAfterFor(retryCount)
	}
	tag := "RETRY" // converted to x-should-retry=true; the SDK will re-send
	if !canRetry {
		tag = "FAIL" // surfaced to the user (request-shape, or retry backstop hit)
	}
	// surface() collapses every retryable failure to a generic 503+api_error so
	// Claude Code can't recognize it as overloaded/rate-limit/timeout and bypass
	// its x-should-retry loop. The log shows the true upstream status arrowed to
	// the surfaced one when masked (e.g. 529->503); last.code carries the cause.
	sStatus, sType := surface(last)
	log.Printf("%-5s %s  %s %s%s  %s%s%s",
		tag, reqWho, last.code, statusField(origStatusOf(last), sStatus), retryField(retryAfter), since(reqStart), att(retryCount), proxyRetryField(proxyRetries))
	rec.noteProxyRetries(proxyRetries)
	rec.note(tag, origStatusOf(last), sStatus, last.code)
	writeAnthropicError(w, canRetry, sStatus, sType, msgFor(last), retryAfter, last.code)
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

	// Surface transport faults AND upstream HTTP errors through the same
	// retry-normalizing path the transactional route uses — so a non-transactional
	// route (count_tokens, non-streaming /v1/messages, model listing) can't leak a
	// recognizable overloaded_error/529 or rate_limit_error/429 that Claude Code
	// would route into its give-up handler either. A 2xx still streams through below.
	var f failure
	failed := true
	switch {
	case err != nil:
		f = classifyTransport(err, ctx)
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		f = classifyHTTPError(resp)
		resp.Body.Close()
	default:
		failed = false
	}
	if failed {
		// Same one rule as the transactional path: retry transient faults within
		// the backstop, with a Retry-After backoff so the client waits.
		retryCount := atoiSafe(r.Header.Get("X-Stainless-Retry-Count"))
		canRetry := f.transient && retryCount < cfg.sdkRetryCap
		retryAfter := 0
		if canRetry {
			if retryAfter = f.retryAfter; retryAfter == 0 { // honor upstream Retry-After (e.g. 429)
				retryAfter = retryAfterFor(retryCount)
			}
		}
		tag := "FAIL"
		if canRetry {
			tag = "RETRY"
		}
		sStatus, sType := surface(f)
		log.Printf("%-5s %s  %s %s%s  %s%s", tag, who(r, body), f.code, statusField(origStatusOf(f), sStatus), retryField(retryAfter), since(start), att(retryCount))
		rec.note(tag, origStatusOf(f), sStatus, f.code)
		writeAnthropicError(w, canRetry, sStatus, sType, msgFor(f), retryAfter, f.code)
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
	log.Printf("OK    %s  %d  %s  %s", who(r, body), resp.StatusCode, hbytes(n), since(start))
	rec.note("OK", resp.StatusCode, resp.StatusCode, "")
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

func whoWithResolvedModel(reqWho, resolvedModel string) string {
	resolvedModel = strings.TrimSpace(resolvedModel)
	if resolvedModel == "" {
		return reqWho
	}
	i := strings.LastIndex(reqWho, "/")
	if i < 0 {
		if reqWho == "" || reqWho == "?" {
			return resolvedModel
		}
		if reqWho == resolvedModel {
			return reqWho
		}
		return reqWho + "->" + resolvedModel
	}
	reqModel, agent := reqWho[:i], reqWho[i:]
	if reqModel == resolvedModel {
		return reqWho
	}
	if reqModel == "" || reqModel == "?" {
		return resolvedModel + agent
	}
	return reqModel + "->" + resolvedModel + agent
}

// att renders the SDK attempt number only when it's non-zero (i.e. a retry), so
// the happy path stays uncluttered.
func att(n int) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("  attempt=%d", n)
}

// retryField prints the backoff only when one was actually handed back (a RETRY);
// a surfaced FAIL gets no Retry-After, so it shows nothing rather than "0s".
func retryField(secs int) string {
	if secs <= 0 {
		return ""
	}
	return fmt.Sprintf("  retry-after=%ds", secs)
}

func logLocalRetry(reqWho string, f failure, wait time.Duration, retryNum, retryMax, sdkAttempt int, mode string) {
	vlog("[local-retry] %s %s %s wait=%s retry=%d/%d%s%s",
		reqWho, f.code, statusField(origStatusOf(f), statusFor(f)), wait.Round(time.Millisecond),
		retryNum, retryMax, att(sdkAttempt), localRetryModeField(mode))
}

func logLocalRetrySkip(reqWho string, f failure, wait time.Duration, sdkAttempt int, reason string) {
	vlog("[local-retry] %s %s %s wait=%s skip=%s%s",
		reqWho, f.code, statusField(origStatusOf(f), statusFor(f)), wait.Round(time.Millisecond), reason, att(sdkAttempt))
}

func localRetryModeField(mode string) string {
	if mode == "" {
		return ""
	}
	return "  mode=" + mode
}

func proxyRetryField(n int) string {
	if n <= 0 {
		return ""
	}
	return fmt.Sprintf("  proxy-retries=%d", n)
}

func localRetryDelay(upstreamRetryAfter int, retryIndex int) time.Duration {
	if upstreamRetryAfter < 0 {
		upstreamRetryAfter = 0
	}
	return time.Duration(upstreamRetryAfter)*time.Second + localRetryExtraBackoff(retryIndex)
}

func localRetryExtraBackoff(retryIndex int) time.Duration {
	if retryIndex < 0 {
		retryIndex = 0
	}
	capDur := cfg.localBackoffCap
	if capDur <= 0 {
		return 0
	}
	d := time.Second
	for i := 0; i < retryIndex && d < capDur; i++ {
		d *= 2
	}
	if d > capDur {
		return capDur
	}
	return d
}

func canWaitForLocalRetry(ctx context.Context, wait time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	if wait <= 0 {
		return true
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= wait+time.Second {
		return false
	}
	return true
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
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

// agentKind reports "sub" for any spawned/SDK-driven agent and "main" for the
// interactive CLI. Two signals mark a non-main request: a parent-agent header
// (a nested subagent spawned by another agent) OR an Agent-SDK User-Agent
// (".../sdk-cli" — workflow agents and top-level SDK agents, which carry no
// parent id). The UA is the broader signal: every parent-header request is also
// sdk-cli, but ~25% of sdk-cli traffic has no parent and would otherwise be
// misreported as "main".
func agentKind(r *http.Request) string {
	if r.Header.Get("X-Claude-Code-Parent-Agent-Id") != "" ||
		strings.Contains(r.Header.Get("User-Agent"), "sdk-cli") {
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
func envNonNegInt(k string, d int) int {
	n := int(envInt64(k, int64(d)))
	if n < 0 {
		return 0
	}
	return n
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
