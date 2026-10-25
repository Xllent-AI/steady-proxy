// steady-proxy — a transactional, self-healing reverse proxy for API clients
// using Anthropic Messages, OpenAI Responses, or native Gemini GenerateContent.
//
//	Agents / SDKs / applications ──HTTP──▶ this proxy (loopback) ──HTTPS──▶ your gateway
//
// It makes the agent's API calls survive transient gateway failures WITHOUT the
// user ever typing "continue", and without any terminal automation. The wire
// format only affects how a stream is interpreted (terminal event, in-band error,
// usage) — see wireModel in wire.go; the transactional engine is shared.
//
// ── DESIGN PRINCIPLE: a transparent stabilizer ───────────────────────────────
//
// This proxy is intentionally NOT a smart retry brain — your upstream gateway
// already owns retry intelligence (routing, provider selection, backoff). The
// proxy's only job is to keep the client alive through outages without changing
// what the upstream answered. So the whole policy is one rule:
//
//	Buffer the full response. Retry what a re-send can fix; relay everything
//	else exactly as the upstream sent it.
//
// "A re-send can fix" follows the official SDKs: transport faults, stalled or
// truncated streams, 408/409/429/5xx, an authoritative retry hint, or an error
// body that explicitly names a transient fault (overloaded, rate limit,
// timeout). Everything else — auth, billing, permission, not-found, validation,
// redirects, an exhausted quota, any other 4xx — is the upstream's final answer:
// relayed verbatim (body and headers) with `x-should-retry: false`, because an
// identical retry would fail the same way and hide the real error.
//
// How it works (transactional mode):
//   - For POST /v1/messages it withholds ALL downstream bytes until it has
//     captured a complete, valid Anthropic SSE stream ending in `message_stop`.
//     Only then does it write `200 OK` and replay the buffered stream.
//   - For POST /v1/responses (Codex) the same engine buffers the OpenAI Responses
//     SSE stream (terminal `response.completed` or `response.incomplete`); a
//     transient start-of-stream `error` / `response.failed` is caught pre-commit
//     and converted to a retry. See wireModel (wire.go).
//   - Native Gemini streamGenerateContent?alt=sse buffers through clean EOF
//     after all requested candidates finish, retaining trailing usage metadata.
//     PROXY_GEMINI_BUFFER_MS bounds the hold before switching to live output.
//   - A retryable failure before that commit point (connection error, 5xx,
//     stalled or truncated stream, transient in-band error, retryable status) is
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
//   - After commit nothing is replayed: a terminal in-band error frame is
//     forwarded and ends the stream; any other failure aborts the response so
//     no client mistakes a cut-off stream for a complete answer.
//
// Re-issuing /v1/messages is safe here because the client never saw a partial
// response, and Claude Code executes tools only after a COMPLETE response — so a
// re-send cannot double-execute client-local tools. (Do NOT enable auto-retry if
// you use server-side / remote MCP tools without their own idempotency keys.)
//
// Stdlib only. Build: `go build -o steady-proxy .`
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	listenAddr           string
	upstream             string // scheme://host[:port], no trailing slash
	upstreamHost         string
	maxBufferMem         int64
	maxResponseBytes     int64
	maxRequestBytes      int64
	respHeaderTO         time.Duration
	upstreamByteIdle     time.Duration
	keepaliveMs          time.Duration
	wfKeepaliveMs        time.Duration
	responsesKeepaliveMs time.Duration
	responsesBufferMs    time.Duration
	geminiBufferMs       time.Duration
	responsesEarlyCommit bool // Responses (Codex): commit on first output (default); false = buffer to terminal under responsesBufferMs, like /v1/messages
	responsesReplayBytes int  // Responses buffered/prefix replay: coalesce adjacent output_text.delta events up to this many source JSON bytes; 0 = raw replay
	maxRequestDur        time.Duration
	sdkRetryCap          int
	txLocalRetries       int
	localBackoffCap      time.Duration
	spoolDir             string
	requestLogDir        string // when non-empty, save each request/response to a file here
	validateJSON         bool
	normalizeToolJSON    bool
	refusalFallback      string // model to re-issue with when a model refusal/safeguard is detected; "" = feature off
	verbose              bool
}

// loadConfig reads every PROXY_* variable from the process environment. It
// fails on a missing upstream or any malformed value instead of running on a
// default the operator did not choose.
func loadConfig() (config, error) { return parseConfig(os.LookupEnv) }

// parseConfig is loadConfig over an arbitrary environment (tests inject one).
// Every problem is collected so one startup failure names all of them.
func parseConfig(lookup func(string) (string, bool)) (config, error) {
	e := &envReader{lookup: lookup}
	up, host, err := parseUpstream(e.str("PROXY_UPSTREAM_URL", ""))
	if err != nil {
		e.errs = append(e.errs, err.Error())
	}
	c := config{
		upstream:             up,
		upstreamHost:         host,
		listenAddr:           e.str("PROXY_LISTEN_ADDR", "127.0.0.1:8789"),
		maxBufferMem:         e.int64("PROXY_MAX_BUFFER_MEM_BYTES", 1<<20, 0),               // 1 MiB in RAM, then temp file
		maxResponseBytes:     e.int64("PROXY_MAX_RESPONSE_BYTES", 128<<20, 1),               // 128 MiB hard cap
		maxRequestBytes:      e.int64("PROXY_MAX_REQUEST_BYTES", 64<<20, 1),                 // 64 MiB request cap
		respHeaderTO:         e.ms("PROXY_RESP_HEADER_TIMEOUT_MS", 60000, 0),                // wait for upstream status line
		upstreamByteIdle:     e.ms("PROXY_UPSTREAM_BYTE_IDLE_MS", 600000, 1),                // abort+retry a wedged silent upstream (covers a sparse turn within the 600s window)
		keepaliveMs:          e.ms("PROXY_KEEPALIVE_MS", 600000, 0),                         // stay fully transactional up to this long, then commit + stream live. REQUIRES *both* client abort timers to exceed it: CLAUDE_CODE_CONNECT_TIMEOUT_MS (~660000) and API_TIMEOUT_MS (~720000). 0 = pure transactional.
		wfKeepaliveMs:        e.ms("PROXY_WORKFLOW_KEEPALIVE_MS", 10000, 0),                 // SMALL window applied ONLY to Workflow-tool agents (see isWorkflowAgent): commit early + stream live so forwarded deltas feed their per-agent stall watchdog (default 180s, no global env override). The watchdog kills on a forwarded-content-delta gap >= stallMs; while buffering the proxy forwards nothing, so the FIRST gap ~= window + upstream TTFB — keep the window small to hold that first gap under stallMs. A large window is not automatically fatal (a steady stream can survive it) but pushes the first gap toward stallMs and delays going live. It CANNOT fix a mid-turn content-silent pause >= stallMs; only a larger per-agent stallMs can. 0 = disable (falls back to full transactional; stall bug returns).
		responsesKeepaliveMs: e.ms("PROXY_RESPONSES_KEEPALIVE_MS", 30000, 0),                // OpenAI Responses route (Codex), applies ONLY in early-commit mode: bounds a silent start (reasoning with no output) before committing + streaming keepalives. With early-commit off the route buffers under PROXY_RESPONSES_BUFFER_MS instead (see config.keepaliveWindow). Keep it below the client's stream-idle timeout. Start-of-stream errors (overload/capacity) arrive before any output, so they are always caught pre-commit regardless of this value.
		responsesBufferMs:    e.ms("PROXY_RESPONSES_BUFFER_MS", 600000, 0),                  // Responses (Codex) hold used ONLY when early-commit is off: buffer to the terminal, committing live at this window as a safety valve (the /v1/messages policy applied to Responses). Responses-OWNED — independent of PROXY_KEEPALIVE_MS — so a mixed Codex+Claude proxy tunes the two horizons separately. The Codex client's stream_idle_timeout_ms must strictly exceed it (nothing is forwarded while buffering).
		geminiBufferMs:       e.ms("PROXY_GEMINI_BUFFER_MS", 600000, 0),                     // Gemini buffers through clean EOF; 0 disables the timed switch to live output.
		responsesEarlyCommit: e.str("PROXY_RESPONSES_EARLY_COMMIT", "1") != "0",             // default on: commit as soon as the first output event is buffered. 0 -> buffer the whole response to response.completed like /v1/messages, under the PROXY_RESPONSES_BUFFER_MS hold (extends hidden-retry protection to mid-stream errors), which needs the Codex client's stream_idle_timeout_ms to exceed that hold.
		responsesReplayBytes: int(e.int64("PROXY_RESPONSES_REPLAY_DELTA_BYTES", 64<<10, 0)), // collapse tiny adjacent Codex text deltas during buffered/prefix replay so a completed turn cannot burst thousands of app-server notifications into the TUI; 0 preserves byte-for-byte replay. Live events remain untouched.
		maxRequestDur:        e.ms("PROXY_MAX_REQUEST_DURATION_MS", 1500000, 1),             // whole-request ceiling, including local retries (25m)
		sdkRetryCap:          int(e.int64("PROXY_SDK_RETRY_CAP", 100, 0)),                   // backstop only; Claude Code's own retry cap still applies
		txLocalRetries:       int(e.int64("PROXY_TRANSACTIONAL_LOCAL_RETRIES", 0, 0)),
		localBackoffCap:      e.ms("PROXY_LOCAL_RETRY_EXTRA_BACKOFF_CAP_MS", 10000, 0),
		spoolDir:             e.str("PROXY_SPOOL_DIR", os.TempDir()),
		requestLogDir:        e.str("PROXY_REQUEST_LOG_DIR", ""),             // "" = disabled; set a dir to save each request/response archive
		validateJSON:         e.str("PROXY_VALIDATE_JSON", "1") != "0",       // default on
		normalizeToolJSON:    e.str("PROXY_NORMALIZE_TOOL_JSON", "1") != "0", // default on: coalesce tool_use input_json_delta chunks before downstream forwarding
		refusalFallback:      loadRefusalFallback(lookup),                    // default claude-opus-5; off/none/empty disables
		verbose:              e.str("PROXY_VERBOSE", "") == "1",
	}
	if len(e.errs) > 0 {
		return config{}, errors.New(strings.Join(e.errs, "\n  "))
	}
	return c, nil
}

// parseUpstream validates PROXY_UPSTREAM_URL: an http(s) URL with a host and
// an optional path prefix. The returned upstream has no trailing slash (request
// paths are appended verbatim); host is what the forwarded Host header carries.
func parseUpstream(raw string) (upstream, host string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", errors.New("PROXY_UPSTREAM_URL is required, e.g. https://gateway.example.com")
	}
	u, perr := url.Parse(raw)
	if perr != nil {
		return "", "", fmt.Errorf("PROXY_UPSTREAM_URL=%q: %v", raw, perr)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", "", fmt.Errorf("PROXY_UPSTREAM_URL=%q: must be http(s)://host[:port][/prefix]", raw)
	}
	if strings.ContainsAny(raw, "?#") { // covers a bare "?"/"#", which url.Parse reports as empty
		return "", "", fmt.Errorf("PROXY_UPSTREAM_URL=%q: must not carry a query or fragment", raw)
	}
	return strings.TrimRight(raw, "/"), u.Host, nil
}

var (
	cfg    config
	client *http.Client
)

func main() {
	if handleVersionCLI(os.Args[1:]) {
		return
	}
	log.SetFlags(log.LstdFlags) // timestamp every line at second resolution: date + HH:MM:SS
	c, err := loadConfig()
	if err != nil {
		log.Fatalf(programName+": invalid configuration:\n  %s", err)
	}
	cfg = c

	client = &http.Client{
		CheckRedirect: stopRedirect,
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
		// Per-request read/write deadlines are installed by handle.
		MaxHeaderBytes: 1 << 20,
	}
	log.Printf(programName+" %s listening on http://%s -> %s  (transactional: Messages + Responses + Gemini SSE, keepalive=%s, wf-keepalive=%s, responses-keepalive=%s, responses-buffer=%s, gemini-buffer=%s, responses-early-commit=%v, responses-replay-delta=%dB, sdkRetryCap=%d, txLocalRetries=%d, refusalFallback=%s; one log line per request)",
		currentVersion().token(), cfg.listenAddr, cfg.upstream, cfg.keepaliveMs, cfg.wfKeepaliveMs, cfg.responsesKeepaliveMs, cfg.responsesBufferMs, cfg.geminiBufferMs, cfg.responsesEarlyCommit, cfg.responsesReplayBytes, cfg.sdkRetryCap, cfg.txLocalRetries, refusalFallbackDesc(cfg.refusalFallback))
	log.Fatal(srv.ListenAndServe())
}

// keepaliveWindow picks the transactional buffer->commit window for one request.
// Each mode owns a distinct, independently-tunable window — no mode borrows another
// protocol's knob, and no single knob serves two roles:
//   - Responses, early-commit ON: responsesKeepaliveMs — a short silent-start bound.
//     The stream commits as soon as output is buffered, so this only caps a
//     reasoning-only silent start before it goes live.
//   - Responses, early-commit OFF (full buffer): responsesBufferMs — buffer to the
//     terminal, committing live at the window only as a safety valve (the /v1/messages
//     policy applied to Responses). This window is Responses-owned, NOT keepaliveMs,
//     so a mixed Codex+Claude proxy tunes the two horizons separately.
//   - Workflow-gated Messages: wfKeepaliveMs — the small stall-watchdog window.
//   - Ordinary Messages: keepaliveMs.
//
// earlyCommit ⇒ isResp and gated ⇒ !isResp, so the case order is unambiguous.
func (c config) keepaliveWindow(isResp, earlyCommit, gated bool) time.Duration {
	switch {
	case earlyCommit:
		return c.responsesKeepaliveMs
	case isResp:
		return c.responsesBufferMs
	case gated:
		return c.wfKeepaliveMs
	default:
		return c.keepaliveMs
	}
}

func handle(w http.ResponseWriter, r *http.Request) {
	if handleVersionRequest(w, r) {
		return
	}
	reqStart := time.Now()
	// Resolve the wire before reading the body so early rejects also use its
	// native error envelope. Only supported SSE generation requests are buffered;
	// other routes remain single-shot with HTTP retry normalization.
	writeErr := errorWriterFor(r.URL.Path)
	// One explicit ceiling covers upload, upstream work, and downstream writes.
	// X-Stainless-Timeout remains forwarded metadata, not a request budget.
	ctx, cancel := context.WithDeadline(r.Context(), reqStart.Add(cfg.maxRequestDur))
	defer cancel()
	deadline, _ := ctx.Deadline()
	control := http.NewResponseController(w)
	if err := control.SetReadDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		writeErr(w, false, http.StatusInternalServerError, "api_error", "proxy: setting request read deadline", 0, "read_deadline")
		return
	}
	if err := control.SetWriteDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		writeErr(w, false, http.StatusInternalServerError, "api_error", "proxy: setting response write deadline", 0, "write_deadline")
		return
	}
	body, tooBig, err := readBody(r)
	if tooBig {
		// Deterministic: the same oversized request can never succeed. surfaceFor keeps
		// 413 for the Anthropic wire but normalizes to 400 for Responses, since Codex
		// ignores x-should-retry and would re-send a 413 to its retry cap.
		st, ty := surfaceFor(r.URL.Path, failure{status: http.StatusRequestEntityTooLarge, atype: "invalid_request_error"})
		writeErr(w, false, st, ty, programName+": request body exceeds limit", 0, "request_too_large")
		return
	}
	if err != nil {
		writeErr(w, false, http.StatusBadGateway, "api_error", "proxy: reading request body", 0, "read_body")
		return
	}

	// Optional: capture this request/response to a file (best-effort, never fatal).
	var rec *reqRecorder
	if requestLogEnabled() {
		rec = newReqRecorder(reqStart, r, body)
		defer rec.finish()
	}

	_, geminiMethod := geminiTarget(r.URL.Path)
	geminiStream := geminiMethod == "streamGenerateContent" && r.URL.Query().Get("alt") == "sse"
	transactional := r.Method == http.MethodPost && (geminiStream ||
		(requestWantsStream(body) && (isMessagesPath(r.URL.Path) || isResponsesPath(r.URL.Path))))
	if !transactional {
		proxyOnce(ctx, w, r, body, rec, reqStart)
		return
	}
	// isResp selects the OpenAI Responses (Codex) wire: it disables the
	// Anthropic-only features (refusal fallback, Workflow stall gating) that don't
	// apply to it. By default it also commits early — streaming as soon as output
	// appears; PROXY_RESPONSES_EARLY_COMMIT=0 turns that off so it buffers to the
	// terminal like /v1/messages (fuller pre-commit protection — mid-stream errors
	// become hidden retries too — at the cost of a longer initial silence the
	// client's stream-idle timeout must cover).
	isResp := isResponsesPath(r.URL.Path)
	isMessages := isMessagesPath(r.URL.Path)
	earlyCommit := isResp && cfg.responsesEarlyCommit

	retryCount := atoiSafe(r.Header.Get("X-Stainless-Retry-Count"))
	budgetLeft := func() bool { return retryCount < cfg.sdkRetryCap }

	var last failure
	proxyRetries := 0
	reqWho := who(r, body)
	reqEffort := reasoningEffortOf(r.URL.Path, body)
	// legacyFastUsed tracks whether the CURRENT request body has already spent its
	// one free fast local retry (the txLocalRetries==0 legacy path). It replaces a
	// bare localAttempt==0 check so a refusal→fallback model swap — which begins a
	// new logical request — resets it and lets the fallback attempt earn its own
	// free fast retry instead of surfacing a 503 that resends the refusing body.
	legacyFastUsed := false

	// Workflow-tool agents carry a per-agent "stall" watchdog (default 180s, no
	// global env override) that aborts a turn streaming no assistant/tool delta for
	// the window. Full transactional buffering starves it, so those get a short
	// commit-early window AND gate the commit on real forwarded progress (see
	// captureSSEWindow); every other caller keeps the full hold. The window choice
	// (per wire + mode) lives in config.keepaliveWindow.
	gated := isMessages && isWorkflowAgent(body)
	kaWindow := cfg.keepaliveWindow(isResp, earlyCommit, gated)
	if geminiStream {
		kaWindow = cfg.geminiBufferMs
	}
	commitBy := bufferDeadline(reqStart, kaWindow)
	canWait := func(wait time.Duration) bool {
		return canWaitForLocalRetry(ctx, wait) && (commitBy.IsZero() || time.Until(commitBy) > wait)
	}
	// waitToRetry sleeps out a local-retry delay. If the request ends during the
	// wait (the client left), that becomes its outcome: the upstream failure the
	// retry was answering is no longer what the client experienced.
	waitToRetry := func(wait time.Duration) bool {
		if sleepWithContext(ctx, wait) {
			return true
		}
		last = classifyTransport(ctx.Err(), ctx)
		return false
	}
	tryLocalRetry := func(f failure) bool {
		if cfg.txLocalRetries <= 0 || proxyRetries >= cfg.txLocalRetries || !f.transient || !budgetLeft() {
			return false
		}
		wait := localRetryDelay(f.retryAfter, proxyRetries)
		if !canWait(wait) {
			logLocalRetrySkip(reqWho, reqEffort, f, wait, retryCount, "not-enough-time")
			return false
		}
		rec.noteAttemptRetry("local_retry", wait)
		logLocalRetry(reqWho, reqEffort, f, wait, proxyRetries+1, cfg.txLocalRetries, retryCount, "")
		if !waitToRetry(wait) {
			return false
		}
		proxyRetries++
		return true
	}
	swapToFallback := func(reason string, st *captureStats) bool {
		from := modelOf(body)
		swapped, ok := swapModel(body, cfg.refusalFallback)
		if !ok {
			return false
		}
		rec.noteModelSwap(reason, from, cfg.refusalFallback)
		if st != nil {
			log.Printf("WARN  %s%s  %s -> retry with %s  in=%s out=%s tok  %s%s%s",
				whoWithResolvedModel(reqWho, st.model), effortField(reqEffort), reason, cfg.refusalFallback,
				htok(st.inTok), htok(st.outTok), since(reqStart), att(retryCount), rec.idField())
		} else {
			log.Printf("WARN  %s%s  %s -> retry with %s  %s%s",
				reqWho, effortField(reqEffort), reason, cfg.refusalFallback, since(reqStart), rec.idField())
		}
		body = swapped
		reqWho = from + "~>" + who(r, body) // keep the requested model visible in every later line
		// The fallback is a fresh logical request: grant it a full hidden local-retry
		// budget rather than inheriting what the refusing model already spent. Both
		// the explicit (proxyRetries) and the legacy (legacyFastUsed) budgets reset so
		// a transient fallback fault is retried locally instead of surfacing a 503 that
		// resends the original refusing body. Bounded: the swap happens at most once.
		proxyRetries = 0
		legacyFastUsed = false
		return true
	}

	// A refusal fallback is best-effort: the refusing model's own answer (a
	// complete refusal stream or a safeguard error) is held while the fallback is
	// tried, and delivered if the fallback cannot answer.
	var heldRefusal *bufferedStream
	var heldSafeguard *failure
	defer func() {
		if heldRefusal != nil {
			heldRefusal.sp.discard()
		}
	}()
	firstWho := reqWho

	// With PROXY_TRANSACTIONAL_LOCAL_RETRIES=0, preserve the old behavior:
	// initial attempt + at most one cheap local retry for fast pre-header faults.
	// When enabled, use the explicit local retry budget for any transient
	// uncommitted transactional failure.
	for {
		// Refusal fallback: if a Claude model returns a COMPLETE stream whose
		// stop_reason is "refusal", re-issue once with the fallback model instead of
		// handing the refusal back to the client. Arm interception only when a real
		// substitution is possible — the body names a Claude model other than the
		// fallback. A concrete model guarantees the later swapModel succeeds, and it
		// bounds this to a single swap: the retried body targets the fallback model,
		// so it is never armed again (no recursion). Other vendors' models are never
		// silently re-routed to a different provider.
		reqModel := modelOf(body)
		interceptRefusal := isMessages && cfg.refusalFallback != "" && isClaudeModel(reqModel) && reqModel != cfg.refusalFallback

		rec.beginAttempt(body, reqWho, proxyRetries)
		resp, started, stopDeadline, rtErr := roundTripBefore(ctx, r, body, commitBy)
		if rtErr != nil {
			last = classifyTransport(rtErr, ctx)
			rec.noteAttemptFailure(last, "transport_error")
			if tryLocalRetry(last) {
				continue
			}
			if cfg.txLocalRetries == 0 && !legacyFastUsed && last.fastRetry && budgetLeft() && time.Since(started) < 3*time.Second && canWait(250*time.Millisecond) {
				wait := 250 * time.Millisecond
				rec.noteAttemptRetry("legacy_fast_retry", wait)
				logLocalRetry(reqWho, reqEffort, last, wait, 1, 1, retryCount, "legacy-fast")
				if !waitToRetry(wait) {
					break
				}
				legacyFastUsed = true
				continue
			}
			break
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			last = classifyUpstreamHTTPError(ctx, r.URL.Path, resp, rec)
			if interceptRefusal && isModelSafeguardRefusal(last) && swapToFallback("safeguard", nil) {
				safeguard := last
				heldSafeguard = &safeguard
				continue
			}
			if tryLocalRetry(last) {
				continue
			}
			if cfg.txLocalRetries == 0 && !legacyFastUsed && last.fastRetry && last.retryAfter == 0 && budgetLeft() && time.Since(started) < 3*time.Second && canWait(250*time.Millisecond) {
				wait := 250 * time.Millisecond
				rec.noteAttemptRetry("legacy_fast_retry", wait)
				logLocalRetry(reqWho, reqEffort, last, wait, 1, 1, retryCount, "legacy-fast")
				if !waitToRetry(wait) {
					break
				}
				legacyFastUsed = true
				continue
			}
			break
		}

		if !isEventStream(resp.Header.Get("Content-Type")) {
			// Not a stream any wire can validate, but a complete body is still the
			// upstream's answer: re-sending would only repeat it and re-bill the
			// generation. Buffer it under the same no-output deadline as an error
			// body (the deadline stays armed until the body closes) and byte-idle
			// bound, so a broken or stalled body is still a clean pre-commit retry.
			rec.noteAttemptResponseHeaders(resp.StatusCode, resp.Header)
			sp, readFail := bufferNonSSE(ctx, resp, rec)
			resp.Body.Close()
			if readFail != nil {
				last = *readFail
				rec.noteAttemptFailure(last, "non_sse_read_error")
				if tryLocalRetry(last) {
					continue
				}
				break
			}
			for k, v := range safeResponseHeaders(resp.Header) {
				w.Header()[k] = v
			}
			w.WriteHeader(resp.StatusCode)
			err := sp.replayRaw(w)
			sp.discard()
			if err != nil {
				// Headers are committed and the length is not declared, so a short
				// replay would end as a clean, truncated success: abort instead.
				rec.noteAttemptResult("drop")
				rec.note("DROP", resp.StatusCode, resp.StatusCode, "non_sse_replay_failed")
				log.Printf("DROP  %s%s  non_sse_replay_failed -> committed  %s%s", reqWho, effortField(reqEffort), since(reqStart), rec.idField())
				panic(http.ErrAbortHandler)
			}
			rec.noteAttemptResult("success")
			log.Printf("OK    %s%s  %d  %s  non-sse %s  %s%s%s%s",
				reqWho, effortField(reqEffort), resp.StatusCode, hbytes(sp.total), dash(resp.Header.Get("Content-Type")), since(reqStart), att(retryCount), proxyRetryField(proxyRetries), rec.idField())
			rec.noteProxyRetries(proxyRetries)
			rec.note("OK", resp.StatusCode, resp.StatusCode, "")
			return
		}

		// CAPTURE: buffer+validate (transactional), or commit+stream live past the
		// keepalive grace. captureSSE writes the downstream response itself.
		var st captureStats
		st.respTee = rec.respWriter() // nil when request-log disabled
		if rec != nil {
			rec.noteAttemptResponseHeaders(resp.StatusCode, resp.Header)
		}
		// The stream engine now owns the same absolute commit deadline; it can
		// switch to live output without the pre-stream timer killing the body.
		stopDeadline()
		wire := wireFor(r.URL.Path, body, interceptRefusal)
		wrote, fail := captureSSECore(ctx, cancel, w, resp.Header, resp.Body, &st, kaWindow, gated, earlyCommit, wire, commitBy)
		resp.Body.Close()
		rec.noteAttemptStats(&st)
		if fail == nil {
			log.Printf("OK    %s%s  in=%s out=%s tok  %s  %s  %s%s%s%s%s",
				whoWithResolvedModel(reqWho, st.model), effortField(reqEffort), htok(st.inTok), htok(st.outTok), dash(st.stop), st.mode, since(reqStart), att(retryCount), proxyRetryField(proxyRetries), prunField(st.maxPingRun), rec.idField())
			rec.noteAttemptResult("success")
			rec.noteProxyRetries(proxyRetries)
			rec.note("OK", http.StatusOK, http.StatusOK, "")
			return
		}
		rec.noteAttemptFailure(*fail, "sse_failure")
		if st.heldRefusal != nil {
			// A complete stream that ended in stop_reason="refusal". Nothing was
			// committed downstream, so swap in the fallback model and re-issue on the
			// next iteration. This is a substitution, not a transient retry: it does
			// not consume the local-retry or SDK-retry budget, and it happens at most
			// once per request (see above).
			refusal := &bufferedStream{sp: st.heldRefusal, header: resp.Header, wire: wire, st: &st}
			st.heldRefusal = nil
			if swapToFallback("refusal", &st) {
				heldRefusal = refusal
				continue
			}
			// The body could not be rewritten: the refusal is still the complete answer.
			heldRefusal = refusal
			deliverHeldRefusal(w, heldRefusal, firstWho, reqEffort, reqStart, retryCount, rec, "swap_failed")
			return
		}
		if !wrote && interceptRefusal && isModelSafeguardRefusal(*fail) && swapToFallback("safeguard", &st) {
			safeguard := *fail
			heldSafeguard = &safeguard
			continue
		}
		if wrote { // failed AFTER committing — can't convert, response already streaming
			if fail.delivered {
				// The upstream's own terminal error frame reached the client, so the
				// stream ended the way the protocol defines; nothing to abort.
				log.Printf("FAIL  %s%s  %s -> delivered in-stream  out=%s tok  %s%s%s%s",
					whoWithResolvedModel(reqWho, st.model), effortField(reqEffort), fail.code, htok(st.outTok), since(reqStart), att(retryCount), proxyRetryField(proxyRetries), rec.idField())
				rec.noteAttemptResult("fail")
				rec.noteProxyRetries(proxyRetries)
				rec.note("FAIL", http.StatusOK, http.StatusOK, fail.code)
				return
			}
			log.Printf("DROP  %s%s  %s -> committed, client recovery required  out=%s tok  %s%s%s%s%s",
				whoWithResolvedModel(reqWho, st.model), effortField(reqEffort), fail.code, htok(st.outTok), since(reqStart), att(retryCount), proxyRetryField(proxyRetries), prunField(st.maxPingRun), rec.idField())
			rec.noteAttemptResult("drop")
			rec.noteProxyRetries(proxyRetries)
			rec.note("DROP", http.StatusOK, http.StatusOK, fail.code)
			// Abort the HTTP response on every wire: a clean end would let an SDK
			// that finalizes from its snapshot (or Gemini, which has no terminal
			// event at all) accept the cut-off stream as a complete answer.
			panic(http.ErrAbortHandler)
		}
		last = *fail
		if tryLocalRetry(last) {
			continue
		}
		break
	}

	// FAILED before committing. Retry only what can succeed on a re-send (or until
	// the backstop); on retry, supply a Retry-After backoff so the client waits.
	// A held fallback answer is delivered only to a client still waiting for it.
	canRetry := last.transient && budgetLeft()
	if !canRetry && r.Context().Err() == nil && (heldRefusal != nil || heldSafeguard != nil) {
		log.Printf("WARN  %s%s  fallback %s failed: %s -> delivering the original answer  %s%s",
			reqWho, effortField(reqEffort), cfg.refusalFallback, last.code, since(reqStart), rec.idField())
		if heldRefusal != nil {
			deliverHeldRefusal(w, heldRefusal, firstWho, reqEffort, reqStart, retryCount, rec, last.code)
			return
		}
		last = *heldSafeguard
		reqWho = firstWho
	}
	retryAfter := 0
	if canRetry {
		retryAfter = last.retryAfter
		if retryAfter == 0 {
			retryAfter = retryAfterFor(retryCount)
		}
	}
	tag := "RETRY" // converted to x-should-retry=true; the SDK will re-send
	if !canRetry {
		tag = "FAIL" // surfaced to the client (the upstream's final answer, or retry backstop hit)
	}
	// surface() collapses every retryable failure to a generic 503+api_error so
	// Claude Code can't recognize it as overloaded/rate-limit/timeout and bypass
	// its x-should-retry loop. The log shows the true upstream status arrowed to
	// the surfaced one when masked (e.g. 529->503); last.code carries the cause.
	sStatus, sType := retrySurfaceFor(r.URL.Path, last, canRetry)
	log.Printf("%-5s %s%s  %s %s%s  %s%s%s%s",
		tag, reqWho, effortField(reqEffort), last.code, statusField(origStatusOf(last), sStatus), retryField(retryAfter), since(reqStart), att(retryCount), proxyRetryField(proxyRetries), rec.idField())
	rec.noteProxyRetries(proxyRetries)
	rec.note(tag, origStatusOf(last), sStatus, last.code)
	writeFailure(w, r.URL.Path, last, canRetry, sStatus, sType, retryAfter)
}

// bufferedStream is a complete, validated upstream stream held in its spool.
type bufferedStream struct {
	sp     *spool
	header http.Header
	wire   wireModel
	st     *captureStats
}

// deliverHeldRefusal replays the original model's complete refusal exactly as a
// buffered commit would have, after the fallback model could not answer.
func deliverHeldRefusal(w http.ResponseWriter, b *bufferedStream, reqWho, reqEffort string, reqStart time.Time, retryCount int, rec *reqRecorder, fallbackCode string) {
	writeStreamHead(w, b.header, "buffered", b.st)
	err := b.wire.replayBuffered(b.sp, w, b.st)
	if fl, ok := w.(http.Flusher); ok {
		fl.Flush()
	}
	if err != nil {
		log.Printf("DROP  %s%s  refusal_replay_failed -> committed  %s%s", reqWho, effortField(reqEffort), since(reqStart), rec.idField())
		rec.note("DROP", http.StatusOK, http.StatusOK, "refusal_replay_failed")
		panic(http.ErrAbortHandler)
	}
	log.Printf("OK    %s%s  in=%s out=%s tok  %s  %s  %s%s  fallback-failed=%s%s",
		whoWithResolvedModel(reqWho, b.st.model), effortField(reqEffort), htok(b.st.inTok), htok(b.st.outTok), dash(b.st.stop), b.st.mode, since(reqStart), att(retryCount), fallbackCode, rec.idField())
	rec.note("OK", http.StatusOK, http.StatusOK, "")
}

// writeFailure surfaces a pre-commit failure. A terminal upstream HTTP error is
// the upstream's own answer and is replayed verbatim (status adjusted only where
// the route's client would otherwise auto-retry it); everything else is
// synthesized in the route's native error envelope.
func writeFailure(w http.ResponseWriter, path string, f failure, canRetry bool, status int, atype string, retryAfter int) {
	if !f.transient && f.upstream != nil {
		h := w.Header()
		for k, v := range safeResponseHeaders(f.upstream.header) {
			h[k] = v
		}
		h.Set("X-Should-Retry", "false")
		h.Set("X-Steady-Proxy-Reason", f.code)
		if status != f.upstream.status {
			// The status was changed only to stop a status-based SDK retry; its
			// retry delay must not contradict that.
			h.Del("Retry-After")
		}
		w.WriteHeader(status)
		_, _ = w.Write(f.upstream.body)
		return
	}
	errorWriterFor(path)(w, canRetry, status, atype, msgFor(f), retryAfter, f.code)
}

// classifyUpstreamHTTPError reads and classifies one non-2xx upstream response,
// keeping the complete reply when it is the upstream's final answer.
func classifyUpstreamHTTPError(ctx context.Context, path string, resp *http.Response, rec *reqRecorder) failure {
	b, complete, readErr := readHTTPErrorBody(resp, rec)
	f := classifyHTTPErrorForPath(ctx, path, resp, b, readErr)
	f.origStatus = resp.StatusCode
	if !f.transient && readErr == nil && complete && resp.StatusCode >= 400 {
		f.upstream = &upstreamReply{status: resp.StatusCode, header: resp.Header.Clone(), body: b}
	}
	rec.noteAttemptFailure(f, "http_error")
	resp.Body.Close()
	return f
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

// httpErrorBodyBytes bounds an upstream error body kept for classification and
// verbatim replay. A longer body is classified from its prefix and, being
// incomplete, surfaced as a synthesized error instead.
const httpErrorBodyBytes = 1 << 20

type prefixCapture struct {
	buf bytes.Buffer
	max int64
}

func (p *prefixCapture) Write(b []byte) (int, error) {
	if room := p.max - int64(p.buf.Len()); room > 0 {
		if int64(len(b)) <= room {
			p.buf.Write(b)
		} else {
			p.buf.Write(b[:room])
		}
	}
	return len(b), nil
}

// readHTTPErrorBody returns up to httpErrorBodyBytes of the error body and
// whether that is the whole body.
func readHTTPErrorBody(resp *http.Response, rec *reqRecorder) ([]byte, bool, error) {
	body := withByteIdle(resp.Body, cfg.upstreamByteIdle)
	defer body.stop()
	if rec == nil {
		b, err := io.ReadAll(io.LimitReader(body, httpErrorBodyBytes+1))
		if len(b) > httpErrorBodyBytes {
			return b[:httpErrorBodyBytes], false, err
		}
		return b, true, err
	}
	rec.noteAttemptResponseHeaders(resp.StatusCode, resp.Header)
	prefix := &prefixCapture{max: httpErrorBodyBytes}
	dst := io.MultiWriter(prefix, rec.respWriter())
	n, err := io.Copy(dst, body)
	rec.noteResponseReadError(err)
	return prefix.buf.Bytes(), n <= httpErrorBodyBytes, err
}

// spoolSink adapts a spool to io.Writer, remembering its own write failure so a
// spool fault is not mistaken for an upstream read error.
type spoolSink struct {
	sp  *spool
	err error
}

func (s *spoolSink) Write(p []byte) (int, error) {
	if err := s.sp.write(p); err != nil {
		s.err = err
		return 0, err
	}
	return len(p), nil
}

// bufferNonSSE reads a whole non-SSE 2xx body into a spool under the same size
// and byte-idle bounds as a captured stream, teeing it into the archive.
func bufferNonSSE(ctx context.Context, resp *http.Response, rec *reqRecorder) (*spool, *failure) {
	sink := &spoolSink{sp: newSpool()}
	body := withByteIdle(resp.Body, cfg.upstreamByteIdle)
	defer body.stop()
	var dst io.Writer = sink
	if archive := rec.respWriter(); archive != nil {
		dst = io.MultiWriter(sink, archive)
	}
	_, err := io.Copy(dst, body)
	rec.noteResponseReadError(err)
	if err == nil {
		return sink.sp, nil
	}
	sink.sp.discard()
	if sink.err != nil {
		return nil, classifyBufferError(sink.err)
	}
	if body.timedOut.Load() && ctx.Err() == nil {
		return nil, &failure{transient: true, status: 504, atype: "timeout_error", code: "upstream_idle", message: "upstream stalled with no bytes"}
	}
	f := classifyTransport(err, ctx)
	return nil, &f
}

// proxyOnce is a plain single-shot reverse proxy for non-transactional routes.
func proxyOnce(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte, rec *reqRecorder, start time.Time) {
	reqWho := who(r, body)
	reqEffort := reasoningEffortOf(r.URL.Path, body)
	rec.beginAttempt(body, reqWho, 0)
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
		rec.noteAttemptFailure(f, "transport_error")
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		f = classifyUpstreamHTTPError(ctx, r.URL.Path, resp, rec)
	default:
		failed = false
	}
	if failed {
		// Same rule as the transactional path: retry transient faults within the
		// backstop, with a Retry-After backoff so the client waits.
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
		sStatus, sType := retrySurfaceFor(r.URL.Path, f, canRetry)
		log.Printf("%-5s %s%s  %s %s%s  %s%s%s", tag, reqWho, effortField(reqEffort), f.code, statusField(origStatusOf(f), sStatus), retryField(retryAfter), since(start), att(retryCount), rec.idField())
		rec.note(tag, origStatusOf(f), sStatus, f.code)
		writeFailure(w, r.URL.Path, f, canRetry, sStatus, sType, retryAfter)
		return
	}
	defer resp.Body.Close()
	rec.noteAttemptResponseHeaders(resp.StatusCode, resp.Header)
	for k, v := range safeResponseHeaders(resp.Header) {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	// Tee the body into the recorder when request-log is on.
	var dst io.Writer = w
	if sink := rec.respWriter(); sink != nil {
		dst = io.MultiWriter(w, sink)
	}
	upstreamBody := withByteIdle(resp.Body, cfg.upstreamByteIdle)
	defer upstreamBody.stop()
	n, copyErr := io.Copy(dst, upstreamBody)
	if copyErr != nil {
		f := failure{transient: true, status: 502, atype: "api_error", code: "response_read_error", message: copyErr.Error()}
		if ctx.Err() != nil {
			f = classifyTransport(copyErr, ctx)
		} else if upstreamBody.timedOut.Load() {
			f = failure{transient: true, status: 504, atype: "timeout_error", code: "upstream_idle", message: "upstream stalled with no bytes"}
		} else if errors.Is(copyErr, os.ErrDeadlineExceeded) {
			f = classifyTransport(context.DeadlineExceeded, ctx)
		}
		rec.noteResponseReadError(copyErr)
		rec.noteAttemptFailure(f, "drop")
		rec.note("DROP", resp.StatusCode, resp.StatusCode, f.code)
		log.Printf("DROP  %s%s  %s  %d  %s  %s%s", reqWho, effortField(reqEffort), f.code, resp.StatusCode, hbytes(n), since(start), rec.idField())
		// Headers may already be committed. Abort the HTTP response so a short
		// upstream body cannot become a clean, shorter downstream success.
		panic(http.ErrAbortHandler)
	}
	rec.noteAttemptResult("success")
	log.Printf("OK    %s%s  %d  %s  %s%s", reqWho, effortField(reqEffort), resp.StatusCode, hbytes(n), since(start), rec.idField())
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
	// Reserved for gateway->proxy hints on responses (x-gateway-retryable is
	// read in classifyHTTPErrorBytesShaped). Never forwarded from a client, so a
	// gateway that echoes request headers cannot be tricked into a verdict.
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

// safeResponseHeaders keeps the upstream's end-to-end headers, including
// Content-Encoding: raw relays forward the body bytes unchanged.
func safeResponseHeaders(src http.Header) http.Header {
	drop := connectionTokens(src)
	out := http.Header{}
	for k, vv := range src {
		lk := strings.ToLower(k)
		if hopByHop[lk] || drop[lk] {
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

func isEventStream(ct string) bool { return strings.Contains(strings.ToLower(ct), "text/event-stream") }

// ---------------------------------------------------- friendly access log ---
// One line per request, optimized for a live `docker logs -f` viewer: a 5-char
// outcome tag, then who it's for (model/agent), then only the fields that vary.
// The method/path/gateway are static for the transactional route, so omitted.

func who(r *http.Request, body []byte) string {
	return requestModelOf(r.URL.Path, body) + "/" + agentKindFull(r, body)
}

func requestModelOf(path string, body []byte) string {
	if model, _ := geminiTarget(path); model != "" {
		return model
	}
	return modelOf(body)
}

// agentKindFull refines agentKind with a body-derived signal: a Workflow-tool
// agent is labeled "wf" (it is neither the interactive "main" session nor a
// generic "sub"agent, and only it carries the Workflow per-agent stall). Falls
// back to the header-only agentKind for everything else.
func agentKindFull(r *http.Request, body []byte) string {
	if isMessagesPath(r.URL.Path) && isWorkflowAgent(body) {
		return "wf"
	}
	return agentKind(r)
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
	if _, fallback, swapped := strings.Cut(reqModel, "~>"); swapped && fallback == resolvedModel {
		return reqWho
	}
	if reqModel == "" || reqModel == "?" {
		return resolvedModel + agent
	}
	return reqModel + "->" + resolvedModel + agent
}

func effortField(effort string) string {
	if effort == "" {
		return ""
	}
	return "  " + effort
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

func logLocalRetry(reqWho, effort string, f failure, wait time.Duration, retryNum, retryMax, sdkAttempt int, mode string) {
	vlog("[local-retry] %s%s %s %s wait=%s retry=%d/%d%s%s",
		reqWho, effortField(effort), f.code, itoa(origStatusOf(f)), wait.Round(time.Millisecond),
		retryNum, retryMax, att(sdkAttempt), localRetryModeField(mode))
}

func logLocalRetrySkip(reqWho, effort string, f failure, wait time.Duration, sdkAttempt int, reason string) {
	vlog("[local-retry] %s%s %s %s wait=%s skip=%s%s",
		reqWho, effortField(effort), f.code, itoa(origStatusOf(f)), wait.Round(time.Millisecond), reason, att(sdkAttempt))
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

// prunField surfaces the longest consecutive upstream ping-run — the tripwire for
// a content-silent gap (Case B). Shown only when >0: a healthy dense stream never
// pings in a run, so any value is worth noticing, and a large one is what a
// Workflow stall watchdog would trip on. See captureStats.maxPingRun.
func prunField(n int) string {
	if n <= 0 {
		return ""
	}
	return fmt.Sprintf("  prun=%d", n)
}

func localRetryDelay(upstreamRetryAfter int, retryIndex int) time.Duration {
	if upstreamRetryAfter < 0 {
		upstreamRetryAfter = 0
	}
	extra := localRetryExtraBackoff(retryIndex)
	maxDelay := time.Duration(math.MaxInt64)
	if int64(upstreamRetryAfter) > int64((maxDelay-extra)/time.Second) {
		return maxDelay
	}
	return time.Duration(upstreamRetryAfter)*time.Second + extra
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
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= time.Second || wait >= remaining-time.Second {
			return false
		}
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

func reasoningEffortOf(path string, body []byte) string {
	if isGeminiPath(path) {
		return geminiSettingsOf(body).thinkingLevel
	}
	var b struct {
		Reasoning struct {
			Effort string `json:"effort"`
		} `json:"reasoning"`
		OutputConfig struct {
			Effort string `json:"effort"`
		} `json:"output_config"`
	}
	_ = json.Unmarshal(body, &b)
	switch {
	case isResponsesPath(path):
		return strings.TrimSpace(b.Reasoning.Effort)
	case isMessagesPath(path):
		return strings.TrimSpace(b.OutputConfig.Effort)
	default:
		return ""
	}
}

// refusalFallbackCode is the sentinel failure code captureSSE returns when a
// complete stream ended in stop_reason "refusal" AND refusal interception was
// armed for the request. It never reaches the client: the engine hands the
// buffered refusal to the handler, which rewrites the request to the fallback
// model and re-issues (see handle()).
const refusalFallbackCode = "refusal_fallback"

// isClaudeModel reports whether a request names a Claude model, the only family
// whose refusals are re-issued: substituting a different vendor's model would
// silently re-route the prompt to another provider.
func isClaudeModel(model string) bool {
	return strings.Contains(strings.ToLower(model), "claude")
}

// isModelSafeguardRefusal catches Fable's pre-stream guardrail shape. Claude Code
// records these as synthetic API errors, not as the normal SSE
// message_delta.stop_reason="refusal" path, so the stream-only interception above
// never sees them. Only a terminal failure qualifies: a transient error that
// merely mentions the phrase is retried like any other.
func isModelSafeguardRefusal(f failure) bool {
	if f.transient {
		return false
	}
	msg := strings.ToLower(f.message)
	return strings.Contains(msg, "safeguards flagged this message") ||
		strings.Contains(msg, "claude code can't respond to this request with fable")
}

// loadRefusalFallback resolves the model to re-issue with when a response
// completes with stop_reason "refusal". Unset -> default claude-opus-5. Set to
// empty / "off" / "none" / "disabled" -> the feature is off and a refusal is
// delivered to the client unchanged (the historical behavior).
func loadRefusalFallback(lookup func(string) (string, bool)) string {
	v, ok := lookup("PROXY_REFUSAL_FALLBACK_MODEL")
	if !ok {
		return "claude-opus-5"
	}
	v = strings.TrimSpace(v)
	if v == "" || strings.EqualFold(v, "off") || strings.EqualFold(v, "none") || strings.EqualFold(v, "disabled") {
		return ""
	}
	return v
}

// swapModel returns a copy of the JSON request body with only its top-level
// "model" field replaced by model. Every other top-level field is preserved as a
// raw message (numbers are never reformatted, unlike a decode into
// map[string]any), so max_tokens, tools, thinking budgets, etc. keep their JSON
// values. The output is compacted and its keys sorted, both JSON-insignificant;
// HTML characters are not escaped. ok is false when the body is not a JSON object
// or has no "model" field, in which case the caller must not substitute.
func swapModel(body []byte, model string) (out []byte, ok bool) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, false
	}
	if _, has := m["model"]; !has {
		return nil, false
	}
	mv, err := json.Marshal(model)
	if err != nil {
		return nil, false
	}
	m["model"] = mv
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		return nil, false
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), true
}

// refusalFallbackDesc renders the configured fallback model for the startup log,
// showing "off" when the feature is disabled.
func refusalFallbackDesc(model string) string {
	if model == "" {
		return "off"
	}
	return model
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

// workflowAgentMarker is the fixed prologue the Claude Code Workflow runtime
// injects into every workflow agent()'s system prompt: "You are a subagent
// spawned by a workflow orchestration script. Use the tools available to
// complete the task." ONLY these agents carry the Workflow tool's per-agent
// stall watchdog (observed default 180000ms, retried a few times, with no
// global env override) that aborts a turn which streams no assistant/user
// message for the window. Regular Task/SDK subagents (system prompt "You are a
// Claude agent, built on … the Agent SDK") and the interactive main session do
// NOT carry it, so they must keep the full transactional window.
var workflowAgentMarker = []byte("workflow orchestration script")

// isWorkflowAgent reports whether a /v1/messages request is a Workflow-tool
// agent. It matches the marker in the request's `system` field ONLY: scoping to
// system (not the whole body) avoids a false positive on an ordinary
// conversation whose messages merely mention workflows — e.g. a main session
// discussing the Workflow tool, which is not itself a workflow agent.
func isWorkflowAgent(body []byte) bool {
	var b struct {
		System json.RawMessage `json:"system"`
	}
	if json.Unmarshal(body, &b) != nil {
		return false
	}
	return bytes.Contains(b.System, workflowAgentMarker)
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

// envReader reads configuration variables and records every malformed value
// (not an integer, or below the knob's minimum) so parseConfig can report them
// all at once. An unset or empty variable yields the default.
type envReader struct {
	lookup func(string) (string, bool)
	errs   []string
}

func (e *envReader) str(k, d string) string {
	if v, ok := e.lookup(k); ok && v != "" {
		return v
	}
	return d
}

func (e *envReader) int64(k string, d, minAllowed int64) int64 {
	v, ok := e.lookup(k)
	if !ok || v == "" {
		return d
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Sprintf("%s=%q: not an integer", k, v))
		return d
	}
	if n < minAllowed {
		e.errs = append(e.errs, fmt.Sprintf("%s=%d: must be >= %d", k, n, minAllowed))
		return d
	}
	return n
}

func (e *envReader) ms(k string, d, minAllowed int64) time.Duration {
	return time.Duration(e.int64(k, d, minAllowed)) * time.Millisecond
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
