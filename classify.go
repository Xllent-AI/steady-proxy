package main

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var errTooLarge = errors.New("response too large to buffer")

func classifyBufferError(err error) *failure {
	if errors.Is(err, errTooLarge) {
		return &failure{status: http.StatusBadGateway, atype: "api_error", code: "response_too_large", message: "response exceeded proxy buffer cap"}
	}
	return &failure{transient: true, status: http.StatusBadGateway, atype: "api_error", code: "spool_error", message: "buffering upstream response: " + err.Error()}
}

// failure describes a non-success outcome and how to surface it to the SDK.
type failure struct {
	transient  bool   // true => eligible to convert into an SDK retry
	fastRetry  bool   // true => one cheap proxy-local re-issue is OK
	delivered  bool   // true => the failure's own terminal frame already reached the client in-stream
	status     int    // HTTP status to surface downstream
	origStatus int    // raw upstream HTTP status; 0 when no HTTP response (transport/SSE fault)
	atype      string // Anthropic error type
	code       string // short internal code (for logs / episode keying)
	message    string
	retryAfter int            // seconds; 0 = none
	upstream   *upstreamReply // complete upstream HTTP error; a terminal failure is replayed from it verbatim
}

// upstreamReply is a fully read upstream HTTP error response. Terminal errors are
// the upstream's own answer, so the client receives these bytes rather than a
// proxy-synthesized envelope.
type upstreamReply struct {
	status int
	header http.Header
	body   []byte
}

func statusFor(f failure) int {
	if f.status == 0 {
		return http.StatusBadGateway
	}
	return f.status
}
func msgFor(f failure) string {
	if f.message == "" {
		return programName + ": upstream failure"
	}
	return programName + ": " + f.message
}

// statusField renders the upstream status for the logs, annotating the
// client-facing surface status only when it differs. Every transient cause is
// masked to a generic 503 (see surface), so "529->503" shows BOTH the true
// upstream status and what Claude Code was actually told; an unmasked outcome
// (e.g. a request-shape 400, or a 503 that was already 503) shows one number.
func statusField(orig, surfaced int) string {
	if surfaced == 0 || orig == surfaced {
		return itoa(orig)
	}
	return itoa(orig) + "->" + itoa(surfaced)
}

// origStatusOf is the true upstream status for display: the raw HTTP status when
// the upstream actually responded, else the classified status. A transient 4xx is
// normalized to 502 in failure.status (so the SDK honors the retry), so reading
// origStatus here is what keeps e.g. a retryable 408 logged as 408, not 502.
func origStatusOf(f failure) int {
	if f.origStatus != 0 {
		return f.origStatus
	}
	return statusFor(f)
}

// Substrings that mark an error as REQUEST-SHAPE: the request itself can never
// succeed as written. HTTP 4xx statuses are already terminal on their own (see
// classifyHTTPErrorBytesShaped); these phrases matter where no such status
// exists or the status alone says "retry": in-band SSE errors of an unknown
// type, and an HTTP 429 whose body names a request-shape problem. Derived from
// real Claude Code transcripts; see docs/TROUBLESHOOTING.md.
//
// Each entry must be specific enough that it cannot match a transient message
// (e.g. a bare "too long" would match "took too long"). Prefer anchored phrases
// over generic fragments.
var requestShapeSigs = []string{
	// request too large / context
	"context_length", "context length", "maximum context", "prompt is too long",
	// malformed conversation / tool blocks
	"tool_use", "tool_result", "tool use concurrency",
	"missing tool result", "duplicate tool_use", "unexpected tool_use_id",
	"thinking blocks", "thinking.budget_tokens", "max_tokens must be",
	// schema / header mismatches (often gateway translation bugs — deterministic).
	// Note: no bare "must be"/"unsupported"/"is required" — those also appear in
	// transient messages ("token must be provided", "unsupported region right now").
	"extra inputs are not permitted", "unexpected value",
	"anthropic-beta", "context_management", "input_examples", "image dimensions",
	"model_not_found",
}

func anyContains(s string, subs []string) bool {
	for _, x := range subs {
		if strings.Contains(s, x) {
			return true
		}
	}
	return false
}

// classifyTransport handles failed upstream I/O and request cancellation.
func classifyTransport(err error, ctx context.Context) failure {
	switch {
	case ctx.Err() == context.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded):
		return failure{transient: true, status: 504, atype: "timeout_error", code: "deadline",
			message: "attempt deadline exceeded"}
	case ctx.Err() == context.Canceled || errors.Is(err, context.Canceled):
		return failure{transient: false, status: 499, atype: "api_error", code: "client_gone",
			message: "client cancelled"}
	default:
		return failure{transient: true, fastRetry: true, status: 502, atype: "api_error",
			code: "transport_error", message: "upstream connection failed"}
	}
}

// errorVerdict is a wire's reading of an upstream error body. HTTP status owns
// the retry decision; the body can only refine it where the wire names an
// explicit identity (see classifyHTTPErrorBytesShaped).
type errorVerdict int

const (
	verdictUnknown   errorVerdict = iota // the body names no retry-relevant identity
	verdictTransient                     // the body names a transient fault (rate limit, overload, timeout)
	verdictPermanent                     // the body names a fault an identical retry cannot fix
)

// errorBodyRule reads parsed diagnostic fields and a bounded lowercased fallback
// only when the response body is not structured JSON.
type errorBodyRule func(atype, code, msg, bodyLower string) errorVerdict

// anthropicErrorVerdict: a known request-shape phrase is permanent; the
// documented transient Anthropic error types are transient. api_error is too
// generic to override a 4xx status, so it is not listed here.
func anthropicErrorVerdict(atype, code, msg, bodyLower string) errorVerdict {
	if anyContains(strings.ToLower(atype+" "+code+" "+msg)+bodyLower, requestShapeSigs) {
		return verdictPermanent
	}
	switch atype {
	case "rate_limit_error", "overloaded_error", "timeout_error":
		return verdictTransient
	}
	return verdictUnknown
}

// classifyHTTPErrorBytes is the body/header inspection for Anthropic-shaped and
// other non-Responses, non-Gemini routes.
func classifyHTTPErrorBytes(resp *http.Response, b []byte) failure {
	return classifyHTTPErrorBytesShaped(resp, b, anthropicErrorVerdict)
}

// httpRetryHint returns the first recognized authoritative retry verdict.
func httpRetryHint(h http.Header) (source string, retry bool) {
	for _, name := range [...]string{"x-gateway-retryable", "x-should-retry"} {
		switch strings.ToLower(h.Get(name)) {
		case "true":
			return name, true
		case "false":
			return name, false
		}
	}
	return "", false
}

// classifyHTTPErrorBytesShaped classifies a non-2xx upstream response with the
// retry rule the Stainless SDKs (Anthropic, OpenAI) apply themselves: retry 408,
// 409, 429 and 5xx; every other status is the upstream's final answer, so
// re-sending the identical request only repeats it. Authoritative headers beat
// every heuristic, and a wire's body verdict refines the status where it names an
// explicit identity: a 4xx naming a transient fault is retried, and a 429 naming
// a permanent one (exhausted quota, request too large) is not.
func classifyHTTPErrorBytesShaped(resp *http.Response, b []byte, rule errorBodyRule) failure {
	st := resp.StatusCode
	if st >= 300 && st < 400 {
		// Redirects are deliberately not followed, so credentials never reach a
		// host the operator did not configure. Relaying the Location would make
		// the client follow it instead; report it as a configuration error.
		return failure{status: http.StatusBadGateway, atype: "api_error", code: "upstream_redirect_" + itoa(st),
			message: "upstream redirected to " + redirectTarget(resp.Header.Get("Location")) + "; fix PROXY_UPSTREAM_URL (redirects are not followed)"}
	}
	var body string
	if !json.Valid(b) {
		// Plain-text and HTML gateways still need the legacy diagnostic fallback.
		// The caller already bounds b; JSON metadata must not override error fields.
		body = strings.ToLower(string(b))
	}
	ra := retryAfterSeconds(resp.Header.Get("Retry-After"))

	var ae struct {
		Error struct{ Type, Code, Message string } `json:"error"`
	}
	_ = json.Unmarshal(b, &ae)
	atype := ae.Error.Type
	if atype == "" {
		atype = "api_error"
	}

	// Trust explicit signals first. x-gateway-retryable is an optional hint a
	// gateway may set on error responses (true = retry, false = surface); it
	// beats every heuristic below. x-should-retry is the Anthropic API's own near-
	// equivalent. Both are absent from a plain provider, which falls through
	// to status/body classification.
	switch source, retry := httpRetryHint(resp.Header); source {
	case "x-gateway-retryable":
		if retry {
			return failure{transient: true, status: mapTransientStatus(st), atype: atype, code: "gateway_retryable_" + itoa(st), message: ae.Error.Message, retryAfter: ra}
		}
		return failure{transient: false, status: st, atype: atype, code: "gateway_permanent", message: ae.Error.Message, retryAfter: ra}
	case "x-should-retry":
		if retry {
			return failure{transient: true, status: mapTransientStatus(st), atype: atype, code: "upstream_retry_" + itoa(st), message: ae.Error.Message, retryAfter: ra}
		}
		return failure{transient: false, status: st, atype: atype, code: "upstream_no_retry", message: ae.Error.Message}
	}

	verdict := rule(ae.Error.Type, ae.Error.Code, ae.Error.Message, body)
	switch {
	case st == 408 || st == 409:
		return failure{transient: true, fastRetry: true, status: st, atype: atype, code: "http_" + itoa(st), message: ae.Error.Message, retryAfter: ra}
	case st == 429 && verdict == verdictPermanent:
		return failure{status: st, atype: atype, code: "http_429", message: ae.Error.Message}
	case st == 429:
		return failure{transient: true, status: 429, atype: "rate_limit_error", code: "rate_limit", message: ae.Error.Message, retryAfter: ra}
	case st >= 500:
		return failure{transient: true, fastRetry: st == 502 || st == 503, status: st, atype: typeFor5xx(st), code: "http_" + itoa(st), message: ae.Error.Message, retryAfter: ra}
	case verdict == verdictTransient:
		// A 4xx whose body explicitly names a transient fault. Normalize to 502
		// so the SDK honors the retry (it refuses to retry most 4xx by status).
		return failure{transient: true, status: 502, atype: "api_error", code: "retryable_4xx_" + itoa(st), message: ae.Error.Message, retryAfter: ra}
	default:
		// Auth, billing, permission, not-found, validation, unknown 4xx: the
		// upstream's final answer. Surface it instead of repeating the request.
		code := "http_" + itoa(st)
		if verdict == verdictPermanent {
			code = "request_shape"
		}
		return failure{status: st, atype: atype, code: code, message: ae.Error.Message}
	}
}

// redirectTarget renders a Location for diagnostics without its query, which may
// carry credentials.
func redirectTarget(location string) string {
	u, err := url.Parse(location)
	if err != nil || location == "" {
		return "an unparseable location"
	}
	u.RawQuery, u.Fragment, u.User = "", "", nil
	return u.String()
}

// anthropicPermanentStatus maps the documented Anthropic error types that an
// identical retry cannot fix to their HTTP status.
func anthropicPermanentStatus(atype string) int {
	switch atype {
	case "invalid_request_error":
		return http.StatusBadRequest
	case "authentication_error":
		return http.StatusUnauthorized
	case "billing_error":
		return http.StatusPaymentRequired
	case "permission_error":
		return http.StatusForbidden
	case "not_found_error":
		return http.StatusNotFound
	case "request_too_large":
		return http.StatusRequestEntityTooLarge
	}
	return 0
}

// classifySSEError handles an `event: error` that arrives mid-stream (after 200).
// The type is the only status-like signal, so the same identities that are final
// over HTTP (auth, billing, permission, not found, too large, invalid request)
// are final here.
func classifySSEError(data string) *failure {
	var e struct {
		Error struct{ Type, Message string } `json:"error"`
	}
	_ = json.Unmarshal([]byte(data), &e)
	switch t := e.Error.Type; t {
	case "overloaded_error":
		return &failure{transient: true, status: 529, atype: "overloaded_error", code: "sse_overloaded", message: e.Error.Message}
	case "rate_limit_error":
		return &failure{transient: true, status: 429, atype: "rate_limit_error", code: "sse_rate_limit", message: e.Error.Message}
	case "api_error", "timeout_error", "":
		return &failure{transient: true, status: 502, atype: "api_error", code: "sse_api_error", message: e.Error.Message}
	case "invalid_request_error":
		// Only this verified transport failure overrides the request-error label.
		if e.Error.Message == "stream error: stream disconnected before completion: stream closed before response.completed" {
			return &failure{transient: true, status: 502, atype: "api_error", code: "sse_stream_disconnect", message: e.Error.Message}
		}
		return &failure{transient: false, status: 400, atype: t, code: "sse_request_shape", message: e.Error.Message}
	default:
		if st := anthropicPermanentStatus(t); st != 0 {
			return &failure{status: st, atype: t, code: "sse_" + t, message: e.Error.Message}
		}
		if anyContains(strings.ToLower(t+" "+e.Error.Message), requestShapeSigs) {
			return &failure{status: 400, atype: t, code: "sse_request_shape", message: e.Error.Message}
		}
		// An unknown type carries no evidence either way; ride it out.
		return &failure{transient: true, status: 502, atype: "api_error", code: "sse_retryable", message: e.Error.Message}
	}
}

// retryAfterFor computes a backoff (in seconds) for the Nth client retry when
// the upstream did not supply its own Retry-After. Exponential from 1s, doubling
// each attempt, capped at 30s, with light jitter so a wave of subagents doesn't
// re-send in lockstep. Always >= 1 so the client actually waits before retrying.
func retryAfterFor(retryCount int) int {
	if retryCount < 0 {
		retryCount = 0
	}
	secs := 1
	for i := 0; i < retryCount && secs < 30; i++ {
		secs *= 2
	}
	if secs > 30 {
		secs = 30
	}
	return secs + rand.Intn(2) // +0..1s jitter
}

func retryAfterSeconds(v string) int {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if n := atoiSafe(v); n > 0 {
		return n
	}
	t, err := http.ParseTime(v)
	if err != nil {
		return 0
	}
	d := time.Until(t)
	if d <= 0 {
		return 0
	}
	return int((d + time.Second - 1) / time.Second)
}

func mapTransientStatus(st int) int {
	if st >= 400 && st < 500 && st != 429 {
		return 502 // don't surface a 4xx with x-should-retry:true; normalize to 502
	}
	return st
}
func typeFor5xx(st int) string {
	switch st {
	case 529:
		return "overloaded_error"
	case 504:
		return "timeout_error"
	default:
		return "api_error"
	}
}

// surface decides what status + error type the CLIENT sees — distinct from what
// we classify and log. EVERY retryable failure is collapsed to one generic shape
// (503 + api_error) so Claude Code cannot recognize a specific error identity and
// route it into a status-specific handler that bypasses the x-should-retry SDK
// loop and gives up after a few tries. The canonical case is overloaded_error/529:
// Claude Code counts repeated overloaded responses on its own small fixed budget
// (~3) and surfaces "Repeated 529 Overloaded errors" — ignoring x-should-retry and
// never incrementing X-Stainless-Retry-Count. rate_limit_error/429 and
// timeout_error/504 are the same hazard. A plain 503 + api_error + x-should-retry:true
// + Retry-After looks like an ordinary retryable server error, so it goes through
// Claude Code's normal SDK retry loop, whose ceiling is the client's own maxRetries
// (raise it with API_MAX_RETRIES). The diagnostic cause is never lost — it stays in
// failure.code (e.g. "sse_overloaded", "rate_limit") for the access log.
//
// Non-transient failures keep their real status + type (and an upstream HTTP
// error is replayed verbatim), so they surface accurately with x-should-retry:false.
const retryableSurfaceStatus = http.StatusServiceUnavailable // 503

func surface(f failure) (status int, atype string) {
	if f.transient {
		return retryableSurfaceStatus, "api_error"
	}
	return statusFor(f), f.atype
}

// surfaceFor is surface() with the one wire-specific adjustment Codex needs. Codex
// ignores x-should-retry and decides retryability from the STATUS alone: it treats
// only 400 (InvalidRequest) and 429 (RetryLimit / usage limit) as terminal, and
// RETRIES every other surfaced non-2xx (5xx -> InternalServerError, anything else
// -> UnexpectedStatus, both retryable in codex-rs error.rs). A transient failure
// still masks to 503, which Codex retries — the intended outcome. A NON-transient
// failure keeps 400 or 429; any other status (401/403/404/413/422, a gateway
// "permanent" 5xx, the proxy's own response_too_large 502) would make Codex loop
// it to its stream-retry cap, so it is forced to 400. This is the single choke
// point for every non-retryable Responses outcome, wherever it was classified.
func surfaceFor(path string, f failure) (status int, atype string) {
	status, atype = surface(f)
	if !f.transient && isResponsesPath(path) && status != http.StatusBadRequest && status != http.StatusTooManyRequests {
		return http.StatusBadRequest, atype
	}
	return status, atype
}

// Clients that ignore X-Should-Retry need a terminal status when retry conversion
// is disabled or exhausted. Keep the original failure intact for diagnostics.
func retrySurfaceFor(path string, f failure, canRetry bool) (int, string) {
	if (isResponsesPath(path) || isGeminiPath(path)) && !canRetry {
		wasTransient := f.transient
		f.transient = false
		status := statusFor(f)
		if isGeminiPath(path) && (wasTransient || status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500) {
			f.status = http.StatusBadRequest
		}
	}
	return surfaceFor(path, f)
}

// writeAnthropicError emits a clean Anthropic-shaped error with the retry signal.
func writeAnthropicError(w http.ResponseWriter, canRetry bool, status int, atype, message string, retryAfter int, code string) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	if canRetry {
		h.Set("X-Should-Retry", "true")
	} else {
		h.Set("X-Should-Retry", "false") // explicit false overrides the SDK's default 5xx retry
	}
	if retryAfter > 0 {
		h.Set("Retry-After", itoa(retryAfter))
	}
	h.Set("X-Steady-Proxy-Reason", code)
	if status <= 0 {
		status = http.StatusBadGateway
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]string{"type": atype, "message": message},
	})
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
