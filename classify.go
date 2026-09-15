package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
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
	status     int    // HTTP status to surface downstream
	origStatus int    // raw upstream HTTP status; 0 when no HTTP response (transport/SSE fault)
	atype      string // Anthropic error type
	code       string // short internal code (for logs / episode keying)
	message    string
	retryAfter int // seconds; 0 = none
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
// origStatus here is what keeps e.g. a retryable 401 logged as 401, not 502.
func origStatusOf(f failure) int {
	if f.origStatus != 0 {
		return f.origStatus
	}
	return statusFor(f)
}

// Substrings that mark an error as REQUEST-SHAPE: the request itself can never
// succeed as written, so retrying it would loop forever. These are the ONLY
// errors we surface immediately — everything else is retried (see the design
// principle in main.go). Derived from real Claude Code transcripts; see
// docs/TROUBLESHOOTING.md.
//
// Deliberately excluded (so they ARE retried): auth, billing, policy, rate
// limits, capacity, and unknown errors — a temporary block or outage should be
// ridden out, not surfaced. The upstream gateway owns retry intelligence; this
// proxy only keeps the client alive.
//
// Each entry must be specific enough that it cannot match a transient/auth/policy
// message — there is no transient fallback anymore, so a false positive here
// surfaces something that should have been retried. (E.g. a bare "is required"
// would match "authentication is required"; "too long" would match "took too
// long".) Prefer anchored phrases over generic fragments.
var requestShapeSigs = []string{
	// request too large / context
	"context_length", "context length", "maximum context", "prompt is too long",
	// malformed conversation / tool blocks
	"tool_use", "tool_result", "tool use concurrency",
	"missing tool result", "duplicate tool_use", "unexpected tool_use_id",
	"thinking blocks", "thinking.budget_tokens", "max_tokens must be",
	// schema / header mismatches (often gateway translation bugs — deterministic).
	// Note: no bare "must be"/"unsupported"/"is required" — those match transient
	// or auth messages ("token must be provided", "unsupported region") that the
	// policy wants to retry. model_not_found stays a code match only: a plain
	// "model not found" on a routing gateway often means "no provider has it now"
	// (transient), so we don't surface that.
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

// classifyTransport handles a failure that occurred before/around the response
// headers (dial/TLS/reset/timeout).
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

// classifyHTTPError inspects a non-2xx upstream response, tagging it with the raw
// upstream status so logs keep the true status even when classifyHTTPErrorBytes
// normalizes failure.status (e.g. a retryable 4xx surfaced as 502).
func classifyHTTPError(resp *http.Response) failure {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	f := classifyHTTPErrorBytes(resp, b)
	f.origStatus = resp.StatusCode
	return f
}

// requestShaped decides whether a non-2xx body names a deterministic
// request-shape error (surface, don't retry). It differs per wire: Anthropic
// matches message substrings; OpenAI Responses keys on the error type/code (see
// responsesRequestShaped). It receives the parsed error type/code/message plus
// the whole lowercased body.
type requestShaped func(atype, code, msg, bodyLower string) bool

// anthropicRequestShaped is the historical Anthropic rule: match a known
// request-shape phrase anywhere in the body.
func anthropicRequestShaped(_, _, _, bodyLower string) bool {
	return anyContains(bodyLower, requestShapeSigs)
}

// classifyHTTPErrorBytes is the body/header inspection. The caller supplies the
// already-read body bytes when it also needs to archive the exact payload.
func classifyHTTPErrorBytes(resp *http.Response, b []byte) failure {
	return classifyHTTPErrorBytesShaped(resp, b, anthropicRequestShaped)
}

// classifyHTTPErrorBytesShaped is classifyHTTPErrorBytes with a pluggable
// request-shape decision so each wire keeps its own "can never succeed" rule
// while sharing the status/header handling.
func classifyHTTPErrorBytesShaped(resp *http.Response, b []byte, reqShaped requestShaped) failure {
	body := strings.ToLower(string(b))
	st := resp.StatusCode
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
	switch strings.ToLower(resp.Header.Get("x-gateway-retryable")) {
	case "true":
		return failure{transient: true, status: mapTransientStatus(st), atype: atype, code: "gateway_retryable_" + itoa(st), message: ae.Error.Message, retryAfter: ra}
	case "false":
		return failure{transient: false, status: st, atype: atype, code: "gateway_permanent", message: ae.Error.Message, retryAfter: ra}
	}
	switch strings.ToLower(resp.Header.Get("x-should-retry")) {
	case "false":
		return failure{transient: false, status: st, atype: atype, code: "upstream_no_retry", message: ae.Error.Message}
	case "true":
		return failure{transient: true, status: mapTransientStatus(st), atype: atype, code: "upstream_retry_" + itoa(st), message: ae.Error.Message, retryAfter: ra}
	}

	switch {
	case st == 408 || st == 409:
		return failure{transient: true, fastRetry: true, status: st, atype: atype, code: "http_" + itoa(st), message: ae.Error.Message}
	case st == 429:
		return failure{transient: true, status: 429, atype: "rate_limit_error", code: "rate_limit", message: ae.Error.Message, retryAfter: ra}
	case st >= 500:
		return failure{transient: true, fastRetry: st == 502 || st == 503, status: st, atype: typeFor5xx(st), code: "http_" + itoa(st), message: ae.Error.Message, retryAfter: ra}
	default: // 4xx other than 408/409/429
		// Surface ONLY a deterministic request-shape error (it can never succeed,
		// so retrying would loop). Everything else — auth, billing, policy,
		// unknown — is treated as a transient hiccup and retried: a temporary
		// block should be ridden out, not surfaced. Normalize to 502 so the SDK
		// always honors the retry (it may refuse to retry some 4xx by status).
		if reqShaped(ae.Error.Type, ae.Error.Code, ae.Error.Message, body) {
			return failure{transient: false, status: st, atype: atype, code: "request_shape", message: ae.Error.Message}
		}
		return failure{transient: true, status: 502, atype: "api_error", code: "retryable_4xx_" + itoa(st), message: ae.Error.Message, retryAfter: ra}
	}
}

// classifySSEError handles an `event: error` that arrives mid-stream (after 200).
func classifySSEError(data string) *failure {
	var e struct {
		Error struct{ Type, Message string } `json:"error"`
	}
	_ = json.Unmarshal([]byte(data), &e)
	switch e.Error.Type {
	case "overloaded_error":
		return &failure{transient: true, status: 529, atype: "overloaded_error", code: "sse_overloaded", message: e.Error.Message}
	case "rate_limit_error":
		return &failure{transient: true, status: 429, atype: "rate_limit_error", code: "sse_rate_limit", message: e.Error.Message}
	case "api_error", "timeout_error", "":
		return &failure{transient: true, status: 502, atype: "api_error", code: "sse_api_error", message: e.Error.Message}
	case "invalid_request_error": // request-shape: can never succeed, surface it
		return &failure{transient: false, status: 400, atype: e.Error.Type, code: "sse_request_shape", message: e.Error.Message}
	default: // authentication_error, permission_error, not_found_error, unknown -> retry
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
	t, err := time.Parse(http.TimeFormat, v)
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
// Request-shape and other non-transient failures keep their real status + type, so
// they surface accurately with x-should-retry:false instead of looping forever.
const retryableSurfaceStatus = http.StatusServiceUnavailable // 503

func surface(f failure) (status int, atype string) {
	if f.transient {
		return retryableSurfaceStatus, "api_error"
	}
	return statusFor(f), f.atype
}

// surfaceFor is surface() with the one wire-specific adjustment Codex needs. Codex
// ignores x-should-retry and decides retryability from the STATUS alone: it treats
// only 400 (InvalidRequest) and 429 (RetryLimit) as terminal, and RETRIES every
// other surfaced non-2xx (5xx -> InternalServerError, anything else ->
// UnexpectedStatus, both retryable in codex-rs error.rs). A transient failure still
// masks to 503, which Codex retries — the intended outcome. But a NON-transient,
// can-never-succeed failure surfaced with its real non-400 status (a 404/413/422
// request-shape, a gateway "permanent", or the proxy's own response_too_large 502)
// would make Codex loop it to its stream-retry cap; force 400 so Codex stops. This
// is the single choke point for every non-retryable Responses outcome, wherever it
// was classified.
func surfaceFor(path string, f failure) (status int, atype string) {
	status, atype = surface(f)
	if !f.transient && isResponsesPath(path) {
		return http.StatusBadRequest, atype
	}
	return status, atype
}

// Codex ignores X-Should-Retry, so exhausting/disabling the proxy retry budget
// must also produce its terminal HTTP status. Keep the original failure intact
// for diagnostics, and preserve Claude's generic error shape with its false header.
func retrySurfaceFor(path string, f failure, canRetry bool) (int, string) {
	if isResponsesPath(path) && !canRetry {
		f.transient = false
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
