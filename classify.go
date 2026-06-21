package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

var errTooLarge = errors.New("response too large to buffer")

// failure describes a non-success outcome and how to surface it to the SDK.
type failure struct {
	transient  bool   // true => eligible to convert into an SDK retry
	fastRetry  bool   // true => one cheap proxy-local re-issue is OK
	status     int    // HTTP status to surface downstream
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
		return "cc-retry-proxy: upstream failure"
	}
	return "cc-retry-proxy: " + f.message
}

// Substrings that mark an error as PERMANENT (never auto-retry — would loop).
// Derived from real Claude Code session transcripts; see docs/ERROR-SITUATIONS.md.
var permanentSigs = []string{
	// request too large / context
	"context_length", "maximum context", "prompt is too long", "too long",
	// malformed conversation / tool blocks
	"invalid_request_error", "tool_use", "tool_result", "tool use concurrency",
	"missing tool result", "duplicate tool_use", "unexpected tool_use_id",
	"thinking blocks", "thinking.budget_tokens", "max_tokens must be",
	// schema / header mismatches (often shim translation bugs — deterministic)
	"extra inputs are not permitted", "not permitted", "unexpected value",
	"anthropic-beta", "context_management", "input_examples", "image dimensions",
	"must be", "is required", "unsupported", "model_not_found",
	// auth / billing / policy
	"invalid api key", "authentication", "permission", "organization has been disabled",
	"billing", "usage credits", "usage policy", "usage limit",
}

// Substrings that suggest a 4xx is actually a transient downstream hiccup.
var transientSigs = []string{
	"upstream", "gateway", "timeout", "timed out", "temporar",
	"unavailable", "overload", "capacity", "connection reset", "connection error",
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

// classifyHTTPError inspects a non-2xx upstream response. It reads (and drains)
// a bounded prefix of the body; the caller still closes resp.Body.
func classifyHTTPError(resp *http.Response) failure {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	body := strings.ToLower(string(b))
	st := resp.StatusCode
	ra := atoiSafe(resp.Header.Get("Retry-After"))

	var ae struct {
		Error struct{ Type, Message string } `json:"error"`
	}
	_ = json.Unmarshal(b, &ae)
	atype := ae.Error.Type
	if atype == "" {
		atype = "api_error"
	}

	// Trust explicit signals first.
	switch strings.ToLower(resp.Header.Get("x-gateway-retryable")) {
	case "true":
		return failure{transient: true, status: mapTransientStatus(st), atype: atype, code: "gateway_retryable", message: ae.Error.Message, retryAfter: ra}
	case "false":
		return failure{transient: false, status: st, atype: atype, code: "gateway_permanent", message: ae.Error.Message, retryAfter: ra}
	}
	switch strings.ToLower(resp.Header.Get("x-should-retry")) {
	case "false":
		return failure{transient: false, status: st, atype: atype, code: "upstream_no_retry", message: ae.Error.Message}
	case "true":
		return failure{transient: true, status: mapTransientStatus(st), atype: atype, code: "upstream_retry", message: ae.Error.Message, retryAfter: ra}
	}

	switch {
	case st == 408 || st == 409:
		return failure{transient: true, fastRetry: true, status: st, atype: atype, code: "http_" + itoa(st), message: ae.Error.Message}
	case st == 429:
		return failure{transient: true, status: 429, atype: "rate_limit_error", code: "rate_limit", message: ae.Error.Message, retryAfter: ra}
	case st >= 500:
		return failure{transient: true, fastRetry: st == 502 || st == 503, status: st, atype: typeFor5xx(st), code: "http_" + itoa(st), message: ae.Error.Message, retryAfter: ra}
	default: // 4xx other than 408/409/429
		if anyContains(body, permanentSigs) {
			return failure{transient: false, status: st, atype: atype, code: "permanent_4xx", message: ae.Error.Message}
		}
		if anyContains(body, transientSigs) {
			return failure{transient: true, status: 502, atype: "api_error", code: "transient_4xx", message: ae.Error.Message, retryAfter: ra}
		}
		// Unknown 4xx: do not guess — surface it.
		return failure{transient: false, status: st, atype: atype, code: "unknown_4xx", message: ae.Error.Message}
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
	default: // invalid_request_error, authentication_error, permission_error, not_found_error, ...
		return &failure{transient: false, status: 400, atype: e.Error.Type, code: "sse_permanent", message: e.Error.Message}
	}
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
	h.Set("X-CC-Retry-Proxy-Reason", code)
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
