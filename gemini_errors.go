package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// GenerateContent uses the Google error envelope, not the Interactions API's
// string-valued error.code: code is an HTTP status and status is a canonical name.
type geminiAPIError struct {
	Code    int    `json:"code"`
	Status  string `json:"status"`
	Message string `json:"message"`
	Details []struct {
		Type       string          `json:"@type"`
		RetryDelay json.RawMessage `json:"retryDelay"`
		Violations json.RawMessage `json:"violations"`
	} `json:"details,omitempty"`
}

func classifyGeminiHTTPErrorBytes(resp *http.Response, b []byte) failure {
	var envelopes [1]struct {
		Error geminiAPIError `json:"error"`
	}
	data := bytes.TrimSpace(b)
	var decodeErr error
	if len(data) > 0 && data[0] == '[' {
		// The non-SSE streaming transport wraps its HTTP error in an array.
		decodeErr = json.Unmarshal(data, &envelopes)
	} else {
		decodeErr = json.Unmarshal(data, &envelopes[0])
	}
	if decodeErr != nil {
		// Not a Google envelope (a gateway page, a proxy's own JSON, another
		// vendor's shape), so error.status gives no verdict. Judge the raw body
		// the way every non-Gemini route does. Header retry verdicts still win.
		return classifyHTTPErrorBytes(resp, b)
	}
	return classifyGeminiErrorBytes(resp, b, envelopes[0].Error)
}

func classifyGeminiErrorBytes(resp *http.Response, b []byte, e geminiAPIError) failure {
	nativeStatus := strings.ToUpper(strings.TrimSpace(e.Status))
	f := classifyHTTPErrorBytesShaped(resp, b, func(atype, code, msg, _ string) errorVerdict {
		if nativeStatus == "" {
			// Decodes as {"error":{...}} but carries no Google identity (e.g. a
			// gateway's own {"error":{"type":"overloaded_error"}}): judge it the
			// way every other route does. Request-shape phrases are matched in
			// the message only, never in details metadata.
			return anthropicErrorVerdict(atype, code, msg, strings.ToLower(e.Message))
		}
		return geminiErrorVerdict(nativeStatus, e)
	})
	// The shared classifier expects a string error.code. Read Google's fields
	// ourselves without translating or re-encoding the archived upstream body.
	f.message = e.Message
	if nativeStatus != "" {
		f.atype = nativeStatus
	} else if !f.transient {
		f.atype = geminiStatusForHTTP(f.status)
	}
	if f.transient && f.retryAfter == 0 {
		f.retryAfter = retryAfterSeconds(resp.Header.Get("Retry-After"))
		if f.retryAfter == 0 {
			for _, detail := range e.Details {
				if detail.Type != "type.googleapis.com/google.rpc.RetryInfo" {
					continue
				}
				var delay string
				if json.Unmarshal(detail.RetryDelay, &delay) == nil {
					f.retryAfter = max(f.retryAfter, geminiRetryDelaySeconds(delay))
				}
			}
		}
	}
	return f
}

// geminiErrorVerdict reads google.rpc.Code in error.status, the field Google
// defines for retry semantics. HTTP status alone cannot: the same 400 carries
// FAILED_PRECONDITION or UNAUTHENTICATED (no retry fixes either) and a
// gateway's mislabeled UNAVAILABLE. Statuses outside the table (CANCELLED,
// DATA_LOSS, vendor extensions) leave the HTTP status to decide.
func geminiErrorVerdict(status string, e geminiAPIError) errorVerdict {
	switch status {
	case "UNAVAILABLE", "DEADLINE_EXCEEDED", "INTERNAL", "UNKNOWN", "ABORTED":
		return verdictTransient
	case "RESOURCE_EXHAUSTED":
		// A per-minute limit clears on its own; a per-day quota stays exhausted
		// for hours, so replaying the request would only spend attempts.
		if geminiDailyQuotaExhausted(e) {
			return verdictPermanent
		}
		return verdictTransient
	case "INVALID_ARGUMENT", "FAILED_PRECONDITION", "OUT_OF_RANGE", "UNAUTHENTICATED",
		"PERMISSION_DENIED", "NOT_FOUND", "ALREADY_EXISTS", "UNIMPLEMENTED":
		return verdictPermanent
	}
	// A status outside the table: the message is the only evidence.
	if anyContains(strings.ToLower(e.Message), requestShapeSigs) {
		return verdictPermanent
	}
	return verdictUnknown
}

// geminiDailyQuotaExhausted reports a google.rpc.QuotaFailure naming a per-day
// quota (e.g. GenerateRequestsPerDayPerProjectPerModel-FreeTier). Violations
// stay raw until a QuotaFailure is found, so another detail type's differently
// shaped "violations" can never invalidate the envelope.
func geminiDailyQuotaExhausted(e geminiAPIError) bool {
	for _, detail := range e.Details {
		if detail.Type != "type.googleapis.com/google.rpc.QuotaFailure" {
			continue
		}
		var violations []struct {
			QuotaID string `json:"quotaId"`
		}
		if json.Unmarshal(detail.Violations, &violations) != nil {
			continue
		}
		for _, v := range violations {
			if strings.Contains(v.QuotaID, "PerDay") {
				return true
			}
		}
	}
	return false
}

// ProtoJSON Duration uses seconds with up to nine fractional digits. Round up
// to the HTTP Retry-After granularity so the client never retries early.
func geminiRetryDelaySeconds(value string) int {
	text, ok := strings.CutSuffix(value, "s")
	if !ok || text == "" {
		return 0
	}
	whole, fraction, fractional := strings.Cut(text, ".")
	if whole == "" || (fractional && (fraction == "" || len(fraction) > 9)) {
		return 0
	}
	for _, part := range [...]string{whole, fraction} {
		for i := range len(part) {
			if part[i] < '0' || part[i] > '9' {
				return 0
			}
		}
	}
	seconds, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || seconds > 315576000000 {
		return 0
	}
	if strings.Trim(fraction, "0") != "" {
		seconds++
	}
	maxInt := int(^uint(0) >> 1)
	if seconds > int64(maxInt) {
		return maxInt
	}
	return int(seconds)
}

// classifyGeminiSSEError is called only after the wire detects an error frame;
// a broken error payload is still a failure, never successful stream content.
func classifyGeminiSSEError(data string) *failure {
	var envelope struct {
		Error geminiAPIError `json:"error"`
	}
	b := []byte(data)
	if json.Unmarshal(b, &envelope) != nil {
		return &failure{transient: true, status: http.StatusBadGateway, atype: "api_error", code: "gemini_sse_error", message: "invalid Gemini error envelope"}
	}
	e := envelope.Error
	status := e.Code
	if status < 400 || status > 599 {
		status = geminiHTTPStatus(strings.ToUpper(strings.TrimSpace(e.Status)))
	}
	// An in-band error has no HTTP response of its own; the code it carries is
	// the upstream status the logs should show. 0 when it names neither.
	upstreamStatus := status
	if status == 0 {
		status = http.StatusBadGateway
	}
	f := classifyGeminiErrorBytes(&http.Response{StatusCode: status}, b, e)
	f.origStatus = upstreamStatus
	f.code = "gemini_sse_" + f.code
	return &f
}

func geminiHTTPStatus(status string) int {
	switch status {
	case "INVALID_ARGUMENT", "FAILED_PRECONDITION", "OUT_OF_RANGE":
		return http.StatusBadRequest
	case "UNAUTHENTICATED":
		return http.StatusUnauthorized
	case "PERMISSION_DENIED":
		return http.StatusForbidden
	case "NOT_FOUND":
		return http.StatusNotFound
	case "ALREADY_EXISTS", "ABORTED":
		return http.StatusConflict
	case "RESOURCE_EXHAUSTED":
		return http.StatusTooManyRequests
	case "CANCELLED":
		return 499
	case "UNKNOWN", "INTERNAL", "DATA_LOSS":
		return http.StatusInternalServerError
	case "UNIMPLEMENTED":
		return http.StatusNotImplemented
	case "UNAVAILABLE":
		return http.StatusServiceUnavailable
	case "DEADLINE_EXCEEDED":
		return http.StatusGatewayTimeout
	default:
		return 0
	}
}

func geminiStatusForHTTP(status int) string {
	switch status {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
		return "INVALID_ARGUMENT"
	case http.StatusUnauthorized:
		return "UNAUTHENTICATED"
	case http.StatusForbidden:
		return "PERMISSION_DENIED"
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return "DEADLINE_EXCEEDED"
	case http.StatusConflict:
		return "ABORTED"
	case http.StatusPreconditionFailed:
		return "FAILED_PRECONDITION"
	case http.StatusRequestedRangeNotSatisfiable:
		return "OUT_OF_RANGE"
	case http.StatusTooManyRequests:
		return "RESOURCE_EXHAUSTED"
	case 499:
		return "CANCELLED"
	case http.StatusInternalServerError:
		return "INTERNAL"
	case http.StatusNotImplemented:
		return "UNIMPLEMENTED"
	case http.StatusServiceUnavailable:
		return "UNAVAILABLE"
	default:
		return "UNKNOWN"
	}
}

func writeGeminiError(w http.ResponseWriter, canRetry bool, status int, atype, message string, retryAfter int, code string) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	if canRetry {
		h.Set("X-Should-Retry", "true")
	} else {
		h.Set("X-Should-Retry", "false")
	}
	if retryAfter > 0 {
		h.Set("Retry-After", itoa(retryAfter))
	}
	h.Set("X-Steady-Proxy-Reason", code)
	if status <= 0 {
		status = http.StatusBadGateway
	}
	// The canonical status is the upstream's own diagnosis and survives a forced
	// HTTP status (a terminal 429 is surfaced as 400 so clients that retry on
	// 429 stop). Only an atype that is not a Google status, such as the
	// Anthropic-style types of proxy-originated errors, is derived from HTTP.
	nativeStatus := strings.ToUpper(strings.TrimSpace(atype))
	if geminiHTTPStatus(nativeStatus) == 0 {
		nativeStatus = geminiStatusForHTTP(status)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error geminiAPIError `json:"error"`
	}{Error: geminiAPIError{Code: status, Status: nativeStatus, Message: message}})
}
