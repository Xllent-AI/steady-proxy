package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
)

// GenerateContent uses the Google error envelope, not the Interactions API's
// string-valued error.code: code is an HTTP status and status is a canonical name.
type geminiAPIError struct {
	Code    int    `json:"code"`
	Status  string `json:"status"`
	Message string `json:"message"`
	Details []struct {
		Type   string `json:"@type"`
		Reason string `json:"reason"`
	} `json:"details,omitempty"`
}

func classifyGeminiHTTPErrorBytes(resp *http.Response, b []byte) failure {
	var envelopes [1]struct {
		Error geminiAPIError `json:"error"`
	}
	data := bytes.TrimSpace(b)
	if len(data) > 0 && data[0] == '[' {
		// The non-SSE streaming transport wraps its HTTP error in an array.
		_ = json.Unmarshal(data, &envelopes)
	} else {
		_ = json.Unmarshal(data, &envelopes[0])
	}
	return classifyGeminiErrorBytes(resp, b, envelopes[0].Error)
}

func classifyGeminiErrorBytes(resp *http.Response, b []byte, e geminiAPIError) failure {
	nativeStatus := strings.ToUpper(strings.TrimSpace(e.Status))
	f := classifyHTTPErrorBytesShaped(resp, b, func(_, _, _, _ string) bool {
		// Google's documented invalid-key response is also INVALID_ARGUMENT.
		// Its machine-readable ErrorInfo keeps authentication on the retry path.
		for _, detail := range e.Details {
			if detail.Type == "type.googleapis.com/google.rpc.ErrorInfo" && detail.Reason == "API_KEY_INVALID" {
				return false
			}
		}
		if nativeStatus == "INVALID_ARGUMENT" || nativeStatus == "NOT_FOUND" {
			return true
		}
		// FAILED_PRECONDITION can mean disabled billing or an unsupported region.
		// A named status owns the decision; bare 400/404 is only a fallback.
		if nativeStatus == "" {
			status := e.Code
			if status == 0 {
				status = resp.StatusCode
			}
			if status == http.StatusBadRequest || status == http.StatusNotFound {
				return true
			}
		}
		return anyContains(strings.ToLower(e.Message), requestShapeSigs)
	})
	// The shared classifier expects a string error.code. Read Google's fields
	// ourselves without translating or re-encoding the archived upstream body.
	f.message = e.Message
	if nativeStatus != "" {
		f.atype = nativeStatus
	} else if !f.transient {
		f.atype = geminiStatusForHTTP(f.status)
	}
	return f
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
		if status == 0 {
			status = http.StatusBadGateway
		}
	}
	f := classifyGeminiErrorBytes(&http.Response{StatusCode: status}, b, e)
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
	nativeStatus := strings.ToUpper(strings.TrimSpace(atype))
	if geminiHTTPStatus(nativeStatus) != status {
		nativeStatus = geminiStatusForHTTP(status)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error geminiAPIError `json:"error"`
	}{Error: geminiAPIError{Code: status, Status: nativeStatus, Message: message}})
}
