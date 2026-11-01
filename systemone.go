package main

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
)

const systemOneMetadataBytes = 1 << 20

func isSystemOnePath(path string) bool {
	return pathIs(path, "/v1/systemone")
}

// System One uses HTTP status for retry eligibility, not error text or the
// arbitrary state and answer data its JSON bodies can contain.
func classifySystemOneHTTPErrorBytes(resp *http.Response, b []byte) failure {
	st := resp.StatusCode
	if st >= 300 && st < 400 {
		// Keep the shared credential-safe redirect diagnostic.
		return classifyHTTPErrorBytesShaped(resp, nil, nil)
	}
	typ, code, message := systemOneErrorIdentity(b)
	f := failure{
		status: st, atype: typ, code: "http_" + itoa(st), message: message,
		origType: typ, origCode: code,
	}
	if f.atype == "" {
		f.atype = "api_error"
	}
	if source, retry := httpRetryHint(resp.Header); source != "" {
		f.transient = retry
		if source == "x-gateway-retryable" {
			f.code = "gateway_permanent"
			if retry {
				f.code = "gateway_retryable_" + itoa(st)
			}
		} else {
			f.code = "upstream_no_retry"
			if retry {
				f.code = "upstream_retry_" + itoa(st)
			}
		}
		if retry {
			f.status = mapTransientStatus(st)
		}
	} else {
		// Both official SDKs default to 408, 429 and 500–599, excluding 409.
		f.transient = st == 408 || st == 429 || (st >= 500 && st <= 599)
	}
	if f.transient {
		f.retryAfter = systemOneRetryAfterSeconds(resp.Header)
	}
	return f
}

func systemOneErrorIdentity(body []byte) (typ, code, message string) {
	var envelope map[string]json.RawMessage
	if json.Unmarshal(body, &envelope) != nil {
		return "", "", ""
	}
	// Native detail is a string or an object. Validation arrays carry no single
	// error identity; the parent replays those terminal responses unchanged.
	if detail := envelope["detail"]; len(detail) != 0 {
		if json.Unmarshal(detail, &message) == nil && strings.TrimSpace(string(detail)) != "null" {
			return "", "", message
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(detail, &fields) == nil && fields != nil {
			return systemOneJSONString(fields["type"]), systemOneJSONString(fields["code"]), systemOneJSONString(fields["message"])
		}
	}
	// Gateways may use the usual error envelope instead of native detail.
	var fields map[string]json.RawMessage
	if json.Unmarshal(envelope["error"], &fields) == nil {
		return systemOneJSONString(fields["type"]), systemOneJSONString(fields["code"]), systemOneJSONString(fields["message"])
	}
	return "", "", ""
}

func systemOneJSONString(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}

func systemOneRetryAfterSeconds(h http.Header) int {
	// The official Python SDK's _core/errors.py parse_retry_after reads the
	// millisecond header first, falling back to Retry-After for invalid values.
	ms, err := strconv.ParseFloat(strings.TrimSpace(h.Get("retry-after-ms")), 64)
	if err != nil || math.IsNaN(ms) || math.IsInf(ms, 0) || ms < 0 {
		return retryAfterSeconds(h.Get("Retry-After"))
	}
	if ms == 0 {
		return 0
	}
	seconds := math.Ceil(ms / 1000)
	// A positive fractional millisecond must still request a real delay, even
	// when dividing a tiny positive float underflows.
	if seconds < 1 {
		return 1
	}
	maxInt := int(^uint(0) >> 1)
	if seconds >= float64(maxInt) {
		return maxInt
	}
	return int(seconds)
}

// writeSystemOneError is only for proxy-originated errors. Terminal upstream
// errors, including object and validation-array detail, are replayed by parent.
func writeSystemOneError(w http.ResponseWriter, canRetry bool, status int, atype, message string, retryAfter int, code string) {
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
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Detail string `json:"detail"`
	}{Detail: message})
}

type systemOneMetadata struct {
	Model        string
	InputTokens  *int
	OutputTokens *int
}

// Metadata is best effort: wrong types or counts invalidate only that field.
// Maps intentionally require exact API keys, unlike case-insensitive struct
// decoding, and never inspect request state, answers or nested user metadata.
func systemOneResponseMetadata(body []byte) systemOneMetadata {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return systemOneMetadata{}
	}
	metadata := systemOneMetadata{Model: systemOneJSONString(fields["model"])}
	var usage map[string]json.RawMessage
	if json.Unmarshal(fields["usage"], &usage) == nil {
		metadata.InputTokens = systemOneTokenCount(usage["input_tokens"])
		metadata.OutputTokens = systemOneTokenCount(usage["output_tokens"])
	}
	return metadata
}

func systemOneTokenCount(raw json.RawMessage) *int {
	var count *int
	if json.Unmarshal(raw, &count) != nil || count == nil || *count < 0 {
		return nil
	}
	return count
}
