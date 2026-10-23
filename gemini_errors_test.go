package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// QuotaFailure details as the Gemini API sends them: a per-day quota stays
// exhausted until the day rolls over, a per-minute one clears on its own. Both
// carry the RetryInfo that only the per-minute case may honor.
const (
	geminiDailyQuotaDetails  = `[{"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[{"quotaMetric":"generativelanguage.googleapis.com/generate_content_free_tier_requests","quotaId":"GenerateRequestsPerDayPerProjectPerModel-FreeTier"}]},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"29s"}]`
	geminiMinuteQuotaDetails = `[{"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[{"quotaMetric":"generativelanguage.googleapis.com/generate_content_free_tier_requests","quotaId":"GenerateRequestsPerMinutePerProjectPerModel-FreeTier"}]},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"29s"}]`
)

func TestClassifyGeminiHTTPError(t *testing.T) {
	cases := []struct {
		name      string
		http      int
		body      string
		transient bool
		status    int
		code      string // terminal failures only
	}{
		{"invalid-argument", 400, `{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"invalid contents"}}`, false, 400, "request_shape"},
		{"not-found", 404, `{"error":{"code":404,"status":"NOT_FOUND","message":"model does not exist"}}`, false, 404, "request_shape"},
		{"normalized-native-status", 400, `{"error":{"code":400,"status":" invalid_argument ","message":"invalid contents"}}`, false, 400, "request_shape"},
		{"numeric-400", 400, `{"error":{"code":400,"message":"invalid contents"}}`, false, 400, "http_400"},
		{"numeric-404", 404, `{"error":{"code":404,"message":"model does not exist"}}`, false, 404, "http_404"},
		{"http-400-fallback", 400, `{"error":{"message":"bad request"}}`, false, 400, "http_400"},
		{"http-404-fallback", 404, `{"error":{"message":"missing resource"}}`, false, 404, "http_404"},

		// An identical retry cannot fix any of these, whatever the message says.
		{"billing-precondition", 400, `{"error":{"code":400,"status":"FAILED_PRECONDITION","message":"enable billing on your project"}}`, false, 400, "request_shape"},
		{"region-precondition", 400, `{"error":{"code":400,"status":"FAILED_PRECONDITION","message":"unsupported region for this account"}}`, false, 400, "request_shape"},
		{"auth-precondition", 400, `{"error":{"code":400,"status":"FAILED_PRECONDITION","message":"authentication is required"}}`, false, 400, "request_shape"},
		{"unauthenticated", 401, `{"error":{"code":401,"status":"UNAUTHENTICATED","message":"invalid API key"}}`, false, 401, "request_shape"},
		{"permission-denied", 403, `{"error":{"code":403,"status":"PERMISSION_DENIED","message":"permission denied"}}`, false, 403, "request_shape"},
		{"invalid-key-invalid-argument", 400, `{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"API key not valid. Please pass a valid API key.","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"API_KEY_INVALID","domain":"googleapis.com"}]}}`, false, 400, "request_shape"},
		{"out-of-range", 400, `{"error":{"code":400,"status":"OUT_OF_RANGE","message":"offset is past the end"}}`, false, 400, "request_shape"},
		{"already-exists", 400, `{"error":{"code":400,"status":"ALREADY_EXISTS","message":"resource exists"}}`, false, 400, "request_shape"},

		// 429 is retried unless the body names a fault a retry cannot fix.
		{"quota", 429, `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"quota exhausted"}}`, true, 429, ""},
		{"quota-per-minute", 429, `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"quota exhausted","details":` + geminiMinuteQuotaDetails + `}}`, true, 429, ""},
		{"quota-per-day", 429, `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"quota exhausted","details":` + geminiDailyQuotaDetails + `}}`, false, 429, "http_429"},
		{"quota-per-day-among-per-minute", 429, `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[{"quotaId":"GenerateRequestsPerMinutePerProjectPerModel-FreeTier"},{"quotaId":"GenerateContentInputTokensPerModelPerDay-FreeTier"}]}]}}`, false, 429, "http_429"},
		{"per-day-id-outside-quota-failure", 429, `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.PreconditionFailure","violations":[{"quotaId":"PerDay"}]}]}}`, true, 429, ""},
		{"malformed-quota-violations", 429, `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":"PerDay"}]}}`, true, 429, ""},
		{"429-request-fault", 429, `{"error":{"code":429,"status":"INVALID_ARGUMENT","message":"bad request"}}`, false, 429, "http_429"},
		{"429-request-signature-without-status", 429, `{"error":{"code":429,"message":"prompt is too long"}}`, false, 429, "http_429"},

		{"internal", 500, `{"error":{"code":500,"status":"INTERNAL","message":"internal error"}}`, true, 500, ""},
		{"unavailable", 503, `{"error":{"code":503,"status":"UNAVAILABLE","message":"busy"}}`, true, 503, ""},
		// A 4xx whose status names a transient fault is retried.
		{"unknown-named-status", 400, `{"error":{"code":400,"status":"UNKNOWN","message":"temporary gateway fault"}}`, true, 502, ""},
		{"deadline-exceeded-on-4xx", 400, `{"error":{"code":400,"status":"DEADLINE_EXCEEDED"}}`, true, 502, ""},
		{"aborted-on-4xx", 400, `{"error":{"code":400,"status":"ABORTED"}}`, true, 502, ""},
		{"named-status-beats-numeric-400", 400, `{"error":{"code":400,"status":"UNAVAILABLE","message":"temporary capacity issue"}}`, true, 502, ""},
		// Without a table entry the HTTP status decides: unknown 4xx is final.
		{"new-named-status", 404, `{"error":{"code":404,"status":"NEW_PROVIDER_ERROR","message":"temporary gateway fault"}}`, false, 404, "http_404"},
		{"unnamed-unknown", 418, `{"error":{"code":418,"message":"unknown provider fault"}}`, false, 418, "http_418"},
		{"native-request-shape-on-403", 403, `{"error":{"code":403,"status":"INVALID_ARGUMENT","message":"bad request"}}`, false, 403, "request_shape"},
		{"numeric-envelope-owns-fallback", 403, `{"error":{"code":400,"message":"bad request"}}`, false, 403, "http_403"},
		{"specific-request-signature", 400, `{"error":{"code":400,"status":"FAILED_PRECONDITION","message":"prompt is too long for the context window"}}`, false, 400, "request_shape"},
		// The message only speaks for an envelope with no status of its own.
		{"status-beats-message-signature", 403, `{"error":{"code":403,"status":"UNKNOWN","message":"context length exceeded"}}`, true, 502, ""},
		{"signature-without-status", 403, `{"error":{"code":403,"message":"context length exceeded"}}`, false, 403, "request_shape"},
		{"metadata-is-not-request-message", 403, `{"error":{"code":403,"message":"access denied","details":[{"description":"context length"}]}}`, false, 403, "http_403"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := classifyGeminiHTTPErrorBytes(&http.Response{StatusCode: tc.http}, []byte(tc.body))
			if f.transient != tc.transient || f.status != tc.status {
				t.Fatalf("want transient=%v status=%d, got %+v", tc.transient, tc.status, f)
			}
			var envelope struct {
				Error struct{ Message string } `json:"error"`
			}
			if err := json.Unmarshal([]byte(tc.body), &envelope); err != nil {
				t.Fatal(err)
			}
			if f.message != envelope.Error.Message {
				t.Fatalf("numeric error.code must not lose message: got %q, want %q", f.message, envelope.Error.Message)
			}
			if !f.transient && f.code != tc.code {
				t.Fatalf("want terminal reason %q, got %+v", tc.code, f)
			}
		})
	}
}

func TestGeminiArrayHTTPErrorRetryPolicy(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		error     string
		transient bool
	}{
		{"invalid-key", 400, `{"code":400,"status":"INVALID_ARGUMENT","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"API_KEY_INVALID"}]`, false},
		{"invalid-request", 400, `{"code":400,"status":"INVALID_ARGUMENT"`, false},
		{"billing", 400, `{"code":400,"status":"FAILED_PRECONDITION"`, false},
		{"quota", 429, `{"code":429,"status":"RESOURCE_EXHAUSTED"`, true},
		{"daily-quota", 429, `{"code":429,"status":"RESOURCE_EXHAUSTED","details":` + geminiDailyQuotaDetails, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(" \n[{\"error\":" + tc.error + `,"message":"native detail"}}]`)
			f := classifyGeminiHTTPErrorBytes(&http.Response{StatusCode: tc.status}, body)
			if f.transient != tc.transient || f.message != "native detail" {
				t.Fatalf("array-wrapped native error lost retry policy or context: %+v", f)
			}
		})
	}
}

// 408/409 and 5xx are retried whatever the body says; 429 is not in this list
// because a body naming a request fault makes it final (see TestClassifyGeminiHTTPError).
func TestGeminiHTTPStatusPrecedence(t *testing.T) {
	body := []byte(`{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"context length exceeded"}}`)
	for _, status := range []int{408, 409, 500, 502, 503, 504} {
		t.Run(itoa(status), func(t *testing.T) {
			f := classifyGeminiHTTPErrorBytes(&http.Response{StatusCode: status}, body)
			if !f.transient || f.status != status {
				t.Fatalf("shared status policy must precede body heuristics: %+v", f)
			}
			wantFastRetry := status == 408 || status == 409 || status == 502 || status == 503
			if f.fastRetry != wantFastRetry {
				t.Fatalf("fastRetry=%v, want %v (%+v)", f.fastRetry, wantFastRetry, f)
			}
		})
	}
}

func TestGeminiHTTPRetryHeaderPrecedence(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		gateway   string
		upstream  string
		transient bool
		want      int
		code      string
		delay     int
	}{
		{"gateway-force-retry", 400, "true", "", true, 502, "gateway_retryable_400", 7},
		{"gateway-force-permanent", 503, "false", "", false, 503, "gateway_permanent", 7},
		{"gateway-true-beats-upstream-false", 400, "TrUe", "false", true, 502, "gateway_retryable_400", 7},
		{"gateway-false-beats-upstream-true", 503, "FaLsE", "true", false, 503, "gateway_permanent", 7},
		{"upstream-force-retry", 404, "", "true", true, 502, "upstream_retry_404", 7},
		{"upstream-force-permanent", 503, "", "false", false, 503, "upstream_no_retry", 0},
		{"unknown-gateway-defers-to-upstream", 400, "unknown", "true", true, 502, "upstream_retry_400", 7},
	}
	body := []byte(`{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"invalid contents","details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"23s"}]}}`)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{StatusCode: tc.status, Header: make(http.Header)}
			resp.Header.Set("X-Gateway-Retryable", tc.gateway)
			resp.Header.Set("X-Should-Retry", tc.upstream)
			resp.Header.Set("Retry-After", "7")
			f := classifyGeminiHTTPErrorBytes(resp, body)
			if f.transient != tc.transient || f.status != tc.want || f.code != tc.code || f.retryAfter != tc.delay {
				t.Fatalf("header precedence mismatch: %+v", f)
			}
			if f.message != "invalid contents" || f.atype != "INVALID_ARGUMENT" {
				t.Fatalf("native fields lost through shared classifier: %+v", f)
			}
		})
	}
}

func TestGeminiHTTPRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		header string
		want   int
	}{
		{"quota-backoff", 429, `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"quota exhausted"}}`, "11", 11},
		{"retryable-4xx-backoff", 400, `{"error":{"code":400,"status":"UNAVAILABLE","message":"capacity unavailable"}}`, "9", 9},
		{"server-backoff", 503, `{"error":{"code":503,"status":"UNAVAILABLE","message":"busy"}}`, "3", 3},
		{"absent-backoff", 429, `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED"}}`, "", 0},
		{"malformed-backoff", 503, `{"error":{"code":503,"status":"UNAVAILABLE"}}`, "not a delay", 0},
		{"negative-backoff", 503, `{"error":{"code":503,"status":"UNAVAILABLE"}}`, "-1", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{StatusCode: tc.status, Header: make(http.Header)}
			resp.Header.Set("Retry-After", tc.header)
			f := classifyGeminiHTTPErrorBytes(resp, []byte(tc.body))
			if !f.transient || f.retryAfter != tc.want {
				t.Fatalf("want transient backoff=%d, got %+v", tc.want, f)
			}
		})
	}
}

func TestGeminiRetryInfoBackoff(t *testing.T) {
	for _, tc := range []struct {
		name   string
		delay  string
		header string
		want   int64
	}{
		{"whole seconds", `"23s"`, "", 23},
		{"fractional seconds round up", `"23.000000001s"`, "", 24},
		{"subsecond cooldown", `"0.000000001s"`, "", 1},
		{"whole fractional form", `"23.000s"`, "", 23},
		{"protobuf maximum seconds", `"315576000000s"`, "", 315576000000},
		{"HTTP header takes precedence", `"23s"`, "7", 7},
		{"invalid header uses native delay", `"23s"`, "invalid", 23},
		{"zero cooldown", `"0s"`, "", 0},
		{"negative cooldown", `"-1s"`, "", 0},
		{"out of range duration", `"315576000001s"`, "", 0},
		{"excess fractional precision", `"0.0000000001s"`, "", 0},
		{"wrong units", `"1m"`, "", 0},
		{"compound duration", `"1s2s"`, "", 0},
		{"invalid field type", `23`, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"quota exhausted","details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":` + tc.delay + `}]}}`
			resp := &http.Response{StatusCode: 429, Header: make(http.Header)}
			resp.Header.Set("Retry-After", tc.header)
			f := classifyGeminiHTTPErrorBytes(resp, []byte(body))
			want := min(tc.want, int64(int(^uint(0)>>1)))
			if !f.transient || int64(f.retryAfter) != want {
				t.Fatalf("HTTP cooldown=%d, want %d: %+v", f.retryAfter, want, f)
			}
			if tc.header == "" {
				f := classifyGeminiSSEError(body)
				if f == nil || !f.transient || int64(f.retryAfter) != want {
					t.Fatalf("SSE cooldown must preserve native RetryInfo: %+v, want %d", f, want)
				}
			}
		})
	}
}

func TestClassifyGeminiSSEError(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		transient bool
		status    int
		orig      int    // the in-band code, or the HTTP status of the canonical name when there is no code
		code      string // terminal failures only
	}{
		{"invalid-argument", `{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"bad contents"}}`, false, 400, 400, "gemini_sse_request_shape"},
		{"not-found", `{"error":{"code":404,"status":"NOT_FOUND","message":"bad model"}}`, false, 404, 404, "gemini_sse_request_shape"},
		{"numeric-400", `{"error":{"code":400}}`, false, 400, 400, "gemini_sse_http_400"},
		{"numeric-404", `{"error":{"code":404}}`, false, 404, 404, "gemini_sse_http_404"},
		{"invalid-argument-without-code", `{"error":{"status":"INVALID_ARGUMENT"}}`, false, 400, 400, "gemini_sse_request_shape"},
		{"not-found-without-code", `{"error":{"status":"NOT_FOUND"}}`, false, 404, 404, "gemini_sse_request_shape"},
		{"billing-without-code", `{"error":{"status":"FAILED_PRECONDITION","message":"enable billing"}}`, false, 400, 400, "gemini_sse_request_shape"},
		{"region", `{"error":{"code":400,"status":"FAILED_PRECONDITION","message":"unsupported region"}}`, false, 400, 400, "gemini_sse_request_shape"},
		{"auth-without-code", `{"error":{"status":"UNAUTHENTICATED"}}`, false, 401, 401, "gemini_sse_request_shape"},
		{"invalid-key-invalid-argument", `{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"API key not valid. Please pass a valid API key.","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"API_KEY_INVALID"}]}}`, false, 400, 400, "gemini_sse_request_shape"},
		{"permission-without-code", `{"error":{"status":"PERMISSION_DENIED"}}`, false, 403, 403, "gemini_sse_request_shape"},
		{"out-of-range-without-code", `{"error":{"status":"OUT_OF_RANGE"}}`, false, 400, 400, "gemini_sse_request_shape"},
		{"quota-without-code", `{"error":{"status":"RESOURCE_EXHAUSTED"}}`, true, 429, 429, ""},
		{"quota-per-minute", `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","details":` + geminiMinuteQuotaDetails + `}}`, true, 429, 429, ""},
		{"quota-per-day", `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","details":` + geminiDailyQuotaDetails + `}}`, false, 429, 429, "gemini_sse_http_429"},
		{"internal-without-code", `{"error":{"status":"INTERNAL"}}`, true, 500, 500, ""},
		{"unknown-canonical-without-code", `{"error":{"status":"UNKNOWN"}}`, true, 500, 500, ""},
		{"unavailable-without-code", `{"error":{"status":"UNAVAILABLE"}}`, true, 503, 503, ""},
		{"deadline-without-code", `{"error":{"status":"DEADLINE_EXCEEDED"}}`, true, 504, 504, ""},
		{"aborted-without-code", `{"error":{"status":"ABORTED"}}`, true, 409, 409, ""},
		{"already-exists-without-code", `{"error":{"status":"ALREADY_EXISTS"}}`, true, 409, 409, ""},
		{"cancelled-without-code", `{"error":{"status":"CANCELLED"}}`, false, 499, 499, "gemini_sse_http_499"},
		{"unimplemented-without-code", `{"error":{"status":"UNIMPLEMENTED"}}`, true, 501, 501, ""},
		{"data-loss-without-code", `{"error":{"status":"DATA_LOSS"}}`, true, 500, 500, ""},
		{"unknown-native-status", `{"error":{"status":"NEW_PROVIDER_ERROR"}}`, true, 502, 0, ""},
		{"normalized-native-status", `{"error":{"status":" unavailable "}}`, true, 503, 503, ""},
		{"numeric-status-precedes-derived-status", `{"error":{"code":503,"status":"INVALID_ARGUMENT"}}`, true, 503, 503, ""},
		{"invalid-numeric-code-uses-native-status", `{"error":{"code":-1,"status":"NOT_FOUND"}}`, false, 404, 404, "gemini_sse_request_shape"},
		{"specific-signature", `{"error":{"status":"FAILED_PRECONDITION","message":"context length exceeded"}}`, false, 400, 400, "gemini_sse_request_shape"},
		{"signature-without-status", `{"error":{"code":400,"message":"context length exceeded"}}`, false, 400, 400, "gemini_sse_request_shape"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := classifyGeminiSSEError(tc.body)
			if f == nil || f.transient != tc.transient || f.status != tc.status {
				t.Fatalf("want transient=%v status=%d, got %+v", tc.transient, tc.status, f)
			}
			// Logs must show the status the upstream reported, not the 502 or
			// normalized status the failure is surfaced with.
			if f.origStatus != tc.orig {
				t.Fatalf("origStatus=%d, want %d: %+v", f.origStatus, tc.orig, f)
			}
			var envelope struct {
				Error struct{ Message string } `json:"error"`
			}
			if err := json.Unmarshal([]byte(tc.body), &envelope); err != nil {
				t.Fatal(err)
			}
			if f.message != envelope.Error.Message {
				t.Fatalf("native SSE message lost: %+v", f)
			}
			if !f.transient && f.code != tc.code {
				t.Fatalf("want terminal reason %q, got %+v", tc.code, f)
			}
		})
	}
}

// A retried 4xx is normalized to 502 for the client; the in-band code still
// reaches the logs through origStatus.
func TestClassifyGeminiSSEErrorLogsRealStatusOfRetried4xx(t *testing.T) {
	f := classifyGeminiSSEError(`{"error":{"code":400,"status":"UNAVAILABLE","message":"capacity"}}`)
	if f == nil || !f.transient || f.status != 502 || origStatusOf(*f) != 400 {
		t.Fatalf("want a transient 502 logged as the upstream 400, got %+v", f)
	}
}

func TestGeminiMalformedErrors(t *testing.T) {
	for _, body := range []string{
		"", "not JSON", `{"error":`, `[]`, `null`, `{}`, `{"error":null}`,
		`{"error":"broken"}`, `{"error":{"code":"400"}}`,
		`{"error":{"status":400}}`, `{"error":{"message":false}}`,
	} {
		t.Run(body, func(t *testing.T) {
			httpFailure := classifyGeminiHTTPErrorBytes(&http.Response{StatusCode: 503}, []byte(body))
			if !httpFailure.transient || httpFailure.status != 503 {
				t.Fatalf("malformed HTTP body must retain server-status policy: %+v", httpFailure)
			}
			f := classifyGeminiSSEError(body)
			if f == nil || !f.transient || f.status != 502 {
				t.Fatalf("detected malformed SSE error must remain a failure: %+v", f)
			}
		})
	}
}

// A body that is not a Google envelope, or decodes as one without error.status,
// has no Google identity to key on, so it is judged like any other route's: by
// HTTP status, the Anthropic error type, and request-shape phrases.
func TestGeminiNonEnvelopeBodyUsesSharedVerdict(t *testing.T) {
	for _, tc := range []struct {
		name      string
		http      int
		body      string
		transient bool
		status    int
		code      string
	}{
		{"plain-text-400", 400, "not JSON", false, 400, "http_400"},
		{"truncated-json-400", 400, `{"error":{"code":400,`, false, 400, "http_400"},
		{"plain-text-401", 401, "Unauthorized", false, 401, "http_401"},
		{"request-shape-phrase", 400, "<html>prompt is too long</html>", false, 400, "request_shape"},
		{"string-code-with-transient-type", 400, `{"error":{"type":"overloaded_error","code":"overloaded","message":"busy"}}`, true, 502, "retryable_4xx_400"},
		{"anthropic-shaped-transient-on-401", 401, `{"error":{"type":"overloaded_error","message":"busy"}}`, true, 502, "retryable_4xx_401"},
		{"anthropic-shaped-auth-401", 401, `{"error":{"type":"authentication_error","message":"bad key"}}`, false, 401, "http_401"},
		{"plain-text-429", 429, "Too Many Requests", true, 429, "rate_limit"},
		{"request-shape-phrase-on-429", 429, "not JSON: prompt is too long", false, 429, "http_429"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := classifyGeminiHTTPErrorBytes(&http.Response{StatusCode: tc.http}, []byte(tc.body))
			if f.transient != tc.transient || f.status != tc.status || f.code != tc.code {
				t.Fatalf("want transient=%v status=%d code=%s, got %+v", tc.transient, tc.status, tc.code, f)
			}
		})
	}
}

func TestWriteGeminiError(t *testing.T) {
	cases := []struct {
		name          string
		status        int
		atype         string
		canRetry      bool
		retryAfter    int
		wantStatus    int
		wantCanonical string
	}{
		{"retry-surface", 503, "api_error", true, 7, 503, "UNAVAILABLE"},
		{"forced-400-keeps-quota", 400, "RESOURCE_EXHAUSTED", false, 0, 400, "RESOURCE_EXHAUSTED"},
		{"forced-400-keeps-unavailable", 400, "UNAVAILABLE", false, 0, 400, "UNAVAILABLE"},
		{"forced-400-keeps-normalized-quota", 400, " resource_exhausted ", false, 0, 400, "RESOURCE_EXHAUSTED"},
		{"invalid-request", 400, "invalid_request_error", false, 0, 400, "INVALID_ARGUMENT"},
		{"native-precondition", 400, "FAILED_PRECONDITION", false, 0, 400, "FAILED_PRECONDITION"},
		{"normalized-native-precondition", 400, " failed_precondition ", false, 0, 400, "FAILED_PRECONDITION"},
		{"not-found", 404, "api_error", false, 0, 404, "NOT_FOUND"},
		{"auth", 401, "authentication_error", false, 0, 401, "UNAUTHENTICATED"},
		{"permission", 403, "permission_error", false, 0, 403, "PERMISSION_DENIED"},
		{"quota", 429, "rate_limit_error", false, 0, 429, "RESOURCE_EXHAUSTED"},
		{"internal", 500, "api_error", false, 0, 500, "INTERNAL"},
		{"deadline", 504, "timeout_error", false, 0, 504, "DEADLINE_EXCEEDED"},
		{"canonical-status-survives-http-mismatch", 404, "INVALID_ARGUMENT", false, 0, 404, "INVALID_ARGUMENT"},
		{"non-canonical-status-derived", 404, "NEW_PROVIDER_ERROR", false, 0, 404, "NOT_FOUND"},
		{"ok-is-not-an-error-status", 503, "OK", false, 0, 503, "UNAVAILABLE"},
		{"default-status", 0, "api_error", false, 0, 502, "UNKNOWN"},
	}
	message := "steady-proxy: quoted \"failure\"\nwith detail"
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeGeminiError(rec, tc.canRetry, tc.status, tc.atype, message, tc.retryAfter, "gemini_test_reason")
			if rec.Code != tc.wantStatus || rec.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("wrong HTTP surface: status=%d headers=%v", rec.Code, rec.Header())
			}
			wantRetry := "false"
			if tc.canRetry {
				wantRetry = "true"
			}
			if rec.Header().Get("X-Should-Retry") != wantRetry || rec.Header().Get("X-Steady-Proxy-Reason") != "gemini_test_reason" {
				t.Fatalf("retry/reason headers lost: %v", rec.Header())
			}
			wantDelay := ""
			if tc.retryAfter > 0 {
				wantDelay = itoa(tc.retryAfter)
			}
			if rec.Header().Get("Retry-After") != wantDelay {
				t.Fatalf("Retry-After=%q, want %q", rec.Header().Get("Retry-After"), wantDelay)
			}
			var body map[string]json.RawMessage
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("invalid JSON: %v", err)
			}
			if len(body) != 1 || body["error"] == nil {
				t.Fatalf("want only native error envelope, got %s", rec.Body.String())
			}
			var native map[string]any
			if err := json.Unmarshal(body["error"], &native); err != nil {
				t.Fatal(err)
			}
			if len(native) != 3 || native["code"] != float64(tc.wantStatus) || native["status"] != tc.wantCanonical || native["message"] != message {
				t.Fatalf("want numeric code/string status/message, got %s", rec.Body.String())
			}
		})
	}
}
