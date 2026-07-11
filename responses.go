package main

import (
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

// ─────────────────────────────────────────────────────── openai responses wire ──
//
// The OpenAI Responses API (`POST /v1/responses`, used by Codex with
// wire_api="responses") streams SSE events framed as
//
//	event: response.output_text.delta
//	data: {"type":"response.output_text.delta", ...}
//
// A stream is complete at `response.completed`. `response.incomplete` (a token-capped
// / content-filtered stop) is NOT a completion: Codex raises a retryable stream error
// on it, so the proxy converts it to a retry too. Failures arrive either as a
// top-level `error` event or as a terminal `response.failed` carrying
// `response.error{code,message}` — Codex treats both as fatal and does NOT retry
// them, which is the failure this proxy exists to absorb. Unlike the Anthropic wire there is no tool-input-JSON
// coalescing to do (Codex accumulates argument deltas itself) and no refusal
// fallback (it targets GPT models), so the model just tracks completion, catches
// in-band errors, scrapes usage, and forwards raw bytes.
type openaiWire struct {
	sawCompleted bool // a valid response.completed seen -> deliverable (incomplete is a retry, not this)
	sawContent   bool // a real output/reasoning/tool event seen -> early commit OK
}

func newOpenAIWire() *openaiWire { return &openaiWire{} }

func (o *openaiWire) scrape(ev event, st *captureStats) {
	if st == nil {
		return
	}
	switch responsesEventType(ev) {
	case "response.created", "response.in_progress":
		var m struct {
			Response struct {
				Model string `json:"model"`
			} `json:"response"`
		}
		if json.Unmarshal([]byte(ev.data), &m) == nil && m.Response.Model != "" {
			st.model = m.Response.Model
		}
	case "response.completed", "response.incomplete", "response.failed":
		var m struct {
			Response struct {
				Model  string `json:"model"`
				Status string `json:"status"`
				Usage  struct {
					Input        int `json:"input_tokens"`
					Output       int `json:"output_tokens"`
					InputDetails struct {
						Cached int `json:"cached_tokens"`
					} `json:"input_tokens_details"`
				} `json:"usage"`
			} `json:"response"`
		}
		if json.Unmarshal([]byte(ev.data), &m) != nil {
			return
		}
		if m.Response.Model != "" {
			st.model = m.Response.Model
		}
		if m.Response.Status != "" {
			st.stop = m.Response.Status
		}
		if in := m.Response.Usage.Input; in > 0 {
			// input_tokens is the TOTAL input incl. the cached portion; split it so
			// inTok totals to input_tokens and cacheRead carries the cached part
			// (mirrors the Anthropic cache_read accounting in captureStats).
			cached := m.Response.Usage.InputDetails.Cached
			nonCached := in - cached
			if nonCached < 0 {
				nonCached, cached = 0, in
			}
			st.setInputUsage(nonCached, 0, cached)
		}
		if out := m.Response.Usage.Output; out > 0 {
			st.outTok = out
		}
	}
}

func (o *openaiWire) buffered(ev event, st *captureStats) (bool, *failure) {
	t := responsesEventType(ev)
	// Terminal error frames are inspected even with JSON validation off — deciding
	// what happens to them is the whole point of this route.
	if isResponsesErrorFrame(t) {
		if fail := o.dispositionError(ev, t); fail != nil {
			return false, fail // convert to a retry / surface non-retryable
		}
		// deliver-native: this terminal error (e.g. context-window) is one Codex
		// recovers from itself, so the buffered stream — including this frame — is
		// handed over byte-for-byte instead of being converted.
		o.sawCompleted = true
		return true, nil
	}
	if t == "response.incomplete" {
		// Codex has an explicit response.incomplete arm that raises a RETRYABLE stream
		// error (ApiError::Stream) — it is NOT a completion. Convert it so the proxy
		// never records success: pre-commit this becomes a retry.
		return false, responsesIncompleteFailure(ev)
	}
	// No per-frame content validation: Codex's Responses parser is lenient — it
	// tolerates missing/null fields (Option<_>) and skips any frame it cannot
	// deserialize, without failing the turn (codex-rs process_sse `continue`). Since
	// the proxy forwards frames verbatim, Codex sees identical bytes and applies that
	// same lenient parse; second-guessing individual content frames here could only
	// diverge from Codex (needlessly converting a stream Codex would accept). The
	// transactional guarantee is at the STREAM level: only a parseable terminal
	// completes it, and a truncated/erroring stream converts to a retry.
	o.advance(ev, t)
	if o.sawCompleted {
		return true, nil
	}
	return false, nil
}

func (o *openaiWire) live(ev event, raw []byte, st *captureStats) ([][]byte, bool, *failure) {
	t := responsesEventType(ev)
	if isResponsesErrorFrame(t) {
		if fail := o.dispositionError(ev, t); fail != nil {
			return nil, false, fail // post-commit -> caller DROPs the stream
		}
		// deliver-native terminal error: forward it verbatim and end.
		o.sawCompleted = true
		return [][]byte{raw}, true, nil
	}
	if t == "response.incomplete" {
		// Post-commit: Codex would raise a retryable stream error on this terminal, so
		// DROP (truncate) rather than forward it as a clean completion; Codex's native
		// stream-retry then re-issues.
		return nil, false, responsesIncompleteFailure(ev)
	}
	// Forward verbatim (see buffered): Codex owns its own lenient per-frame parse, so
	// the proxy does not second-guess content frames on the live path either.
	o.advance(ev, t)
	return [][]byte{raw}, o.sawCompleted, nil
}

func (o *openaiWire) replayBuffered(sp *spool, w io.Writer, st *captureStats) error {
	return sp.replayRaw(w)
}

func (o *openaiWire) replayPrefix(sp *spool, w io.Writer) error { return sp.replayRaw(w) }

func (o *openaiWire) forwardable() bool { return o.sawContent }

// keepalive is a skippable heartbeat for the Responses wire. Codex resets its
// stream idle timer only when its SSE reader yields an EVENT (a spec comment
// yields nothing and is discarded — so a comment cannot keep Codex alive). This
// frame is a well-formed SSE event carrying an unknown `type`, which Codex parses
// and then ignores (process_responses_event's catch-all arm emits nothing and
// touches no output/index state), so it keeps the connection alive without
// perturbing the response. It is injected live only, never buffered, so the
// replayed response stays byte-for-byte faithful.
func (o *openaiWire) keepalive() []byte {
	return []byte("event: response.proxy_keepalive\ndata: {\"type\":\"response.proxy_keepalive\"}\n\n")
}

// advance updates completion/content state from one NON-error event.
func (o *openaiWire) advance(ev event, t string) {
	switch {
	case t == "response.completed":
		// Codex only counts this terminal if its `response` deserializes as
		// ResponseCompleted (see responsesCompletedValid). A completed Codex would
		// reject does NOT terminate the buffered stream, so it stays un-committed and
		// EOF converts it to a truncated_stream retry (which may fetch a clean terminal),
		// exactly as Codex would fail-and-retry rather than silently succeed.
		if responsesCompletedValid(ev) {
			o.sawCompleted = true
		}
	case isResponsesOutputEvent(t):
		// real model output -> an early commit can start streaming live.
		o.sawContent = true
	}
	// Everything else (response.created / in_progress / queued / metadata / any
	// other lifecycle or unknown frame) is deliberately NOT treated as forwardable
	// content: committing on it would push a start-of-stream error past the commit
	// boundary and forfeit the hidden local-retry protection this route exists for.
}

// responsesCompletedValid mirrors Codex's ResponseCompleted deserialize (codex-rs
// codex-api/src/sse/responses.rs): the terminal counts ONLY if `response` is present
// with a non-empty string `id`, AND — when a `usage` object is present — that object
// carries integer `input_tokens`, `output_tokens`, and `total_tokens` (the required
// i64 fields; the *_details sub-objects are optional). A missing/non-string id, or a
// present-but-incomplete usage, is a frame Codex would reject as a stream error, so
// the proxy must not record it as success. `usage` absent or null is fine (Codex's
// field is `#[serde(default)] Option`).
//
// One deliberate stricter-than-Codex check: an EMPTY id ("") is rejected even though
// Codex's `id: String` would deserialize it. The invariant is that a stream replayed
// as a real success must reference an addressable response; a real Responses stream
// always carries a non-empty id, so an empty one is a malformed terminal. Converting
// it to a retry (which may fetch a clean terminal) avoids logging a hollow success and
// cannot loop a legitimate stream, which never emits one.
func responsesCompletedValid(ev event) bool {
	var m struct {
		Response *struct {
			ID    string `json:"id"`
			Usage *struct {
				Input        *int64 `json:"input_tokens"`
				Output       *int64 `json:"output_tokens"`
				Total        *int64 `json:"total_tokens"`
				InputDetails *struct {
					Cached *int64 `json:"cached_tokens"`
				} `json:"input_tokens_details"`
				OutputDetails *struct {
					Reasoning *int64 `json:"reasoning_tokens"`
				} `json:"output_tokens_details"`
			} `json:"usage"`
		} `json:"response"`
	}
	// A non-string id or non-integer usage field fails the unmarshal outright, exactly
	// as it fails Codex's typed parse.
	if json.Unmarshal([]byte(ev.data), &m) != nil || m.Response == nil || m.Response.ID == "" {
		return false
	}
	if u := m.Response.Usage; u != nil {
		if u.Input == nil || u.Output == nil || u.Total == nil {
			return false
		}
		// The *_details objects are optional, but Codex's structs make their fields
		// required when the object is present — so `input_tokens_details: {}` fails
		// Codex's parse. Mirror that: reject a present detail object missing its field.
		if u.InputDetails != nil && u.InputDetails.Cached == nil {
			return false
		}
		if u.OutputDetails != nil && u.OutputDetails.Reasoning == nil {
			return false
		}
	}
	return true
}

// responsesIncompleteFailure converts a response.incomplete terminal into a
// retryable failure, matching how Codex classifies it: its response.incomplete arm
// raises ApiError::Stream (a retryable stream error), NOT a completion. Keyed as
// transient so a pre-commit incomplete rides out as a retry and a post-commit one
// DROPs — never logged as a clean success.
func responsesIncompleteFailure(ev event) *failure {
	return &failure{transient: true, status: 502, atype: "api_error", code: "responses_incomplete",
		message: "incomplete response returned, reason: " + responsesIncompleteReason(ev)}
}

// responsesIncompleteReason best-effort extracts response.incomplete_details.reason
// (max_output_tokens / content_filter / …) for the log; "unknown" if absent.
func responsesIncompleteReason(ev event) string {
	var m struct {
		Response struct {
			IncompleteDetails struct {
				Reason string `json:"reason"`
			} `json:"incomplete_details"`
		} `json:"response"`
	}
	if json.Unmarshal([]byte(ev.data), &m) == nil && m.Response.IncompleteDetails.Reason != "" {
		return m.Response.IncompleteDetails.Reason
	}
	return "unknown"
}

// isResponsesErrorFrame reports whether an event type is a terminal error frame:
// a top-level `error` event or a `response.failed`.
func isResponsesErrorFrame(t string) bool {
	return t == "error" || t == "response.failed"
}

// isResponsesOutputEvent reports whether an event type carries real model output
// (deltas, output items, content parts, tool-call args, reasoning, refusals) —
// the only thing that justifies committing early and streaming live. This is an
// allowlist on purpose: an unknown lifecycle/metadata frame must default to
// non-forwardable so it can never trigger a premature commit; the worst case for a
// genuinely-new output event is that the stream waits out the (short) commit
// window instead of committing on first byte.
func isResponsesOutputEvent(t string) bool {
	switch {
	case strings.HasPrefix(t, "response.output_item"),
		strings.HasPrefix(t, "response.content_part"),
		strings.HasPrefix(t, "response.output_text"),
		strings.HasPrefix(t, "response.output_audio"),
		strings.HasPrefix(t, "response.function_call"),
		strings.HasPrefix(t, "response.custom_tool_call"),
		strings.HasPrefix(t, "response.reasoning"),
		strings.HasPrefix(t, "response.refusal"),
		// Forward-compatible with a new `response.*.delta` family, but bounded: it must
		// be a response-namespaced delta, not any type that merely contains ".delta"
		// (which could let an unrelated frame trip an early commit past the boundary).
		strings.HasPrefix(t, "response.") && strings.HasSuffix(t, ".delta"):
		return true
	}
	return false
}

// dispositionError decides what to do with a terminal error frame:
//
//	fail != nil -> convert (retry / surface non-retryable); the frame is NOT
//	               forwarded to Codex.
//	fail == nil -> deliver the native frame to Codex, which recovers from it on
//	               its own (context-window exhaustion drives Codex's compaction path;
//	               converting it to a synthetic error would hide it from that handler).
//
// Native delivery is gated on the frame being a `response.failed`: Codex's
// context-compaction (is_context_window_error) lives ONLY in its response.failed
// branch. A top-level `error` event is not matched by Codex's parser at all (it is
// ignored, then the stream fails EOF-before-completion), so forwarding one natively
// would yield a generic retry, never compaction — such an error is classified and
// converted instead (a context error there surfaces as a deterministic 400).
func (o *openaiWire) dispositionError(ev event, t string) *failure {
	typ, code, msg := parseResponsesError(ev, t)
	if t == "response.failed" && isCodexRecoverableError(code) {
		return nil
	}
	return classifyResponsesError(typ, code, msg)
}

// parseResponsesError extracts (type, code, message) from either terminal error
// shape: `response.failed.response.error{...}`, or the top-level `error` event —
// which may carry the fields at the top level (standard) or nested under `error`
// (the variant some gateways emit).
func parseResponsesError(ev event, t string) (typ, code, msg string) {
	if t == "response.failed" {
		var e struct {
			Response struct {
				Error struct{ Type, Code, Message string } `json:"error"`
			} `json:"response"`
		}
		_ = json.Unmarshal([]byte(ev.data), &e)
		return e.Response.Error.Type, e.Response.Error.Code, e.Response.Error.Message
	}
	var e struct {
		Code    string                               `json:"code"`    // standard: top-level
		Message string                               `json:"message"` // standard: top-level
		Error   struct{ Type, Code, Message string } `json:"error"`   // gateway: nested
	}
	_ = json.Unmarshal([]byte(ev.data), &e)
	return e.Error.Type, firstNonEmpty(e.Error.Code, e.Code), firstNonEmpty(e.Error.Message, e.Message)
}

// isCodexRecoverableError reports whether a terminal error is one Codex handles
// natively, so the proxy must deliver the original SSE frame rather than convert
// it. Codex's Responses parser maps EXACTLY the code `context_length_exceeded`
// onto its ContextWindowExceeded/compaction path (see codex-rs
// codex-api/src/sse/responses.rs `is_context_window_error`) — no message-based or
// near-synonym matching. Anything broader would forward a frame Codex treats as a
// generic (retryable) stream error, looping the unchanged oversized request to its
// stream-retry cap; those instead fall through to classifyResponsesError, which
// surfaces them deterministically (string_above_max_length / a context message
// are request-shape via isResponsesRequestShapeCode / requestShapeSigs). The caller
// gates this on the frame being a response.failed, the only place Codex maps the
// code to compaction.
func isCodexRecoverableError(code string) bool {
	return strings.TrimSpace(code) == "context_length_exceeded"
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// responsesEventType returns the event's type, read from the JSON `data` payload
// ONLY. Codex deserializes each frame's data into a struct whose `type` field is
// required and ignores the SSE `event:` line entirely (codex-rs
// codex-api/src/sse/responses.rs), so the proxy reads type the same way: a frame
// with no parseable JSON type is one Codex skips, and returning "" here makes the
// proxy treat it as a non-terminal frame it forwards verbatim (Codex then skips it
// exactly as it would on a direct connection) rather than a terminal it acts on.
// Keying on the `event:` name would let the proxy disagree with Codex — either
// mis-reading a generic `event: message` frame (whose real kind is in JSON) or
// crediting a type to a frame Codex cannot classify.
func responsesEventType(ev event) string {
	var t struct {
		Type string `json:"type"`
	}
	if json.Unmarshal([]byte(ev.data), &t) == nil {
		return t.Type
	}
	return ""
}

// classifyResponsesError maps an OpenAI Responses error (type/code/message) to a
// failure. Same principle as the Anthropic path: ride out everything transient
// (overload, rate limit, capacity, auth, api errors) and surface ONLY a
// deterministic request-shape error, which can never succeed as written.
func classifyResponsesError(errType, errCode, message string) *failure {
	t := strings.ToLower(strings.TrimSpace(errType))
	c := strings.ToLower(strings.TrimSpace(errCode))
	msg := strings.ToLower(message)
	switch {
	case t == "invalid_request_error" || isResponsesRequestShapeCode(c) || anyContains(msg, requestShapeSigs):
		return &failure{transient: false, status: 400, atype: "invalid_request_error", code: "responses_request_shape", message: message}
	case t == "rate_limit_error" || c == "rate_limit_exceeded":
		// The advertised delay is commonly only in the message ("Please try again
		// in 11.054s"). Preserve it so both hidden local retries and the surfaced
		// 503 wait the real amount instead of the proxy's short fallback backoff.
		return &failure{transient: true, status: 429, atype: "rate_limit_error", code: "responses_rate_limit", message: message, retryAfter: parseRetryAfterFromMessage(message)}
	case t == "overloaded_error" || t == "service_unavailable_error" || c == "server_is_overloaded" || c == "overloaded":
		return &failure{transient: true, status: 529, atype: "overloaded_error", code: "responses_overloaded", message: message}
	default:
		// api_error, server_error, timeout, authentication, permission, not_found,
		// unknown -> transient: a temporary block is ridden out, not surfaced.
		return &failure{transient: true, status: 502, atype: "api_error", code: "responses_error", message: message}
	}
}

// retryAfterMsgRe extracts an advertised delay from a rate-limit message such as
// "Please try again in 11.054s" / "try again in 500ms" / "try again in 2m".
var retryAfterMsgRe = regexp.MustCompile(`(?i)try again in\s+([0-9]+(?:\.[0-9]+)?)\s*(ms|s|m)`)

// parseRetryAfterFromMessage returns the advertised retry delay in whole seconds
// (rounded up, min 1), or 0 when the message carries none.
func parseRetryAfterFromMessage(message string) int {
	m := retryAfterMsgRe.FindStringSubmatch(message)
	if m == nil {
		return 0
	}
	val, err := strconv.ParseFloat(m[1], 64)
	if err != nil || val <= 0 {
		return 0
	}
	switch strings.ToLower(m[2]) {
	case "ms":
		val /= 1000
	case "m":
		val *= 60
	}
	secs := int(val)
	if float64(secs) < val { // round up any fractional second
		secs++
	}
	if secs < 1 {
		secs = 1
	}
	return secs
}

// isResponsesRequestShapeCode reports whether an OpenAI error `code` names a
// deterministic request-shape failure that can never succeed on retry. Keying on
// the code (not the type or a message substring) is essential because a
// `response.failed` error object commonly carries only `code` + `message`.
func isResponsesRequestShapeCode(code string) bool {
	switch code {
	case "context_length_exceeded", "string_above_max_length", "invalid_prompt",
		"invalid_image", "invalid_image_format", "invalid_image_url",
		"invalid_base64_image", "image_parse_error", "image_too_large",
		"unknown_parameter", "unsupported_parameter", "missing_required_parameter",
		"model_not_found", "unsupported_value", "invalid_type",
		// Deterministic policy blocks: Codex maps these to non-retryable
		// InvalidRequest / CyberPolicy and stops, so surface them rather than loop.
		"bio_policy", "cyber_policy":
		return true
	}
	return false
}

// classifyResponsesHTTPErrorBytes classifies a pre-SSE HTTP error for the OpenAI
// Responses route. It shares the status/header handling with the Anthropic path
// but routes the 4xx request-shape decision through classifyResponsesError, so
// there is a SINGLE OpenAI error brain (keyed on type+code+message) for both
// in-band SSE errors and HTTP-level errors — an OpenAI request-shape can never
// slip through as "retry forever" on one path but not the other.
func classifyResponsesHTTPErrorBytes(resp *http.Response, b []byte) failure {
	f := classifyHTTPErrorBytesShaped(resp, b, responsesRequestShaped)
	// The advertised backoff for an OpenAI rate limit is commonly only in the error
	// message ("Please try again in 11.054s"), not a Retry-After header. The shared
	// classifier's status branches (429/5xx) read only the header, so parse the
	// message here to fill a missing delay — the SAME parser the in-band SSE path
	// uses in classifyResponsesError, so a hidden retry and a surfaced Retry-After
	// wait the real amount on BOTH the HTTP and SSE paths. A header, when present,
	// is authoritative and left untouched.
	if f.transient && f.retryAfter == 0 {
		if d := parseRetryAfterFromMessage(f.message); d > 0 {
			f.retryAfter = d
		}
	}
	return f
}

func responsesRequestShaped(atype, code, msg, _ string) bool {
	return !classifyResponsesError(atype, code, msg).transient
}

// classifyHTTPErrorForPath dispatches HTTP-error classification by wire.
func classifyHTTPErrorForPath(path string, resp *http.Response, b []byte) failure {
	if isResponsesPath(path) {
		return classifyResponsesHTTPErrorBytes(resp, b)
	}
	return classifyHTTPErrorBytes(resp, b)
}

// ─────────────────────────────────────────────────────────── error rendering ──

// errorWriter renders a proxy-originated error in a wire-appropriate shape, with
// identical retry signalling headers (X-Should-Retry / Retry-After / reason).
type errorWriter func(w http.ResponseWriter, canRetry bool, status int, atype, message string, retryAfter int, code string)

func errorWriterFor(path string) errorWriter {
	if isResponsesPath(path) {
		return writeOpenAIError
	}
	return writeAnthropicError
}

// writeOpenAIError emits an OpenAI-shaped error body: {"error":{message,type,...}}.
func writeOpenAIError(w http.ResponseWriter, canRetry bool, status int, atype, message string, retryAfter int, code string) {
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
	h.Set("X-CC-Retry-Proxy-Reason", code)
	if status <= 0 {
		status = http.StatusBadGateway
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    atype,
			"code":    nil,
			"param":   nil,
		},
	})
}
