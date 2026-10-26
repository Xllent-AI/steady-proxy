package main

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ─────────────────────────────────────────────────────── openai responses wire ──
//
// The OpenAI Responses API (`POST /v1/responses`, used by Codex with
// wire_api="responses") streams SSE events framed as
//
//	event: response.output_text.delta
//	data: {"type":"response.output_text.delta", ...}
//
// A stream ends at one of two terminals, and the proxy delivers either verbatim:
// `response.completed`, or `response.incomplete` (a token-capped / content-filtered
// stop). The OpenAI API defines incomplete as a legitimate end of the turn and Codex
// >= 0.163 treats it as terminal (openai/codex PR #53451, issue #38831); older Codex
// retries it on its own, so a proxy retry would only repeat that and re-bill the
// generation. A terminal snapshot that Codex's ResponseCompleted parse would reject
// (a response.completed, or an incomplete with reason "interrupted", which Codex
// parses as a completion) fails at that event as responses_invalid_completed.
//
// Failures arrive either as a top-level `error` event or as a terminal
// `response.failed` carrying `response.error{code,message}` — Codex treats both as
// fatal and does NOT retry them, which is the failure this proxy exists to absorb. A
// transient one is retried before commit and DROPped after it; a terminal one is
// surfaced before commit and delivered in-stream after it.
//
// Unlike the Anthropic wire there is no tool-input-JSON coalescing to do (Codex
// accumulates argument deltas itself) and no refusal fallback (it targets GPT
// models), so the model just tracks content, catches in-band errors, scrapes usage,
// and forwards raw bytes.
type openaiWire struct {
	sawContent       bool // a real output/reasoning/tool event seen -> early commit OK
	replayDeltaBytes int  // buffered/prefix replay coalescing target; 0 preserves raw frames
}

func newOpenAIWire() *openaiWire {
	return &openaiWire{replayDeltaBytes: cfg.responsesReplayBytes}
}

func (o *openaiWire) completeOnEOF() bool { return false }

func (o *openaiWire) scrape(ev event, st *captureStats) {
	if st == nil {
		return
	}
	typ := responsesEventType(ev)
	switch typ {
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
				IncompleteDetails struct {
					Reason string `json:"reason"`
				} `json:"incomplete_details"`
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
		if typ == "response.incomplete" {
			// The reason (max_output_tokens / content_filter / …) is what tells an
			// operator why the turn ended short, so it rides along in the stop field.
			st.stop = "incomplete"
			if r := m.Response.IncompleteDetails.Reason; r != "" {
				st.stop += ":" + r
			}
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
	switch {
	case isResponsesErrorFrame(t):
		// Terminal error frames are inspected even with JSON validation off — deciding
		// what happens to them is the whole point of this route.
		fail, native := classifyResponsesFrame(ev, t)
		if !native {
			return false, fail // convert to a retry / surface non-retryable
		}
		// deliver-native: this terminal error (e.g. context-window) is one Codex
		// recovers from itself, so the buffered stream — including this frame — is
		// handed over byte-for-byte instead of being converted.
		return true, nil
	case t == "response.completed" || t == "response.incomplete":
		// Fail HERE, under its own code, instead of leaving the stream unterminated for
		// EOF to report as truncated_stream: the stream did terminate, and the log must
		// name the malformed terminal. Pre-commit this becomes a retry. A valid
		// incomplete is a legitimate terminal (see the file header), delivered verbatim.
		if !responsesTerminalValid(ev, t) {
			return false, responsesInvalidCompleted()
		}
		return true, nil
	}
	// No per-frame content validation: Codex's Responses parser is lenient — it
	// tolerates missing/null fields (Option<_>) and skips any frame it cannot
	// deserialize, without failing the turn (codex-rs process_sse `continue`). Since
	// the proxy forwards frames verbatim, Codex sees identical bytes and applies that
	// same lenient parse; second-guessing individual content frames here could only
	// diverge from Codex (needlessly converting a stream Codex would accept). The
	// transactional guarantee is at the STREAM level: only a parseable terminal
	// completes it, and a truncated/erroring stream converts to a retry.
	//
	// Only real model output makes the stream forwardable. Every other frame
	// (response.created / in_progress / queued / metadata / unknown) deliberately is
	// not: committing on it would push a start-of-stream error past the commit
	// boundary and forfeit the hidden local-retry protection this route exists for.
	if isResponsesOutputEvent(t) {
		o.sawContent = true
	}
	return false, nil
}

func (o *openaiWire) live(ev event, raw []byte, st *captureStats) ([][]byte, bool, *failure) {
	t := responsesEventType(ev)
	switch {
	case isResponsesErrorFrame(t):
		fail, native := classifyResponsesFrame(ev, t)
		if fail.transient && !native {
			// The fault may clear on a re-send, but the response is already streaming:
			// DROP so the client's own stream retry re-issues the request.
			return nil, false, fail
		}
		// A terminal verdict is the upstream's final answer and an identical re-send
		// fails the same way, so a DROP would only send the client into a retry loop.
		// Forward the upstream's own frame — Codex maps cyber_policy, invalid_prompt,
		// insufficient_quota, context_length_exceeded … to its fatal errors itself —
		// and mark it delivered so the stream ends cleanly.
		fail.delivered = true
		return [][]byte{raw}, true, fail
	case t == "response.completed" || t == "response.incomplete":
		if !responsesTerminalValid(ev, t) {
			// Forward the frame — it is what the upstream sent, and the client applies
			// its own parse exactly as on a direct connection — but do not count it as
			// a completion: fail at this event so the handler DROPs (delivered=false)
			// rather than logging a clean success.
			return [][]byte{raw}, false, responsesInvalidCompleted()
		}
		return [][]byte{raw}, true, nil
	}
	// Forward verbatim (see buffered): Codex owns its own lenient per-frame parse, so
	// the proxy does not second-guess content frames on the live path either.
	return [][]byte{raw}, false, nil
}

func (o *openaiWire) replayBuffered(sp *spool, w io.Writer, st *captureStats) error {
	return o.replay(sp, w)
}

func (o *openaiWire) replayPrefix(sp *spool, w io.Writer) error { return o.replay(sp, w) }

// replay preserves the transactional guarantee while preventing its successful
// commit from becoming an event storm. Some Responses gateways emit one
// response.output_text.delta per token fragment (often only 1-15 bytes). A fully
// buffered turn would otherwise replay thousands of those frames in one write;
// Codex turns each frame into an in-process app-server notification, whose bounded
// queue can overflow before the TUI drains it.
//
// Only adjacent, structurally compatible text deltas for the same item/output/
// content route are combined. Non-text events are ordering barriers. Live events
// are still forwarded verbatim by live(), and setting the limit to 0 restores raw
// replay. The coalescer is streaming, so a spool that spilled to disk is not read
// wholesale back into memory.
func (o *openaiWire) replay(sp *spool, w io.Writer) error {
	if o.replayDeltaBytes <= 0 {
		return sp.replayRaw(w)
	}
	r, err := sp.reader()
	if err != nil {
		return err
	}
	return replayResponsesReader(r, w, o.replayDeltaBytes)
}

// replayResponsesReader frames only events small enough to coalesce. Once a
// frame exceeds the grouping budget it flushes any pending text and copies the
// rest of that frame as it is read. This is deliberately separate from
// sseParser: that parser must materialize a complete event for live validation,
// whereas replay already holds a validated stream on disk and must not pull a
// 128 MiB response.completed frame back into RAM merely to pass it through.
func replayResponsesReader(r io.Reader, w io.Writer, maxGroupBytes int) error {
	if maxGroupBytes <= 0 {
		_, err := io.Copy(w, r)
		return err
	}
	c := responsesReplayCoalescer{w: w, maxGroupBytes: maxGroupBytes}
	frame := make([]byte, 0, min(maxGroupBytes, 32*1024))
	passthrough := false
	appendBytes := func(chunk []byte) error {
		if len(chunk) == 0 {
			return nil
		}
		if !passthrough && len(frame)+len(chunk) > maxGroupBytes {
			if err := c.flush(); err != nil {
				return err
			}
			if err := writeResponsesReplayBytes(w, frame); err != nil {
				return err
			}
			frame = frame[:0]
			passthrough = true
		}
		if passthrough {
			return writeResponsesReplayBytes(w, chunk)
		}
		frame = append(frame, chunk...)
		return nil
	}
	finishFrame := func() error {
		if !passthrough {
			if err := c.acceptFrame(frame); err != nil {
				return err
			}
		}
		frame = frame[:0]
		passthrough = false
		return nil
	}
	var buf [32 * 1024]byte
	lineHasData, pendingCR, frameDone := false, false, false
	for {
		n, readErr := r.Read(buf[:])
		start := 0
		for i, b := range buf[:n] {
			if pendingCR {
				pendingCR = false
				end := i
				if b == '\n' {
					end++
				}
				if frameDone {
					if err := appendBytes(buf[start:end]); err != nil {
						return err
					}
					if err := finishFrame(); err != nil {
						return err
					}
					start, frameDone = end, false
				}
				if b == '\n' {
					continue
				}
			}
			switch b {
			case '\r':
				// Keep a following LF in the same raw frame across reader chunks.
				pendingCR, frameDone = true, !lineHasData
				lineHasData = false
			case '\n':
				if !lineHasData {
					if err := appendBytes(buf[start : i+1]); err != nil {
						return err
					}
					if err := finishFrame(); err != nil {
						return err
					}
					start = i + 1
				}
				lineHasData = false
			default:
				lineHasData = true
			}
		}
		if err := appendBytes(buf[start:n]); err != nil {
			return err
		}
		if readErr != nil {
			if readErr != io.EOF {
				return readErr
			}
			if pendingCR && frameDone {
				if err := finishFrame(); err != nil {
					return err
				}
			}
			// Preserve a defensive unterminated tail as an opaque barrier.
			if err := c.flush(); err != nil {
				return err
			}
			return writeResponsesReplayBytes(w, frame)
		}
	}
}

func (c *responsesReplayCoalescer) acceptFrame(raw []byte) error {
	events := (&sseParser{}).feedChunk(raw, true)
	if len(events) != 1 || len(events[0].raw) != len(raw) {
		if err := c.flush(); err != nil {
			return err
		}
		return writeResponsesReplayBytes(c.w, raw)
	}
	return c.accept(events[0])
}

func writeResponsesReplayBytes(w io.Writer, p []byte) error {
	n, err := w.Write(p)
	if err == nil && n != len(p) {
		return io.ErrShortWrite
	}
	return err
}

type responsesReplayCoalescer struct {
	w             io.Writer
	maxGroupBytes int
	pending       *responsesTextDeltaGroup
}

type responsesTextDeltaGroup struct {
	eventName      string
	fields         map[string]json.RawMessage
	raw            []byte
	delta          strings.Builder
	payloadBytes   int
	count          int
	itemID         string
	outputIndex    int64
	contentIndex   int64
	hasLogprobs    bool
	hasSequence    bool
	hasObfuscation bool
}

func (c *responsesReplayCoalescer) accept(ev event) error {
	// Do not decode a single event larger than the grouping budget. Apart from
	// being impossible to combine within the bound, retaining its decoded map and
	// logprobs alongside the raw frame would needlessly pull a potentially huge
	// disk-spooled payload back into memory.
	if len(ev.data) > c.maxGroupBytes {
		if err := c.flush(); err != nil {
			return err
		}
		_, err := c.w.Write(ev.raw)
		return err
	}
	d, ok := parseResponsesTextDelta(ev)
	if !ok {
		if err := c.flush(); err != nil {
			return err
		}
		_, err := c.w.Write(ev.raw)
		return err
	}
	if c.pending == nil {
		c.pending = d
		return nil
	}
	if !c.pending.compatible(d) || c.pending.payloadBytes+d.payloadBytes > c.maxGroupBytes {
		if err := c.flush(); err != nil {
			return err
		}
		c.pending = d
		return nil
	}
	c.pending.merge(d)
	return nil
}

func (c *responsesReplayCoalescer) flush() error {
	g := c.pending
	if g == nil {
		return nil
	}
	c.pending = nil
	if g.count == 1 {
		_, err := c.w.Write(g.raw)
		return err
	}
	delta, err := json.Marshal(g.delta.String())
	if err != nil {
		return err
	}
	g.fields["delta"] = delta
	data, err := json.Marshal(g.fields)
	if err != nil {
		return err
	}
	if g.eventName != "" {
		if _, err := io.WriteString(c.w, "event: "+g.eventName+"\n"); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(c.w, "data: "); err != nil {
		return err
	}
	if _, err := c.w.Write(data); err != nil {
		return err
	}
	_, err = io.WriteString(c.w, "\n\n")
	return err
}

func parseResponsesTextDelta(ev event) (*responsesTextDeltaGroup, bool) {
	if !simpleResponsesSSEFrame(ev) {
		return nil, false
	}
	fields, ok := decodeUniqueResponsesObject(ev.data)
	if !ok || responsesString(fields["type"]) != "response.output_text.delta" {
		return nil, false
	}
	for key := range fields {
		switch key {
		case "type", "item_id", "output_index", "content_index", "delta", "logprobs", "sequence_number", "obfuscation":
		default:
			// Unknown fields may gain semantics in a future Responses revision. Keep
			// such an event byte-for-byte instead of guessing how to aggregate it.
			return nil, false
		}
	}
	var itemID, delta string
	var outputIndex, contentIndex int64
	if raw, ok := fields["item_id"]; !ok || json.Unmarshal(raw, &itemID) != nil {
		return nil, false
	}
	if raw, ok := fields["output_index"]; !ok || json.Unmarshal(raw, &outputIndex) != nil {
		return nil, false
	}
	if raw, ok := fields["content_index"]; !ok || json.Unmarshal(raw, &contentIndex) != nil {
		return nil, false
	}
	if raw, ok := fields["delta"]; !ok || json.Unmarshal(raw, &delta) != nil {
		return nil, false
	}
	g := &responsesTextDeltaGroup{
		eventName: ev.name, fields: fields, raw: ev.raw,
		payloadBytes: len(ev.data), count: 1, itemID: itemID,
		outputIndex: outputIndex, contentIndex: contentIndex,
	}
	g.delta.WriteString(delta)
	if raw, ok := fields["logprobs"]; ok {
		var logprobs []json.RawMessage
		if json.Unmarshal(raw, &logprobs) != nil || logprobs == nil || len(logprobs) != 0 {
			// Codex ignores logprobs for text delivery, but other Responses clients
			// validate their nested schema. Keep every non-empty (or non-array)
			// payload raw rather than let one malformed entry invalidate valid text
			// that would otherwise be combined into the same synthesized event.
			return nil, false
		}
		g.hasLogprobs = true
	}
	if raw, ok := fields["sequence_number"]; ok {
		var sequence int64
		if json.Unmarshal(raw, &sequence) != nil {
			return nil, false
		}
		g.hasSequence = true
	}
	if raw, ok := fields["obfuscation"]; ok {
		var obfuscation string
		if json.Unmarshal(raw, &obfuscation) != nil {
			return nil, false
		}
		g.hasObfuscation = true
	}
	return g, true
}

// decodeUniqueResponsesObject deliberately does not use json.Unmarshal into a
// map: that silently accepts duplicate object keys and keeps the last value,
// while Codex's serde event structs reject duplicate known fields and skip the
// whole frame. The same applies to typed nested objects such as logprobs:
// combining one malformed frame with valid neighbors could make Codex skip all
// of their text. Treat a duplicate key at any depth as an opaque ordering
// barrier instead.
func decodeUniqueResponsesObject(data string) (map[string]json.RawMessage, bool) {
	return decodeResponsesObject(data, true)
}

// Envelope duplicates are invalid only for fields recognized by Codex. Nested
// response/error/item values are opaque until snapshot validation; coalescing
// instead requires every key to be unique so it never rewrites ambiguous data.
func decodeResponsesObject(data string, uniqueNested bool) (map[string]json.RawMessage, bool) {
	if !validResponsesJSONUnicode(data) {
		return nil, false
	}
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()
	opening, err := dec.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, false
	}
	fields := make(map[string]json.RawMessage)
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, false
		}
		key, ok := tok.(string)
		if !ok {
			return nil, false
		}
		if _, duplicate := fields[key]; duplicate {
			if uniqueNested {
				return nil, false
			}
			switch key {
			case "type", "headers", "metadata", "response", "error", "item", "item_id",
				"call_id", "delta", "text", "summary_index", "content_index", "safety_buffering":
				return nil, false
			}
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, false
		}
		if uniqueNested && !uniqueResponsesJSONValue(string(value)) {
			return nil, false
		}
		fields[key] = value
	}
	closing, err := dec.Token()
	if err != nil || closing != json.Delim('}') {
		return nil, false
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, false
	}
	return fields, true
}

// encoding/json deliberately replaces invalid UTF-8 and lone UTF-16 surrogate
// escapes with U+FFFD. serde_json, which Codex uses, rejects those strings and
// skips the event. Reject them before decoding so coalescing cannot turn a frame
// Codex would ignore into visible replacement text.
func validResponsesJSONUnicode(data string) bool {
	if !utf8.ValidString(data) {
		return false
	}
	inString := false
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case '"':
			inString = !inString
		case '\\':
			if !inString || i+1 >= len(data) {
				continue
			}
			i++
			if data[i] != 'u' {
				continue
			}
			if i+4 >= len(data) {
				return false
			}
			code, ok := responsesHex4(data[i+1 : i+5])
			if !ok {
				return false
			}
			i += 4
			switch {
			case code >= 0xd800 && code <= 0xdbff:
				if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
					return false
				}
				low, ok := responsesHex4(data[i+3 : i+7])
				if !ok || low < 0xdc00 || low > 0xdfff {
					return false
				}
				i += 6
			case code >= 0xdc00 && code <= 0xdfff:
				return false
			}
		}
	}
	return true
}

func responsesHex4(s string) (uint16, bool) {
	if len(s) != 4 {
		return 0, false
	}
	var value uint16
	for i := 0; i < 4; i++ {
		value <<= 4
		switch c := s[i]; {
		case c >= '0' && c <= '9':
			value |= uint16(c - '0')
		case c >= 'a' && c <= 'f':
			value |= uint16(c-'a') + 10
		case c >= 'A' && c <= 'F':
			value |= uint16(c-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func uniqueResponsesJSONValue(data string) bool {
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()
	if !consumeUniqueResponsesJSONValue(dec) {
		return false
	}
	_, err := dec.Token()
	return err == io.EOF
}

func consumeUniqueResponsesJSONValue(dec *json.Decoder) bool {
	tok, err := dec.Token()
	if err != nil {
		return false
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return true
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return false
			}
			key, ok := keyToken.(string)
			if !ok {
				return false
			}
			if _, duplicate := seen[key]; duplicate {
				return false
			}
			seen[key] = struct{}{}
			if !consumeUniqueResponsesJSONValue(dec) {
				return false
			}
		}
		closing, err := dec.Token()
		return err == nil && closing == json.Delim('}')
	case '[':
		for dec.More() {
			if !consumeUniqueResponsesJSONValue(dec) {
				return false
			}
		}
		closing, err := dec.Token()
		return err == nil && closing == json.Delim(']')
	default:
		return false
	}
}

func (g *responsesTextDeltaGroup) compatible(next *responsesTextDeltaGroup) bool {
	return g.eventName == next.eventName &&
		g.itemID == next.itemID &&
		g.outputIndex == next.outputIndex &&
		g.contentIndex == next.contentIndex &&
		g.hasLogprobs == next.hasLogprobs &&
		g.hasSequence == next.hasSequence &&
		g.hasObfuscation == next.hasObfuscation
}

func (g *responsesTextDeltaGroup) merge(next *responsesTextDeltaGroup) {
	g.delta.WriteString(next.delta.String())
	g.payloadBytes += next.payloadBytes
	g.count += next.count
	// The combined event represents the stream through the last constituent
	// delta, so retain that event's monotonic sequence and opaque padding value.
	if g.hasSequence {
		g.fields["sequence_number"] = next.fields["sequence_number"]
	}
	if g.hasObfuscation {
		g.fields["obfuscation"] = next.fields["obfuscation"]
	}
}

// simpleResponsesSSEFrame makes synthesis conservative: if an upstream frame
// carries id/retry/custom SSE fields or multiline data, keep it raw. Normal
// Responses frames contain exactly one optional event field and one data field.
func simpleResponsesSSEFrame(ev event) bool {
	raw := strings.ReplaceAll(string(ev.raw), "\r\n", "\n")
	raw = strings.ReplaceAll(raw, "\r", "\n")
	eventLines, dataLines := 0, 0
	for _, line := range strings.Split(raw, "\n") {
		if line == "" {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if !found {
			return false
		}
		if strings.HasPrefix(value, " ") {
			value = value[1:]
		}
		switch field {
		case "event":
			eventLines++
			if value != ev.name {
				return false
			}
		case "data":
			dataLines++
			if value != ev.data {
				return false
			}
		default:
			return false
		}
	}
	if ev.name == "" {
		return eventLines == 0 && dataLines == 1
	}
	return eventLines == 1 && dataLines == 1
}

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

// responsesCompletedValid mirrors Codex's ResponseCompleted deserialize (codex-rs
// codex-api/src/sse/responses.rs): the terminal counts ONLY if `response` is present
// with a non-empty string `id`, AND — when a `usage` object is present — that object
// carries integer `input_tokens`, `output_tokens`, and `total_tokens` (the required
// i64 fields; the *_details sub-objects are optional). A missing/non-string id, or a
// present-but-incomplete usage, is a frame Codex would reject as a stream error, so
// the proxy must not record it as success: the caller fails the stream at that event
// (responsesInvalidCompleted). `usage` absent or null is fine (Codex's field is
// `#[serde(default)] Option`).
//
// One deliberate stricter-than-Codex check: an EMPTY id ("") is rejected even though
// Codex's `id: String` would deserialize it. The invariant is that a stream replayed
// as a real success must reference an addressable response; a real Responses stream
// always carries a non-empty id, so an empty one is a malformed terminal. Converting
// it to a retry (which may fetch a clean terminal) avoids logging a hollow success and
// cannot loop a legitimate stream, which never emits one.
func responsesCompletedValid(ev event) bool {
	fields, ok := decodeResponsesObject(ev.data, false)
	if !ok || !uniqueResponsesJSONValue(string(fields["response"])) {
		return false
	}
	response := responsesObject(fields["response"])
	if responsesString(response["id"]) == "" {
		return false
	}
	usageRaw := response["usage"]
	if len(usageRaw) == 0 || string(usageRaw) == "null" {
		return true
	}
	usage := responsesObject(usageRaw)
	for _, key := range [...]string{"input_tokens", "output_tokens", "total_tokens"} {
		if !responsesRequiredInteger(usage[key]) {
			return false
		}
	}
	for _, detail := range [...]struct{ object, field string }{
		{"input_tokens_details", "cached_tokens"},
		{"output_tokens_details", "reasoning_tokens"},
	} {
		raw := usage[detail.object]
		if len(raw) != 0 && string(raw) != "null" && !responsesRequiredInteger(responsesObject(raw)[detail.field]) {
			return false
		}
	}
	return true
}

// Maps keep protocol field names case-sensitive, unlike Go's struct decoder.
// Callers validate JSON syntax and duplicate keys before interpreting snapshots.
func responsesObject(raw json.RawMessage) map[string]json.RawMessage {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil
	}
	return fields
}

func responsesString(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}

func responsesRequiredInteger(raw json.RawMessage) bool {
	var value *int64
	return json.Unmarshal(raw, &value) == nil && value != nil
}

// responsesTerminalValid applies Codex's terminal parse. Codex reads an
// incomplete snapshot as a ResponseCompleted only for reason "interrupted"
// (ending the turn without end_turn); every other reason is its terminal
// ResponseIncomplete, built without that parse.
func responsesTerminalValid(ev event, t string) bool {
	if t == "response.incomplete" {
		fields := responsesObject(json.RawMessage(ev.data))
		response := responsesObject(fields["response"])
		details := responsesObject(response["incomplete_details"])
		if responsesString(details["reason"]) != "interrupted" {
			return true
		}
	}
	return responsesCompletedValid(ev)
}

// responsesInvalidCompleted is the failure for a terminal snapshot Codex's
// ResponseCompleted parse would reject (see responsesTerminalValid). It is raised at
// that frame under its own code: the stream did terminate, so waiting for EOF would
// misreport it as truncated_stream. Transient, because a re-send may fetch a clean
// terminal and it cannot loop a legitimate stream, which never emits one.
func responsesInvalidCompleted() *failure {
	return &failure{transient: true, status: 502, atype: "api_error", code: "responses_invalid_completed",
		message: "upstream terminal response snapshot is malformed (needs a response id and complete integer usage)"}
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

// classifyResponsesFrame classifies a terminal error frame and reports whether Codex
// recovers from it natively:
//
//	native == true  -> the buffered path hands the frame to Codex untouched: Codex
//	                   recovers on its own (context-window exhaustion drives Codex's
//	                   compaction path; converting it to a synthetic error would hide
//	                   it from that handler).
//	native == false -> the buffered path converts it: a transient fault becomes a
//	                   retry, a terminal one is surfaced; the frame is NOT forwarded.
//
// Live mode cannot convert (headers are already sent): it forwards a terminal
// verdict's own frame (fail.delivered) and DROPs a transient one.
//
// Native delivery is gated on the frame being a `response.failed`: Codex's
// context-compaction (is_context_window_error) lives ONLY in its response.failed
// branch. A top-level `error` event is not matched by Codex's parser at all (it is
// ignored, then the stream fails EOF-before-completion), so forwarding one natively
// would yield a generic retry, never compaction — such an error is classified and
// converted instead (a context error there surfaces as a deterministic 400).
func classifyResponsesFrame(ev event, t string) (fail *failure, native bool) {
	typ, code, msg := parseResponsesError(ev, t)
	return classifyResponsesError(typ, code, msg), t == "response.failed" && isCodexRecoverableError(code)
}

// parseResponsesError extracts (type, code, message) from either terminal error
// shape: `response.failed.response.error{...}`, or the top-level `error` event —
// which may carry the fields at the top level (standard) or nested under `error`
// (the variant some gateways emit).
func parseResponsesError(ev event, t string) (typ, code, msg string) {
	fields := responsesObject(json.RawMessage(ev.data))
	if t == "response.failed" {
		fields = responsesObject(fields["response"])
	}
	errFields := responsesObject(fields["error"])
	typ = responsesString(errFields["type"])
	code = responsesString(errFields["code"])
	msg = responsesString(errFields["message"])
	if t != "response.failed" {
		code = firstNonEmpty(code, responsesString(fields["code"]))
		msg = firstNonEmpty(msg, responsesString(fields["message"]))
	}
	return typ, code, msg
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
	fields, ok := decodeResponsesObject(ev.data, false)
	if !ok {
		return ""
	}
	typ := responsesString(fields["type"])
	if typ == "" {
		return ""
	}
	// These are the typed optional fields in Codex's ResponsesStreamEvent.
	// A wrong type invalidates the whole envelope, including terminal/error
	// events. Unknown keys and arbitrary JSON response/item/error values remain
	// opaque here; an actual completion snapshot is checked separately.
	for _, key := range [...]string{"item_id", "call_id", "delta", "text"} {
		var value *string
		if raw, present := fields[key]; present && json.Unmarshal(raw, &value) != nil {
			return ""
		}
	}
	for _, key := range [...]string{"summary_index", "content_index"} {
		var value *int64
		if raw, present := fields[key]; present && json.Unmarshal(raw, &value) != nil {
			return ""
		}
	}
	return typ
}

// responsesFault is the retry-relevant identity of an OpenAI error, read once by
// responsesFaultOf. The in-band classifier and the HTTP body rule both consume it, so
// one error cannot be "retry" on one path and "terminal" on the other.
type responsesFault int

const (
	// No explicit identity: server_error, api_error, unrecognized types.
	faultUnknown responsesFault = iota
	faultTimeout
	faultRateLimit
	faultOverloaded
	// The request itself can never succeed as written.
	faultRequestShape
	// Exhausted quota or plan limit: waiting does not refill it.
	faultQuota
	// The caller was refused: authentication_error, permission_error, not_found_error.
	faultRejected
)

// responsesRequestTooLarge anchors OpenAI's per-request TPM overflow ("Request too
// large for … on tokens per min (TPM): Limit 30000, Requested 35000. The input or
// output tokens must be reduced in order to run successfully."). It arrives as a 429
// with the transient code rate_limit_exceeded, yet no wait shrinks the request.
const responsesRequestTooLarge = "must be reduced in order to run successfully"

// responsesFaultOf reads the identity of an OpenAI error from its normalized
// (lowercased, trimmed) type and code and its lowercased message.
func responsesFaultOf(t, c, msg string) responsesFault {
	// A specific, recognized code owns the classification: gateways attach a generic
	// invalid_request_error type to capacity and timeout faults, and a
	// rate_limit_error type to exhausted quota.
	switch {
	case isResponsesQuotaIdentity(t), isResponsesQuotaIdentity(c):
		return faultQuota
	case strings.Contains(msg, responsesRequestTooLarge), isResponsesRequestShapeCode(c):
		return faultRequestShape
	case c == "request_timeout":
		return faultTimeout
	case c == "rate_limit_exceeded":
		return faultRateLimit
	case c == "server_is_overloaded", c == "overloaded":
		return faultOverloaded
	}
	switch t {
	case "timeout_error":
		return faultTimeout
	case "rate_limit_error":
		return faultRateLimit
	case "overloaded_error", "service_unavailable_error":
		return faultOverloaded
	case "invalid_request_error":
		return faultRequestShape
	case "":
		// The Anthropic-derived phrases are evidence only when the upstream named no
		// type. An explicit server/transient type (server_error "unexpected value
		// returned by backend provider") is a backend fault that the next attempt may
		// not repeat, so a phrase must not turn it terminal.
		if anyContains(msg, requestShapeSigs) {
			return faultRequestShape
		}
	default:
		if responsesRejectedStatus(t) != 0 {
			return faultRejected
		}
	}
	return faultUnknown
}

// responsesIdent normalizes an error type or code for comparison.
func responsesIdent(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// isResponsesQuotaIdentity reports whether an error type or code names an exhausted
// quota or plan limit, which a retry cannot refill.
func isResponsesQuotaIdentity(s string) bool {
	switch s {
	case "insufficient_quota",
		// Codex ChatGPT-plan limits: the allowance is spent (or never included Codex)
		// until the plan resets.
		"usage_limit_reached", "usage_not_included":
		return true
	}
	return false
}

// responsesRejectedStatus maps the OpenAI error types that say the caller itself was
// refused to their HTTP status, or 0 for any other type. Over HTTP the status already
// makes these final; in-band there is no status, so the type has to say it.
func responsesRejectedStatus(t string) int {
	switch t {
	case "authentication_error":
		return http.StatusUnauthorized
	case "permission_error":
		return http.StatusForbidden
	case "not_found_error":
		return http.StatusNotFound
	}
	return 0
}

// classifyResponsesError maps an OpenAI Responses error (type/code/message) to a
// failure. Same principle as the Anthropic path: ride out everything that may clear
// (overload, rate limit, timeout, and every error naming no identity) and surface
// what an identical re-send cannot fix: a request-shape error, exhausted quota, and
// a refused caller (authentication / permission / not found), which is as final
// in-band as its HTTP status is over HTTP.
func classifyResponsesError(errType, errCode, message string) *failure {
	t, c := responsesIdent(errType), responsesIdent(errCode)
	switch responsesFaultOf(t, c, strings.ToLower(message)) {
	case faultTimeout:
		return &failure{transient: true, status: 504, atype: "timeout_error", code: "responses_timeout", message: message}
	case faultRateLimit:
		// The advertised delay is commonly only in the message ("Please try again
		// in 11.054s"). Preserve it so both hidden local retries and the surfaced
		// 503 wait the real amount instead of the proxy's short fallback backoff.
		return &failure{transient: true, status: 429, atype: "rate_limit_error", code: "responses_rate_limit", message: message, retryAfter: parseRetryAfterFromMessage(message)}
	case faultOverloaded:
		return &failure{transient: true, status: 529, atype: "overloaded_error", code: "responses_overloaded", message: message}
	case faultQuota:
		// Keep the quota identity as the type and a 429 as the status: surfaceFor
		// keeps 429, which Codex treats as terminal and words as a usage limit.
		qtype := t
		if !isResponsesQuotaIdentity(qtype) {
			qtype = c
		}
		return &failure{status: http.StatusTooManyRequests, atype: qtype, code: "responses_quota", message: message}
	case faultRequestShape:
		return &failure{status: http.StatusBadRequest, atype: "invalid_request_error", code: "responses_request_shape", message: message}
	case faultRejected:
		return &failure{status: responsesRejectedStatus(t), atype: t, code: "responses_" + t, message: message}
	}
	// api_error, server_error and unknown types carry no evidence either way: a
	// temporary block is ridden out, not surfaced.
	return &failure{transient: true, status: 502, atype: "api_error", code: "responses_error", message: message}
}

// retryAfterMsgRe finds the advertised delay in a rate-limit message: one or more
// number+unit terms after "try again in". retryAfterTermRe splits them. Both compact
// Go durations ("11.054s", "500ms", "1m30s", "1h2m") and spelled-out ones ("2
// milliseconds", "6 seconds") occur.
var (
	retryAfterMsgRe  = regexp.MustCompile(`(?i)try again in\s+((?:[0-9]+(?:\.[0-9]+)?\s*[a-z]+\s*)+)`)
	retryAfterTermRe = regexp.MustCompile(`(?i)([0-9]+(?:\.[0-9]+)?)\s*([a-z]+)`)
)

// retryAfterUnitSeconds returns the length in seconds of a duration unit, or 0 for
// anything else. Units match whole words, so "milliseconds" is never read as "m".
func retryAfterUnitSeconds(unit string) float64 {
	switch strings.ToLower(unit) {
	case "ms", "millisecond", "milliseconds":
		return 0.001
	case "s", "sec", "secs", "second", "seconds":
		return 1
	case "m", "min", "mins", "minute", "minutes":
		return 60
	case "h", "hour", "hours":
		return 3600
	}
	return 0
}

// parseRetryAfterFromMessage returns the advertised retry delay in whole seconds
// (rounded up, min 1), or 0 when the message carries none.
func parseRetryAfterFromMessage(message string) int {
	m := retryAfterMsgRe.FindStringSubmatch(message)
	if m == nil {
		return 0
	}
	var total float64
	for _, term := range retryAfterTermRe.FindAllStringSubmatch(m[1], -1) {
		unit := retryAfterUnitSeconds(term[2])
		val, err := strconv.ParseFloat(term[1], 64)
		if unit == 0 || err != nil {
			break // keep what parsed cleanly; an unknown unit vouches for nothing after it
		}
		total += val * unit
	}
	if total <= 0 {
		return 0
	}
	if total >= math.MaxInt32 {
		return math.MaxInt32
	}
	// The epsilon absorbs float noise (2000 * 0.001 is not exactly 2) so a whole
	// number of seconds is not rounded up to the next one.
	return max(1, int(math.Ceil(total-1e-9)))
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
		"invalid_image_mode", "empty_image_file",
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
// Responses route. The shared classifier owns the status rule (408/409/429/5xx are
// transient, every other 4xx is the upstream's final answer); responsesErrorVerdict
// refines it only where the body names an explicit identity. Both it and the in-band
// classifier read that identity through responsesFaultOf, so there is a SINGLE
// OpenAI error brain (keyed on type+code+message) for in-band SSE errors and
// HTTP-level errors alike.
func classifyResponsesHTTPErrorBytes(resp *http.Response, b []byte) failure {
	f := classifyHTTPErrorBytesShaped(resp, b, responsesErrorVerdict)
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

// responsesErrorVerdict is the Responses wire's HTTP errorBodyRule. The status owns
// the retry decision, so the body speaks only for an explicit identity:
//
//   - a timeout / rate-limit / overload type or code is transient, even on a 4xx
//     (gateways attach 400 to a dropped stream or a capacity fault);
//   - a request-shape code or type, or an exhausted quota or plan limit, is
//     permanent, which also keeps a 429 from being retried;
//   - anything else — authentication, permission, not_found, server_error,
//     api_error, a flat body such as vLLM's — names no retry-relevant identity: a 4xx
//     stays terminal and a 5xx transient, by status alone.
func responsesErrorVerdict(atype, code, msg, _ string) errorVerdict {
	switch responsesFaultOf(responsesIdent(atype), responsesIdent(code), strings.ToLower(msg)) {
	case faultTimeout, faultRateLimit, faultOverloaded:
		return verdictTransient
	case faultRequestShape, faultQuota:
		return verdictPermanent
	}
	return verdictUnknown
}

// classifyHTTPErrorForPath applies read failures before body heuristics, retaining
// explicit retry hints unless the request context itself has ended.
func classifyHTTPErrorForPath(ctx context.Context, path string, resp *http.Response, b []byte, readErr error) failure {
	if readErr != nil {
		if source, _ := httpRetryHint(resp.Header); source == "" || ctx.Err() != nil {
			f := classifyTransport(readErr, ctx)
			if f.transient {
				f.retryAfter = retryAfterSeconds(resp.Header.Get("Retry-After"))
			}
			return f
		}
		// An authoritative header owns the verdict; the incomplete body cannot
		// supply trustworthy error metadata.
		b = nil
	}
	if isResponsesPath(path) {
		return classifyResponsesHTTPErrorBytes(resp, b)
	}
	if isGeminiPath(path) {
		return classifyGeminiHTTPErrorBytes(resp, b)
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
	if isGeminiPath(path) {
		return writeGeminiError
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
	h.Set("X-Steady-Proxy-Reason", code)
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
