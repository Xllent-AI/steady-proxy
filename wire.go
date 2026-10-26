package main

import (
	"io"
	"net/http"
	"strings"
)

// ─────────────────────────────────────────────────────────────── wire model ──
//
// The transactional engine (spool + keepalive→commit→live + local-retry +
// idle/deadline, see captureSSECore / handle) is protocol-agnostic: its one rule
// is "buffer the full response; convert any pre-commit failure into a retry; only
// give up on a request that can never succeed." Each wire interprets completion,
// in-band errors, and usage for Anthropic Messages, OpenAI Responses, or Gemini
// GenerateContent. One instance is created per upstream attempt.
type wireModel interface {
	// scrape pulls best-effort access-log facts (model/usage/stop) out of one
	// event into st. Called for every event; must never fail a stream.
	scrape(ev event, st *captureStats)

	// buffered ingests one event on the pre-commit BUFFERING path: it validates
	// shape/order/JSON, detects in-band errors, and advances terminal state — but
	// does NOT touch the spool (the engine writes ev.raw after a clean return). Any
	// wire-specific policy (e.g. the Anthropic refusal intercept) is owned by the wire
	// instance, so the shared engine passes no per-wire flags here.
	//   fail != nil -> abort this attempt (engine discards the buffer). This
	//                  covers an in-band error, a malformed/out-of-order event, and
	//                  the refusal sentinel (Anthropic only).
	//   done == true -> the buffered stream is now a complete, deliverable response
	//                   (engine writes this event, then replays via replayBuffered).
	buffered(ev event, st *captureStats) (done bool, fail *failure)

	// completeOnEOF accepts a clean end of transport for protocols without a
	// terminal SSE event. The engine also requires a fully framed final event.
	completeOnEOF() bool

	// live ingests one event on the post-commit LIVE path: it returns the exact
	// downstream bytes to forward (empty -> engine emits keepalive()), whether the
	// stream terminated, and fail != nil for an in-band error or malformed
	// content. The engine writes out even when fail != nil, then ends the stream:
	//   fail.delivered == true  -> out carried the upstream's own terminal error
	//                              frame; the handler ends the response cleanly.
	//   fail.delivered == false -> the handler aborts the HTTP response (DROP) so
	//                              the client's native stream-retry takes over.
	live(ev event, raw []byte, st *captureStats) (out [][]byte, done bool, fail *failure)

	// keepalive is the byte frame the engine writes downstream during a silent gap
	// to keep the client's connection/idle timer alive. It MUST be one the client
	// treats as a no-op. Messages and Gemini use a standard SSE comment. Codex's
	// reader discards comments without resetting its idle timer, so Responses
	// sends a skippable unknown-type event (see openaiWire.keepalive).
	keepalive() []byte

	// replayBuffered writes the whole buffered stream downstream once, at the
	// terminal-event or clean-EOF commit.
	replayBuffered(sp *spool, w io.Writer, st *captureStats) error

	// replayPrefix writes the buffered prefix at a keepalive/early commit, before
	// live() continues to stream (main-session Anthropic shares coalescing state).
	replayPrefix(sp *spool, w io.Writer) error

	// forwardable reports whether the buffered prefix would forward real
	// downstream progress — gates an early/workflow commit.
	forwardable() bool
}

// wireFor selects a stream interpreter. Refusal interception is Anthropic-only.
func wireFor(path string, body []byte, interceptRefusal bool) wireModel {
	if isResponsesPath(path) {
		return newOpenAIWire()
	}
	if isGeminiPath(path) {
		return newGeminiWire(body)
	}
	return newAnthropicWire(interceptRefusal)
}

// pathIs matches a route at a segment boundary: the exact base, or base + "/..."
// (a sub-resource). It deliberately does NOT match "/v1/responses-legacy" or
// "/v1/messagesfoo", so a look-alike path can never be misrouted to a wire engine.
func pathIs(path, base string) bool {
	return path == base || strings.HasPrefix(path, base+"/")
}

func isMessagesPath(path string) bool {
	return pathIs(path, "/v1/messages") && !strings.Contains(path, "count_tokens")
}

func isResponsesPath(path string) bool {
	return pathIs(path, "/v1/responses")
}

// geminiTarget recognizes native Developer API and Vertex AI model methods
// (project-scoped and express-mode publisher paths). Any method name is a
// Gemini route (generateContent, embedContent, batchEmbedContents, ...) so its
// errors are classified and rendered in Google's envelope; only
// streamGenerateContent?alt=sse is buffered, which main.go gates on the method.
// Exact resource boundaries keep look-alike and unrelated routes single-shot.
func geminiTarget(path string) (model, method string) {
	version, resource, ok := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	if !ok {
		return "", ""
	}
	switch version {
	case "v1", "v1beta", "v1alpha", "v1beta1":
	default:
		return "", ""
	}
	var name string
	if rest, vertex := strings.CutPrefix(resource, "projects/"); vertex {
		project, rest, ok := strings.Cut(rest, "/locations/")
		if !ok || project == "" || strings.Contains(project, "/") {
			return "", ""
		}
		location, rest, ok := strings.Cut(rest, "/publishers/google/models/")
		if !ok || location == "" || strings.Contains(location, "/") {
			return "", ""
		}
		name = rest
	} else if rest, express := strings.CutPrefix(resource, "publishers/google/models/"); express {
		name = rest
	} else {
		name, ok = strings.CutPrefix(resource, "models/")
		if !ok {
			return "", ""
		}
	}
	if strings.Contains(name, "/") {
		return "", ""
	}
	model, method, ok = strings.Cut(name, ":")
	if !ok || model == "" || !isASCIILetters(method) {
		return "", ""
	}
	return model, method
}

// isASCIILetters reports a non-empty run of A-Z/a-z: Google RPC verbs are
// camelCase words, so digits, punctuation, and non-ASCII are never a method.
func isASCIILetters(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if c := s[i]; (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}

func isGeminiPath(path string) bool {
	_, method := geminiTarget(path)
	return method != ""
}

// ─────────────────────────────────────────────────────── anthropic wire model ──
//
// anthropicWire is a thin adapter over the Anthropic stream machinery
// (streamValidator, toolJSONValidator, toolJSONReplayNormalizer,
// scrapeUsage, classifySSEError, spool.replayWithUsage / replayWithNormalizer).
// Its behavior is pinned by the sse/situations tests.
type anthropicWire struct {
	val              *streamValidator
	toolVal          *toolJSONValidator
	toolNorm         *toolJSONReplayNormalizer
	sawProgress      bool
	streamToolDeltas bool // Workflow cutover forwards raw fragments instead of withholding them
	interceptRefusal bool // return the refusal sentinel on a complete stop_reason=="refusal"
}

func newAnthropicWire(interceptRefusal bool) *anthropicWire {
	return &anthropicWire{
		val:              &streamValidator{},
		toolVal:          &toolJSONValidator{},
		toolNorm:         &toolJSONReplayNormalizer{},
		interceptRefusal: interceptRefusal,
	}
}

func (a *anthropicWire) scrape(ev event, st *captureStats) { scrapeUsage(ev, st) }

func (a *anthropicWire) completeOnEOF() bool { return false }

func (a *anthropicWire) buffered(ev event, st *captureStats) (bool, *failure) {
	if cfg.validateJSON {
		if f := validateSSEEventShape(ev); f != nil {
			return false, f
		}
	}
	if ev.name == "error" {
		return false, classifySSEError(ev.data)
	}
	if cfg.validateJSON {
		if f := a.toolVal.accept(ev); f != nil {
			return false, f
		}
	}
	a.val.accept(ev)
	if a.val.invalid {
		return false, &failure{transient: true, status: 502, atype: "api_error", code: "malformed_sse", message: "out-of-order stream event"}
	}
	if ev.name == "content_block_delta" {
		a.sawProgress = true
	}
	if a.val.terminal() {
		if a.interceptRefusal && st != nil && st.stop == "refusal" {
			// api_error/502 are defensive defaults: the handler keys on .code and
			// never surfaces this, but a benign shape keeps any future leak clean.
			return false, &failure{status: http.StatusBadGateway, atype: "api_error", code: refusalFallbackCode, message: "model refused; retrying with fallback model"}
		}
		return true, nil
	}
	return false, nil
}

func (a *anthropicWire) live(ev event, raw []byte, st *captureStats) ([][]byte, bool, *failure) {
	if cfg.validateJSON {
		if f := validateSSEEventShape(ev); f != nil {
			return nil, false, f
		}
	}
	if ev.name == "error" {
		fail := classifySSEError(ev.data)
		if !fail.transient {
			// An auth/billing/request-shape error is the upstream's final answer:
			// a DROP would send the client into a retry that cannot succeed, so
			// deliver the frame verbatim and end the stream cleanly. A transient
			// error is never forwarded (see process): the client's native
			// stream-retry must take over instead.
			fail.delivered = true
			return [][]byte{raw}, false, fail
		}
		return nil, false, fail
	}
	if cfg.validateJSON {
		if f := a.toolVal.accept(ev); f != nil {
			return nil, false, f
		}
	}
	if a.streamToolDeltas {
		a.val.accept(ev)
		return [][]byte{raw}, ev.name == "message_stop", nil
	}
	out, err := a.toolNorm.accept(ev, raw)
	if err != nil {
		if err == errTooLarge {
			return nil, false, classifyBufferError(err)
		}
		if err == errInvalidToolJSON {
			return nil, false, malformedSSE(invalidToolInputMessage)
		}
		return nil, false, malformedSSE("invalid JSON in stream event")
	}
	a.val.accept(ev)
	// message_stop ends the stream even if the validator flagged an ordering
	// fault earlier (terminal() is then never true). The client has the whole
	// response by now; waiting for more would hold it until the idle/deadline
	// timer and log a complete response as truncated_stream.
	return out, ev.name == "message_stop" || a.val.terminal(), nil
}

func (a *anthropicWire) replayBuffered(sp *spool, w io.Writer, st *captureStats) error {
	return sp.replayWithUsage(w, st)
}

func (a *anthropicWire) replayPrefix(sp *spool, w io.Writer) error {
	if a.streamToolDeltas {
		return sp.replayRaw(w)
	}
	return sp.replayWithNormalizer(w, a.toolNorm)
}

func (a *anthropicWire) forwardable() bool { return a.sawProgress }

func (a *anthropicWire) keepalive() []byte { return []byte(": keepalive\n\n") }
