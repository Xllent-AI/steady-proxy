package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// ─────────────────────────────────────────────────────────────── wire model ──
//
// The transactional engine (spool + keepalive→commit→live + local-retry +
// idle/deadline, see captureSSECore / handle) is protocol-agnostic: its one rule
// is "buffer the full response; convert any pre-commit failure into a retry; only
// give up on a request that can never succeed." What differs between the Anthropic
// Messages wire (`POST /v1/messages`) and the OpenAI Responses wire
// (`POST /v1/responses`, used by Codex) is only how the byte stream is
// *interpreted*: when it is complete, which event is an error, and how tokens are
// scraped. wireModel owns exactly that; one instance is created per upstream
// attempt.
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

	// live ingests one event on the post-commit LIVE path: it returns the exact
	// downstream bytes to forward (empty -> engine emits keepalive()), whether the
	// stream terminated, and fail != nil for an in-band error (the engine ends the
	// stream as a DROP so the client's native stream-retry takes over) or malformed
	// content.
	live(ev event, raw []byte, st *captureStats) (out [][]byte, done bool, fail *failure)

	// keepalive is the byte frame the engine writes downstream during a silent gap
	// to keep the client's connection/idle timer alive. It MUST be one the client
	// treats as a no-op. The two wires differ: the Anthropic SDK is fine with a
	// spec SSE comment, but Codex's SSE reader only resets its idle timer on a
	// yielded event and discards comments — so the Responses wire sends a skippable
	// unknown-type event instead (see openaiWire.keepalive).
	keepalive() []byte

	// replayBuffered writes the whole buffered stream downstream once, at the
	// terminal-in-buffer commit.
	replayBuffered(sp *spool, w io.Writer, st *captureStats) error

	// replayPrefix writes the buffered prefix at a keepalive/early commit, before
	// live() continues to stream (Anthropic shares its tool-coalescing state).
	replayPrefix(sp *spool, w io.Writer) error

	// forwardable reports whether the buffered prefix would forward real
	// downstream progress — gates an early/workflow commit.
	forwardable() bool
}

// wireFor selects the wire model for a request path. Only the two transactional
// routes reach here; everything else uses proxyOnce and never needs a model. The
// Anthropic-only refusal intercept is baked into the returned wire, so the shared
// engine never carries it (interceptRefusal is meaningless for Responses).
func wireFor(path string, interceptRefusal bool) wireModel {
	if isResponsesPath(path) {
		return newOpenAIWire()
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

// ─────────────────────────────────────────────────────── anthropic wire model ──
//
// anthropicWire is a thin adapter over the existing, unchanged Anthropic stream
// machinery (streamValidator, toolJSONValidator, toolJSONReplayNormalizer,
// progressTracker, scrapeUsage, classifySSEError, spool.replayWithUsage /
// replayWithNormalizer). It preserves the historical behavior exactly — the
// existing test suite is the safety net.
type anthropicWire struct {
	val              *streamValidator
	toolVal          *toolJSONValidator
	toolNorm         *toolJSONReplayNormalizer
	prog             *progressTracker
	interceptRefusal bool // return the refusal sentinel on a complete stop_reason=="refusal"
}

func newAnthropicWire(interceptRefusal bool) *anthropicWire {
	return &anthropicWire{
		val:              &streamValidator{},
		toolVal:          &toolJSONValidator{},
		toolNorm:         &toolJSONReplayNormalizer{},
		prog:             &progressTracker{},
		interceptRefusal: interceptRefusal,
	}
}

func (a *anthropicWire) scrape(ev event, st *captureStats) { scrapeUsage(ev, st) }

func (a *anthropicWire) buffered(ev event, st *captureStats) (bool, *failure) {
	if cfg.validateJSON {
		if f := validateSSEEventShape(ev); f != nil {
			return false, f
		}
	}
	if ev.name == "error" {
		return false, classifySSEError(ev.data)
	}
	if cfg.validateJSON && ev.data != "" && !json.Valid([]byte(ev.data)) {
		return false, &failure{transient: true, status: 502, atype: "api_error", code: "malformed_sse", message: "invalid JSON in stream event"}
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
	a.prog.accept(ev)
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
		return nil, false, classifySSEError(ev.data)
	}
	out, err := a.toolNorm.accept(ev, raw)
	if err != nil {
		return nil, false, malformedSSE("invalid tool input JSON in stream event")
	}
	a.val.accept(ev)
	return out, a.val.terminal(), nil
}

func (a *anthropicWire) replayBuffered(sp *spool, w io.Writer, st *captureStats) error {
	return sp.replayWithUsage(w, st)
}

func (a *anthropicWire) replayPrefix(sp *spool, w io.Writer) error {
	return sp.replayWithNormalizer(w, a.toolNorm)
}

func (a *anthropicWire) forwardable() bool { return a.prog.sawForwardable }

func (a *anthropicWire) keepalive() []byte { return []byte(": keepalive\n\n") }
