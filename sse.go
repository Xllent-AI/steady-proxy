package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"time"
)

// ------------------------------------------------------------------ spool ----
// Buffers validated SSE events in RAM up to cfg.maxBufferMem, then spills to an
// unlinked temp file. Only fully-validated event bytes are written, so we never
// buffer (and never replay) bytes past a terminal message_stop.

type spool struct {
	mem     *bytes.Buffer
	f       *os.File
	spilled bool
	total   int64
}

func newSpool() *spool { return &spool{mem: &bytes.Buffer{}} }

func (s *spool) write(p []byte) error {
	s.total += int64(len(p))
	if s.total > cfg.maxResponseBytes {
		return errTooLarge
	}
	if !s.spilled {
		if int64(s.mem.Len())+int64(len(p)) <= cfg.maxBufferMem {
			s.mem.Write(p)
			return nil
		}
		f, err := os.CreateTemp(cfg.spoolDir, "steady-proxy-*.sse")
		if err != nil {
			return err
		}
		os.Remove(f.Name()) // unlink now; the open fd keeps the data
		if _, err := f.Write(s.mem.Bytes()); err != nil {
			f.Close()
			return err
		}
		s.f = f
		s.spilled = true
		s.mem = nil
	}
	_, err := s.f.Write(p)
	return err
}

func (s *spool) replayWithUsage(w io.Writer, st *captureStats) error {
	if (st == nil || !st.shouldBackfillMessageStartUsage()) && !normalizeToolJSONEnabled() {
		return s.replayRaw(w)
	}
	r, err := s.reader()
	if err != nil {
		return err
	}
	return replayBuffered(r, w, st)
}

func (s *spool) replayWithNormalizer(w io.Writer, norm *toolJSONReplayNormalizer) error {
	r, err := s.reader()
	if err != nil {
		return err
	}
	return replayBufferedWithNormalizer(r, w, nil, norm)
}

func (s *spool) replayRaw(w io.Writer) error {
	if !s.spilled {
		_, err := w.Write(s.mem.Bytes())
		return err
	}
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err := io.Copy(w, s.f)
	return err
}

func (s *spool) reader() (io.Reader, error) {
	if !s.spilled {
		return bytes.NewReader(s.mem.Bytes()), nil
	}
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return s.f, nil
}

func (s *spool) discard() {
	if s.f != nil {
		s.f.Close()
		s.f = nil
	}
	s.mem = nil
}

// --------------------------------------------------------------- sse parse --
// Incremental SSE parser. Each emitted event carries its exact raw bytes so the
// spool replays byte-for-byte and stops precisely at the terminal event.

type event struct {
	name, data string
	raw        []byte
}

type curEvent struct {
	has        bool
	name, data string
}

type sseParser struct {
	maxEventBytes int64 // 0 for already bounded/trusted replay input
	err           error
	pending       []byte
	curRaw        []byte
	cur           curEvent
}

func (p *sseParser) feed(b []byte) []event {
	var out []event
	for len(b) > 0 && p.err == nil {
		// Consume only one line fragment at a time, checking before allocation.
		// A transport chunk can contain many small events; its total size is not
		// an event-size violation.
		i := bytes.IndexByte(b, '\n')
		n := len(b)
		if i >= 0 {
			n = i + 1
		}
		if p.maxEventBytes > 0 && int64(len(p.curRaw))+int64(len(p.pending))+int64(n) > p.maxEventBytes {
			p.err = errTooLarge
			break
		}
		p.pending = append(p.pending, b[:n]...)
		b = b[n:]
		if i < 0 {
			break
		}
		p.curRaw = append(p.curRaw, p.pending...)
		line := p.pending[:len(p.pending)-1]
		p.pending = p.pending[:0]
		if n := len(line); n > 0 && line[n-1] == '\r' {
			line = line[:n-1]
		}
		if len(line) == 0 {
			if p.cur.has {
				out = append(out, event{name: p.cur.name, data: p.cur.data, raw: append([]byte(nil), p.curRaw...)})
				p.cur = curEvent{}
			}
			p.curRaw = p.curRaw[:0]
			continue
		}
		if line[0] == ':' {
			continue
		}
		field, value := line, []byte(nil)
		if j := bytes.IndexByte(line, ':'); j >= 0 {
			field = line[:j]
			value = line[j+1:]
			if len(value) > 0 && value[0] == ' ' {
				value = value[1:]
			}
		}
		p.cur.has = true
		switch string(field) {
		case "event":
			p.cur.name = string(value)
		case "data":
			if p.cur.data != "" {
				p.cur.data += "\n"
			}
			p.cur.data += string(value)
		}
	}
	return out
}

// ----------------------------------------------------- anthropic validator --

type streamValidator struct {
	sawStart bool
	open     int
	sawStop  bool
	invalid  bool
}

func (v *streamValidator) accept(ev event) {
	switch ev.name {
	case "message_start":
		if v.sawStart || v.sawStop {
			v.invalid = true
		}
		v.sawStart = true
	case "content_block_start":
		if !v.sawStart || v.sawStop {
			v.invalid = true
		}
		v.open++
	case "content_block_stop":
		if v.open <= 0 || v.sawStop {
			v.invalid = true
		} else {
			v.open--
		}
	case "content_block_delta", "message_delta":
		if !v.sawStart || v.sawStop {
			v.invalid = true
		}
	case "message_stop":
		if !v.sawStart || v.open != 0 {
			v.invalid = true
		}
		v.sawStop = true
	default:
		// ping and unknown future events are tolerated.
	}
}

func (v *streamValidator) terminal() bool {
	return v.sawStart && v.sawStop && v.open == 0 && !v.invalid
}

// captureStats collects human-friendly facts about a streamed response so the
// caller can log one access-log line per request. All fields are best-effort.
type captureStats struct {
	mode                  string    // "buffered" | "live" | "" (never committed any bytes)
	bytes                 int64     // SSE bytes of the response
	maxPingRun            int       // longest run of consecutive upstream `ping` events — the content-silent-gap (Case B / stall) tripwire; 0 = content never went silent
	curPingRun            int       // internal: current consecutive-ping counter feeding maxPingRun
	inTok                 int       // total input tokens incl. cache read/create (latest usage event)
	inputTok              int       // usage.input_tokens component of inTok
	cacheCreationTok      int       // usage.cache_creation_input_tokens component of inTok
	cacheReadTok          int       // usage.cache_read_input_tokens component of inTok
	startInputTok         int       // message_start usage.input_tokens
	startCacheCreationTok int       // message_start usage.cache_creation_input_tokens
	startCacheReadTok     int       // message_start usage.cache_read_input_tokens
	deltaInputUsage       bool      // message_delta carried authoritative input usage
	outTok                int       // usage.output_tokens (from message_delta)
	stop                  string    // delta.stop_reason (e.g. end_turn, max_tokens, tool_use)
	model                 string    // resolved model echoed back by the upstream
	respTee               io.Writer // optional: when set, every raw response event is teed here (request-log)
	heldRefusal           *spool    // complete buffered refusal handed to the caller on the refusal sentinel; the caller must discard it
}

// ------------------------------------------------------------- captureSSE ---
// captureSSE consumes the upstream SSE stream and writes the downstream
// response itself. It stays TRANSACTIONAL (buffers, validates, then replays on a
// complete message_stop) until cfg.keepaliveMs elapses without completing; then,
// to survive the client's ~300s no-bytes idle ceiling, it COMMITS (sends 200 +
// the buffered prefix) and streams the rest live with keepalive pings.
//
// Returns (wrote, fail):
//   - wrote=true,  fail=nil   -> a full, valid response was sent (buffered or live)
//   - wrote=true,  fail!=nil  -> failed AFTER committing; can't convert, caller logs
//   - wrote=false, fail!=nil  -> failed BEFORE committing; caller converts to a retry
func captureSSE(ctx context.Context, cancel context.CancelFunc, w http.ResponseWriter, upstreamHdr http.Header, body io.Reader, st *captureStats) (bool, *failure) {
	return captureSSEWindow(ctx, cancel, w, upstreamHdr, body, st, cfg.keepaliveMs, false, false)
}

// captureSSEWindow is captureSSE with an explicit transactional/keepalive window
// so a caller can vary it per request. Below keepaliveMs the stream is buffered
// (a pre-commit failure converts cleanly to a retry); once the window elapses the
// buffered prefix is committed and the rest streams live with keepalive pings.
//
// progressGated is for callers behind a Workflow-style stall watchdog that only
// counts real, downstream-forwarded assistant/tool deltas as progress (keepalive
// comments and ping do not). When set, the commit is deferred until such a delta
// is buffered: committing a content-less prefix would feed the watchdog nothing
// while needlessly forfeiting the clean pre-commit retry path. When unset (the
// main session and ordinary subagents, which have no such watchdog), the window
// commits on time regardless — so a long ping-only stream still gets keepalives
// and never trips the client's no-bytes ceiling.
//
// Workflow-tool agents pass a shorter window than the main session so the commit
// happens before their per-agent stall watchdog fires (see isWorkflowAgent);
// keepaliveMs<=0 keeps the stream fully transactional.
//
// interceptRefusal, when set, makes a COMPLETE stream whose stop_reason is
// "refusal" return the refusalFallbackCode sentinel (wrote=false, fail!=nil)
// instead of being replayed downstream — so the caller can re-issue with a
// fallback model. It only fires on the uncommitted (buffered) path: once the
// stream has committed live the refusal bytes are already downstream and cannot
// be swapped. It requires st!=nil; stop_reason is scraped whenever interception
// is armed, even if full JSON validation is disabled.
func captureSSEWindow(ctx context.Context, cancel context.CancelFunc, w http.ResponseWriter, upstreamHdr http.Header, body io.Reader, st *captureStats, keepaliveMs time.Duration, progressGated, interceptRefusal bool) (bool, *failure) {
	return captureSSECore(ctx, cancel, w, upstreamHdr, body, st, keepaliveMs, progressGated, false, newAnthropicWire(interceptRefusal), bufferDeadline(time.Now(), keepaliveMs))
}

// captureSSECore is the wire-agnostic transactional engine. model interprets the
// stream (terminal / in-band error / usage scrape / downstream bytes) for the
// request's wire format; every other concern — buffering, the
// keepalive→commit→live transition, idle/deadline handling, and the wrote/fail
// contract — is identical across wires. earlyCommit (OpenAI/Codex) commits as
// soon as the first forwardable content is buffered, without waiting out the
// window, so streaming UX is preserved while a start-of-stream error is still
// caught pre-commit and converted to a retry.
func captureSSECore(ctx context.Context, cancel context.CancelFunc, w http.ResponseWriter, upstreamHdr http.Header, body io.Reader, st *captureStats, keepaliveMs time.Duration, progressGated, earlyCommit bool, model wireModel, commitBy time.Time) (bool, *failure) {
	sp := newSpool()
	defer func() {
		if st == nil || st.heldRefusal != sp {
			sp.discard()
		}
	}()
	parser := &sseParser{maxEventBytes: max(cfg.maxResponseBytes, 1)}
	committed := false
	headerWritten := false

	flush := func() {
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
	}
	writeHead := func(mode string) {
		if headerWritten {
			return
		}
		writeStreamHead(w, upstreamHdr, mode, st)
		headerWritten = true
	}

	// Reader goroutine so the select loop can fire keepalives during silence.
	type rr struct {
		data []byte
		err  error
	}
	ch := make(chan rr, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := body.Read(buf)
			var cp []byte
			if n > 0 {
				cp = append([]byte(nil), buf[:n]...)
			}
			select {
			case ch <- rr{cp, err}:
			case <-done:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	keepEvery := keepaliveMs
	firstKeepalive := timerOr(keepEvery)
	if !commitBy.IsZero() {
		firstKeepalive = max(time.Until(commitBy), 0)
	}
	keepTimer := time.NewTimer(firstKeepalive)
	defer keepTimer.Stop()
	idleTimer := time.NewTimer(cfg.upstreamByteIdle)
	defer idleTimer.Stop()

	// commitLive sends headers + the buffered prefix, then switches to live
	// streaming. Returns a failure only if replaying the buffer fails.
	commitLive := func() *failure {
		writeHead("live")
		committed = true
		if err := model.replayPrefix(sp, w); err != nil {
			sp.discard()
			return &failure{transient: true, status: http.StatusBadGateway, atype: "api_error", code: "response_replay_failed", message: "failed replaying buffered response"}
		}
		sp.discard()
		return nil
	}
	graceElapsed := false // the transactional window has fired at least once

	for {
		select {
		case r := <-ch:
			if len(r.data) > 0 {
				idleTimer.Reset(cfg.upstreamByteIdle)
				if st != nil && st.respTee != nil {
					st.respTee.Write(r.data)
				}
				wrote, fail, ret := process(r.data, sp, parser, model, w, &committed, writeHead, flush, st)
				if ret {
					return wrote, fail
				}
			}
			// Gemini has no terminal SSE event: accept clean EOF only after the
			// wire reports completion and no partial frame remains. Read faults and
			// cancellation must still fail even after a candidate's finishReason.
			if r.err != nil {
				if r.err == io.EOF && ctx.Err() == nil && !parser.cur.has && len(parser.pending) == 0 && model.completeOnEOF() {
					if !committed {
						return true, replayCaptured(sp, model, w, st, writeHead, flush)
					}
					flush()
					return true, nil
				}
				return onReadErr(r.err, ctx, committed, flush)
			}
			// Clean read, gated caller: the window already elapsed and real
			// forwarded content is now buffered — commit immediately instead of
			// waiting for the next keepalive tick (a full window away, long enough
			// for the watchdog/TTFB ceiling to fire on a healthy stream).
			if !committed && model.forwardable() && (earlyCommit || (progressGated && graceElapsed)) {
				if f := commitLive(); f != nil {
					return true, f
				}
				flush()
			}
		case <-keepTimer.C:
			if keepEvery <= 0 {
				continue
			}
			graceElapsed = true
			if !committed {
				// Gated caller (Workflow stall watchdog): only commit once real
				// forwarded content (a non-withheld content_block_delta) is buffered.
				// Committing a content-less prefix (message_start/ping) would send
				// 200 + keepalive comments — which the watchdog ignores (no progress)
				// — while forfeiting the clean pre-commit retry path. Stay buffered
				// until content arrives; the per-chunk check above then commits
				// promptly, and the idle watchdog + deadline still bound a silent
				// upstream. Ungated callers commit here regardless, so a long
				// ping-only stream keeps getting keepalives and never times out.
				if progressGated && !model.forwardable() {
					keepTimer.Reset(keepEvery)
					continue
				}
				if f := commitLive(); f != nil {
					return true, f
				}
			}
			// Keep the client's idle timer alive during a silent gap. The frame is
			// wire-specific: the Anthropic SDK accepts a spec SSE comment, but Codex's
			// SSE reader only resets its idle timer on a yielded event and discards
			// comments — so the Responses wire sends a skippable unknown-type event that
			// Codex ignores but still counts as activity (see wireModel.keepalive). A
			// genuinely byte-silent upstream is bounded by idleTimer regardless.
			w.Write(model.keepalive())
			flush()
			keepTimer.Reset(keepEvery)
		case <-idleTimer.C:
			f := &failure{transient: true, status: 504, atype: "timeout_error", code: "upstream_idle", message: "upstream stalled with no bytes"}
			if committed {
				return true, f
			}
			sp.discard()
			return false, f
		case <-ctx.Done():
			f := &failure{transient: false, status: 499, atype: "api_error", code: "client_gone", message: "context done"}
			if ctx.Err() == context.DeadlineExceeded {
				f = &failure{transient: true, status: 504, atype: "timeout_error", code: "deadline", message: "attempt deadline exceeded"}
			}
			if committed {
				return true, f
			}
			sp.discard()
			return false, f
		}
		_ = cancel // cancel is owned by the caller's defer
	}
}

// process handles one chunk. ret=true means captureSSE should return (wrote,fail).
// The wire-specific decisions — is this an in-band error, is the stream complete,
// what bytes to forward, what usage to scrape — are delegated to model; process
// owns only the engine mechanics (ping tripwire, spool, commit boundary).
func process(data []byte, sp *spool, parser *sseParser, model wireModel, w http.ResponseWriter,
	committed *bool, writeHead func(string), flush func(), st *captureStats) (bool, *failure, bool) {
	for _, ev := range parser.feed(data) {
		if st != nil {
			st.bytes += int64(len(ev.raw))
			// Tripwire: track the longest run of consecutive upstream `ping` events.
			// A ping run with no intervening content is exactly a content-silent gap
			// — what a Workflow stall watchdog counts toward a kill. Any non-ping
			// event breaks the run. Healthy dense streams stay at 0; a climbing value
			// is the signal Case B (mid-turn silence) has started to appear.
			if ev.name == "ping" {
				st.curPingRun++
				if st.curPingRun > st.maxPingRun {
					st.maxPingRun = st.curPingRun
				}
			} else {
				st.curPingRun = 0
			}
			// scrape is best-effort and cheap — it only unmarshals the few events that
			// carry usage/model/stop, ignoring the rest — so run it unconditionally. That
			// keeps access-log facts (and the Anthropic wire's own stop_reason==refusal
			// check) populated even when JSON validation is off (PROXY_VALIDATE_JSON=0),
			// without the engine needing to know any wire-specific policy.
			model.scrape(ev, st)
		}
		if *committed {
			// Live mode: headers are already sent, so we can't convert to a
			// retryable status — but never forward the raw special identity
			// (overloaded_error / response.failed) of a transient fault. model.live
			// ends the stream on an in-band error; it returns the frames the client
			// should still receive (e.g. a terminal error frame, with
			// fail.delivered) alongside the failure.
			out, done, fail := model.live(ev, ev.raw, st)
			for _, p := range out {
				w.Write(p)
			}
			if fail != nil {
				flush()
				return true, fail, true
			}
			if len(out) == 0 {
				w.Write(model.keepalive())
			}
			flush()
			if done {
				return true, nil, true
			}
			continue
		}
		// uncommitted: validate + advance without touching the spool, then buffer.
		done, fail := model.buffered(ev, st)
		if fail != nil {
			if fail.code == refusalFallbackCode {
				// A complete refusal: keep it (terminal event included) so the
				// caller can deliver it if the fallback model cannot answer.
				if err := sp.write(ev.raw); err != nil {
					sp.discard()
					return false, classifyBufferError(err), true
				}
				st.heldRefusal = sp
				return false, fail, true
			}
			// In-band error or malformed/out-of-order event: nothing is downstream
			// yet, so discard and let the caller convert to a retry.
			sp.discard()
			return false, fail, true
		}
		if err := sp.write(ev.raw); err != nil {
			sp.discard()
			return false, classifyBufferError(err), true
		}
		if done {
			return true, replayCaptured(sp, model, w, st, writeHead, flush), true
		}
	}
	if parser.err != nil {
		return *committed, classifyBufferError(parser.err), true
	}
	return false, nil, false
}

func replayCaptured(sp *spool, model wireModel, w io.Writer, st *captureStats, writeHead func(string), flush func()) *failure {
	writeHead("buffered")
	if err := model.replayBuffered(sp, w, st); err != nil {
		return &failure{transient: true, status: http.StatusBadGateway, atype: "api_error", code: "response_replay_failed", message: "failed replaying buffered response"}
	}
	flush()
	sp.discard()
	return nil
}

// writeStreamHead commits a 200 SSE response carrying the upstream's safe headers.
func writeStreamHead(w http.ResponseWriter, upstreamHdr http.Header, mode string, st *captureStats) {
	for k, v := range safeResponseHeaders(upstreamHdr) {
		w.Header()[k] = v
	}
	// The stream is parsed and re-framed as plain SSE text, never relayed as an
	// encoded body.
	w.Header().Del("Content-Encoding")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Steady-Proxy-Mode", mode)
	if st != nil {
		st.mode = mode
	}
	w.WriteHeader(http.StatusOK)
}

func validateSSEEventShape(ev event) *failure {
	switch {
	case ev.name == "ping" && ev.data == "":
		// The Anthropic SDK skips a ping before it reads data, so a bare
		// `event: ping` is a valid keepalive. Every other named event needs data.
		return nil
	case ev.name != "" && ev.data == "":
		return malformedSSE("stream event missing JSON data")
	case ev.name == "" && ev.data != "":
		return malformedSSE("stream event missing event name")
	default:
		return nil
	}
}

// toolJSONValidator catches an Anthropic edge case the per-event JSON check
// cannot see: a stream can contain valid SSE event JSON while the accumulated
// tool/server-tool input_json_delta fragments form invalid JSON. Claude Code reports
// that downstream as "JSON Parse error: Unexpected EOF"; in transactional mode
// we can classify it before committing bytes.
//
// The one excused case is a max_tokens stop: the token cap cut the input
// mid-JSON, and a retry regenerates the same truncation and bills it again, so
// that stream is the upstream's final answer. stop_reason only arrives in the
// message_delta after the block stop, so the verdict waits for the event that
// follows an invalid block (see judgeTruncatedToolInput).
type toolJSONValidator struct {
	blocks    map[int]*toolJSONBlock
	truncated bool // a tool block closed with invalid input; the next event decides
}

type toolJSONBlock struct {
	jsonInput     bool
	sawInputDelta bool
	input         bytes.Buffer
}

func (v *toolJSONValidator) accept(ev event) *failure {
	if ev.data == "" {
		return nil
	}
	var p streamPayload
	if err := json.Unmarshal([]byte(ev.data), &p); err != nil {
		return malformedSSE("invalid JSON in stream event")
	}
	if v.truncated {
		switch judgeTruncatedToolInput(ev, &p) {
		case truncWait:
		case truncExcused:
			v.truncated = false
		default:
			return malformedSSE(invalidToolInputMessage)
		}
		return nil
	}
	switch p.Type {
	case "content_block_start":
		if blockUsesInputJSON(p.ContentBlock.Type) {
			if v.blocks == nil {
				v.blocks = map[int]*toolJSONBlock{}
			}
			if len(p.ContentBlock.Input) > 0 && !json.Valid(p.ContentBlock.Input) {
				return malformedSSE(invalidToolInputMessage)
			}
			v.blocks[p.Index] = &toolJSONBlock{jsonInput: true}
		}
	case "content_block_delta":
		if p.Delta.Type != "input_json_delta" {
			return nil
		}
		b := v.blocks[p.Index]
		if b == nil {
			// Be forward-compatible with future Anthropic content block types that
			// stream JSON input. We still validate the accumulator at block stop.
			if v.blocks == nil {
				v.blocks = map[int]*toolJSONBlock{}
			}
			b = &toolJSONBlock{jsonInput: true}
			v.blocks[p.Index] = b
		}
		b.sawInputDelta = true
		b.input.WriteString(p.Delta.PartialJSON)
	case "content_block_stop":
		b := v.blocks[p.Index]
		if b != nil {
			if b.jsonInput && b.sawInputDelta && b.input.Len() > 0 && !validJSONObject(b.input.Bytes()) {
				v.truncated = true
			}
			delete(v.blocks, p.Index)
		}
	}
	return nil
}

func malformedSSE(message string) *failure {
	return &failure{transient: true, status: 502, atype: "api_error", code: "malformed_sse", message: message}
}

// invalidToolInputMessage is the failure text for tool input that is not JSON.
const invalidToolInputMessage = "invalid tool input JSON in stream event"

// truncVerdict is the outcome for the event that follows a tool block whose
// accumulated input is not valid JSON.
type truncVerdict int

const (
	truncReject  truncVerdict = iota // not excused: a malformed stream
	truncWait                        // ping: says nothing, keep waiting
	truncExcused                     // message_delta stop_reason=max_tokens: the cap cut the input
)

// judgeTruncatedToolInput decides what an invalid tool input means from the
// event right after its block closes. Only a max_tokens stop excuses it; a ping
// may sit in between, as the API allows ping events anywhere in the stream.
func judgeTruncatedToolInput(ev event, p *streamPayload) truncVerdict {
	switch {
	case ev.name == "ping":
		return truncWait
	case p.Type == "message_delta" && p.Delta.StopReason == "max_tokens":
		return truncExcused
	default:
		return truncReject
	}
}

// scrapeUsage pulls the friendly numbers out of the events that carry them.
// Best-effort: any parse failure is silently ignored (logging must never break a
// stream). Data here is already JSON-validated on the uncommitted path.
func scrapeUsage(ev event, st *captureStats) {
	switch ev.name {
	case "message_start":
		// input tokens + the resolved model. NOTE: output_tokens here is a
		// non-authoritative placeholder (usually 1) — the real count arrives in
		// message_delta, so we deliberately do NOT read it here.
		var m struct {
			Message struct {
				Model string `json:"model"`
				Usage struct {
					Input         int `json:"input_tokens"`
					CacheCreation int `json:"cache_creation_input_tokens"`
					CacheRead     int `json:"cache_read_input_tokens"`
				} `json:"usage"`
			} `json:"message"`
		}
		if json.Unmarshal([]byte(ev.data), &m) == nil {
			if m.Message.Model != "" {
				st.model = m.Message.Model
			}
			st.startInputTok = m.Message.Usage.Input
			st.startCacheCreationTok = m.Message.Usage.CacheCreation
			st.startCacheReadTok = m.Message.Usage.CacheRead
			st.setInputUsage(m.Message.Usage.Input, m.Message.Usage.CacheCreation, m.Message.Usage.CacheRead)
		}
	case "message_delta":
		var d struct {
			Usage struct {
				Input         int `json:"input_tokens"`
				CacheCreation int `json:"cache_creation_input_tokens"`
				CacheRead     int `json:"cache_read_input_tokens"`
				Output        int `json:"output_tokens"`
			} `json:"usage"`
			Delta struct {
				Stop string `json:"stop_reason"`
			} `json:"delta"`
		}
		if json.Unmarshal([]byte(ev.data), &d) == nil {
			// Some Anthropic-compatible gateways put the authoritative input usage
			// on message_delta rather than message_start. Merge it per field: a
			// delta value overrides only when present and > 0, so a delta carrying
			// input_tokens alone keeps message_start's cache counts instead of
			// zeroing them, and GPT-routed streams do not log "in=0".
			u := d.Usage
			if u.Input > 0 || u.CacheCreation > 0 || u.CacheRead > 0 {
				st.deltaInputUsage = true
				st.setInputUsage(positiveOr(u.Input, st.inputTok), positiveOr(u.CacheCreation, st.cacheCreationTok), positiveOr(u.CacheRead, st.cacheReadTok))
			}
			if d.Usage.Output > 0 {
				st.outTok = d.Usage.Output
			}
			if d.Delta.Stop != "" {
				st.stop = d.Delta.Stop
			}
		}
	}
}

func (st *captureStats) setInputUsage(input, cacheCreation, cacheRead int) {
	if totalInputTokens(input, cacheCreation, cacheRead) <= 0 {
		return
	}
	st.inputTok = input
	st.cacheCreationTok = cacheCreation
	st.cacheReadTok = cacheRead
	st.inTok = totalInputTokens(input, cacheCreation, cacheRead)
}

// positiveOr returns v when it is a real count, else the value already held.
func positiveOr(v, held int) int {
	if v > 0 {
		return v
	}
	return held
}

// shouldBackfillMessageStartUsage reports whether the usage merged from
// message_start and message_delta differs from what message_start carried, so
// a delta that merely repeats (or zeroes) the start numbers changes nothing.
func (st *captureStats) shouldBackfillMessageStartUsage() bool {
	if !cfg.validateJSON || st == nil || !st.deltaInputUsage || st.inTok <= 0 {
		return false
	}
	return st.startInputTok != st.inputTok ||
		st.startCacheCreationTok != st.cacheCreationTok ||
		st.startCacheReadTok != st.cacheReadTok
}

func totalInputTokens(input, cacheCreation, cacheRead int) int {
	return input + cacheCreation + cacheRead
}

type streamPayload struct {
	Type         string `json:"type"`
	Index        int    `json:"index"`
	ContentBlock struct {
		Type  string          `json:"type"`
		Input json.RawMessage `json:"input"`
	} `json:"content_block"`
	Delta struct {
		Type        string `json:"type"`
		PartialJSON string `json:"partial_json"`
		StopReason  string `json:"stop_reason"`
	} `json:"delta"`
}

func replayBuffered(r io.Reader, w io.Writer, st *captureStats) error {
	return replayBufferedWithNormalizer(r, w, st, &toolJSONReplayNormalizer{})
}

func replayBufferedWithNormalizer(r io.Reader, w io.Writer, st *captureStats, toolNorm *toolJSONReplayNormalizer) error {
	parser := &sseParser{}
	buf := make([]byte, 32*1024)
	backfill := st != nil && st.shouldBackfillMessageStartUsage()
	startPatched := false
	for {
		n, err := r.Read(buf)
		if n > 0 {
			for _, ev := range parser.feed(buf[:n]) {
				raw := ev.raw
				switch {
				case backfill && !startPatched && ev.name == "message_start":
					if patched, ok := backfillMessageStartUsage(ev, st); ok {
						raw = patched
						startPatched = true
					}
				case startPatched && ev.name == "message_delta":
					// The input usage now lives in message_start. Strip it from the
					// delta only once that backfill happened: a message_start that
					// could not be patched must not lose the numbers.
					if patched, ok := stripMessageDeltaInputUsage(ev); ok {
						raw = patched
					}
				}
				out, err := toolNorm.accept(ev, raw)
				if err != nil {
					return err
				}
				for _, p := range out {
					if _, err := w.Write(p); err != nil {
						return err
					}
				}
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

type toolJSONReplayNormalizer struct {
	blocks map[int]*toolReplayBlock
	// held is the coalesced delta and content_block_stop of a tool block whose
	// input is not valid JSON, plus any pings since, kept until the next event
	// says whether max_tokens cut the input (see judgeTruncatedToolInput). Live
	// streaming cannot look ahead, so the frames wait here instead of failing.
	held [][]byte
}

type toolReplayBlock struct {
	jsonInput     bool
	sawInputDelta bool
	input         bytes.Buffer
	rawDeltas     [][]byte
}

func (n *toolJSONReplayNormalizer) accept(ev event, raw []byte) ([][]byte, error) {
	if !normalizeToolJSONEnabled() {
		return [][]byte{raw}, nil
	}
	if f := validateSSEEventShape(ev); f != nil {
		return nil, errInvalidSSEEventShape
	}
	var p streamPayload
	if ev.data != "" {
		if err := json.Unmarshal([]byte(ev.data), &p); err != nil {
			return nil, err
		}
	}
	if n.held != nil {
		switch judgeTruncatedToolInput(ev, &p) {
		case truncWait:
			n.held = append(n.held, raw)
			return nil, nil
		case truncExcused:
			out := append(n.held, raw)
			n.held = nil
			return out, nil
		default:
			return nil, errInvalidToolJSON
		}
	}
	if ev.data == "" {
		return [][]byte{raw}, nil
	}
	switch p.Type {
	case "content_block_start":
		if blockUsesInputJSON(p.ContentBlock.Type) {
			if n.blocks == nil {
				n.blocks = map[int]*toolReplayBlock{}
			}
			n.blocks[p.Index] = &toolReplayBlock{jsonInput: true}
		}
		return [][]byte{raw}, nil
	case "content_block_delta":
		if p.Delta.Type != "input_json_delta" {
			return [][]byte{raw}, nil
		}
		b := n.blocks[p.Index]
		if b == nil {
			if n.blocks == nil {
				n.blocks = map[int]*toolReplayBlock{}
			}
			b = &toolReplayBlock{jsonInput: true}
			n.blocks[p.Index] = b
		}
		b.sawInputDelta = true
		b.input.WriteString(p.Delta.PartialJSON)
		b.rawDeltas = append(b.rawDeltas, raw)
		return nil, nil
	case "content_block_stop":
		b := n.blocks[p.Index]
		if b == nil || !b.jsonInput || !b.sawInputDelta {
			delete(n.blocks, p.Index)
			return [][]byte{raw}, nil
		}
		if b.input.Len() == 0 {
			delete(n.blocks, p.Index)
			return append(b.rawDeltas, raw), nil
		}
		delete(n.blocks, p.Index)
		if !validJSONObject(b.input.Bytes()) {
			// Cut off or garbled: the upstream's own max_tokens answer or a
			// malformed stream, depending on the next event. Hold both frames.
			n.held = [][]byte{toolInputDeltaEvent(p.Index, b.input.String()), raw}
			return nil, nil
		}
		return [][]byte{toolInputDeltaEvent(p.Index, b.input.String()), raw}, nil
	default:
		return [][]byte{raw}, nil
	}
}

// withholding reports tool input still withheld for a block that has not
// closed: ending the stream now would hand the client a tool call without it.
func (n *toolJSONReplayNormalizer) withholding() bool {
	for _, b := range n.blocks {
		if len(b.rawDeltas) > 0 {
			return true
		}
	}
	return false
}

var errInvalidToolJSON = errors.New("invalid tool input JSON")
var errInvalidSSEEventShape = errors.New("invalid SSE event shape")

func normalizeToolJSONEnabled() bool {
	return cfg.validateJSON && cfg.normalizeToolJSON
}

// progressTracker mirrors, during buffering, whether the buffered prefix would
// forward at least one incremental content_block_delta downstream when replayed —
// the real progress a Workflow stall watchdog counts (keepalive comments and ping
// do not). It accounts for tool-JSON normalization, which withholds
// input_json_delta fragments until the content_block_stop that coalesces them
// (see toolJSONReplayNormalizer): a text/thinking delta is progress immediately,
// a tool block's input becomes progress at its stop, and with normalization off
// every delta is forwarded as-is.
type progressTracker struct {
	sawForwardable   bool
	toolInputPending map[int]bool
}

func (p *progressTracker) accept(ev event) {
	if p.sawForwardable {
		return
	}
	switch ev.name {
	case "content_block_delta":
		if !normalizeToolJSONEnabled() {
			p.sawForwardable = true // nothing is withheld; forwarded as-is
			return
		}
		var s streamPayload
		if json.Unmarshal([]byte(ev.data), &s) != nil {
			p.sawForwardable = true // unparseable here; forwarded as-is at replay
			return
		}
		if s.Delta.Type != "input_json_delta" {
			p.sawForwardable = true // text/thinking delta forwarded as-is
			return
		}
		if p.toolInputPending == nil {
			p.toolInputPending = map[int]bool{}
		}
		p.toolInputPending[s.Index] = true // withheld until this block's stop
	case "content_block_stop":
		var s streamPayload
		if json.Unmarshal([]byte(ev.data), &s) != nil {
			return
		}
		if p.toolInputPending[s.Index] {
			p.sawForwardable = true // the coalesced tool delta is emitted at this stop
			delete(p.toolInputPending, s.Index)
		}
	}
}

func blockUsesInputJSON(blockType string) bool {
	switch blockType {
	case "tool_use", "server_tool_use":
		return true
	default:
		return false
	}
}

func toolInputDeltaEvent(index int, partial string) []byte {
	frame, err := encodeSSEFrame("content_block_delta", map[string]any{
		"type":  "content_block_delta",
		"index": index,
		"delta": map[string]string{
			"type":         "input_json_delta",
			"partial_json": partial,
		},
	})
	if err != nil {
		return nil
	}
	return frame
}

func validJSONObject(p []byte) bool {
	p = bytes.TrimSpace(p)
	return len(p) > 0 && p[0] == '{' && json.Valid(p)
}

// encodeSSEFrame renders one SSE event for a payload the proxy re-encodes.
// HTML escaping is off: encoding/json's default rewrites <, > and & as \u003c,
// \u003e and \u0026, silently changing the text of a tool input or message the
// proxy only meant to re-frame.
func encodeSSEFrame(name string, payload any) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("event: ")
	buf.WriteString(name)
	buf.WriteString("\ndata: ")
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(payload); err != nil { // ends the data line with "\n"
		return nil, err
	}
	buf.WriteByte('\n') // the blank line that terminates the event
	return buf.Bytes(), nil
}

func backfillMessageStartUsage(ev event, st *captureStats) ([]byte, bool) {
	var root map[string]any
	if err := json.Unmarshal([]byte(ev.data), &root); err != nil {
		return nil, false
	}
	msg, ok := root["message"].(map[string]any)
	if !ok {
		return nil, false
	}
	usage, ok := msg["usage"].(map[string]any)
	if !ok {
		usage = map[string]any{}
		msg["usage"] = usage
	}
	usage["input_tokens"] = st.inputTok
	usage["cache_creation_input_tokens"] = st.cacheCreationTok
	usage["cache_read_input_tokens"] = st.cacheReadTok

	frame, err := encodeSSEFrame("message_start", root)
	if err != nil {
		return nil, false
	}
	return frame, true
}

func stripMessageDeltaInputUsage(ev event) ([]byte, bool) {
	var root map[string]any
	if err := json.Unmarshal([]byte(ev.data), &root); err != nil {
		return nil, false
	}
	usage, ok := root["usage"].(map[string]any)
	if !ok {
		return nil, false
	}
	changed := false
	for _, key := range []string{"input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"} {
		if _, ok := usage[key]; ok {
			delete(usage, key)
			changed = true
		}
	}
	if !changed {
		return nil, false
	}
	frame, err := encodeSSEFrame("message_delta", root)
	if err != nil {
		return nil, false
	}
	return frame, true
}

func onReadErr(err error, ctx context.Context, committed bool, flush func()) (bool, *failure) {
	var f *failure
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		f = &failure{transient: true, status: 504, atype: "timeout_error", code: "deadline", message: "attempt deadline exceeded"}
	case ctx.Err() == context.Canceled:
		f = &failure{transient: false, status: 499, atype: "api_error", code: "client_gone", message: "client cancelled"}
	case err == io.EOF || err == io.ErrUnexpectedEOF:
		f = &failure{transient: true, status: 502, atype: "api_error", code: "truncated_stream", message: "stream ended before a complete response"}
	default:
		f = &failure{transient: true, status: 502, atype: "api_error", code: "stream_read_error", message: err.Error()}
	}
	if committed {
		flush()
		return true, f
	}
	return false, f
}

func timerOr(d time.Duration) time.Duration {
	if d <= 0 {
		return time.Hour // effectively disabled
	}
	return d
}
