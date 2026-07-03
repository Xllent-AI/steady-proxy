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
		f, err := os.CreateTemp(cfg.spoolDir, "ccrp-*.sse")
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

func (s *spool) replay(w io.Writer) error {
	return s.replayWithUsage(w, nil)
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
	pending []byte
	curRaw  []byte
	cur     curEvent
}

func (p *sseParser) feed(b []byte) []event {
	p.pending = append(p.pending, b...)
	var out []event
	for {
		i := bytes.IndexByte(p.pending, '\n')
		if i < 0 {
			break
		}
		p.curRaw = append(p.curRaw, p.pending[:i+1]...) // keep exact bytes incl '\n'
		line := p.pending[:i]
		p.pending = p.pending[i+1:]
		if n := len(line); n > 0 && line[n-1] == '\r' {
			line = line[:n-1]
		}
		if len(line) == 0 { // event terminator
			if p.cur.has {
				out = append(out, event{name: p.cur.name, data: p.cur.data, raw: append([]byte(nil), p.curRaw...)})
				p.cur = curEvent{}
			}
			p.curRaw = p.curRaw[:0]
			continue
		}
		if line[0] == ':' { // comment / keepalive
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
	if len(p.pending) == 0 {
		p.pending = p.pending[:0]
	} else if cap(p.pending) > 1<<16 && cap(p.pending) > 4*len(p.pending) {
		p.pending = append([]byte(nil), p.pending...)
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
	return captureSSEWindow(ctx, cancel, w, upstreamHdr, body, st, cfg.keepaliveMs, false)
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
func captureSSEWindow(ctx context.Context, cancel context.CancelFunc, w http.ResponseWriter, upstreamHdr http.Header, body io.Reader, st *captureStats, keepaliveMs time.Duration, progressGated bool) (bool, *failure) {
	sp := newSpool()
	parser := &sseParser{}
	val := &streamValidator{}
	toolVal := &toolJSONValidator{}
	toolNorm := &toolJSONReplayNormalizer{}
	prog := &progressTracker{}
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
		for k, v := range safeResponseHeaders(upstreamHdr) {
			w.Header()[k] = v
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-CC-Retry-Proxy-Mode", mode)
		if st != nil {
			st.mode = mode
		}
		w.WriteHeader(http.StatusOK)
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
	keepTimer := time.NewTimer(timerOr(keepEvery))
	defer keepTimer.Stop()
	idleTimer := time.NewTimer(cfg.upstreamByteIdle)
	defer idleTimer.Stop()

	// commitLive sends headers + the buffered prefix, then switches to live
	// streaming. Returns a failure only if replaying the buffer fails.
	commitLive := func() *failure {
		writeHead("live")
		committed = true
		if err := sp.replayWithNormalizer(w, toolNorm); err != nil {
			sp.discard()
			return &failure{transient: true, status: http.StatusBadGateway, atype: "api_error", code: "response_replay_failed", message: "failed replaying buffered response"}
		}
		return nil
	}
	graceElapsed := false // the transactional window has fired at least once

	for {
		select {
		case r := <-ch:
			if len(r.data) > 0 {
				idleTimer.Reset(cfg.upstreamByteIdle)
				wrote, fail, ret := process(r.data, sp, parser, val, toolVal, toolNorm, prog, w, &committed, writeHead, flush, st)
				if ret {
					return wrote, fail
				}
			}
			// Check the read error BEFORE any deferred commit: a read can return
			// data together with io.EOF, and if that final read is a truncation
			// (no message_stop) we must convert it to a clean uncommitted retry
			// rather than commit the partial buffer and log a post-commit DROP.
			if r.err != nil {
				return onReadErr(r.err, ctx, committed, flush)
			}
			// Clean read, gated caller: the window already elapsed and real
			// forwarded content is now buffered — commit immediately instead of
			// waiting for the next keepalive tick (a full window away, long enough
			// for the watchdog/TTFB ceiling to fire on a healthy stream).
			if progressGated && !committed && graceElapsed && prog.sawForwardable {
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
				if progressGated && !prog.sawForwardable {
					keepTimer.Reset(keepEvery)
					continue
				}
				if f := commitLive(); f != nil {
					return true, f
				}
			}
			io.WriteString(w, ": keepalive\n\n")
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
func process(data []byte, sp *spool, parser *sseParser, val *streamValidator, toolVal *toolJSONValidator, toolNorm *toolJSONReplayNormalizer, prog *progressTracker, w http.ResponseWriter,
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
			if cfg.validateJSON {
				scrapeUsage(ev, st)
			}
			if st.respTee != nil { // capture the full stream, incl. a mid-stream error
				st.respTee.Write(ev.raw)
			}
		}
		if cfg.validateJSON {
			if f := validateSSEEventShape(ev); f != nil {
				sp.discard()
				if *committed {
					return true, f, true
				}
				return false, f, true
			}
		}
		if *committed {
			// Live mode: headers are already sent, so we can't convert to a
			// retryable status — but never forward the raw special identity
			// (overloaded_error / rate_limit_error). End the stream as a DROP so
			// Claude Code's native truncated-stream retry takes over, and the
			// access log keeps the true cause instead of a generic truncation.
			if ev.name == "error" {
				return true, classifySSEError(ev.data), true
			}
			out, err := toolNorm.accept(ev, ev.raw)
			if err != nil {
				return true, malformedSSE("invalid tool input JSON in stream event"), true
			}
			for _, p := range out {
				w.Write(p)
			}
			if len(out) == 0 {
				io.WriteString(w, ": keepalive\n\n")
			}
			flush()
			val.accept(ev)
			if val.terminal() {
				return true, nil, true
			}
			continue
		}
		// uncommitted: validate before spooling/committing anything.
		if ev.name == "error" {
			sp.discard()
			return false, classifySSEError(ev.data), true
		}
		if cfg.validateJSON && ev.data != "" && !json.Valid([]byte(ev.data)) {
			sp.discard()
			return false, &failure{transient: true, status: 502, atype: "api_error", code: "malformed_sse", message: "invalid JSON in stream event"}, true
		}
		if cfg.validateJSON {
			if f := toolVal.accept(ev); f != nil {
				sp.discard()
				return false, f, true
			}
		}
		val.accept(ev)
		if val.invalid {
			sp.discard()
			return false, &failure{transient: true, status: 502, atype: "api_error", code: "malformed_sse", message: "out-of-order stream event"}, true
		}
		prog.accept(ev) // does the buffered prefix now forward incremental progress?
		if err := sp.write(ev.raw); err != nil {
			sp.discard()
			// transient:false is intentional — the same request will always
			// overflow PROXY_MAX_RESPONSE_BYTES, so retrying cannot help. 502
			// (not 500) signals an upstream-shaped condition, not a proxy fault.
			return false, &failure{status: http.StatusBadGateway, atype: "api_error", code: "response_too_large", message: "response exceeded proxy buffer cap"}, true
		}
		if val.terminal() {
			writeHead("buffered")
			if err := sp.replayWithUsage(w, st); err != nil {
				sp.discard()
				return true, &failure{transient: true, status: http.StatusBadGateway, atype: "api_error", code: "response_replay_failed", message: "failed replaying buffered response"}, true
			}
			flush()
			sp.discard()
			return true, nil, true
		}
	}
	return false, nil, false
}

func validateSSEEventShape(ev event) *failure {
	switch {
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
type toolJSONValidator struct {
	blocks map[int]*toolJSONBlock
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
	switch p.Type {
	case "content_block_start":
		if blockUsesInputJSON(p.ContentBlock.Type) {
			if v.blocks == nil {
				v.blocks = map[int]*toolJSONBlock{}
			}
			if len(p.ContentBlock.Input) > 0 && !json.Valid(p.ContentBlock.Input) {
				return malformedSSE("invalid tool input JSON in stream event")
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
				return malformedSSE("invalid tool input JSON in stream event")
			}
			delete(v.blocks, p.Index)
		}
	}
	return nil
}

func malformedSSE(message string) *failure {
	return &failure{transient: true, status: 502, atype: "api_error", code: "malformed_sse", message: message}
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
			// on message_delta rather than message_start. Prefer the latest non-zero
			// total so GPT-routed streams do not log "in=0".
			if in := totalInputTokens(d.Usage.Input, d.Usage.CacheCreation, d.Usage.CacheRead); in > 0 {
				st.deltaInputUsage = true
				st.setInputUsage(d.Usage.Input, d.Usage.CacheCreation, d.Usage.CacheRead)
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
	} `json:"delta"`
}

func replayBuffered(r io.Reader, w io.Writer, st *captureStats) error {
	return replayBufferedWithNormalizer(r, w, st, &toolJSONReplayNormalizer{})
}

func replayBufferedWithNormalizer(r io.Reader, w io.Writer, st *captureStats, toolNorm *toolJSONReplayNormalizer) error {
	parser := &sseParser{}
	buf := make([]byte, 32*1024)
	startPatched := false
	for {
		n, err := r.Read(buf)
		if n > 0 {
			for _, ev := range parser.feed(buf[:n]) {
				raw := ev.raw
				if st != nil && st.shouldBackfillMessageStartUsage() && !startPatched && ev.name == "message_start" {
					patched, ok := backfillMessageStartUsage(ev, st)
					if ok {
						raw = patched
						startPatched = true
					}
				}
				if st != nil && st.shouldBackfillMessageStartUsage() && ev.name == "message_delta" {
					patched, ok := stripMessageDeltaInputUsage(ev)
					if ok {
						raw = patched
						ev.raw = raw
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
}

type toolReplayBlock struct {
	jsonInput     bool
	sawInputDelta bool
	input         bytes.Buffer
	rawDeltas     [][]byte
}

func (n *toolJSONReplayNormalizer) accept(ev event, raw []byte) ([][]byte, error) {
	if normalizeToolJSONEnabled() {
		if f := validateSSEEventShape(ev); f != nil {
			return nil, errInvalidSSEEventShape
		}
	}
	if !normalizeToolJSONEnabled() || ev.data == "" {
		return [][]byte{raw}, nil
	}
	var p streamPayload
	if err := json.Unmarshal([]byte(ev.data), &p); err != nil {
		return nil, err
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
		if !validJSONObject(b.input.Bytes()) {
			return nil, errInvalidToolJSON
		}
		delete(n.blocks, p.Index)
		return [][]byte{toolInputDeltaEvent(p.Index, b.input.String()), raw}, nil
	default:
		return [][]byte{raw}, nil
	}
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
	payload := map[string]any{
		"type":  "content_block_delta",
		"index": index,
		"delta": map[string]string{
			"type":         "input_json_delta",
			"partial_json": partial,
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	return []byte("event: content_block_delta\ndata: " + string(data) + "\n\n")
}

func validJSONObject(p []byte) bool {
	p = bytes.TrimSpace(p)
	return len(p) > 0 && p[0] == '{' && json.Valid(p)
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

	data, err := json.Marshal(root)
	if err != nil {
		return nil, false
	}
	return []byte("event: message_start\ndata: " + string(data) + "\n\n"), true
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
	data, err := json.Marshal(root)
	if err != nil {
		return nil, false
	}
	return []byte("event: message_delta\ndata: " + string(data) + "\n\n"), true
}

func onReadErr(err error, ctx context.Context, committed bool, flush func()) (bool, *failure) {
	var f *failure
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		f = &failure{transient: true, status: 504, atype: "timeout_error", code: "deadline", message: "attempt deadline exceeded"}
	case ctx.Err() == context.Canceled:
		f = &failure{transient: false, status: 499, atype: "api_error", code: "client_gone", message: "client cancelled"}
	case err == io.EOF || err == io.ErrUnexpectedEOF:
		f = &failure{transient: true, status: 502, atype: "api_error", code: "truncated_stream", message: "EOF before message_stop"}
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
