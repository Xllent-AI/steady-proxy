package main

import (
	"bytes"
	"context"
	"encoding/json"
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

func (v *streamValidator) terminal() bool { return v.sawStart && v.sawStop && v.open == 0 && !v.invalid }

// captureStats collects human-friendly facts about a streamed response so the
// caller can log one access-log line per request. All fields are best-effort.
type captureStats struct {
	mode    string    // "buffered" | "live" | "" (never committed any bytes)
	bytes   int64     // SSE bytes of the response
	inTok   int       // usage.input_tokens (from message_start)
	outTok  int       // usage.output_tokens (from message_delta)
	stop    string    // delta.stop_reason (e.g. end_turn, max_tokens, tool_use)
	model   string    // resolved model echoed back by the upstream
	respTee io.Writer // optional: when set, every raw response event is teed here (request-log)
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
	sp := newSpool()
	parser := &sseParser{}
	val := &streamValidator{}
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

	keepEvery := cfg.keepaliveMs
	keepTimer := time.NewTimer(timerOr(keepEvery))
	defer keepTimer.Stop()
	idleTimer := time.NewTimer(cfg.upstreamByteIdle)
	defer idleTimer.Stop()

	for {
		select {
		case r := <-ch:
			if len(r.data) > 0 {
				idleTimer.Reset(cfg.upstreamByteIdle)
				wrote, fail, ret := process(r.data, sp, parser, val, w, &committed, writeHead, flush, st)
				if ret {
					return wrote, fail
				}
			}
			if r.err != nil {
				return onReadErr(r.err, ctx, committed, flush)
			}
		case <-keepTimer.C:
			if keepEvery <= 0 {
				continue
			}
			if !committed {
				writeHead("live") // grace elapsed: commit buffered prefix, go live
				sp.replay(w)
				committed = true
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
func process(data []byte, sp *spool, parser *sseParser, val *streamValidator, w http.ResponseWriter,
	committed *bool, writeHead func(string), flush func(), st *captureStats) (bool, *failure, bool) {
	for _, ev := range parser.feed(data) {
		if st != nil {
			st.bytes += int64(len(ev.raw))
			scrapeUsage(ev, st)
			if st.respTee != nil { // capture the full stream, incl. a mid-stream error
				st.respTee.Write(ev.raw)
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
			w.Write(ev.raw)
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
		val.accept(ev)
		if val.invalid {
			sp.discard()
			return false, &failure{transient: true, status: 502, atype: "api_error", code: "malformed_sse", message: "out-of-order stream event"}, true
		}
		if err := sp.write(ev.raw); err != nil {
			sp.discard()
			// transient:false is intentional — the same request will always
			// overflow PROXY_MAX_RESPONSE_BYTES, so retrying cannot help. 502
			// (not 500) signals an upstream-shaped condition, not a proxy fault.
			return false, &failure{status: http.StatusBadGateway, atype: "api_error", code: "response_too_large", message: "response exceeded proxy buffer cap"}, true
		}
		if val.terminal() {
			writeHead("buffered")
			sp.replay(w)
			flush()
			sp.discard()
			return true, nil, true
		}
	}
	return false, nil, false
}

// scrapeUsage pulls the friendly numbers out of the two events that carry them.
// Best-effort: any parse failure is silently ignored (logging must never break a
// stream). Data here is already JSON-validated on the uncommitted path.
func scrapeUsage(ev event, st *captureStats) {
	switch ev.name {
	case "message_start":
		// input_tokens + the resolved model. NOTE: output_tokens here is a
		// non-authoritative placeholder (usually 1) — the real count arrives in
		// message_delta, so we deliberately do NOT read it here.
		var m struct {
			Message struct {
				Model string `json:"model"`
				Usage struct {
					Input int `json:"input_tokens"`
				} `json:"usage"`
			} `json:"message"`
		}
		if json.Unmarshal([]byte(ev.data), &m) == nil {
			if m.Message.Model != "" {
				st.model = m.Message.Model
			}
			if m.Message.Usage.Input > 0 {
				st.inTok = m.Message.Usage.Input
			}
		}
	case "message_delta":
		var d struct {
			Usage struct {
				Output int `json:"output_tokens"`
			} `json:"usage"`
			Delta struct {
				Stop string `json:"stop_reason"`
			} `json:"delta"`
		}
		if json.Unmarshal([]byte(ev.data), &d) == nil {
			if d.Usage.Output > 0 {
				st.outTok = d.Usage.Output
			}
			if d.Delta.Stop != "" {
				st.stop = d.Delta.Stop
			}
		}
	}
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
