package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Real-shape request bodies (system prompt structure taken from captured
// 2.1.198 traffic). Only the Workflow-tool agent carries the runtime's injected
// prologue in its `system` field.
var (
	// Workflow agent: full CC system + the workflow prologue as a system block.
	wfAgentBody = []byte(`{"model":"gpt-5.5","system":[` +
		`{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.198; cc_entrypoint=cli; cc_is_subagent=true;"},` +
		`{"type":"text","text":"You are Claude Code, Anthropic's official CLI for Claude."},` +
		`{"type":"text","text":"You are a subagent spawned by a workflow orchestration script. Use the tools available to complete the task."}` +
		`],"messages":[{"role":"user","content":"write an essay"}]}`)

	// Workflow agent with `system` sent as a plain string (defensive variant).
	wfAgentStringSystem = []byte(`{"model":"gpt-5.5",` +
		`"system":"You are a subagent spawned by a workflow orchestration script.",` +
		`"messages":[{"role":"user","content":"x"}]}`)

	// Regular Task/SDK subagent: Agent-SDK identity, no workflow prologue.
	taskAgentBody = []byte(`{"model":"claude-opus-4-8","system":[` +
		`{"type":"text","text":"x-anthropic-billing-header: cc_is_subagent=true;"},` +
		`{"type":"text","text":"You are a Claude agent, built on Anthropic's Claude Agent SDK."},` +
		`{"type":"text","text":"You are an agent for Claude Code, Anthropic's official CLI."}` +
		`],"messages":[{"role":"user","content":"probe"}]}`)

	// Interactive main session: NO workflow prologue in system. Critically, its
	// *messages* mention the marker phrase (a session discussing the Workflow
	// tool). This must NOT be misdetected as a workflow agent — the check is
	// scoped to `system` precisely to avoid this false positive.
	mainDiscussingWorkflowsBody = []byte(`{"model":"claude-opus-4-8","system":[` +
		`{"type":"text","text":"You are Claude Code, Anthropic's official CLI for Claude."}` +
		`],"messages":[{"role":"user","content":"Explain: workflow agents are subagents spawned by a workflow orchestration script."}]}`)
)

func TestIsWorkflowAgent(t *testing.T) {
	cases := []struct {
		name string
		body []byte
		want bool
	}{
		{"workflow agent (system blocks)", wfAgentBody, true},
		{"workflow agent (system string)", wfAgentStringSystem, true},
		{"regular task/SDK subagent", taskAgentBody, false},
		{"main session discussing workflows (marker only in messages)", mainDiscussingWorkflowsBody, false},
		{"no system field", []byte(`{"model":"m","messages":[]}`), false},
		{"malformed body", []byte(`{not json`), false},
		{"empty body", []byte(``), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isWorkflowAgent(tc.body); got != tc.want {
				t.Fatalf("isWorkflowAgent = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestAgentKindFullLabelsWorkflow guards the log label: a workflow agent is
// "wf" (not "main" or "sub") regardless of headers; everything else falls back
// to the header-only agentKind.
func TestAgentKindFullLabelsWorkflow(t *testing.T) {
	newReq := func(hdr map[string]string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		for k, v := range hdr {
			r.Header.Set(k, v)
		}
		return r
	}
	cases := []struct {
		name string
		hdr  map[string]string
		body []byte
		want string
	}{
		{"workflow body -> wf (even with cli UA / no parent)", map[string]string{"User-Agent": "claude-cli/2.1.198 (external, cli)"}, wfAgentBody, "wf"},
		{"task subagent body + parent header -> sub", map[string]string{"X-Claude-Code-Parent-Agent-Id": "abc"}, taskAgentBody, "sub"},
		{"task subagent body + sdk-cli UA -> sub", map[string]string{"User-Agent": "claude-cli/2.1.198 (external, sdk-cli)"}, taskAgentBody, "sub"},
		{"main body + cli UA -> main", map[string]string{"User-Agent": "claude-cli/2.1.198 (external, cli)"}, mainDiscussingWorkflowsBody, "main"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := agentKindFull(newReq(tc.hdr), tc.body); got != tc.want {
				t.Fatalf("agentKindFull = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCaptureSSEWindowEarlyCommit verifies the core of the fix: a short window
// commits the buffered prefix and streams the rest live (so a Workflow agent
// sees progress), while a long window keeps the same stream fully buffered.
func TestCaptureSSEWindowEarlyCommit(t *testing.T) {
	useDefaultConfig(t)
	cfg.upstreamByteIdle = 5 * time.Second // don't trip the idle watchdog during gaps

	// Deliver a complete, valid stream in chunks with a gap between each, so the
	// stream takes well over the short window to finish arriving.
	chunks := splitEvery(goodStream, 60)

	run := func(window time.Duration) (mode string, wrote bool, f *failure) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rec := httptest.NewRecorder()
		r := &gapReader{ctx: ctx, chunks: chunks, gap: 40 * time.Millisecond}
		wrote, f = captureSSEWindow(ctx, cancel, rec, http.Header{}, r, nil, window, true, false)
		return rec.Header().Get("X-Steady-Proxy-Mode"), wrote, f
	}

	t.Run("short window commits live before completion", func(t *testing.T) {
		mode, wrote, f := run(15 * time.Millisecond)
		if f != nil {
			t.Fatalf("expected success, got failure %+v", *f)
		}
		if mode != "live" {
			t.Fatalf("short window: want live (early commit), got %q", mode)
		}
		if !wrote {
			t.Fatalf("short window: expected committed/live write")
		}
	})

	t.Run("long window stays fully buffered", func(t *testing.T) {
		mode, _, f := run(10 * time.Second)
		if f != nil {
			t.Fatalf("expected success, got failure %+v", *f)
		}
		if mode != "buffered" {
			t.Fatalf("long window: want buffered, got %q", mode)
		}
	})
}

// TestWindowCommitGatedOnProgress verifies the window does NOT commit while only
// content-less events (message_start / ping) are buffered. Committing then would
// send 200 + keepalive comments — which a Workflow stall watchdog ignores (no
// progress) — while forfeiting the clean pre-commit retry path for nothing. So an
// error arriving before any content_block_delta must still stay uncommitted and
// convert to a clean retry, even though the window has already elapsed.
func TestWindowCommitGatedOnProgress(t *testing.T) {
	useDefaultConfig(t)
	cfg.upstreamByteIdle = 3 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	// The 20ms window elapses over several keepalive ticks while only a
	// content-less message_start + ping are buffered; then an overloaded error.
	r := &gapReader{ctx: ctx, chunks: []string{
		"event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
		"event: ping\ndata: {\"type\":\"ping\"}\n\n",
		"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n",
	}, gap: 30 * time.Millisecond}
	wrote, f := captureSSEWindow(ctx, cancel, rec, http.Header{}, r, nil, 20*time.Millisecond, true, false)

	if wrote {
		t.Fatalf("content-less prefix must stay uncommitted (cleanly retryable), but committed; body=%q", rec.Body.String())
	}
	if f == nil || !f.transient || f.code != "sse_overloaded" {
		t.Fatalf("want transient sse_overloaded pre-commit retry, got %+v", f)
	}
	if mode := rec.Header().Get("X-Steady-Proxy-Mode"); mode != "" {
		t.Fatalf("no bytes should be committed, but mode=%q", mode)
	}
}

// TestWindowGateIgnoresWithheldToolDelta verifies the gate does not treat a tool
// input_json_delta as forwarded progress. With normalization on (default), those
// fragments are coalesced until content_block_stop, so committing on one would
// deliver only start events + keepalive comments — no progress for the watchdog —
// while dropping the clean retry path. So a gated stream whose only delta so far
// is a withheld tool fragment must stay uncommitted and convert an error cleanly.
func TestWindowGateIgnoresWithheldToolDelta(t *testing.T) {
	useDefaultConfig(t) // tool-JSON normalization is on by default
	cfg.upstreamByteIdle = 3 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	// The 20ms window elapses while only a tool block's start + input_json_delta
	// (withheld by the normalizer) are buffered; then an overloaded error.
	r := &gapReader{ctx: ctx, chunks: []string{
		"event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\"}}\n\n",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"a\\\":\"}}\n\n",
		"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n",
	}, gap: 30 * time.Millisecond}
	wrote, f := captureSSEWindow(ctx, cancel, rec, http.Header{}, r, nil, 20*time.Millisecond, true, false)

	if wrote {
		t.Fatalf("withheld tool delta must not trip the gate (stay cleanly retryable), but committed; body=%q", rec.Body.String())
	}
	if f == nil || !f.transient || f.code != "sse_overloaded" {
		t.Fatalf("want transient sse_overloaded pre-commit retry, got %+v", f)
	}
	if mode := rec.Header().Get("X-Steady-Proxy-Mode"); mode != "" {
		t.Fatalf("no bytes should be committed, but mode=%q", mode)
	}
}

// TestWindowGateOpensOnToolBlockStop verifies the gate DOES open when a
// tool/server-tool block's withheld input becomes forwardable: the normalizer
// emits the coalesced content_block_delta at content_block_stop, so that stop is
// real progress. A gated stream that completes a tool block after the window must
// therefore commit live (feeding the watchdog) rather than stay buffered.
func TestWindowGateOpensOnToolBlockStop(t *testing.T) {
	useDefaultConfig(t) // tool-JSON normalization is on by default
	cfg.upstreamByteIdle = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	// window=20ms; the tool block (start + complete input_json_delta) closes after
	// grace, and message_stop arrives in a later chunk so the live commit at the
	// tool stop is observable before terminal.
	r := &gapReader{ctx: ctx, chunks: []string{
		"event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\"}}\n\n",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"q\\\":\\\"hi\\\"}\"}}\n\n",
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n" +
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
	}, gap: 40 * time.Millisecond}
	wrote, f := captureSSEWindow(ctx, cancel, rec, http.Header{}, r, nil, 20*time.Millisecond, true, false)

	if f != nil {
		t.Fatalf("expected success, got failure %+v", *f)
	}
	if !wrote {
		t.Fatalf("expected a committed write")
	}
	if mode := rec.Header().Get("X-Steady-Proxy-Mode"); mode != "live" {
		t.Fatalf("a completed tool block after grace should commit live, got %q", mode)
	}
	if body := rec.Body.String(); !strings.Contains(body, "message_stop") {
		t.Fatalf("streamed body missing message_stop: %q", body)
	}
}

// dataThenEOFReader returns `first` on the first read, then (after `delay`) the
// `second` chunk together with io.EOF in a single read — exercising the standard
// io contract where a Reader may return n>0 with a terminal error in one call.
type dataThenEOFReader struct {
	first, second string
	delay         time.Duration
	i             int
}

func (r *dataThenEOFReader) Read(p []byte) (int, error) {
	switch r.i {
	case 0:
		r.i++
		return copy(p, r.first), nil
	case 1:
		r.i++
		time.Sleep(r.delay)
		return copy(p, r.second), io.EOF
	default:
		return 0, io.EOF
	}
}

// TestGatedCommitSkippedOnDataPlusEOF verifies that when the final read delivers
// the first real delta together with io.EOF (a truncation, no message_stop), the
// gated caller converts it to a clean uncommitted retry instead of committing the
// partial buffer and logging a post-commit DROP. The read error must be handled
// before the deferred commit.
func TestGatedCommitSkippedOnDataPlusEOF(t *testing.T) {
	useDefaultConfig(t)
	cfg.upstreamByteIdle = 3 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	r := &dataThenEOFReader{
		first: "event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
		second: "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0}\n\n" +
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n",
		delay: 60 * time.Millisecond, // lands after the 20ms window (grace) elapses
	}
	wrote, f := captureSSEWindow(ctx, cancel, rec, http.Header{}, r, nil, 20*time.Millisecond, true, false)

	if wrote {
		t.Fatalf("a data+EOF truncation must stay uncommitted (clean retry), but committed; body=%q", rec.Body.String())
	}
	if f == nil || !f.transient || f.code != "truncated_stream" {
		t.Fatalf("want transient truncated_stream pre-commit retry, got %+v", f)
	}
	if mode := rec.Header().Get("X-Steady-Proxy-Mode"); mode != "" {
		t.Fatalf("no bytes should be committed, but mode=%q", mode)
	}
}

// TestWindowCommitsLiveOnDelayedContent locks the fix for the delayed-content
// case: when the window elapses while only content-less events are buffered and
// the first real content_block_delta arrives afterward, the proxy must commit
// live *on that delta* — not wait a whole extra window for the next keepalive
// tick, which could let a healthy long stream trip the client's TTFB ceiling.
// The timing is chosen so a "reset to a full new window" bug would instead let
// the stream complete in buffered mode before the next tick (mode="buffered").
func TestWindowCommitsLiveOnDelayedContent(t *testing.T) {
	useDefaultConfig(t)
	cfg.upstreamByteIdle = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	// window=150ms; grace fires at 150ms with only message_start+ping buffered.
	// The delta lands at ~180ms (must commit live then); the stream completes at
	// ~270ms — before a buggy second tick at 300ms would have committed buffered.
	r := &gapReader{ctx: ctx, chunks: []string{
		"event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
		"event: ping\ndata: {\"type\":\"ping\"}\n\n",
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0}\n\n" +
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n",
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
			"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n" +
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
	}, gap: 90 * time.Millisecond}
	wrote, f := captureSSEWindow(ctx, cancel, rec, http.Header{}, r, nil, 150*time.Millisecond, true, false)

	if f != nil {
		t.Fatalf("expected success, got failure %+v", *f)
	}
	if !wrote {
		t.Fatalf("expected a committed write")
	}
	if mode := rec.Header().Get("X-Steady-Proxy-Mode"); mode != "live" {
		t.Fatalf("delayed content after grace must commit live promptly, got %q", mode)
	}
	if body := rec.Body.String(); !strings.Contains(body, "message_stop") {
		t.Fatalf("streamed body missing message_stop: %q", body)
	}
}

// TestCaptureSSEWrapperUsesConfiguredKeepalive ensures the thin captureSSE
// wrapper still honors cfg.keepaliveMs (so existing callers/tests are unchanged).
func TestCaptureSSEWrapperUsesConfiguredKeepalive(t *testing.T) {
	useDefaultConfig(t)
	cfg.keepaliveMs = 15 * time.Millisecond
	cfg.upstreamByteIdle = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	r := &gapReader{ctx: ctx, chunks: splitEvery(goodStream, 60), gap: 40 * time.Millisecond}
	if _, f := captureSSE(ctx, cancel, rec, http.Header{}, r, nil); f != nil {
		t.Fatalf("expected success, got %+v", *f)
	}
	if mode := rec.Header().Get("X-Steady-Proxy-Mode"); mode != "live" {
		t.Fatalf("wrapper should honor cfg.keepaliveMs (live), got %q", mode)
	}
	if body := rec.Body.String(); !strings.Contains(body, "message_stop") {
		t.Fatalf("replayed body missing message_stop: %q", body)
	}
}
