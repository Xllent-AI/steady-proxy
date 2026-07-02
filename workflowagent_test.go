package main

import (
	"context"
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
		wrote, f = captureSSEWindow(ctx, cancel, rec, http.Header{}, r, nil, window)
		return rec.Header().Get("X-CC-Retry-Proxy-Mode"), wrote, f
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
	if mode := rec.Header().Get("X-CC-Retry-Proxy-Mode"); mode != "live" {
		t.Fatalf("wrapper should honor cfg.keepaliveMs (live), got %q", mode)
	}
	if body := rec.Body.String(); !strings.Contains(body, "message_stop") {
		t.Fatalf("replayed body missing message_stop: %q", body)
	}
}
