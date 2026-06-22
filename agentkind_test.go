package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestAgentKind guards the main-vs-sub classifier. The regression it protects
// against: workflow / top-level SDK agents carry the Agent-SDK User-Agent
// (".../sdk-cli") but no parent-agent header, and were misreported as "main".
func TestAgentKind(t *testing.T) {
	const ua = "User-Agent"
	cases := []struct {
		name    string
		headers map[string]string
		want    string
	}{
		{
			name:    "interactive cli is main",
			headers: map[string]string{ua: "claude-cli/2.1.185 (external, cli)"},
			want:    "main",
		},
		{
			name:    "nested subagent via parent header",
			headers: map[string]string{ua: "claude-cli/2.1.185 (external, sdk-cli)", "X-Claude-Code-Parent-Agent-Id": "ab6b76b6be6f95284"},
			want:    "sub",
		},
		{
			name:    "sdk agent without parent header is still sub",
			headers: map[string]string{ua: "claude-cli/2.1.185 (external, sdk-cli)"},
			want:    "sub",
		},
		{
			name:    "parent header alone (no UA) is sub",
			headers: map[string]string{"X-Claude-Code-Parent-Agent-Id": "ab6b76b6be6f95284"},
			want:    "sub",
		},
		{
			name:    "no signals defaults to main",
			headers: map[string]string{},
			want:    "main",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			for k, v := range tc.headers {
				r.Header.Set(k, v)
			}
			if got := agentKind(r); got != tc.want {
				t.Fatalf("agentKind = %q, want %q", got, tc.want)
			}
		})
	}
}
