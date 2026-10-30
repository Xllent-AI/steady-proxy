package main

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

func envOf(kv map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := kv[k]
		return v, ok
	}
}

func TestParseConfigFlagsAndOverrides(t *testing.T) {
	c, err := parseConfig(envOf(map[string]string{
		"PROXY_UPSTREAM_URL":                   "http://user:pass@127.0.0.1:9000/api/",
		"PROXY_VALIDATE_JSON":                  "0",
		"PROXY_VERBOSE":                        "1",
		"PROXY_KEEPALIVE_MS":                   "0",
		"PROXY_TRANSACTIONAL_LOCAL_RETRIES":    " 3 ",
		"PROXY_CLAUDE_REFUSAL_FALLBACK_MODELS": "off",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.upstream != "http://user:pass@127.0.0.1:9000/api" || c.upstreamHost != "127.0.0.1:9000" {
		t.Fatalf("upstream/host = %q/%q: prefix must be kept, userinfo must not reach the Host header", c.upstream, c.upstreamHost)
	}
	if c.validateJSON || !c.verbose || c.keepaliveMs != 0 || c.txLocalRetries != 3 || len(c.claudeRefusalFallbacks) != 0 {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestParseConfigRejectsBadValues(t *testing.T) {
	_, err := parseConfig(envOf(map[string]string{
		"PROXY_UPSTREAM_URL":            "https://gw.example.com",
		"PROXY_KEEPALIVE_MS":            "60s",
		"PROXY_GEMINI_BUFFER_MS":        "-1",
		"PROXY_UPSTREAM_BYTE_IDLE_MS":   "0",
		"PROXY_SDK_RETRY_CAP":           "-1",
		"PROXY_MAX_REQUEST_BYTES":       "0",
		"PROXY_MAX_RESPONSE_BYTES":      "0",
		"PROXY_MAX_REQUEST_DURATION_MS": "0",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{
		"PROXY_KEEPALIVE_MS", "PROXY_GEMINI_BUFFER_MS",
		"PROXY_UPSTREAM_BYTE_IDLE_MS", "PROXY_SDK_RETRY_CAP",
		"PROXY_MAX_REQUEST_BYTES", "PROXY_MAX_RESPONSE_BYTES",
		"PROXY_MAX_REQUEST_DURATION_MS",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should name %q; got:\n%v", want, err)
		}
	}
}

func TestParseUpstreamRejectsUnusableURLs(t *testing.T) {
	for _, raw := range []string{"", "   ", "gw.example.com", "ftp://gw.example.com", "https://",
		"https://gw.example.com?x=1", "https://gw.example.com#f",
		"https://gw.example.com/api?", "https://gw.example.com/api#"} {
		if _, _, err := parseUpstream(raw); err == nil || !strings.Contains(err.Error(), "PROXY_UPSTREAM_URL") {
			t.Errorf("parseUpstream(%q) = %v, want an error naming PROXY_UPSTREAM_URL", raw, err)
		}
	}
	if _, err := parseConfig(envOf(nil)); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("missing upstream must fail startup, got %v", err)
	}
}

func TestDurationConfigurationBounds(t *testing.T) {
	const maxMS = math.MaxInt64 / int64(time.Millisecond)
	for _, key := range []string{
		"PROXY_MAX_REQUEST_DURATION_MS", "PROXY_KEEPALIVE_MS",
		"PROXY_RESP_HEADER_TIMEOUT_MS", "PROXY_UPSTREAM_BYTE_IDLE_MS",
		"PROXY_WORKFLOW_KEEPALIVE_MS", "PROXY_RESPONSES_KEEPALIVE_MS",
		"PROXY_RESPONSES_BUFFER_MS", "PROXY_GEMINI_BUFFER_MS",
		"PROXY_LOCAL_RETRY_EXTRA_BACKOFF_CAP_MS",
	} {
		t.Run(key, func(t *testing.T) {
			values := map[string]string{"PROXY_UPSTREAM_URL": "https://example.com", key: strconv.FormatInt(maxMS+1, 10)}
			if _, err := parseConfig(envOf(values)); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("overflowing duration must name the rejected setting: %v", err)
			}
			values[key] = strconv.FormatInt(maxMS, 10)
			if _, err := parseConfig(envOf(values)); err != nil {
				t.Fatalf("largest representable millisecond duration rejected: %v", err)
			}
		})
	}
	useDefaultConfig(t)
	cfg.localBackoffCap = time.Duration(maxMS) * time.Millisecond
	if got := localRetryExtraBackoff(100); got != cfg.localBackoffCap {
		t.Fatalf("large retry delay overflowed: got %v, want cap %v", got, cfg.localBackoffCap)
	}
}

func TestInvalidUpstreamErrorsDoNotExposeCredentials(t *testing.T) {
	for _, raw := range []string{
		"http://audit-user:audit-password@example.com/%zz",
		"ftp://audit-user:audit-password@example.com",
		"http://audit-user:audit-password@example.com?key=query-secret",
	} {
		_, _, err := parseUpstream(raw)
		if err == nil {
			t.Fatalf("invalid upstream accepted: %q", raw)
		}
		for _, secret := range []string{"audit-user", "audit-password", "query-secret"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("configuration error leaked credential: %v", err)
			}
		}
	}
}

func TestClaudeRefusalFallbackConfiguration(t *testing.T) {
	const key = "PROXY_CLAUDE_REFUSAL_FALLBACK_MODELS"
	for _, tc := range []struct {
		value   string
		want    string
		invalid bool
	}{
		{"", "", false}, {"  ", "", false}, {"off", "", false},
		{"None", "", false}, {" disabled ", "", false},
		{" claude-a , claude-b,claude-a ", "claude-a,claude-b", false},
		{"claude-a,", "", true}, {",claude-a", "", true},
		{"claude-a, ,claude-b", "", true}, {"claude-a,qwen", "", true},
		{"qwen", "", true}, {"claude-a,off", "", true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			c, err := parseConfig(envOf(map[string]string{"PROXY_UPSTREAM_URL": "http://example.com", key: tc.value}))
			if tc.invalid {
				if err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("invalid list must name %s: %v", key, err)
				}
				return
			}
			if err != nil || strings.Join(c.claudeRefusalFallbacks, ",") != tc.want {
				t.Fatalf("fallbacks = %v, err = %v; want %q", c.claudeRefusalFallbacks, err, tc.want)
			}
		})
	}
	for _, old := range []string{"", "off", "claude-opus-5"} {
		_, err := parseConfig(envOf(map[string]string{
			"PROXY_UPSTREAM_URL":           "http://example.com",
			"PROXY_REFUSAL_FALLBACK_MODEL": old,
			key:                            "claude-a",
		}))
		if err == nil || !strings.Contains(err.Error(), "PROXY_REFUSAL_FALLBACK_MODEL") || !strings.Contains(err.Error(), key) {
			t.Fatalf("legacy setting must explain migration even when empty or new key is present: %v", err)
		}
	}
}
