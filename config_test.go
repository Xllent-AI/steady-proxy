package main

import (
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

func TestParseConfigDefaults(t *testing.T) {
	c, err := parseConfig(envOf(map[string]string{
		"PROXY_UPSTREAM_URL": "https://gw.example.com/",
		"PROXY_KEEPALIVE_MS": "", // set but empty (how compose passes an unset knob) => default
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.upstream != "https://gw.example.com" || c.upstreamHost != "gw.example.com" {
		t.Fatalf("upstream/host = %q/%q", c.upstream, c.upstreamHost)
	}
	if c.listenAddr != "127.0.0.1:8789" || c.keepaliveMs != 600*time.Second || c.upstreamByteIdle != 600*time.Second ||
		c.maxRequestBytes != 64<<20 || c.sdkRetryCap != 100 || c.txLocalRetries != 0 {
		t.Fatalf("defaults changed: %+v", c)
	}
	if !c.validateJSON || !c.normalizeToolJSON || !c.responsesEarlyCommit || c.verbose {
		t.Fatalf("flag defaults changed: validate=%v normalize=%v earlyCommit=%v verbose=%v",
			c.validateJSON, c.normalizeToolJSON, c.responsesEarlyCommit, c.verbose)
	}
	if c.refusalFallback != "claude-opus-5" {
		t.Fatalf("refusalFallback default = %q", c.refusalFallback)
	}
}

func TestParseConfigFlagsAndOverrides(t *testing.T) {
	c, err := parseConfig(envOf(map[string]string{
		"PROXY_UPSTREAM_URL":                "http://user:pass@127.0.0.1:9000/api/",
		"PROXY_VALIDATE_JSON":               "0",
		"PROXY_VERBOSE":                     "1",
		"PROXY_KEEPALIVE_MS":                "0",
		"PROXY_TRANSACTIONAL_LOCAL_RETRIES": " 3 ",
		"PROXY_REFUSAL_FALLBACK_MODEL":      "off",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.upstream != "http://user:pass@127.0.0.1:9000/api" || c.upstreamHost != "127.0.0.1:9000" {
		t.Fatalf("upstream/host = %q/%q: prefix must be kept, userinfo must not reach the Host header", c.upstream, c.upstreamHost)
	}
	if c.validateJSON || !c.verbose || c.keepaliveMs != 0 || c.txLocalRetries != 3 || c.refusalFallback != "" {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestParseConfigRejectsBadValues(t *testing.T) {
	_, err := parseConfig(envOf(map[string]string{
		"PROXY_UPSTREAM_URL":            "https://gw.example.com",
		"PROXY_KEEPALIVE_MS":            "60s",
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
		`PROXY_KEEPALIVE_MS="60s": not an integer`,
		`PROXY_UPSTREAM_BYTE_IDLE_MS=0: must be >= 1`,
		`PROXY_SDK_RETRY_CAP=-1: must be >= 0`,
		`PROXY_MAX_REQUEST_BYTES=0: must be >= 1`,
		`PROXY_MAX_RESPONSE_BYTES=0: must be >= 1`,
		`PROXY_MAX_REQUEST_DURATION_MS=0: must be >= 1`,
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
