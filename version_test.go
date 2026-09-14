package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"strings"
	"testing"
)

func withVersionVars(t *testing.T, v, c, d, dirtyFlag string) {
	t.Helper()
	oldV, oldC, oldD, oldDirty := version, commit, buildDate, dirty
	version, commit, buildDate, dirty = v, c, d, dirtyFlag
	t.Cleanup(func() {
		version, commit, buildDate, dirty = oldV, oldC, oldD, oldDirty
	})
}

func TestVersionInfoPrefersStampedValues(t *testing.T) {
	withVersionVars(t, "1.2.3", "abcdef1234567890", "2026-07-05T05:00:00Z", "true")

	vi := currentVersion()
	if vi.Version != "1.2.3" || vi.Commit != "abcdef1234567890" || vi.BuildDate != "2026-07-05T05:00:00Z" {
		t.Fatalf("unexpected version info: %+v", vi)
	}
	if !vi.DirtyKnown || !vi.Dirty {
		t.Fatalf("dirty flag not preserved: %+v", vi)
	}
	if got := vi.token(); got != "1.2.3+abcdef123456.dirty" {
		t.Fatalf("token = %q", got)
	}
	if got := vi.line(); !strings.Contains(got, "version=1.2.3") || !strings.Contains(got, "commit=abcdef1234567890") || !strings.Contains(got, "dirty=true") {
		t.Fatalf("line missing stamped fields: %q", got)
	}
}

func TestApplyVCSBuildSettingsFillsMissingValues(t *testing.T) {
	vi := versionInfo{Program: "steady-proxy", Version: "dev"}
	applyVCSBuildSettings(&vi, []debug.BuildSetting{
		{Key: "vcs.revision", Value: "1234567890abcdef"},
		{Key: "vcs.time", Value: "2026-07-05T05:01:00Z"},
		{Key: "vcs.modified", Value: "false"},
	})

	if vi.Commit != "1234567890abcdef" || vi.BuildDate != "2026-07-05T05:01:00Z" {
		t.Fatalf("VCS settings not applied: %+v", vi)
	}
	if !vi.DirtyKnown || vi.Dirty {
		t.Fatalf("dirty flag not parsed: %+v", vi)
	}
}

func TestVersionCLIOutput(t *testing.T) {
	withVersionVars(t, "1.2.3", "abcdef1234567890", "", "false")

	out, ok := versionCLIOutput([]string{"--version"})
	if !ok {
		t.Fatalf("--version not recognized")
	}
	if !strings.Contains(out, "steady-proxy version=1.2.3") || !strings.Contains(out, "commit=abcdef1234567890") {
		t.Fatalf("unexpected version output: %q", out)
	}
	if _, ok := versionCLIOutput([]string{"--help"}); ok {
		t.Fatalf("--help should not be handled by versionCLIOutput")
	}
}

func TestVersionEndpoint(t *testing.T) {
	withVersionVars(t, "1.2.3", "abcdef1234567890", "2026-07-05T05:00:00Z", "false")

	req := httptest.NewRequest(http.MethodGet, "/__version", nil)
	rec := httptest.NewRecorder()
	if !handleVersionRequest(rec, req) {
		t.Fatalf("version request was not handled")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var got versionInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode version response: %v", err)
	}
	if got.Program != "steady-proxy" || got.Version != "1.2.3" || got.Commit != "abcdef1234567890" {
		t.Fatalf("unexpected version response: %+v", got)
	}

	req = httptest.NewRequest(http.MethodPost, "/__version", nil)
	rec = httptest.NewRecorder()
	if !handleVersionRequest(rec, req) || rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /__version should be handled as 405, got %d", rec.Code)
	}
}
