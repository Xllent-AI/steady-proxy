package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"strings"
)

const programName = "steady-proxy"

var (
	version   = "dev"
	commit    = ""
	buildDate = ""
	dirty     = ""
)

type versionInfo struct {
	Program    string `json:"program"`
	Version    string `json:"version"`
	Commit     string `json:"commit,omitempty"`
	BuildDate  string `json:"build_date,omitempty"`
	Dirty      bool   `json:"dirty"`
	DirtyKnown bool   `json:"dirty_known"`
}

func currentVersion() versionInfo {
	vi := versionInfo{
		Program: programName,
		Version: defaultString(version, "dev"),
	}
	if c := strings.TrimSpace(commit); c != "" {
		vi.Commit = c
	}
	if d := strings.TrimSpace(buildDate); d != "" {
		vi.BuildDate = d
	}
	if d, ok := parseDirty(dirty); ok {
		vi.Dirty = d
		vi.DirtyKnown = true
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		applyVCSBuildSettings(&vi, bi.Settings)
	}
	return vi
}

func applyVCSBuildSettings(vi *versionInfo, settings []debug.BuildSetting) {
	for _, s := range settings {
		switch s.Key {
		case "vcs.revision":
			if vi.Commit == "" {
				vi.Commit = strings.TrimSpace(s.Value)
			}
		case "vcs.time":
			if vi.BuildDate == "" {
				vi.BuildDate = strings.TrimSpace(s.Value)
			}
		case "vcs.modified":
			if !vi.DirtyKnown {
				if d, ok := parseDirty(s.Value); ok {
					vi.Dirty = d
					vi.DirtyKnown = true
				}
			}
		}
	}
}

func parseDirty(s string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes":
		return true, true
	case "0", "false", "no":
		return false, true
	default:
		return false, false
	}
}

func defaultString(s, fallback string) string {
	if s = strings.TrimSpace(s); s != "" {
		return s
	}
	return fallback
}

func (vi versionInfo) token() string {
	tok := tokenPart(defaultString(vi.Version, "dev"))
	if c := shortCommit(vi.Commit); c != "" {
		tok += "+" + c
	}
	if vi.DirtyKnown && vi.Dirty {
		tok += ".dirty"
	}
	return tok
}

func (vi versionInfo) line() string {
	parts := []string{
		defaultString(vi.Program, programName),
		"version=" + defaultString(vi.Version, "dev"),
	}
	if vi.Commit != "" {
		parts = append(parts, "commit="+vi.Commit)
	}
	if vi.BuildDate != "" {
		parts = append(parts, "build_date="+vi.BuildDate)
	}
	if vi.DirtyKnown {
		parts = append(parts, fmt.Sprintf("dirty=%t", vi.Dirty))
	} else {
		parts = append(parts, "dirty=unknown")
	}
	return strings.Join(parts, " ")
}

func shortCommit(c string) string {
	c = strings.TrimSpace(c)
	if c == "" || c == "unknown" {
		return ""
	}
	if len(c) > 12 {
		return c[:12]
	}
	return c
}

func tokenPart(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '.', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, s)
}

func versionCLIOutput(args []string) (string, bool) {
	if len(args) != 1 {
		return "", false
	}
	switch args[0] {
	case "--version", "-version", "version":
		return currentVersion().line(), true
	default:
		return "", false
	}
}

func handleVersionCLI(args []string) bool {
	out, ok := versionCLIOutput(args)
	if !ok {
		return false
	}
	fmt.Println(out)
	return true
}

func handleVersionRequest(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path != "/__version" {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return true
	}
	if err := json.NewEncoder(w).Encode(currentVersion()); err != nil {
		vlog("[version] encode: %v", err)
	}
	return true
}
