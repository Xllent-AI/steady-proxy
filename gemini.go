package main

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
)

// Gemini has no separate terminal SSE event. Every requested candidate must
// finish, but a finishReason can precede a final usage-only chunk. The engine
// therefore waits for clean EOF before replaying a successful buffered response.
// Candidate stops (including token limits and safety) and prompt blocks remain
// native responses; they are not transport failures or Anthropic refusals.
type geminiWire struct {
	candidateCount  int
	candidates      map[int]geminiCandidateState
	chunk           uint64
	finished        int
	blocked         bool
	failed          bool
	sawContent      bool
	candidateTokens int
	thoughtTokens   int
}

type geminiCandidateState struct {
	chunk    uint64
	finished bool
}

func newGeminiWire(body []byte) *geminiWire {
	return &geminiWire{candidateCount: geminiSettingsOf(body).candidateCount}
}

type geminiRequestSettings struct {
	candidateCount int
	thinkingLevel  string
}

func geminiSettingsOf(body []byte) geminiRequestSettings {
	settings := geminiRequestSettings{candidateCount: 1}
	config := geminiRequestConfig{settings: &settings}
	request := struct {
		Config      geminiRequestConfig `json:"generationConfig"`
		ProtoConfig geminiRequestConfig `json:"generation_config"`
	}{config, config}
	if json.Unmarshal(body, &request) != nil {
		return geminiRequestSettings{candidateCount: 1}
	}
	if settings.candidateCount < 1 {
		settings.candidateCount = 1
	}
	return settings
}

// Both ProtoJSON spellings update one configuration in JSON field order. A later
// config replaces the earlier config, including resetting omitted settings.
type geminiRequestConfig struct{ settings *geminiRequestSettings }

func (c geminiRequestConfig) UnmarshalJSON(data []byte) error {
	*c.settings = geminiRequestSettings{candidateCount: 1}
	count := geminiRequestCount{value: &c.settings.candidateCount}
	thinking := geminiThinkingConfig{level: &c.settings.thinkingLevel}
	fields := struct {
		Count         geminiRequestCount   `json:"candidateCount"`
		ProtoCount    geminiRequestCount   `json:"candidate_count"`
		Thinking      geminiThinkingConfig `json:"thinkingConfig"`
		ProtoThinking geminiThinkingConfig `json:"thinking_config"`
	}{count, count, thinking, thinking}
	return json.Unmarshal(data, &fields)
}

type geminiThinkingConfig struct{ level *string }

func (c geminiThinkingConfig) UnmarshalJSON(data []byte) error {
	*c.level = ""
	level := geminiThinkingLevel{value: c.level}
	fields := struct {
		Level      geminiThinkingLevel `json:"thinkingLevel"`
		ProtoLevel geminiThinkingLevel `json:"thinking_level"`
	}{level, level}
	return json.Unmarshal(data, &fields)
}

type geminiThinkingLevel struct{ value *string }

func (l geminiThinkingLevel) UnmarshalJSON(data []byte) error {
	// Diagnostic metadata must not invalidate otherwise usable candidate counts.
	// Do not infer a level from a token budget or a model's implicit defaults.
	var level string
	_ = json.Unmarshal(data, &level)
	switch level = strings.ToLower(strings.TrimSpace(level)); level {
	case "minimal", "low", "medium", "high":
		*l.value = level
	default:
		*l.value = ""
	}
	return nil
}

type geminiRequestCount struct{ value *int }

func (c geminiRequestCount) UnmarshalJSON(data []byte) error {
	*c.value = 0
	text := string(data)
	if text == "null" {
		return nil
	}
	if len(data) > 0 && data[0] == '"' {
		if err := json.Unmarshal(data, &text); err != nil {
			return err
		}
	}
	// ProtoJSON int32 fields accept quoted numbers, exponent notation, and
	// zero fractional parts. int32's range is exactly representable in float64.
	number, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(number) || number < math.MinInt32 || number > math.MaxInt32 || number != math.Trunc(number) {
		return errors.New("invalid Gemini candidate count")
	}
	*c.value = int(number)
	return nil
}

// Logging is updated by accept's single decode, not by a second scrape decode.
func (g *geminiWire) scrape(ev event, st *captureStats) {}

func (g *geminiWire) buffered(ev event, st *captureStats) (bool, *failure) {
	f := g.accept(ev, st)
	g.failed = g.failed || f != nil
	return false, f
}

func (g *geminiWire) live(ev event, raw []byte, st *captureStats) ([][]byte, bool, *failure) {
	if f := g.accept(ev, st); f != nil {
		g.failed = true
		return nil, false, f
	}
	return [][]byte{raw}, false, nil
}

func (g *geminiWire) completeOnEOF() bool {
	if g.blocked {
		return !g.failed && len(g.candidates) == 0
	}
	return !g.failed && g.finished == g.candidateCount
}

func (g *geminiWire) replayBuffered(sp *spool, w io.Writer, st *captureStats) error {
	return sp.replayRaw(w)
}

func (g *geminiWire) replayPrefix(sp *spool, w io.Writer) error {
	return sp.replayRaw(w)
}

func (g *geminiWire) forwardable() bool { return g.sawContent }

func (g *geminiWire) keepalive() []byte { return []byte(": keepalive\n\n") }

// Only the fields needed for completion and logging are decoded. In particular,
// tool arguments, thought signatures, and all unknown metadata stay opaque and
// are always forwarded/replayed as the original SSE bytes.
type geminiChunk struct {
	Candidates     []*geminiCandidate `json:"candidates"`
	PromptFeedback *struct {
		BlockReason string `json:"blockReason"`
	} `json:"promptFeedback"`
	UsageMetadata *struct {
		PromptTokenCount        int  `json:"promptTokenCount"`
		CachedContentTokenCount int  `json:"cachedContentTokenCount"`
		CandidatesTokenCount    *int `json:"candidatesTokenCount"`
		ThoughtsTokenCount      *int `json:"thoughtsTokenCount"`
	} `json:"usageMetadata"`
	ModelVersion string          `json:"modelVersion"`
	Error        json.RawMessage `json:"error"`
}

type geminiCandidate struct {
	Index        geminiCandidateIndex `json:"index"`
	FinishReason string               `json:"finishReason"`
	Content      *struct {
		Parts []geminiPart `json:"parts"`
	} `json:"content"`
}

// Proto JSON may omit the default index 0. An explicit null, non-integer, or
// overflowing index is malformed, not another spelling of that default.
type geminiCandidateIndex int

func (i *geminiCandidateIndex) UnmarshalJSON(data []byte) error {
	n, err := strconv.Atoi(string(data))
	if err != nil {
		return err
	}
	*i = geminiCandidateIndex(n)
	return nil
}

// Validate a part's object shape without materializing/copying its text, inline
// media, function arguments, or opaque thought signature. encoding/json already
// validates the complete JSON syntax before invoking this hook.
type geminiPart struct{}

func (*geminiPart) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || data[0] != '{' {
		return errors.New("Gemini content part is not an object")
	}
	return nil
}

func (g *geminiWire) accept(ev event, st *captureStats) *failure {
	data := strings.TrimSpace(ev.data)
	// SSE control frames and empty data messages carry no model response.
	if ev.data == "" && ev.name != "error" {
		return nil
	}
	if data == "" || data[0] != '{' {
		return malformedSSE("Gemini stream event is not a JSON object")
	}
	var chunk geminiChunk
	if err := json.Unmarshal([]byte(data), &chunk); err != nil {
		return malformedSSE("invalid JSON or field shape in Gemini stream event")
	}
	// Both data-only Google envelopes and explicitly named error events must be
	// caught even with PROXY_VALIDATE_JSON=0, including after a finishReason. A
	// literal "error": null is an unset field on a data chunk, not an error.
	hasError := len(chunk.Error) != 0 && string(chunk.Error) != "null"
	if ev.name == "error" || hasError {
		if f := classifyGeminiSSEError(data); f != nil {
			return f
		}
		return malformedSSE("invalid error envelope in Gemini stream event")
	}

	g.chunk++
	if len(chunk.Candidates) > 0 && g.candidates == nil {
		g.candidates = make(map[int]geminiCandidateState)
	}
	for _, candidate := range chunk.Candidates {
		if candidate == nil {
			return malformedSSE("Gemini stream candidate is not an object")
		}
		index := int(candidate.Index)
		if index < 0 || index >= g.candidateCount {
			return malformedSSE("Gemini stream candidate index is outside the requested range")
		}
		state := g.candidates[index]
		if state.chunk == g.chunk {
			return malformedSSE("duplicate Gemini candidate index in stream event")
		}
		state.chunk = g.chunk
		if candidate.Content != nil && len(candidate.Content.Parts) > 0 {
			g.sawContent = true
		}
		if reason := candidate.FinishReason; reason != "" && reason != "FINISH_REASON_UNSPECIFIED" {
			if !state.finished {
				state.finished = true
				g.finished++
			}
			if st != nil {
				st.stop = reason
			}
		}
		g.candidates[index] = state
	}
	if feedback := chunk.PromptFeedback; feedback != nil {
		if reason := feedback.BlockReason; reason != "" && reason != "BLOCK_REASON_UNSPECIFIED" {
			g.blocked = true
			if st != nil {
				st.stop = reason
			}
		}
	}
	if st != nil && chunk.ModelVersion != "" {
		st.model = chunk.ModelVersion
	}
	if usage := chunk.UsageMetadata; usage != nil {
		if st != nil && usage.PromptTokenCount > 0 {
			// promptTokenCount already includes cachedContentTokenCount: split
			// that total, never add the cached portion to it a second time.
			cached := min(max(usage.CachedContentTokenCount, 0), usage.PromptTokenCount)
			st.setInputUsage(usage.PromptTokenCount-cached, 0, cached)
		}
		// Usage values are cumulative snapshots, not per-chunk deltas. Keep
		// independently supplied output components without summing repeats.
		if usage.CandidatesTokenCount != nil && *usage.CandidatesTokenCount >= 0 {
			g.candidateTokens = *usage.CandidatesTokenCount
		}
		if usage.ThoughtsTokenCount != nil && *usage.ThoughtsTokenCount >= 0 {
			g.thoughtTokens = *usage.ThoughtsTokenCount
		}
		if st != nil {
			st.outTok = g.candidateTokens + g.thoughtTokens
		}
	}
	return nil
}
