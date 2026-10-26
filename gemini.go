package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
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
	err := geminiObject(body, func(name string, value json.RawMessage) error {
		if name == "generationConfig" || name == "generation_config" {
			return config.UnmarshalJSON(value)
		}
		return nil
	})
	if err != nil {
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
	return geminiObject(data, func(name string, value json.RawMessage) error {
		switch name {
		case "candidateCount", "candidate_count":
			count, err := geminiInt32(value)
			c.settings.candidateCount = count
			return err
		case "thinkingConfig", "thinking_config":
			return (geminiThinkingConfig{level: &c.settings.thinkingLevel}).UnmarshalJSON(value)
		}
		return nil
	})
}

type geminiThinkingConfig struct{ level *string }

func (c geminiThinkingConfig) UnmarshalJSON(data []byte) error {
	*c.level = ""
	return geminiObject(data, func(name string, value json.RawMessage) error {
		if name == "thinkingLevel" || name == "thinking_level" {
			return (geminiThinkingLevel{value: c.level}).UnmarshalJSON(value)
		}
		return nil
	})
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

// Walk only the semantic objects we interpret, in wire order. Alternate names
// share a destination and replace it (including null), rather than merging
// messages or choosing an alias according to Go struct field order.
func geminiObject(data []byte, field func(string, json.RawMessage) error) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return errors.New("Gemini field is not an object")
	}
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return err
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return err
		}
		if err := field(key.(string), value); err != nil {
			return err
		}
	}
	if _, err := d.Token(); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing JSON in Gemini object")
	}
	return nil
}

// ProtoJSON int32 accepts null, quoted numbers, and integral decimal/exponent
// forms. Work in decimal so tiny fractions and values just outside int32 do
// not round to an accepted integer through float64.
func geminiInt32(data []byte) (int, error) {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return 0, nil
	}
	var number json.Number
	if err := json.Unmarshal(data, &number); err != nil {
		return 0, err
	}
	text := number.String()
	negative := strings.HasPrefix(text, "-")
	text = strings.TrimPrefix(text, "-")
	mantissa, exponentText := text, "0"
	if pos := strings.IndexAny(text, "eE"); pos >= 0 {
		mantissa, exponentText = text[:pos], text[pos+1:]
	}
	first, last, digits, fraction := -1, -1, 0, 0
	afterDot := false
	for _, digit := range mantissa {
		if digit == '.' {
			afterDot = true
			continue
		}
		if digit != '0' {
			if first < 0 {
				first = digits
			}
			last = digits
		}
		digits++
		if afterDot {
			fraction++
		}
	}
	if first < 0 {
		return 0, nil
	}
	exponent, err := strconv.ParseInt(exponentText, 10, 32)
	if err != nil {
		return 0, errors.New("invalid Gemini int32 exponent")
	}
	scale := exponent - int64(fraction) + int64(digits-last-1)
	if scale < 0 || int64(last-first+1)+scale > 10 {
		return 0, errors.New("non-integral or overflowing Gemini int32")
	}
	var value int64
	position := 0
	for _, digit := range mantissa {
		if digit == '.' {
			continue
		}
		if position >= first && position <= last {
			value = value*10 + int64(digit-'0')
		}
		position++
	}
	for ; scale > 0; scale-- {
		value *= 10
	}
	if negative {
		value = -value
	}
	if value < -2147483648 || value > 2147483647 {
		return 0, errors.New("overflowing Gemini int32")
	}
	return int(value), nil
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
		if !f.transient {
			// Only a classified upstream error frame is terminal (malformed data
			// is transient). It is the upstream's final answer: an abort would send
			// the client into a retry that cannot succeed, so deliver the frame
			// verbatim and end the stream cleanly, as a direct connection would.
			f.delivered = true
			return [][]byte{raw}, false, f
		}
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
	Candidates     []*geminiCandidate
	PromptFeedback *geminiPromptFeedback
	UsageMetadata  *geminiUsage
	ModelVersion   string
	Error          json.RawMessage
}

func (c *geminiChunk) UnmarshalJSON(data []byte) error {
	*c = geminiChunk{}
	return geminiObject(data, func(name string, value json.RawMessage) error {
		switch name {
		case "candidates":
			c.Candidates = nil
			return json.Unmarshal(value, &c.Candidates)
		case "promptFeedback", "prompt_feedback":
			c.PromptFeedback = nil
			return json.Unmarshal(value, &c.PromptFeedback)
		case "usageMetadata", "usage_metadata":
			c.UsageMetadata = nil
			return json.Unmarshal(value, &c.UsageMetadata)
		case "modelVersion", "model_version":
			c.ModelVersion = ""
			return json.Unmarshal(value, &c.ModelVersion)
		case "error":
			c.Error = value
		}
		return nil
	})
}

type geminiCandidate struct {
	Index        int
	FinishReason string
	Content      *geminiContent
}

func (c *geminiCandidate) UnmarshalJSON(data []byte) error {
	*c = geminiCandidate{}
	return geminiObject(data, func(name string, value json.RawMessage) error {
		var err error
		switch name {
		case "index":
			c.Index, err = geminiInt32(value)
		case "finishReason", "finish_reason":
			c.FinishReason, err = geminiEnum(value, geminiFinishReasons[:])
		case "content":
			c.Content = nil
			err = json.Unmarshal(value, &c.Content)
		}
		return err
	})
}

type geminiContent struct{ Parts []geminiPart }

func (c *geminiContent) UnmarshalJSON(data []byte) error {
	c.Parts = nil
	return geminiObject(data, func(name string, value json.RawMessage) error {
		if name == "parts" {
			c.Parts = nil
			return json.Unmarshal(value, &c.Parts)
		}
		return nil
	})
}

type geminiPromptFeedback struct{ BlockReason string }

func (p *geminiPromptFeedback) UnmarshalJSON(data []byte) error {
	p.BlockReason = ""
	return geminiObject(data, func(name string, value json.RawMessage) error {
		if name == "blockReason" || name == "block_reason" {
			var err error
			p.BlockReason, err = geminiEnum(value, geminiBlockReasons[:])
			return err
		}
		return nil
	})
}

// Developer API and Vertex share only these numeric enum names. Keep other
// numbers as numbers rather than assign a service-specific name incorrectly.
var geminiFinishReasons = [...]string{
	"FINISH_REASON_UNSPECIFIED", "STOP", "MAX_TOKENS", "SAFETY", "RECITATION",
	"OTHER",
}

var geminiBlockReasons = [...]string{
	"BLOCK_REASON_UNSPECIFIED", "SAFETY", "OTHER", "BLOCKLIST",
	"PROHIBITED_CONTENT",
}

func geminiEnum(data []byte, names []string) (string, error) {
	if len(data) > 0 && data[0] == '"' {
		var name string
		err := json.Unmarshal(data, &name)
		return name, err
	}
	n, err := geminiInt32(data)
	if err != nil {
		return "", err
	}
	if n >= 0 && n < len(names) {
		return names[n], nil
	}
	return strconv.Itoa(n), nil
}

type geminiUsage struct {
	PromptTokenCount        int
	CachedContentTokenCount int
	CandidatesTokenCount    *int
	ThoughtsTokenCount      *int
}

func (u *geminiUsage) UnmarshalJSON(data []byte) error {
	*u = geminiUsage{}
	return geminiObject(data, func(name string, value json.RawMessage) error {
		switch name {
		case "promptTokenCount", "prompt_token_count", "cachedContentTokenCount", "cached_content_token_count",
			"candidatesTokenCount", "candidates_token_count", "thoughtsTokenCount", "thoughts_token_count":
			n, err := geminiInt32(value)
			if err != nil {
				return err
			}
			switch name {
			case "promptTokenCount", "prompt_token_count":
				u.PromptTokenCount = n
			case "cachedContentTokenCount", "cached_content_token_count":
				u.CachedContentTokenCount = n
			default:
				var count *int
				if !bytes.Equal(value, []byte("null")) {
					count = &n
				}
				if name == "candidatesTokenCount" || name == "candidates_token_count" {
					u.CandidatesTokenCount = count
				} else {
					u.ThoughtsTokenCount = count
				}
			}
		}
		return nil
	})
}

// Validate only a part's object shape. Its text, media, function arguments and
// thought signatures are not interpreted or rewritten. encoding/json already
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
		index := candidate.Index
		if index < 0 || index >= g.candidateCount {
			return malformedSSE("Gemini stream candidate index is outside the requested range")
		}
		state := g.candidates[index]
		if state.chunk == g.chunk {
			return malformedSSE("duplicate Gemini candidate index in stream event")
		}
		state.chunk = g.chunk
		if candidate.Content != nil && len(candidate.Content.Parts) > 0 {
			if state.finished {
				return malformedSSE("Gemini stream candidate content follows its finish reason")
			}
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
