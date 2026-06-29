package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const goodStream = `event: message_start
data: {"type":"message_start","message":{"id":"msg_1"}}

event: content_block_start
data: {"type":"content_block_start","index":0}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"}}

event: message_stop
data: {"type":"message_stop"}

`

func init() { cfg = loadConfig() }

func capture(t *testing.T, s string) (*httptest.ResponseRecorder, *failure) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	_, f := captureSSE(ctx, cancel, rec, http.Header{}, strings.NewReader(s), nil)
	return rec, f
}

func TestCaptureSuccess(t *testing.T) {
	rec, f := capture(t, goodStream)
	if f != nil {
		t.Fatalf("expected success, got failure %+v", *f)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "message_stop") || !strings.Contains(body, "Hi") {
		t.Fatalf("replayed body missing content: %q", body)
	}
	if rec.Header().Get("X-CC-Retry-Proxy-Mode") != "buffered" {
		t.Fatalf("want buffered mode, got %q", rec.Header().Get("X-CC-Retry-Proxy-Mode"))
	}
}

func TestCaptureSuccessChunked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	_, f := captureSSE(ctx, cancel, rec, http.Header{}, iotest1byte(goodStream), nil)
	if f != nil {
		t.Fatalf("expected success on byte-chunked stream, got %+v", *f)
	}
	if !strings.Contains(rec.Body.String(), "message_stop") {
		t.Fatalf("missing terminal event in replay")
	}
}

// TestCaptureStats covers the model/token/stop scraping that feeds the access log.
func TestCaptureStats(t *testing.T) {
	const stream = `event: message_start
data: {"type":"message_start","message":{"id":"msg_1","model":"claude-sonnet-4-6","usage":{"input_tokens":1234,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":42}}

event: message_stop
data: {"type":"message_stop"}

`
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	var st captureStats
	if _, f := captureSSE(ctx, cancel, rec, http.Header{}, strings.NewReader(stream), &st); f != nil {
		t.Fatalf("expected success, got %+v", *f)
	}
	if st.model != "claude-sonnet-4-6" {
		t.Errorf("model = %q, want claude-sonnet-4-6", st.model)
	}
	if st.inTok != 1234 || st.outTok != 42 {
		t.Errorf("tokens in/out = %d/%d, want 1234/42", st.inTok, st.outTok)
	}
	if st.stop != "end_turn" {
		t.Errorf("stop = %q, want end_turn", st.stop)
	}
	if st.mode != "buffered" {
		t.Errorf("mode = %q, want buffered", st.mode)
	}
	if st.bytes == 0 {
		t.Errorf("bytes = 0, want >0")
	}
}

func TestCaptureStatsGPTUsageFromMessageDelta(t *testing.T) {
	const stream = `event: message_start
data: {"type":"message_start","message":{"id":"resp_1","model":"gpt-5.5","usage":{"input_tokens":0,"output_tokens":0}}}

event: content_block_start
data: {"type":"content_block_start","index":0}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":574,"cache_creation_input_tokens":10,"cache_read_input_tokens":24576,"output_tokens":12376}}

event: message_stop
data: {"type":"message_stop"}

`
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	var st captureStats
	if _, f := captureSSE(ctx, cancel, rec, http.Header{}, strings.NewReader(stream), &st); f != nil {
		t.Fatalf("expected success, got %+v", *f)
	}
	if st.model != "gpt-5.5" {
		t.Errorf("model = %q, want gpt-5.5", st.model)
	}
	if st.inTok != 25160 || st.outTok != 12376 {
		t.Errorf("tokens in/out = %d/%d, want 25160/12376", st.inTok, st.outTok)
	}
	if st.stop != "end_turn" {
		t.Errorf("stop = %q, want end_turn", st.stop)
	}
	usage := messageStartUsage(t, rec.Body.String())
	if usage.Input != 574 || usage.CacheCreation != 10 || usage.CacheRead != 24576 {
		t.Errorf("replayed message_start usage = %d/%d/%d, want 574/10/24576",
			usage.Input, usage.CacheCreation, usage.CacheRead)
	}
	deltaUsage := messageDeltaUsage(t, rec.Body.String())
	assertNoInputUsage(t, deltaUsage)
	if got := int(deltaUsage["output_tokens"].(float64)); got != 12376 {
		t.Errorf("message_delta output_tokens = %d, want 12376", got)
	}
}

func TestCaptureStatsGPTMiniUsageBackfill(t *testing.T) {
	const stream = `event: message_start
data: {"type":"message_start","message":{"id":"resp_1","model":"gpt-5.4-mini-2026-03-17","usage":{"input_tokens":0,"output_tokens":0}}}

event: content_block_start
data: {"type":"content_block_start","index":0}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":5784,"cache_creation_input_tokens":0,"cache_read_input_tokens":117760,"output_tokens":7916}}

event: message_stop
data: {"type":"message_stop"}

`
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	var st captureStats
	if _, f := captureSSE(ctx, cancel, rec, http.Header{}, strings.NewReader(stream), &st); f != nil {
		t.Fatalf("expected success, got %+v", *f)
	}
	if st.inTok != 123544 || st.outTok != 7916 {
		t.Errorf("tokens in/out = %d/%d, want 123544/7916", st.inTok, st.outTok)
	}
	usage := messageStartUsage(t, rec.Body.String())
	if usage.Input != 5784 || usage.CacheCreation != 0 || usage.CacheRead != 117760 {
		t.Errorf("replayed message_start usage = %d/%d/%d, want 5784/0/117760",
			usage.Input, usage.CacheCreation, usage.CacheRead)
	}
	deltaUsage := messageDeltaUsage(t, rec.Body.String())
	assertNoInputUsage(t, deltaUsage)
	if got := int(deltaUsage["output_tokens"].(float64)); got != 7916 {
		t.Errorf("message_delta output_tokens = %d, want 7916", got)
	}
}

func TestToolUseInputJSONDeltaConvertsWhenAccumulatedJSONIsInvalid(t *testing.T) {
	cfg = loadConfig()
	stream := toolUseStream([]string{`{"session_id":`})
	_, f := capture(t, stream)
	if f == nil || !f.transient || f.code != "malformed_sse" {
		t.Fatalf("want transient malformed_sse for incomplete tool input JSON, got %+v", f)
	}
}

func TestToolUseInputJSONDeltaNormalizedOnBufferedReplay(t *testing.T) {
	cfg = loadConfig()
	prompt := strings.Repeat("tool-fragment-", 600)
	input := `{"session_id":"s_92d0f1da7c9b","prompt":"` + prompt + `","timeout_ms":3900000}`
	chunks := append([]string{""}, splitEvery(input, 8)...)
	if len(chunks) < 800 {
		t.Fatalf("test setup must exercise a highly fragmented tool input, got %d chunks", len(chunks))
	}
	rec, f := capture(t, toolUseStream(chunks))
	if f != nil {
		t.Fatalf("expected success, got %+v", *f)
	}
	deltas := inputJSONDeltas(t, rec.Body.String())
	if len(deltas) != 1 {
		t.Fatalf("want one normalized input_json_delta, got %d: %#v", len(deltas), deltas)
	}
	if deltas[0] != input {
		t.Fatalf("normalized tool input mismatch:\n got: %s\nwant: %s", deltas[0], input)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(deltas[0]), &parsed); err != nil {
		t.Fatalf("normalized tool input is not valid JSON: %v", err)
	}
}

func TestToolUseInputJSONDeltaNormalizedAcrossLiveHandoff(t *testing.T) {
	cfg = loadConfig()
	cfg.keepaliveMs = 20 * time.Millisecond
	cfg.upstreamByteIdle = time.Second

	input := `{"session_id":"s_live_handoff","prompt":"` + strings.Repeat("handoff-fragment-", 40) + `","timeout_ms":1000}`
	chunks := splitEvery(input, 9)
	parts := toolUseStreamParts(chunks, 5)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	r := &gapReader{ctx: ctx, chunks: parts, gap: 80 * time.Millisecond}
	wrote, f := captureSSE(ctx, cancel, rec, http.Header{}, r, nil)
	if f != nil || !wrote {
		t.Fatalf("expected live handoff success, wrote=%v failure=%+v", wrote, f)
	}
	if rec.Header().Get("X-CC-Retry-Proxy-Mode") != "live" {
		t.Fatalf("want live mode, got %q", rec.Header().Get("X-CC-Retry-Proxy-Mode"))
	}
	deltas := inputJSONDeltas(t, rec.Body.String())
	if len(deltas) != 1 {
		t.Fatalf("want one normalized input_json_delta across live handoff, got %d: %#v", len(deltas), deltas)
	}
	if deltas[0] != input {
		t.Fatalf("normalized live handoff tool input mismatch:\n got: %s\nwant: %s", deltas[0], input)
	}
}

func TestServerToolUseInputJSONDeltaNormalized(t *testing.T) {
	cfg = loadConfig()
	input := `{"query":"OEIS A048625 Pisot sequence P(4,6) linear recurrence proof Boyd"}`
	rec, f := capture(t, inputJSONStream(inputJSONStreamSpec{
		blockType: "server_tool_use",
		toolID:    "srvtoolu_1",
		name:      "web_search",
		chunks:    append([]string{""}, splitEvery(input, 7)...),
	}))
	if f != nil {
		t.Fatalf("expected server_tool_use success, got %+v", *f)
	}
	deltas := inputJSONDeltas(t, rec.Body.String())
	if len(deltas) != 1 {
		t.Fatalf("want one normalized server_tool_use input_json_delta, got %d: %#v", len(deltas), deltas)
	}
	if deltas[0] != input {
		t.Fatalf("normalized server tool input mismatch:\n got: %s\nwant: %s", deltas[0], input)
	}
}

func TestEmptyInputJSONDeltaIsAllowed(t *testing.T) {
	for _, blockType := range []string{"tool_use", "server_tool_use"} {
		t.Run(blockType, func(t *testing.T) {
			cfg = loadConfig()
			rec, f := capture(t, inputJSONStream(inputJSONStreamSpec{
				blockType: blockType,
				toolID:    "toolu_empty",
				name:      "Tool",
				chunks:    []string{""},
			}))
			if f != nil {
				t.Fatalf("expected empty input_json_delta success, got %+v", *f)
			}
			deltas := inputJSONDeltas(t, rec.Body.String())
			if len(deltas) != 1 || deltas[0] != "" {
				t.Fatalf("want one preserved empty input_json_delta, got %#v", deltas)
			}
		})
	}
}

func TestValidateJSONDisabledSkipsReplayParsing(t *testing.T) {
	cfg = loadConfig()
	cfg.validateJSON = false
	const stream = `event: message_start
data: {"type":"message_start","message":{"id":"msg_1"}}

event: content_block_start
data: {"type":"content_block_start","index":0}

event: content_block_delta
data: {oops not json

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"}}

event: message_stop
data: {"type":"message_stop"}

`
	rec, f := capture(t, stream)
	if f != nil {
		t.Fatalf("expected validateJSON=0 to forward raw malformed event, got %+v", *f)
	}
	if !strings.Contains(rec.Body.String(), "{oops not json") {
		t.Fatalf("raw malformed event was not forwarded: %q", rec.Body.String())
	}
}

func TestValidateJSONDisabledSkipsUsageBackfillParsing(t *testing.T) {
	cfg = loadConfig()
	cfg.validateJSON = false
	const stream = `event: message_start
data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":0,"output_tokens":0}}}

event: content_block_start
data: {"type":"content_block_start","index":0}

event: content_block_delta
data: {oops not json

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":99,"output_tokens":3}}

event: message_stop
data: {"type":"message_stop"}

`
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	var st captureStats
	_, f := captureSSE(ctx, cancel, rec, http.Header{}, strings.NewReader(stream), &st)
	if f != nil {
		t.Fatalf("expected validateJSON=0 to forward raw malformed event, got %+v", *f)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"{oops not json",
		`"usage":{"input_tokens":0,"output_tokens":0}`,
		`"usage":{"input_tokens":99,"output_tokens":3}`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("validateJSON=0 replay should preserve %q in raw body:\n%s", want, body)
		}
	}
}

func TestToolUseInputJSONDeltaNormalizationCanBeDisabled(t *testing.T) {
	cfg = loadConfig()
	cfg.normalizeToolJSON = false
	chunks := []string{`{"session_id":`, `"s_raw"`, `,"timeout_ms":1000}`}
	rec, f := capture(t, toolUseStream(chunks))
	if f != nil {
		t.Fatalf("expected success, got %+v", *f)
	}
	deltas := inputJSONDeltas(t, rec.Body.String())
	if len(deltas) != len(chunks) {
		t.Fatalf("want raw input_json_delta fragments when normalization is disabled, got %d: %#v", len(deltas), deltas)
	}
	for i := range chunks {
		if deltas[i] != chunks[i] {
			t.Fatalf("raw fragment %d mismatch: got %q want %q", i, deltas[i], chunks[i])
		}
	}
}

func TestCaptureTruncated(t *testing.T) {
	trunc := goodStream[:strings.Index(goodStream, "content_block_stop")]
	_, f := capture(t, trunc)
	if f == nil || !f.transient || f.code != "truncated_stream" {
		t.Fatalf("want transient truncated_stream, got %+v", f)
	}
}

func TestCaptureRejectsNamedEventWithoutData(t *testing.T) {
	cfg = loadConfig()
	s := strings.Replace(goodStream,
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n",
		"event: content_block_delta\n: keep-alive\n\n"+
			"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n",
		1)
	rec, f := capture(t, s)
	if f == nil || !f.transient || f.code != "malformed_sse" {
		t.Fatalf("want transient malformed_sse, got %+v", f)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("malformed stream should fail before commit, body=%q", rec.Body.String())
	}
}

func TestCaptureRejectsDataWithoutEventName(t *testing.T) {
	cfg = loadConfig()
	s := strings.Replace(goodStream,
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n",
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n",
		1)
	rec, f := capture(t, s)
	if f == nil || !f.transient || f.code != "malformed_sse" {
		t.Fatalf("want transient malformed_sse, got %+v", f)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("malformed stream should fail before commit, body=%q", rec.Body.String())
	}
}

func TestCaptureAllowsCommentKeepaliveBetweenEvents(t *testing.T) {
	cfg = loadConfig()
	s := strings.Replace(goodStream,
		"event: content_block_start",
		": keep-alive\n\n"+"event: content_block_start",
		1)
	rec, f := capture(t, s)
	if f != nil {
		t.Fatalf("expected success with standalone comment keepalive, got %+v", *f)
	}
	if !strings.Contains(rec.Body.String(), "message_stop") {
		t.Fatalf("missing terminal event in replay")
	}
}

func TestCaptureAllowsSSEMetadataRecords(t *testing.T) {
	cfg = loadConfig()
	s := strings.Replace(goodStream,
		"event: content_block_start",
		"id: event-42\nretry: 1000\n\n"+"event: content_block_start",
		1)
	rec, f := capture(t, s)
	if f != nil {
		t.Fatalf("expected success with SSE metadata records, got %+v", *f)
	}
	body := rec.Body.String()
	for _, want := range []string{"id: event-42", "retry: 1000", "message_stop"} {
		if !strings.Contains(body, want) {
			t.Fatalf("replayed body missing %q: %q", want, body)
		}
	}
}

func TestCaptureSSEErrorOverloaded(t *testing.T) {
	s := "event: message_start\ndata: {\"type\":\"message_start\"}\n\n" +
		"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"overloaded\"}}\n\n"
	_, f := capture(t, s)
	if f == nil || !f.transient || f.status != 529 {
		t.Fatalf("want transient 529, got %+v", f)
	}
}

func TestCaptureSSEErrorPermanent(t *testing.T) {
	s := "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"bad\"}}\n\n"
	_, f := capture(t, s)
	if f == nil || f.transient {
		t.Fatalf("want permanent failure, got %+v", f)
	}
}

func mkResp(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
}

func TestClassifyHTTP(t *testing.T) {
	cases := []struct {
		name      string
		resp      *http.Response
		transient bool
	}{
		{"500", mkResp(500, `{"error":{"type":"api_error","message":"boom"}}`), true},
		{"529", mkResp(529, `{"error":{"type":"overloaded_error"}}`), true},
		{"429", mkResp(429, `{"error":{"type":"rate_limit_error"}}`), true},
		{"400-invalid", mkResp(400, `{"error":{"type":"invalid_request_error","message":"messages.0: tool_use ids must..."}}`), false},
		{"400-gateway", mkResp(400, `{"error":{"type":"api_error","message":"upstream gateway timeout"}}`), true},
		{"401", mkResp(401, `{"error":{"type":"authentication_error","message":"invalid api key"}}`), true}, // auth now rides out (may be a temporary block)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := classifyHTTPError(c.resp)
			if f.transient != c.transient {
				t.Fatalf("%s: want transient=%v, got %+v", c.name, c.transient, f)
			}
		})
	}
}

func TestGatewayProvenanceHeaderWins(t *testing.T) {
	r := mkResp(400, `{"error":{"type":"invalid_request_error","message":"x"}}`)
	r.Header.Set("x-gateway-retryable", "true")
	if f := classifyHTTPError(r); !f.transient {
		t.Fatalf("provenance header should force transient, got %+v", f)
	}
}

// iotest1byte returns a reader that yields one byte per Read call.
func iotest1byte(s string) io.Reader { return &oneByte{s: s} }

type oneByte struct {
	s string
	i int
}

func (o *oneByte) Read(p []byte) (int, error) {
	if o.i >= len(o.s) {
		return 0, io.EOF
	}
	p[0] = o.s[o.i]
	o.i++
	return 1, nil
}

type testUsage struct {
	Input         int `json:"input_tokens"`
	CacheCreation int `json:"cache_creation_input_tokens"`
	CacheRead     int `json:"cache_read_input_tokens"`
}

func messageStartUsage(t *testing.T, body string) testUsage {
	t.Helper()
	var parser sseParser
	for _, ev := range parser.feed([]byte(body)) {
		if ev.name != "message_start" {
			continue
		}
		var m struct {
			Message struct {
				Usage testUsage `json:"usage"`
			} `json:"message"`
		}
		if err := json.Unmarshal([]byte(ev.data), &m); err != nil {
			t.Fatalf("message_start JSON: %v", err)
		}
		return m.Message.Usage
	}
	t.Fatalf("message_start not found in %q", body)
	return testUsage{}
}

func messageDeltaUsage(t *testing.T, body string) map[string]any {
	t.Helper()
	var parser sseParser
	for _, ev := range parser.feed([]byte(body)) {
		if ev.name != "message_delta" {
			continue
		}
		var m struct {
			Usage map[string]any `json:"usage"`
		}
		if err := json.Unmarshal([]byte(ev.data), &m); err != nil {
			t.Fatalf("message_delta JSON: %v", err)
		}
		return m.Usage
	}
	t.Fatalf("message_delta not found in %q", body)
	return nil
}

func assertNoInputUsage(t *testing.T, usage map[string]any) {
	t.Helper()
	for _, key := range []string{"input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"} {
		if _, ok := usage[key]; ok {
			t.Fatalf("message_delta still contains %s in %#v", key, usage)
		}
	}
}

func toolUseStream(chunks []string) string {
	parts := inputJSONStreamParts(inputJSONStreamSpec{
		blockType: "tool_use",
		toolID:    "toolu_1",
		name:      "Tool",
		chunks:    chunks,
		split:     len(chunks),
	})
	return parts[0] + parts[1]
}

func toolUseStreamParts(chunks []string, split int) []string {
	return inputJSONStreamParts(inputJSONStreamSpec{
		blockType: "tool_use",
		toolID:    "toolu_1",
		name:      "Tool",
		chunks:    chunks,
		split:     split,
	})
}

type inputJSONStreamSpec struct {
	blockType string
	toolID    string
	name      string
	chunks    []string
	split     int
}

func inputJSONStream(spec inputJSONStreamSpec) string {
	spec.split = len(spec.chunks)
	parts := inputJSONStreamParts(spec)
	return parts[0] + parts[1]
}

func inputJSONStreamParts(spec inputJSONStreamSpec) []string {
	var b strings.Builder
	var tail strings.Builder
	writeInputJSONPrelude(&b, spec.blockType, spec.toolID, spec.name)
	for i, chunk := range spec.chunks {
		dst := &b
		if i >= spec.split {
			dst = &tail
		}
		writeInputJSONDelta(dst, chunk)
	}
	writeInputJSONPostlude(&tail)
	return []string{b.String(), tail.String()}
}

func writeInputJSONPrelude(b *strings.Builder, blockType, id, name string) {
	b.WriteString(sseJSON("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id": "msg_1", "type": "message", "role": "assistant", "model": "m",
			"content": []any{}, "usage": map[string]any{"input_tokens": 1, "output_tokens": 1},
		},
	}))
	b.WriteString(sseJSON("content_block_start", map[string]any{
		"type":  "content_block_start",
		"index": 0,
		"content_block": map[string]any{
			"type": blockType, "id": id, "name": name, "input": map[string]any{},
		},
	}))
}

func writeInputJSONDelta(b *strings.Builder, chunk string) {
	b.WriteString(sseJSON("content_block_delta", map[string]any{
		"type":  "content_block_delta",
		"index": 0,
		"delta": map[string]any{"type": "input_json_delta", "partial_json": chunk},
	}))
}

func writeInputJSONPostlude(b *strings.Builder) {
	b.WriteString(sseJSON("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0}))
	b.WriteString(sseJSON("message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": "tool_use", "stop_sequence": nil},
		"usage": map[string]any{"output_tokens": 1},
	}))
	b.WriteString(sseJSON("message_stop", map[string]any{"type": "message_stop"}))
}

func sseJSON(eventName string, payload any) string {
	b, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return "event: " + eventName + "\ndata: " + string(b) + "\n\n"
}

func splitEvery(s string, n int) []string {
	var out []string
	for len(s) > 0 {
		if len(s) < n {
			n = len(s)
		}
		out = append(out, s[:n])
		s = s[n:]
	}
	return out
}

func inputJSONDeltas(t *testing.T, body string) []string {
	t.Helper()
	var parser sseParser
	var out []string
	for _, ev := range parser.feed([]byte(body)) {
		if ev.name != "content_block_delta" {
			continue
		}
		var m struct {
			Delta struct {
				Type        string `json:"type"`
				PartialJSON string `json:"partial_json"`
			} `json:"delta"`
		}
		if err := json.Unmarshal([]byte(ev.data), &m); err != nil {
			t.Fatalf("content_block_delta JSON: %v", err)
		}
		if m.Delta.Type == "input_json_delta" {
			out = append(out, m.Delta.PartialJSON)
		}
	}
	return out
}
