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

const streamDisconnectMessage = "stream error: stream disconnected before completion: stream closed before response.completed"

const mislabeledDisconnectStream = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"" + streamDisconnectMessage + "\"}}\n\n"

func init() { cfg = testConfig() }

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
	if rec.Header().Get("X-Steady-Proxy-Mode") != "buffered" {
		t.Fatalf("want buffered mode, got %q", rec.Header().Get("X-Steady-Proxy-Mode"))
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
	useDefaultConfig(t)
	stream := toolUseStream([]string{`{"session_id":`})
	_, f := capture(t, stream)
	if f == nil || !f.transient || f.code != "malformed_sse" {
		t.Fatalf("want transient malformed_sse for incomplete tool input JSON, got %+v", f)
	}
}

func TestToolUseInputJSONDeltaNormalizedOnBufferedReplay(t *testing.T) {
	useDefaultConfig(t)
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
	useDefaultConfig(t)
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
	if rec.Header().Get("X-Steady-Proxy-Mode") != "live" {
		t.Fatalf("want live mode, got %q", rec.Header().Get("X-Steady-Proxy-Mode"))
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
	useDefaultConfig(t)
	input := `{"query":"a search query long enough to be split across many small deltas"}`
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
			useDefaultConfig(t)
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
	useDefaultConfig(t)
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
	useDefaultConfig(t)
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
	useDefaultConfig(t)
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
	useDefaultConfig(t)
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
	useDefaultConfig(t)
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
	useDefaultConfig(t)
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
	useDefaultConfig(t)
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

func TestCaptureSSEErrorClassification(t *testing.T) {
	useDefaultConfig(t)
	for _, tc := range []struct {
		name, typ, message string
		transient          bool
	}{
		{"mislabeled_disconnect", "invalid_request_error", streamDisconnectMessage, true},
		{"typed_disconnect", "timeout_error", streamDisconnectMessage, true},
		{"unknown_request_error", "invalid_request_error", "bad", false},
		{"invalid_schema", "invalid_request_error", "invalid tool schema", false},
		{"malformed_tool_message", "invalid_request_error", "messages.0: unexpected tool_use_id in tool_result", false},
		{"context_limit", "invalid_request_error", "prompt is too long for context", false},
		{"stream_validation", "invalid_request_error", "invalid tool schema for stream", false},
		{"stream_error_validation", "invalid_request_error", "stream error: invalid tool schema", false},
		{"partial_disconnect", "invalid_request_error", "stream disconnected before completion", false},
		{"different_stream_error", "invalid_request_error", "stream error: stream disconnected before completion: invalid tool schema", false},
		{"disconnect_with_validation", "invalid_request_error", streamDisconnectMessage + ": invalid tool schema", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(map[string]any{
				"type": "error", "error": map[string]string{"type": tc.typ, "message": tc.message},
			})
			if err != nil {
				t.Fatal(err)
			}
			rec, f := capture(t, "event: error\ndata: "+string(data)+"\n\n")
			if f == nil || f.transient != tc.transient || f.message != tc.message {
				t.Fatalf("want transient=%v with original message, got %+v", tc.transient, f)
			}
			status, typ := http.StatusBadRequest, "invalid_request_error"
			if tc.transient {
				status, typ = http.StatusBadGateway, "api_error"
			}
			if f.status != status || f.atype != typ {
				t.Fatalf("want status=%d type=%s, got %+v", status, typ, f)
			}
			if rec.Body.Len() != 0 {
				t.Fatalf("failed attempt leaked output: %s", rec.Body.String())
			}
		})
	}
}

func mkResp(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
}

// The Messages route follows the Stainless SDK retry rule: 408/409/429/5xx are
// retried; every other 4xx is the upstream's final answer unless its body
// explicitly names a transient fault. (Real bodies: TestClassifyRealMessages.)
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
		{"400-generic-api-error", mkResp(400, `{"error":{"type":"api_error","message":"upstream gateway timeout"}}`), false},
		{"400-names-overload", mkResp(400, `{"error":{"type":"overloaded_error","message":"gateway overloaded"}}`), true},
		{"401", mkResp(401, `{"error":{"type":"authentication_error","message":"invalid api key"}}`), false},
		{"403", mkResp(403, `{"error":{"type":"permission_error","message":"organization disabled"}}`), false},
		{"404-html", mkResp(404, `<html>not found</html>`), false},
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

// TestPingRunTripwire verifies captureStats records the LONGEST run of consecutive
// upstream `ping` events — the content-silent-gap tripwire. A dense stream with a
// 3-ping run and a later 1-ping run must report maxPingRun=3; any non-ping event
// (here, content deltas) breaks the run.
func TestPingRunTripwire(t *testing.T) {
	useDefaultConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	var st captureStats
	ping := "event: ping\ndata: {\"type\":\"ping\"}\n\n"
	delta := func(s string) string {
		return "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"" + s + "\"}}\n\n"
	}
	r := &gapReader{ctx: ctx, chunks: []string{
		"event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0}\n\n",
		delta("Hi"),
		ping, ping, ping, // longest run = 3
		delta("!"),
		ping, // isolated run = 1
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
			"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n" +
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
	}, gap: 2 * time.Millisecond}
	// Large window + ungated: buffer the whole turn and commit at message_stop, so
	// every event (incl. pings) passes through process() and is counted.
	wrote, f := captureSSEWindow(ctx, cancel, rec, http.Header{}, r, &st, time.Hour, false, false)
	if f != nil {
		t.Fatalf("unexpected failure %+v", *f)
	}
	if !wrote {
		t.Fatalf("expected a committed write")
	}
	if st.maxPingRun != 3 {
		t.Fatalf("maxPingRun=%d, want 3 (longest consecutive ping run)", st.maxPingRun)
	}
}

// ---------------------------------------------------------------- helpers ----

const (
	pingFrame        = "event: ping\ndata: {\"type\":\"ping\"}\n\n"
	messageStopFrame = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
)

func contentBlockStopFrame(index int) string {
	return sseJSON("content_block_stop", map[string]any{"type": "content_block_stop", "index": index})
}

func messageDeltaFrame(stopReason string) string {
	return sseJSON("message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": stopReason},
		"usage": map[string]any{"output_tokens": 7},
	})
}

// truncatedToolChunks is a tool input the token cap cut off inside a string.
var truncatedToolChunks = []string{`{"session_id":`, `"s_1","prompt":"cut o`}

// toolHead is a tool block of blockType up to and including its input deltas.
func toolHead(blockType string, chunks []string) string {
	var b strings.Builder
	writeInputJSONPrelude(&b, blockType, "toolu_1", "Tool")
	for _, c := range chunks {
		writeInputJSONDelta(&b, c)
	}
	return b.String()
}

// toolStreamThenTail is a tool block carrying chunks, closed, then tail frames.
func toolStreamThenTail(blockType string, chunks []string, tail ...string) string {
	return toolHead(blockType, chunks) + contentBlockStopFrame(0) + strings.Join(tail, "")
}

func textHead() string {
	return sseJSON("message_start", map[string]any{"type": "message_start", "message": map[string]any{"id": "msg_1"}}) +
		sseJSON("content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text", "text": ""}}) +
		sseJSON("content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": "Hi"}})
}

func eventNames(body string) []string {
	var parser sseParser
	var names []string
	for _, ev := range parser.feed([]byte(body)) {
		names = append(names, ev.name)
	}
	return names
}

func captureWithStats(t *testing.T, s string) (*httptest.ResponseRecorder, captureStats, *failure) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	var st captureStats
	_, f := captureSSE(ctx, cancel, rec, http.Header{}, strings.NewReader(s), &st)
	return rec, st, f
}

// liveCapture streams parts through a 20ms transactional window with a 100ms gap
// between parts, so the proxy commits live before the later parts arrive.
func liveCapture(t *testing.T, parts []string) (*httptest.ResponseRecorder, bool, *failure) {
	t.Helper()
	useDefaultConfig(t)
	cfg.upstreamByteIdle = 2 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	r := &gapReader{ctx: ctx, chunks: parts, gap: 100 * time.Millisecond}
	wrote, f := captureSSEWindow(ctx, cancel, rec, http.Header{}, r, nil, 20*time.Millisecond, false, false)
	return rec, wrote, f
}

// hangReader yields its chunks like gapReader, then stays open (no EOF) until
// the context ends: an upstream that sent everything but keeps the connection.
type hangReader struct{ gapReader }

func (h *hangReader) Read(p []byte) (int, error) {
	if h.i >= len(h.chunks) {
		<-h.ctx.Done()
		return 0, h.ctx.Err()
	}
	return h.gapReader.Read(p)
}

type liveStep struct {
	out  [][]byte
	done bool
	fail *failure
}

// feedLive drives a wire's live() with every event of stream, as process does.
func feedLive(w *anthropicWire, stream string) []liveStep {
	var parser sseParser
	var steps []liveStep
	for _, ev := range parser.feed([]byte(stream)) {
		out, done, fail := w.live(ev, ev.raw, nil)
		steps = append(steps, liveStep{out, done, fail})
	}
	return steps
}

// --------------------------------------------- max_tokens-truncated tool input ----

// A max_tokens stop cuts the tool input mid-JSON. Retrying regenerates the same
// truncation and bills it again, so the stream is the upstream's final answer.
func TestMaxTokensTruncatedToolInputIsDelivered(t *testing.T) {
	joined := strings.Join(truncatedToolChunks, "")
	const head = "message_start content_block_start content_block_delta content_block_stop"
	for _, blockType := range []string{"tool_use", "server_tool_use"} {
		for _, tc := range []struct {
			name      string
			tail      []string
			wantNames string
		}{
			{"direct", []string{messageDeltaFrame("max_tokens"), messageStopFrame}, head + " message_delta message_stop"},
			{"ping between", []string{pingFrame, "event: ping\n\n", messageDeltaFrame("max_tokens"), messageStopFrame}, head + " ping ping message_delta message_stop"},
		} {
			t.Run(blockType+"/"+tc.name, func(t *testing.T) {
				useDefaultConfig(t)
				rec, f := capture(t, toolStreamThenTail(blockType, truncatedToolChunks, tc.tail...))
				if f != nil {
					t.Fatalf("max_tokens-truncated tool input must be delivered, got %+v", *f)
				}
				body := rec.Body.String()
				if deltas := inputJSONDeltas(t, body); len(deltas) != 1 || deltas[0] != joined {
					t.Fatalf("want one coalesced partial delta %q, got %#v", joined, deltas)
				}
				if got := strings.Join(eventNames(body), " "); got != tc.wantNames {
					t.Fatalf("event order:\n got: %s\nwant: %s", got, tc.wantNames)
				}
				if !strings.Contains(body, `"stop_reason":"max_tokens"`) {
					t.Fatalf("message_delta must reach the client unchanged: %s", body)
				}
			})
		}
	}
}

func TestMaxTokensTruncatedToolInputKeepsRawFragmentsWhenNormalizationOff(t *testing.T) {
	useDefaultConfig(t)
	cfg.normalizeToolJSON = false
	rec, f := capture(t, toolStreamThenTail("tool_use", truncatedToolChunks, messageDeltaFrame("max_tokens"), messageStopFrame))
	if f != nil {
		t.Fatalf("max_tokens-truncated tool input must be delivered, got %+v", *f)
	}
	deltas := inputJSONDeltas(t, rec.Body.String())
	if len(deltas) != len(truncatedToolChunks) {
		t.Fatalf("want the raw fragments, got %#v", deltas)
	}
	for i, want := range truncatedToolChunks {
		if deltas[i] != want {
			t.Fatalf("fragment %d = %q, want %q", i, deltas[i], want)
		}
	}
}

// Only a max_tokens stop excuses invalid tool input, and only when it is the very
// next thing after the block closes.
func TestInvalidToolInputRejectedUnlessMaxTokens(t *testing.T) {
	textBlockStart := sseJSON("content_block_start", map[string]any{
		"type": "content_block_start", "index": 1, "content_block": map[string]any{"type": "text", "text": ""},
	})
	for _, tc := range []struct {
		name string
		tail []string
	}{
		{"tool_use stop", []string{messageDeltaFrame("tool_use"), messageStopFrame}},
		{"end_turn stop", []string{messageDeltaFrame("end_turn"), messageStopFrame}},
		{"stop_sequence stop", []string{messageDeltaFrame("stop_sequence"), messageStopFrame}},
		{"empty stop_reason", []string{messageDeltaFrame(""), messageStopFrame}},
		{"no stop_reason", []string{"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{},\"usage\":{\"output_tokens\":7}}\n\n", messageStopFrame}},
		{"no message_delta", []string{messageStopFrame}},
		{"content follows the invalid block", []string{textBlockStart, contentBlockStopFrame(1), messageDeltaFrame("max_tokens"), messageStopFrame}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useDefaultConfig(t)
			rec, f := capture(t, toolStreamThenTail("tool_use", truncatedToolChunks, tc.tail...))
			if f == nil || !f.transient || f.code != "malformed_sse" {
				t.Fatalf("want transient malformed_sse for invalid tool input, got %+v", f)
			}
			if rec.Body.Len() != 0 {
				t.Fatalf("a rejected attempt must not leak output: %q", rec.Body.String())
			}
		})
	}
}

func TestMaxTokensTruncatedToolInputAcrossLiveHandoff(t *testing.T) {
	joined := strings.Join(truncatedToolChunks, "")
	head := toolHead("tool_use", truncatedToolChunks)
	stop := contentBlockStopFrame(0)
	tail := messageDeltaFrame("max_tokens") + messageStopFrame
	const base = "message_start content_block_start content_block_delta content_block_stop"
	for _, tc := range []struct {
		name      string
		parts     []string
		wantNames string
	}{
		{"commit before the block closes", []string{head, stop, tail}, base + " message_delta message_stop"},
		{"commit after the block closes", []string{head + stop, tail}, base + " message_delta message_stop"},
		{"ping while held", []string{head, stop, pingFrame, tail}, base + " ping message_delta message_stop"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, wrote, f := liveCapture(t, tc.parts)
			if f != nil || !wrote {
				t.Fatalf("want live success, wrote=%v failure=%+v", wrote, f)
			}
			if mode := rec.Header().Get("X-Steady-Proxy-Mode"); mode != "live" {
				t.Fatalf("want live mode, got %q", mode)
			}
			body := rec.Body.String()
			if deltas := inputJSONDeltas(t, body); len(deltas) != 1 || deltas[0] != joined {
				t.Fatalf("want one coalesced partial delta %q, got %#v", joined, deltas)
			}
			if got := strings.Join(eventNames(body), " "); got != tc.wantNames {
				t.Fatalf("event order:\n got: %s\nwant: %s", got, tc.wantNames)
			}
		})
	}
}

// A live stream cannot be retried, but invalid tool input that max_tokens does not
// explain is still malformed: nothing from the held block may reach the client.
func TestInvalidToolInputOnLiveStreamIsMalformedUnlessMaxTokens(t *testing.T) {
	head := toolHead("tool_use", truncatedToolChunks)
	stop := contentBlockStopFrame(0)
	for _, tc := range []struct {
		name string
		next string
	}{
		{"end_turn", messageDeltaFrame("end_turn") + messageStopFrame},
		{"message_stop without message_delta", messageStopFrame},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, wrote, f := liveCapture(t, []string{head, stop, tc.next})
			if !wrote || f == nil || !f.transient || f.code != "malformed_sse" || f.delivered {
				t.Fatalf("want committed malformed_sse abort, wrote=%v failure=%+v", wrote, f)
			}
			body := rec.Body.String()
			for _, leaked := range []string{"input_json_delta", "content_block_stop", "message_delta", "message_stop"} {
				if strings.Contains(body, leaked) {
					t.Fatalf("held frames leaked %q: %s", leaked, body)
				}
			}
		})
	}
}

func TestAnthropicWireLiveHoldsInvalidToolBlock(t *testing.T) {
	useDefaultConfig(t)
	stream := toolStreamThenTail("tool_use", truncatedToolChunks, pingFrame, messageDeltaFrame("max_tokens"), messageStopFrame)
	steps := feedLive(newAnthropicWire(false), stream)
	// message_start, content_block_start, delta, delta, content_block_stop, ping, message_delta, message_stop
	if len(steps) != 8 {
		t.Fatalf("want 8 events, got %d", len(steps))
	}
	for i, s := range steps {
		if s.fail != nil {
			t.Fatalf("event %d: unexpected failure %+v", i, *s.fail)
		}
	}
	for _, i := range []int{2, 3, 4, 5} {
		if steps[i].out != nil {
			t.Fatalf("event %d must be withheld, got %d frames", i, len(steps[i].out))
		}
	}
	released := steps[6].out
	if len(released) != 4 {
		t.Fatalf("message_delta must release delta+stop+ping+itself, got %d frames", len(released))
	}
	if deltas := inputJSONDeltas(t, string(released[0])); len(deltas) != 1 || deltas[0] != strings.Join(truncatedToolChunks, "") {
		t.Fatalf("released delta = %#v", deltas)
	}
	if !steps[7].done {
		t.Fatalf("message_stop must end the stream")
	}
}

// --------------------------------------------------------------- empty ping ----

// The Anthropic SDK skips a ping without parsing its data, so a bare `event: ping`
// is a valid keepalive; every other named event still needs JSON data.
func TestCaptureAcceptsPingWithoutData(t *testing.T) {
	for _, ping := range []string{"event: ping\n\n", "event: ping\ndata:\n\n"} {
		t.Run(strings.ReplaceAll(ping, "\n", "|"), func(t *testing.T) {
			useDefaultConfig(t)
			s := strings.Replace(goodStream, "event: content_block_start", ping+"event: content_block_start", 1)
			rec, f := capture(t, s)
			if f != nil {
				t.Fatalf("empty-data ping must be accepted, got %+v", *f)
			}
			body := rec.Body.String()
			if !strings.Contains(body, ping) || !strings.Contains(body, "message_stop") {
				t.Fatalf("ping must be replayed as sent: %q", body)
			}
		})
	}
}

func TestCaptureRejectsOtherNamedEventsWithoutData(t *testing.T) {
	for _, name := range []string{"message_start", "content_block_start", "content_block_delta", "content_block_stop", "message_delta", "message_stop", "error", "pong"} {
		t.Run(name, func(t *testing.T) {
			useDefaultConfig(t)
			rec, f := capture(t, "event: "+name+"\n\n"+goodStream)
			if f == nil || !f.transient || f.code != "malformed_sse" {
				t.Fatalf("want transient malformed_sse, got %+v", f)
			}
			if rec.Body.Len() != 0 {
				t.Fatalf("malformed stream should fail before commit, body=%q", rec.Body.String())
			}
		})
	}
}

func TestAnthropicWireLiveAcceptsPingWithoutData(t *testing.T) {
	useDefaultConfig(t)
	steps := feedLive(newAnthropicWire(false), "event: ping\n\n")
	if len(steps) != 1 || steps[0].fail != nil || len(steps[0].out) != 1 || string(steps[0].out[0]) != "event: ping\n\n" {
		t.Fatalf("live empty-data ping must pass through, got %+v", steps)
	}
}

// ------------------------------------------------------------- usage backfill ----

func usageMergeStream(startUsage, deltaUsage string) string {
	return "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"m\",\"usage\":" + startUsage + "}}\n\n" +
		usageMergeTail(deltaUsage)
}

// usageMergeTail is everything after message_start, ending in a message_delta
// that carries deltaUsage.
func usageMergeTail(deltaUsage string) string {
	return "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n" +
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":" + deltaUsage + "}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
}

// message_delta usage merges into message_start per field: a delta value wins only
// when present and > 0. A delta that omits or zeroes a field must never erase the
// real count message_start reported (it used to zero cache_read_input_tokens).
func TestUsageBackfillMergesPerField(t *testing.T) {
	for _, tc := range []struct {
		name, start, delta string
		want               testUsage
		total              int
		backfilled         bool
		deltaKeepsInput    bool
	}{
		{
			name:  "delta omits cache fields",
			start: `{"input_tokens":3,"cache_creation_input_tokens":0,"cache_read_input_tokens":5000,"output_tokens":1}`,
			delta: `{"input_tokens":7,"output_tokens":20}`,
			want:  testUsage{Input: 7, CacheRead: 5000}, total: 5007, backfilled: true,
		},
		{
			name:  "delta zeroes cache fields",
			start: `{"input_tokens":3,"cache_creation_input_tokens":0,"cache_read_input_tokens":5000,"output_tokens":1}`,
			delta: `{"input_tokens":7,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":20}`,
			want:  testUsage{Input: 7, CacheRead: 5000}, total: 5007, backfilled: true,
		},
		{
			name:  "delta carries only cache_read",
			start: `{"input_tokens":3,"cache_creation_input_tokens":100,"cache_read_input_tokens":0,"output_tokens":1}`,
			delta: `{"cache_read_input_tokens":40,"output_tokens":2}`,
			want:  testUsage{Input: 3, CacheCreation: 100, CacheRead: 40}, total: 143, backfilled: true,
		},
		{
			name:  "positive delta fields override",
			start: `{"input_tokens":3,"cache_creation_input_tokens":100,"cache_read_input_tokens":5000,"output_tokens":1}`,
			delta: `{"input_tokens":7,"cache_creation_input_tokens":200,"cache_read_input_tokens":6000,"output_tokens":2}`,
			want:  testUsage{Input: 7, CacheCreation: 200, CacheRead: 6000}, total: 6207, backfilled: true,
		},
		{
			// Merged values equal message_start's: nothing to backfill, so neither
			// frame is rewritten.
			name:  "delta repeats input and zeroes the rest",
			start: `{"input_tokens":10,"cache_creation_input_tokens":0,"cache_read_input_tokens":500,"output_tokens":1}`,
			delta: `{"input_tokens":10,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":9}`,
			want:  testUsage{Input: 10, CacheRead: 500}, total: 510, deltaKeepsInput: true,
		},
		{
			name:  "delta without input usage",
			start: `{"input_tokens":10,"cache_creation_input_tokens":0,"cache_read_input_tokens":500,"output_tokens":1}`,
			delta: `{"output_tokens":9}`,
			want:  testUsage{Input: 10, CacheRead: 500}, total: 510,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useDefaultConfig(t)
			rec, st, f := captureWithStats(t, usageMergeStream(tc.start, tc.delta))
			if f != nil {
				t.Fatalf("unexpected failure %+v", *f)
			}
			body := rec.Body.String()
			if got := messageStartUsage(t, body); got != tc.want {
				t.Errorf("replayed message_start usage = %+v, want %+v", got, tc.want)
			}
			if st.inTok != tc.total || st.inputTok != tc.want.Input || st.cacheCreationTok != tc.want.CacheCreation || st.cacheReadTok != tc.want.CacheRead {
				t.Errorf("logged usage total=%d in/create/read=%d/%d/%d, want total=%d %+v",
					st.inTok, st.inputTok, st.cacheCreationTok, st.cacheReadTok, tc.total, tc.want)
			}
			deltaUsage := messageDeltaUsage(t, body)
			if tc.backfilled {
				assertNoInputUsage(t, deltaUsage)
			} else {
				if _, has := deltaUsage["input_tokens"]; has != tc.deltaKeepsInput {
					t.Errorf("message_delta usage = %#v, want input_tokens present=%v", deltaUsage, tc.deltaKeepsInput)
				}
				if !strings.Contains(body, `"usage":`+tc.start) {
					t.Errorf("message_start must be replayed untouched when nothing differs: %s", body)
				}
			}
		})
	}
}

// The input fields leave message_delta only once message_start actually received
// them; a message_start that cannot be patched must not lose the numbers.
func TestUsageStrippedFromDeltaOnlyWhenBackfilled(t *testing.T) {
	useDefaultConfig(t)
	// message_start without a message object cannot take the backfill.
	s := "event: message_start\ndata: {\"type\":\"message_start\"}\n\n" + usageMergeTail(`{"input_tokens":99,"output_tokens":3}`)
	rec, st, f := captureWithStats(t, s)
	if f != nil {
		t.Fatalf("unexpected failure %+v", *f)
	}
	if st.inTok != 99 {
		t.Errorf("inTok = %d, want 99", st.inTok)
	}
	if got := messageDeltaUsage(t, rec.Body.String()); got["input_tokens"] != float64(99) {
		t.Errorf("message_delta lost its input usage although message_start was not patched: %#v", got)
	}
}

// ------------------------------------------------------------- HTML escaping ----

// Frames the proxy re-encodes must keep <, > and & literal; encoding/json would
// otherwise rewrite them as \u003c, \u003e and \u0026.
func TestToolInputCoalescingKeepsHTMLCharacters(t *testing.T) {
	useDefaultConfig(t)
	input := `{"cmd":"if a<b && c>d; then echo <a&b>; fi"}`
	rec, f := capture(t, toolUseStream(splitEvery(input, 6)))
	if f != nil {
		t.Fatalf("unexpected failure %+v", *f)
	}
	body := rec.Body.String()
	for _, escaped := range []string{`\u003c`, `\u003e`, `\u0026`} {
		if strings.Contains(body, escaped) {
			t.Fatalf("coalesced delta HTML-escaped %s: %s", escaped, body)
		}
	}
	if !strings.Contains(body, `<a&b>`) {
		t.Fatalf("coalesced delta lost literal <a&b>: %s", body)
	}
	if deltas := inputJSONDeltas(t, body); len(deltas) != 1 || deltas[0] != input {
		t.Fatalf("coalesced input = %#v, want %q", deltas, input)
	}
}

func TestUsageReencodingKeepsHTMLCharacters(t *testing.T) {
	useDefaultConfig(t)
	s := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_<1>&\",\"model\":\"m<&>\",\"usage\":{\"input_tokens\":0,\"output_tokens\":0}}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0}\n\n" +
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":\"</end>\"},\"usage\":{\"input_tokens\":99,\"output_tokens\":3}}\n\n" +
		messageStopFrame
	rec, _, f := captureWithStats(t, s)
	if f != nil {
		t.Fatalf("unexpected failure %+v", *f)
	}
	body := rec.Body.String()
	if got := messageStartUsage(t, body).Input; got != 99 {
		t.Fatalf("test setup: message_start must have been backfilled, input=%d", got)
	}
	for _, want := range []string{`"id":"msg_<1>&"`, `"model":"m<&>"`, `"stop_sequence":"</end>"`} {
		if !strings.Contains(body, want) {
			t.Errorf("re-encoded frame lost literal %s:\n%s", want, body)
		}
	}
	for _, escaped := range []string{`\u003c`, `\u003e`, `\u0026`} {
		if strings.Contains(body, escaped) {
			t.Errorf("re-encoded frame HTML-escaped %s:\n%s", escaped, body)
		}
	}
}

// ---------------------------------------------------------------- live path ----

// Once message_stop is forwarded the stream is complete, even if the validator
// flagged an ordering fault earlier; otherwise terminal() is never true.
func TestAnthropicWireLiveDoneAtMessageStopAfterOrderFault(t *testing.T) {
	useDefaultConfig(t)
	w := newAnthropicWire(false)
	steps := feedLive(w, textHead()+contentBlockStopFrame(0)+contentBlockStopFrame(0)+messageDeltaFrame("end_turn")+messageStopFrame)
	if !w.val.invalid {
		t.Fatalf("test setup: the duplicate content_block_stop must flag the validator")
	}
	for i, s := range steps {
		if s.fail != nil {
			t.Fatalf("event %d: live must not fail on an order fault, got %+v", i, *s.fail)
		}
		if want := i == len(steps)-1; s.done != want {
			t.Fatalf("event %d: done=%v, want %v", i, s.done, want)
		}
	}
}

// A message_stop arriving while a tool block's input is still withheld (the
// block never closed) must not end the stream cleanly: the client would get a
// tool call without its arguments.
func TestAnthropicWireLiveMessageStopWithWithheldToolInputFails(t *testing.T) {
	useDefaultConfig(t)
	w := newAnthropicWire(false)
	steps := feedLive(w, toolHead("tool_use", []string{`{"path":`, `"/tmp/x"}`})+messageDeltaFrame("tool_use")+messageStopFrame)
	last := steps[len(steps)-1]
	if last.fail == nil || last.done || len(last.out) != 0 {
		t.Fatalf("message_stop with withheld tool input must fail without forwarding, got done=%v out=%d fail=%+v", last.done, len(last.out), last.fail)
	}
}

func TestLiveStreamEndsPromptlyAfterMessageStop(t *testing.T) {
	useDefaultConfig(t)
	cfg.upstreamByteIdle = 400 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	tail := contentBlockStopFrame(0) + contentBlockStopFrame(0) + messageDeltaFrame("end_turn") + messageStopFrame
	r := &hangReader{gapReader{ctx: ctx, chunks: []string{textHead(), tail}, gap: 100 * time.Millisecond}}
	wrote, f := captureSSEWindow(ctx, cancel, rec, http.Header{}, r, nil, 20*time.Millisecond, false, false)
	if f != nil || !wrote {
		t.Fatalf("a complete live stream must end at message_stop (upstream still open), wrote=%v failure=%+v", wrote, f)
	}
	if rec.Header().Get("X-Steady-Proxy-Mode") != "live" || !strings.Contains(rec.Body.String(), "message_stop") {
		t.Fatalf("want a live response carrying message_stop, mode=%q body=%q", rec.Header().Get("X-Steady-Proxy-Mode"), rec.Body.String())
	}
}

// An in-band error that is the upstream's final answer is forwarded verbatim and
// marked delivered; a transient one never reaches the client (the handler aborts
// so the client's own stream retry takes over).
func TestAnthropicWireLiveErrorForwarding(t *testing.T) {
	useDefaultConfig(t)
	for _, tc := range []struct {
		typ, msg  string
		forwarded bool
	}{
		{"authentication_error", "invalid x-api-key", true},
		{"permission_error", "forbidden", true},
		{"billing_error", "credits required", true},
		{"not_found_error", "model: nope", true},
		{"request_too_large", "too many bytes", true},
		{"invalid_request_error", "bad request", true},
		{"invalid_request_error", streamDisconnectMessage, false},
		{"overloaded_error", "Overloaded", false},
		{"rate_limit_error", "slow down", false},
		{"api_error", "boom", false},
		{"timeout_error", "timed out", false},
		{"something_new", "unknown", false},
	} {
		t.Run(tc.typ+"/"+tc.msg, func(t *testing.T) {
			frame := sseJSON("error", map[string]any{"type": "error", "error": map[string]string{"type": tc.typ, "message": tc.msg}})
			steps := feedLive(newAnthropicWire(false), frame)
			if len(steps) != 1 || steps[0].fail == nil || steps[0].done {
				t.Fatalf("want one failing, not-done step, got %+v", steps)
			}
			s := steps[0]
			if tc.forwarded {
				if s.fail.transient || !s.fail.delivered || len(s.out) != 1 || string(s.out[0]) != frame {
					t.Fatalf("want verbatim forward with delivered=true, got out=%q fail=%+v", s.out, *s.fail)
				}
				return
			}
			if !s.fail.transient || s.fail.delivered || s.out != nil {
				t.Fatalf("a transient error must not be forwarded, got out=%q fail=%+v", s.out, *s.fail)
			}
		})
	}
}

func TestLiveInBandErrorDelivery(t *testing.T) {
	auth := sseJSON("error", map[string]any{"type": "error", "error": map[string]string{"type": "authentication_error", "message": "invalid x-api-key"}})
	overloaded := sseJSON("error", map[string]any{"type": "error", "error": map[string]string{"type": "overloaded_error", "message": "Overloaded"}})

	rec, wrote, f := liveCapture(t, []string{textHead(), auth})
	if !wrote || f == nil || f.transient || !f.delivered {
		t.Fatalf("want committed non-transient failure with delivered=true, wrote=%v failure=%+v", wrote, f)
	}
	if !strings.Contains(rec.Body.String(), auth) {
		t.Fatalf("the upstream's error frame must reach the client verbatim: %q", rec.Body.String())
	}

	rec, wrote, f = liveCapture(t, []string{textHead(), overloaded})
	if !wrote || f == nil || !f.transient || f.delivered {
		t.Fatalf("want committed transient failure with delivered=false, wrote=%v failure=%+v", wrote, f)
	}
	if strings.Contains(rec.Body.String(), "overloaded_error") {
		t.Fatalf("a transient error must not be forwarded: %q", rec.Body.String())
	}
}

func TestToolInputRetentionBudget(t *testing.T) {
	start := func(index int) string {
		return sseJSON("content_block_start", map[string]any{
			"type": "content_block_start", "index": index,
			"content_block": map[string]any{"type": "tool_use", "id": "toolu_2", "name": "Tool", "input": map[string]any{}},
		})
	}
	delta := func(index int, input string) string {
		return sseJSON("content_block_delta", map[string]any{
			"type": "content_block_delta", "index": index,
			"delta": map[string]any{"type": "input_json_delta", "partial_json": input},
		})
	}
	input := `{"value":"` + strings.Repeat("x", 256) + `"}`
	first, second := delta(0, input), delta(1, input)
	interleaved := toolHead("tool_use", nil) + first + start(1) + second
	sequential := toolHead("tool_use", nil) + first + contentBlockStopFrame(0) +
		start(1) + second + contentBlockStopFrame(1) + messageDeltaFrame("tool_use") + messageStopFrame
	for _, tc := range []struct {
		name   string
		stream string
		limit  int64
	}{
		{"interleaved-blocks", interleaved, int64(len(first) + len(second) - 1)},
		{"empty-fragments", toolHead("tool_use", nil) + strings.Repeat(delta(0, ""), 3), int64(2 * len(delta(0, "")))},
		{"held-pings", toolStreamThenTail("tool_use", truncatedToolChunks, strings.Repeat(pingFrame, 32)), 512},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useDefaultConfig(t)
			cfg.maxResponseBytes = tc.limit
			var f *failure
			for _, step := range feedLive(newAnthropicWire(false), tc.stream) {
				if step.fail != nil {
					f = step.fail
					break
				}
			}
			if f == nil || f.code != "response_too_large" || f.transient {
				t.Fatalf("retained input must fail without retry at its cap: %+v", f)
			}
		})
	}
	t.Run("exact-cap-and-release", func(t *testing.T) {
		useDefaultConfig(t)
		cfg.maxResponseBytes = int64(len(first))
		var output strings.Builder
		steps := feedLive(newAnthropicWire(false), sequential)
		for _, step := range steps {
			if step.fail != nil {
				t.Fatalf("completed tool block did not release its budget: %+v", step.fail)
			}
			for _, raw := range step.out {
				output.Write(raw)
			}
		}
		inputs := inputJSONDeltas(t, output.String())
		if !steps[len(steps)-1].done || len(inputs) != 2 || inputs[0] != input || inputs[1] != input {
			t.Fatalf("sequential tool inputs were not delivered completely: %q", output.String())
		}
	})
	t.Run("normalization-disabled", func(t *testing.T) {
		useDefaultConfig(t)
		cfg.maxResponseBytes = 512
		cfg.normalizeToolJSON = false
		largeInput := `{"value":"` + strings.Repeat("x", 8192) + `"}`
		stream := toolStreamThenTail("tool_use", splitEvery(largeInput, 128), messageDeltaFrame("tool_use"), messageStopFrame)
		var output strings.Builder
		steps := feedLive(newAnthropicWire(false), stream)
		for _, step := range steps {
			if step.fail != nil {
				t.Fatalf("unbuffered tool fragments must not consume the retention budget: %+v", step.fail)
			}
			for _, raw := range step.out {
				output.Write(raw)
			}
		}
		if !steps[len(steps)-1].done || strings.Join(inputJSONDeltas(t, output.String()), "") != largeInput {
			t.Fatal("unbuffered tool fragments were not delivered completely")
		}
	})
	for _, tc := range []struct {
		name     string
		stream   string
		limit    int64
		wantFail bool
	}{
		{"validator-interleaved", interleaved, int64(2*len(input) - 1), true},
		{"validator-release", sequential, int64(len(input)), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useDefaultConfig(t)
			cfg.maxResponseBytes = tc.limit
			var validator toolJSONValidator
			var parser sseParser
			var f *failure
			for _, ev := range parser.feed([]byte(tc.stream)) {
				if f = validator.accept(ev); f != nil {
					break
				}
			}
			if tc.wantFail {
				if f == nil || f.code != "response_too_large" || f.transient {
					t.Fatalf("decoded input must fail without retry at its cap: %+v", f)
				}
			} else if f != nil {
				t.Fatalf("completed validation block did not release its budget: %+v", f)
			}
		})
	}
}
