package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

func TestCaptureTruncated(t *testing.T) {
	trunc := goodStream[:strings.Index(goodStream, "content_block_stop")]
	_, f := capture(t, trunc)
	if f == nil || !f.transient || f.code != "truncated_stream" {
		t.Fatalf("want transient truncated_stream, got %+v", f)
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
