package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A minimal but structurally faithful OpenAI Responses SSE stream (Codex wire).
const goodResponsesStream = `event: response.created
data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-5.6-sol","status":"in_progress"}}

event: response.output_item.added
data: {"type":"response.output_item.added","output_index":0,"item":{"id":"msg_1","type":"message"}}

event: response.content_part.added
data: {"type":"response.content_part.added","item_id":"msg_1","output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}

event: response.output_text.delta
data: {"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"Hi"}

event: response.output_text.done
data: {"type":"response.output_text.done","item_id":"msg_1","output_index":0,"content_index":0,"text":"Hi"}

event: response.completed
data: {"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.6-sol","status":"completed","usage":{"input_tokens":1234,"output_tokens":56,"total_tokens":1290,"input_tokens_details":{"cached_tokens":34}}}}

`

func captureResp(t *testing.T, r io.Reader, st *captureStats, window time.Duration, early bool) (*httptest.ResponseRecorder, bool, *failure) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	wrote, f := captureSSECore(ctx, cancel, rec, http.Header{}, r, st, window, false, early, newOpenAIWire(), bufferDeadline(time.Now(), window))
	return rec, wrote, f
}

func responsesDeltaFrame(item string, output, content int, delta string, sequence int, obfuscation string, logprobs []any) string {
	data, _ := json.Marshal(map[string]any{
		"type":            "response.output_text.delta",
		"item_id":         item,
		"output_index":    output,
		"content_index":   content,
		"delta":           delta,
		"logprobs":        logprobs,
		"sequence_number": sequence,
		"obfuscation":     obfuscation,
	})
	return "event: response.output_text.delta\ndata: " + string(data) + "\n\n"
}

func replayResponsesForTest(t *testing.T, input string, limit int) string {
	t.Helper()
	sp := newSpool()
	t.Cleanup(sp.discard)
	if err := sp.write([]byte(input)); err != nil {
		t.Fatalf("spool write: %v", err)
	}
	var out bytes.Buffer
	if err := (&openaiWire{replayDeltaBytes: limit}).replay(sp, &out); err != nil {
		t.Fatalf("replay: %v", err)
	}
	return out.String()
}

func parsedResponsesEvents(s string) []event {
	return (&sseParser{}).feed([]byte(s))
}

type replayWriteAwareReader struct {
	data            []byte
	wrote           *bool
	readBeforeWrite int
	maxBeforeWrite  int
}

func (r *replayWriteAwareReader) Read(p []byte) (int, error) {
	if !*r.wrote && r.readBeforeWrite >= r.maxBeforeWrite {
		return 0, errors.New("replay consumed an oversized frame before writing it")
	}
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	if !*r.wrote {
		r.readBeforeWrite += n
	}
	return n, nil
}

type replayWriteAwareBuffer struct {
	bytes.Buffer
	wrote *bool
}

func (w *replayWriteAwareBuffer) Write(p []byte) (int, error) {
	*w.wrote = true
	return w.Buffer.Write(p)
}

func TestResponsesCaptureSuccessBuffered(t *testing.T) {
	var st captureStats
	// One read (strings.Reader) + no early commit -> pure buffered path.
	rec, wrote, f := captureResp(t, strings.NewReader(goodResponsesStream), &st, time.Hour, false)
	if f != nil {
		t.Fatalf("expected success, got failure %+v", *f)
	}
	if !wrote {
		t.Fatalf("expected wrote=true")
	}
	if rec.Header().Get("X-Steady-Proxy-Mode") != "buffered" {
		t.Fatalf("want buffered mode, got %q", rec.Header().Get("X-Steady-Proxy-Mode"))
	}
	if rec.Body.String() != goodResponsesStream {
		t.Fatalf("buffered replay must be byte-for-byte identical.\n got: %q", rec.Body.String())
	}
	// usage/model/stop scraped for the access log.
	if st.model != "gpt-5.6-sol" || st.inTok != 1234 || st.cacheReadTok != 34 || st.outTok != 56 || st.stop != "completed" {
		t.Fatalf("scrape mismatch: model=%q inTok=%d cacheRead=%d outTok=%d stop=%q", st.model, st.inTok, st.cacheReadTok, st.outTok, st.stop)
	}
}

func TestResponsesBufferedReplayCoalescesBurstWithoutChangingSemantics(t *testing.T) {
	var input strings.Builder
	for i := 0; i < 2750; i++ {
		input.WriteString(responsesDeltaFrame("msg_1", 3, 4, "x", i+10, "padding", []any{}))
	}
	input.WriteString("event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"item_id\":\"msg_1\",\"output_index\":3,\"content_index\":4,\"text\":\"done\"}\n\n")

	out := replayResponsesForTest(t, input.String(), 64<<10)
	events := parsedResponsesEvents(out)
	var deltas []event
	for _, ev := range events {
		if responsesEventType(ev) == "response.output_text.delta" {
			deltas = append(deltas, ev)
		}
	}
	if len(deltas) <= 1 || len(deltas) >= 20 {
		t.Fatalf("2750 tiny deltas should replay as a small set of bounded groups, got %d", len(deltas))
	}
	var mergedText strings.Builder
	lastSequence := 0
	for _, deltaEvent := range deltas {
		var merged struct {
			Delta          string            `json:"delta"`
			ItemID         string            `json:"item_id"`
			OutputIndex    int               `json:"output_index"`
			ContentIndex   int               `json:"content_index"`
			SequenceNumber int               `json:"sequence_number"`
			Obfuscation    string            `json:"obfuscation"`
			Logprobs       []json.RawMessage `json:"logprobs"`
		}
		if err := json.Unmarshal([]byte(deltaEvent.data), &merged); err != nil {
			t.Fatalf("merged delta JSON: %v", err)
		}
		if merged.ItemID != "msg_1" || merged.OutputIndex != 3 || merged.ContentIndex != 4 {
			t.Fatalf("merged routing mismatch: item=%q output=%d content=%d", merged.ItemID, merged.OutputIndex, merged.ContentIndex)
		}
		if merged.Obfuscation != "padding" || len(merged.Logprobs) != 0 {
			t.Fatalf("merged metadata mismatch: obfuscation=%q logprobs=%d", merged.Obfuscation, len(merged.Logprobs))
		}
		mergedText.WriteString(merged.Delta)
		lastSequence = merged.SequenceNumber
	}
	if mergedText.String() != strings.Repeat("x", 2750) {
		t.Fatalf("merged text mismatch: bytes=%d", mergedText.Len())
	}
	if lastSequence != 2759 {
		t.Fatalf("final merged sequence mismatch: got %d want 2759", lastSequence)
	}
	if len(events) != len(deltas)+1 || responsesEventType(events[len(events)-1]) != "response.output_text.done" {
		t.Fatalf("non-delta ordering barrier was not preserved: %#v", events)
	}
}

func TestResponsesBufferedReplayKeepsNonEmptyLogprobsAsRawBarriers(t *testing.T) {
	validLogprob := responsesDeltaFrame("msg_1", 0, 0, "a", 1, "a", []any{map[string]any{
		"token": "a", "logprob": -0.1,
	}})
	malformedLogprob := responsesDeltaFrame("msg_1", 0, 0, "b", 2, "b", []any{map[string]any{
		"token": "missing-required-logprob",
	}})
	empty1 := responsesDeltaFrame("msg_1", 0, 0, "c", 3, "c", []any{})
	empty2 := responsesDeltaFrame("msg_1", 0, 0, "d", 4, "d", []any{})
	input := validLogprob + malformedLogprob + empty1 + empty2

	out := replayResponsesForTest(t, input, 64<<10)
	if !strings.HasPrefix(out, validLogprob+malformedLogprob) {
		t.Fatalf("non-empty logprob frames must remain byte-exact barriers\n got: %q", out)
	}
	events := parsedResponsesEvents(out)
	if len(events) != 3 {
		t.Fatalf("want two raw logprob barriers plus one empty-logprob group, got %d", len(events))
	}
	var merged struct {
		Delta          string            `json:"delta"`
		SequenceNumber int               `json:"sequence_number"`
		Logprobs       []json.RawMessage `json:"logprobs"`
	}
	if err := json.Unmarshal([]byte(events[2].data), &merged); err != nil {
		t.Fatalf("empty-logprob group JSON: %v", err)
	}
	if merged.Delta != "cd" || merged.SequenceNumber != 4 || len(merged.Logprobs) != 0 {
		t.Fatalf("empty-logprob group mismatch: %+v", merged)
	}
}

func TestResponsesReplayLeavesDuplicateKeyFramesRaw(t *testing.T) {
	tests := map[string]string{
		"top-level known field": "event: response.output_text.delta\n" +
			"data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":null,\"delta\":\"invented\"}\n\n",
		"nested logprob field": "event: response.output_text.delta\n" +
			"data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"bad\",\"logprobs\":[{\"token\":\"bad\",\"token\":\"still-bad\"}]}\n\n",
	}
	for name, duplicate := range tests {
		t.Run(name, func(t *testing.T) {
			// Codex's serde event structs reject duplicate known fields. The proxy
			// must not normalize the frame or combine it with valid neighboring text.
			input := duplicate + responsesDeltaFrame("msg_1", 0, 0, "real", 2, "padding", []any{})
			if got := replayResponsesForTest(t, input, 64<<10); got != input {
				t.Fatalf("duplicate-key frame must remain a raw ordering barrier\n got: %q\nwant: %q", got, input)
			}
		})
	}
}

func TestResponsesReplayStreamsOversizedFramesIncrementally(t *testing.T) {
	input := "event: response.completed\ndata: {\"type\":\"response.completed\",\"blob\":\"" +
		strings.Repeat("x", 256<<10) + "\"}\n\n"
	wrote := false
	r := &replayWriteAwareReader{
		data: []byte(input), wrote: &wrote,
		// The framing reader may fill its 32 KiB buffer once. It must begin
		// passthrough before asking for a second buffer, rather than wait for EOF.
		maxBeforeWrite: 48 << 10,
	}
	w := &replayWriteAwareBuffer{wrote: &wrote}
	if err := replayResponsesReader(r, w, 1024); err != nil {
		t.Fatalf("incremental oversized replay: %v", err)
	}
	if w.String() != input {
		t.Fatalf("oversized frame changed during passthrough: got %d bytes, want %d", w.Len(), len(input))
	}
}

func TestResponsesReplayLeavesInvalidUnicodeFramesRaw(t *testing.T) {
	frame := func(deltaJSON, sequence string) string {
		return "event: response.output_text.delta\n" +
			"data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"" + deltaJSON +
			"\",\"logprobs\":[],\"sequence_number\":" + sequence + ",\"obfuscation\":\"padding\"}\n\n"
	}
	validNeighbor := frame("real", "2")
	for name, invalid := range map[string]string{
		"lone high surrogate": frame(`\ud800`, "1"),
		"lone low surrogate":  frame(`\udc00`, "1"),
		"unpaired high":       frame(`\ud800x`, "1"),
	} {
		t.Run(name, func(t *testing.T) {
			input := invalid + validNeighbor
			if got := replayResponsesForTest(t, input, 64<<10); got != input {
				t.Fatalf("invalid-Unicode frame must remain a raw ordering barrier\n got: %q\nwant: %q", got, input)
			}
		})
	}

	// A correctly paired surrogate remains eligible and decodes to one scalar.
	events := parsedResponsesEvents(replayResponsesForTest(t, frame(`\ud83d\ude00`, "1")+validNeighbor, 64<<10))
	if len(events) != 1 {
		t.Fatalf("valid surrogate pair should coalesce, got %d events", len(events))
	}
	var merged struct {
		Delta string `json:"delta"`
	}
	if err := json.Unmarshal([]byte(events[0].data), &merged); err != nil || merged.Delta != "😀real" {
		t.Fatalf("valid surrogate pair mismatch: delta=%q err=%v", merged.Delta, err)
	}
}

func TestResponsesReplayCoalescingCanBeDisabled(t *testing.T) {
	input := responsesDeltaFrame("msg_1", 0, 0, "a", 1, "a", []any{}) +
		responsesDeltaFrame("msg_1", 0, 0, "b", 2, "b", []any{})
	if got := replayResponsesForTest(t, input, 0); got != input {
		t.Fatalf("zero limit must preserve byte-for-byte replay\n got: %q\nwant: %q", got, input)
	}
}

func TestResponsesLiveStreamingRemainsByteExactWithReplayCoalescing(t *testing.T) {
	stream := responsesDeltaFrame("msg_1", 0, 0, "a", 1, "a", []any{}) +
		responsesDeltaFrame("msg_1", 0, 0, "b", 2, "b", []any{}) +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_live\",\"status\":\"completed\"}}\n\n"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	wrote, f := captureSSECore(ctx, cancel, rec, http.Header{}, iotest1byte(stream), nil, time.Hour, false, true, &openaiWire{replayDeltaBytes: 64 << 10}, time.Now().Add(time.Hour))
	if f != nil || !wrote {
		t.Fatalf("live stream failed: wrote=%v f=%+v", wrote, f)
	}
	if rec.Body.String() != stream {
		t.Fatalf("live stream must stay byte-for-byte exact\n got: %q\nwant: %q", rec.Body.String(), stream)
	}
}

func TestResponsesCaptureSuccessChunkedEarlyCommit(t *testing.T) {
	// 1-byte chunks + early commit -> commits live once output appears, then
	// streams the rest. OpenAI forwards raw, so downstream must still equal input.
	rec, wrote, f := captureResp(t, iotest1byte(goodResponsesStream), nil, time.Hour, true)
	if f != nil {
		t.Fatalf("expected success, got failure %+v", *f)
	}
	if !wrote {
		t.Fatalf("expected wrote=true")
	}
	if rec.Body.String() != goodResponsesStream {
		t.Fatalf("live stream must be byte-for-byte identical.\n got: %q", rec.Body.String())
	}
	if m := rec.Header().Get("X-Steady-Proxy-Mode"); m != "live" {
		t.Fatalf("want live mode after early commit, got %q", m)
	}
}

func TestResponsesStartErrorConvertsToRetry(t *testing.T) {
	// The exact failure that halted Codex: an overload error at stream start.
	stream := `event: error
data: {"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded."}}

`
	rec, wrote, f := captureResp(t, strings.NewReader(stream), nil, time.Hour, true)
	if f == nil {
		t.Fatalf("expected a failure for start-of-stream error")
	}
	if wrote {
		t.Fatalf("start error must be pre-commit (wrote=false), got wrote=true")
	}
	if !f.transient {
		t.Fatalf("overload must be transient/retryable, got %+v", *f)
	}
	if f.code != "responses_overloaded" {
		t.Fatalf("want code responses_overloaded, got %q", f.code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("nothing should be written downstream pre-commit, got %q", rec.Body.String())
	}
}

func TestResponsesFailedEventConvertsToRetry(t *testing.T) {
	stream := `event: response.failed
data: {"type":"response.failed","response":{"status":"failed","error":{"code":"server_is_overloaded","message":"overloaded"}}}

`
	_, wrote, f := captureResp(t, strings.NewReader(stream), nil, time.Hour, true)
	if f == nil || wrote {
		t.Fatalf("response.failed must convert pre-commit; wrote=%v f=%v", wrote, f)
	}
	if !f.transient || f.code != "responses_overloaded" {
		t.Fatalf("want transient responses_overloaded, got %+v", *f)
	}
}

func TestResponsesRequestShapeSurfaced(t *testing.T) {
	// A non-context request-shape (bad parameter) can never succeed, so it is
	// surfaced non-retryable rather than delivered or retried.
	stream := `event: response.failed
data: {"type":"response.failed","response":{"status":"failed","error":{"type":"invalid_request_error","code":"unknown_parameter","message":"Unknown parameter: foo"}}}

`
	_, wrote, f := captureResp(t, strings.NewReader(stream), nil, time.Hour, true)
	if f == nil || wrote {
		t.Fatalf("request-shape must convert pre-commit; wrote=%v f=%v", wrote, f)
	}
	if f.transient {
		t.Fatalf("invalid_request_error must NOT be retried (would loop), got transient")
	}
	if f.code != "responses_request_shape" {
		t.Fatalf("want responses_request_shape, got %q", f.code)
	}
}

func TestResponsesPostCommitErrorDrops(t *testing.T) {
	// created + output (commit) + an in-band error, no completed. After the early
	// commit the error must DROP (wrote=true, fail!=nil) rather than be forwarded.
	stream := `event: response.created
data: {"type":"response.created","response":{"id":"resp_1","model":"m"}}

event: response.output_item.added
data: {"type":"response.output_item.added","output_index":0,"item":{"id":"msg_1","type":"message"}}

event: response.output_text.delta
data: {"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"Hi"}

event: error
data: {"type":"error","error":{"type":"api_error","message":"mid-stream boom"}}

`
	rec, wrote, f := captureResp(t, iotest1byte(stream), nil, time.Hour, true)
	if !wrote {
		t.Fatalf("expected commit before the error (wrote=true)")
	}
	if f == nil {
		t.Fatalf("expected a DROP failure after commit")
	}
	if rec.Code != 200 {
		t.Fatalf("committed stream must have sent 200 before dropping, got %d", rec.Code)
	}
	// The fatal frame itself must never reach the client.
	if strings.Contains(rec.Body.String(), "mid-stream boom") {
		t.Fatalf("post-commit error frame was forwarded to the client")
	}
}

func TestResponsesBufferedModeCatchesMidStreamError(t *testing.T) {
	// early-commit OFF (PROXY_RESPONSES_EARLY_COMMIT=0): the Responses wire buffers to
	// the terminal like /v1/messages. A mid-stream error that arrives AFTER output —
	// which early-commit DROPs post-commit (TestResponsesPostCommitErrorDrops) — is now
	// caught PRE-commit and converted to a retry (wrote=false), extending the proxy's
	// hidden-retry protection to mid-stream failures. This is the whole point of the opt-out.
	stream := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"m\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hi\"}\n\n" +
		"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"mid-stream boom\"}}\n\n"
	rec, wrote, f := captureResp(t, iotest1byte(stream), nil, time.Hour, false) // early=false -> buffered
	if wrote || f == nil {
		t.Fatalf("buffered mode must catch the mid-stream error pre-commit; wrote=%v f=%v", wrote, f)
	}
	if !f.transient {
		t.Fatalf("a mid-stream api_error must convert to a retry, got %+v", *f)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("nothing should be forwarded pre-commit, got %q", rec.Body.String())
	}
}

func TestResponsesTruncatedStreamRetryable(t *testing.T) {
	// EOF before response.completed -> pre-commit truncation -> retryable.
	trunc := goodResponsesStream[:strings.Index(goodResponsesStream, "response.completed")]
	_, wrote, f := captureResp(t, strings.NewReader(trunc), nil, time.Hour, false)
	if f == nil || wrote {
		t.Fatalf("truncated stream must convert to retry; wrote=%v f=%v", wrote, f)
	}
	if !f.transient || f.code != "truncated_stream" {
		t.Fatalf("want transient truncated_stream, got %+v", *f)
	}
}

func TestResponsesLifecycleEventsDoNotCommitEarly(t *testing.T) {
	// response.queued (and any unknown lifecycle/metadata frame) is NOT output: a
	// following overload error must still be caught pre-commit (wrote=false) so the
	// hidden local retries apply. This guards the blocklist->allowlist fix.
	stream := `event: response.created
data: {"type":"response.created","response":{"id":"r","model":"m"}}

event: response.queued
data: {"type":"response.queued","response":{"id":"r"}}

event: error
data: {"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"overloaded"}}

`
	rec, wrote, f := captureResp(t, iotest1byte(stream), nil, time.Hour, true)
	if wrote {
		t.Fatalf("lifecycle events must not trigger early commit; the error should be pre-commit")
	}
	if f == nil || !f.transient || f.code != "responses_overloaded" {
		t.Fatalf("want pre-commit transient overload, got wrote=%v f=%v", wrote, f)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("nothing should have been committed downstream, got %q", rec.Body.String())
	}
}

func TestResponsesStandardTopLevelErrorEvent(t *testing.T) {
	// Standard OpenAI `error` event: top-level code/message, no nested error object.
	stream := "event: error\ndata: {\"type\":\"error\",\"code\":\"invalid_prompt\",\"message\":\"Your input was flagged\"}\n\n"
	_, wrote, f := captureResp(t, strings.NewReader(stream), nil, time.Hour, true)
	if wrote || f == nil {
		t.Fatalf("expected a pre-commit failure, got wrote=%v f=%v", wrote, f)
	}
	if f.transient || f.code != "responses_request_shape" {
		t.Fatalf("top-level invalid_prompt must surface as request-shape, got %+v", *f)
	}
}

func TestResponsesContextWindowDeliversNative(t *testing.T) {
	// A context-window failure is one Codex recovers from itself (compaction), so
	// the proxy must DELIVER the native response.failed frame (wrote=true, f=nil)
	// rather than convert it — otherwise Codex's context path never sees it.
	stream := "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"context_length_exceeded\",\"message\":\"input exceeds the context window\"}}}\n\n"
	rec, wrote, f := captureResp(t, strings.NewReader(stream), nil, time.Hour, true)
	if !wrote || f != nil {
		t.Fatalf("context-window must deliver native (wrote=true, f=nil), got wrote=%v f=%v", wrote, f)
	}
	if !strings.Contains(rec.Body.String(), "context_length_exceeded") {
		t.Fatalf("native response.failed must be delivered to Codex, got %q", rec.Body.String())
	}
}

func TestResponsesStringTooLongSurfacedNotNative(t *testing.T) {
	// Codex maps ONLY the exact code context_length_exceeded to its native context
	// path; a near-neighbor like string_above_max_length is NOT recovered natively,
	// so it must surface as a deterministic request-shape (wrote=false, non-transient)
	// rather than be delivered as a frame Codex would retry to its stream cap.
	stream := "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"string_above_max_length\",\"message\":\"string too long\"}}}\n\n"
	rec, wrote, f := captureResp(t, strings.NewReader(stream), nil, time.Hour, true)
	if wrote || f == nil {
		t.Fatalf("string_above_max_length must convert pre-commit, not deliver native; wrote=%v f=%v", wrote, f)
	}
	if f.transient || f.code != "responses_request_shape" {
		t.Fatalf("want non-transient responses_request_shape, got %+v", *f)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("the native frame must NOT be forwarded, got %q", rec.Body.String())
	}
}

func TestResponsesCompletedWithoutIDRetryable(t *testing.T) {
	// Codex's ResponseCompleted requires a string `id`; a completed lacking one is a
	// stream error on Codex's side, not a completion. The proxy must not record
	// success — it leaves the stream un-terminated so EOF converts to a retry (which
	// may fetch a clean terminal), mirroring Codex's fail-and-retry.
	stream := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"model\":\"m\"}}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
	_, wrote, f := captureResp(t, strings.NewReader(stream), nil, time.Hour, false)
	if wrote || f == nil || f.code != "truncated_stream" {
		t.Fatalf("id-less completed must convert to a retry; wrote=%v f=%v", wrote, f)
	}
}

func TestResponsesCompletedUsageMustBeComplete(t *testing.T) {
	// Codex's ResponseCompleted requires input/output/total_tokens when `usage` is
	// present; a usage object missing total_tokens fails its parse -> stream error. The
	// proxy must mirror that and NOT record success (else it logs OK while Codex fails).
	partial := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"model\":\"m\"}}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}}\n\n"
	_, wrote, f := captureResp(t, strings.NewReader(partial), nil, time.Hour, false)
	if wrote || f == nil || f.code != "truncated_stream" {
		t.Fatalf("completed with incomplete usage must convert to a retry; wrote=%v f=%v", wrote, f)
	}
	// usage ABSENT is valid (Codex's field is Option/default): a completed with just an
	// id completes normally.
	ok := "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\"}}\n\n"
	_, wrote2, f2 := captureResp(t, strings.NewReader(ok), nil, time.Hour, false)
	if !wrote2 || f2 != nil {
		t.Fatalf("completed with id and no usage must succeed; wrote=%v f=%v", wrote2, f2)
	}
	// A present usage detail object missing its required field fails Codex's parse
	// (input_tokens_details requires cached_tokens); the proxy must not record success.
	emptyDetails := "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5,\"total_tokens\":15,\"input_tokens_details\":{}}}}\n\n"
	_, wrote3, f3 := captureResp(t, strings.NewReader(emptyDetails), nil, time.Hour, false)
	if wrote3 || f3 == nil || f3.code != "truncated_stream" {
		t.Fatalf("empty input_tokens_details must convert to a retry; wrote=%v f=%v", wrote3, f3)
	}
	// The same completed WITH the detail fields present succeeds.
	full := "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5,\"total_tokens\":15,\"input_tokens_details\":{\"cached_tokens\":3},\"output_tokens_details\":{\"reasoning_tokens\":1}}}}\n\n"
	_, wrote4, f4 := captureResp(t, strings.NewReader(full), nil, time.Hour, false)
	if !wrote4 || f4 != nil {
		t.Fatalf("complete usage+details must succeed; wrote=%v f=%v", wrote4, f4)
	}
}

func TestResponsesIncompleteConvertsToRetry(t *testing.T) {
	// response.incomplete is a RETRYABLE stream error for Codex (explicit arm ->
	// ApiError::Stream), NOT a completion. The proxy converts it pre-commit rather than
	// recording success, and captures the reason for the log.
	stream := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"model\":\"m\"}}\n\n" +
		"event: response.incomplete\ndata: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"r\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n"
	_, wrote, f := captureResp(t, strings.NewReader(stream), nil, time.Hour, false)
	if wrote || f == nil {
		t.Fatalf("response.incomplete must convert, not succeed; wrote=%v f=%v", wrote, f)
	}
	if !f.transient || f.code != "responses_incomplete" {
		t.Fatalf("want transient responses_incomplete, got %+v", *f)
	}
	if !strings.Contains(f.message, "max_output_tokens") {
		t.Fatalf("incomplete reason should be captured, got %q", f.message)
	}
}

func TestResponsesPostCommitIncompleteDrops(t *testing.T) {
	// created + output (commit) + response.incomplete. Post-commit, Codex raises a
	// retryable stream error on the incomplete terminal, so the proxy must DROP
	// (truncate) rather than forward it as a clean completion; the committed prefix is
	// already delivered and Codex's native stream-retry re-issues. Complements the
	// pre-commit TestResponsesIncompleteConvertsToRetry.
	stream := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"m\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hi\"}\n\n" +
		"event: response.incomplete\ndata: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_1\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n"
	rec, wrote, f := captureResp(t, iotest1byte(stream), nil, time.Hour, true)
	if !wrote {
		t.Fatalf("expected commit before the incomplete terminal (wrote=true)")
	}
	if f == nil || !f.transient || f.code != "responses_incomplete" {
		t.Fatalf("post-commit incomplete must DROP as a retryable failure, got wrote=%v f=%v", wrote, f)
	}
	if rec.Code != 200 {
		t.Fatalf("committed stream must have sent 200 before dropping, got %d", rec.Code)
	}
	// The incomplete terminal must never reach the client as a clean completion.
	if strings.Contains(rec.Body.String(), "response.incomplete") || strings.Contains(rec.Body.String(), "max_output_tokens") {
		t.Fatalf("post-commit incomplete frame was forwarded to the client: %q", rec.Body.String())
	}
	// The committed prefix was still delivered.
	if !strings.Contains(rec.Body.String(), "response.output_text.delta") {
		t.Fatalf("committed prefix should have been streamed, got %q", rec.Body.String())
	}
}

func TestResponsesTopLevelContextErrorSurfaces(t *testing.T) {
	// context_length_exceeded on a TOP-LEVEL error event is NOT Codex's compaction
	// path (that lives only in the response.failed branch); Codex ignores a top-level
	// error and fails EOF-before-completion. So the proxy must not deliver it native —
	// it surfaces as a deterministic request-shape that stops the loop.
	stream := "event: error\ndata: {\"type\":\"error\",\"code\":\"context_length_exceeded\",\"message\":\"input exceeds the context window\"}\n\n"
	_, wrote, f := captureResp(t, strings.NewReader(stream), nil, time.Hour, true)
	if wrote || f == nil {
		t.Fatalf("top-level context error must convert pre-commit; wrote=%v f=%v", wrote, f)
	}
	if f.transient || f.code != "responses_request_shape" {
		t.Fatalf("must surface non-retryable request-shape, got %+v", *f)
	}
}

func TestResponsesLenientContentForwardedVerbatim(t *testing.T) {
	// Codex's Responses parser is lenient: a delta lacking its "delta" field parses
	// and yields nothing (Option<String> = None), not an error. Since the proxy
	// forwards bytes verbatim, Codex sees the same frame and applies the same lenient
	// parse — so the proxy must NOT second-guess it. A field-less delta followed by a
	// valid completed is delivered byte-for-byte and the turn completes.
	stream := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"model\":\"m\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"m\",\"output_index\":0}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\n"
	rec, wrote, f := captureResp(t, strings.NewReader(stream), nil, time.Hour, false)
	if !wrote || f != nil {
		t.Fatalf("lenient content must be delivered, not converted; wrote=%v f=%v", wrote, f)
	}
	if rec.Body.String() != stream {
		t.Fatalf("field-less delta must be forwarded byte-for-byte, got %q", rec.Body.String())
	}
}

func TestParseRetryAfterFromMessage(t *testing.T) {
	cases := []struct {
		msg  string
		want int
	}{
		{"Please try again in 11.054s", 12},
		{"Rate limit reached. Try again in 20s", 20},
		{"try again in 500ms.", 1},
		{"try again in 2m", 120},
		{"no delay advertised here", 0},
	}
	for _, c := range cases {
		if got := parseRetryAfterFromMessage(c.msg); got != c.want {
			t.Errorf("parseRetryAfterFromMessage(%q)=%d want %d", c.msg, got, c.want)
		}
	}
}

func TestClassifyResponsesRateLimitPreservesDelay(t *testing.T) {
	f := classifyResponsesError("rate_limit_error", "", "Please try again in 8.5s")
	if f.retryAfter != 9 {
		t.Fatalf("rate-limit must carry the advertised delay; want 9, got %d", f.retryAfter)
	}
}

func TestResponsesLiveMalformedFrameForwarded(t *testing.T) {
	// After an early commit, a frame with broken JSON is forwarded verbatim (Codex
	// yields it from its SSE reader, fails to deserialize, and skips it — exactly as
	// on a direct connection), and a following valid response.completed still
	// completes the turn. The proxy does not truncate a stream Codex would accept.
	stream := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"model\":\"m\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\" BROKEN\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\n"
	rec, wrote, f := captureResp(t, iotest1byte(stream), nil, time.Hour, true)
	if !wrote || f != nil {
		t.Fatalf("committed stream with a skippable frame must complete, not drop; wrote=%v f=%v", wrote, f)
	}
	if rec.Code != 200 {
		t.Fatalf("committed stream must have sent 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "response.completed") || !strings.Contains(rec.Body.String(), "BROKEN") {
		t.Fatalf("both the skippable frame and the terminal must be forwarded verbatim, got %q", rec.Body.String())
	}
}

func TestClassifyResponsesHTTPError(t *testing.T) {
	cases := []struct {
		name          string
		status        int
		body          string
		wantTransient bool
	}{
		{"400-unknown-parameter", 400, `{"error":{"type":"invalid_request_error","code":"unknown_parameter","message":"Unknown parameter: foo"}}`, false},
		{"400-model-not-found-code", 400, `{"error":{"code":"model_not_found","message":"the model does not exist"}}`, false},
		{"400-context-length-code", 400, `{"error":{"code":"context_length_exceeded","message":"input exceeds the context window"}}`, false},
		{"400-generic-rides-out", 400, `{"error":{"type":"api_error","message":"transient hiccup"}}`, true},
		{"429-rate-limited", 429, `{"error":{"code":"rate_limit_exceeded","message":"slow down"}}`, true},
		{"503-overloaded", 503, `{"error":{"message":"overloaded"}}`, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := classifyResponsesHTTPErrorBytes(mkResp(c.status, c.body), []byte(c.body))
			if f.transient != c.wantTransient {
				t.Errorf("transient=%v want %v (%+v)", f.transient, c.wantTransient, f)
			}
		})
	}
}

func TestClassifyResponsesHTTPRateLimitParsesMessageDelay(t *testing.T) {
	// An HTTP 429 whose backoff is only in the OpenAI message (no Retry-After
	// header) must still yield the advertised delay — parity with the in-band SSE
	// rate-limit path, so a hidden retry and a surfaced Retry-After wait the real
	// amount on BOTH paths rather than the proxy's short generic backoff.
	body := `{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"Rate limit reached. Please try again in 7.2s"}}`
	f := classifyResponsesHTTPErrorBytes(mkResp(429, body), []byte(body))
	if !f.transient {
		t.Fatalf("HTTP 429 must be transient, got %+v", f)
	}
	if f.retryAfter != 8 {
		t.Fatalf("want message-parsed retryAfter 8, got %d (%+v)", f.retryAfter, f)
	}
}

func TestClassifyResponsesHTTPHeaderRetryAfterWins(t *testing.T) {
	// When the upstream sends a Retry-After header, it is authoritative and the
	// message parse must not override it.
	body := `{"error":{"type":"rate_limit_error","message":"Please try again in 30s"}}`
	resp := mkResp(429, body)
	resp.Header.Set("Retry-After", "3")
	f := classifyResponsesHTTPErrorBytes(resp, []byte(body))
	if f.retryAfter != 3 {
		t.Fatalf("explicit Retry-After header must win; want 3, got %d", f.retryAfter)
	}
}

func TestE2EResponsesHTTPRequestShapeNotRetried(t *testing.T) {
	// Pre-SSE HTTP 400 with a deterministic OpenAI code must surface (not loop).
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		io.WriteString(w, `{"error":{"type":"invalid_request_error","code":"unknown_parameter","message":"Unknown parameter: foo"}}`)
	}))
	defer up.Close()
	setupForTest(up.URL)

	rec := doStreamResponses(`{"stream":true,"model":"gpt-5.6-sol"}`)
	if got := rec.Header().Get("X-Should-Retry"); got != "false" {
		t.Fatalf("deterministic HTTP request-shape must NOT be retried; want false, got %q (code %d)", got, rec.Code)
	}
}

func TestE2EResponsesNonRetryableSurfaces400(t *testing.T) {
	// A deterministic HTTP error with a NON-400 status (e.g. 422) must surface to
	// Codex as 400: Codex ignores x-should-retry and would retry any other non-2xx
	// (422 -> UnexpectedStatus, retryable) to its stream cap, looping a request that
	// can never succeed. 400 maps to Codex's terminal InvalidRequest.
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(422)
		io.WriteString(w, `{"error":{"type":"invalid_request_error","code":"unknown_parameter","message":"Unknown parameter: foo"}}`)
	}))
	defer up.Close()
	setupForTest(up.URL)

	rec := doStreamResponses(`{"stream":true,"model":"gpt-5.6-sol"}`)
	if rec.Header().Get("X-Should-Retry") != "false" {
		t.Fatalf("deterministic error must not be retried; got %q", rec.Header().Get("X-Should-Retry"))
	}
	if rec.Code != 400 {
		t.Fatalf("non-retryable Responses error must surface HTTP 400 (Codex terminal), got %d", rec.Code)
	}
}

func TestE2EResponsesProxyOnceNonRetryable422Surfaces400(t *testing.T) {
	// A NON-streaming Responses request goes through proxyOnce, not the transactional
	// engine. A deterministic 422 there must still normalize to 400 for Codex — the
	// same class as the streaming path, closed at the shared surfaceFor choke point.
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(422)
		io.WriteString(w, `{"error":{"type":"invalid_request_error","code":"unknown_parameter","message":"Unknown parameter: foo"}}`)
	}))
	defer up.Close()
	setupForTest(up.URL)

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"stream":false,"model":"gpt-5.6-sol"}`))
	rec := httptest.NewRecorder()
	handle(rec, req)
	if rec.Code != 400 {
		t.Fatalf("proxyOnce deterministic error must surface 400, got %d", rec.Code)
	}
	if rec.Header().Get("X-Should-Retry") != "false" {
		t.Fatalf("want x-should-retry false, got %q", rec.Header().Get("X-Should-Retry"))
	}
}

func TestSurfaceForResponsesNonRetryable(t *testing.T) {
	// Non-transient on the Responses path -> 400 regardless of the classified status;
	// transient still masks to 503 (which Codex retries).
	if s, _ := surfaceFor("/v1/responses", failure{transient: false, status: 502, atype: "api_error"}); s != 400 {
		t.Fatalf("non-transient Responses must surface 400, got %d", s)
	}
	if s, _ := surfaceFor("/v1/responses", failure{transient: true, status: 529}); s != 503 {
		t.Fatalf("transient Responses must still mask to 503, got %d", s)
	}
	// Anthropic path is unaffected: a non-transient keeps its real status.
	if s, _ := surfaceFor("/v1/messages", failure{transient: false, status: 400, atype: "invalid_request_error"}); s != 400 {
		t.Fatalf("anthropic non-transient keeps its status, got %d", s)
	}
}

func TestOpenAIKeepaliveIsSkippableEvent(t *testing.T) {
	// The Responses keepalive must be a real SSE event (so Codex's reader yields it
	// and resets the idle timer) carrying an unknown type (so Codex ignores it) —
	// NOT a comment (which Codex discards without resetting the timer).
	ka := string(newOpenAIWire().keepalive())
	if strings.HasPrefix(strings.TrimSpace(ka), ":") {
		t.Fatalf("keepalive must not be an SSE comment: %q", ka)
	}
	if responsesEventType(event{data: `{"type":"response.proxy_keepalive"}`}) != "response.proxy_keepalive" {
		t.Fatal("keepalive type helper mismatch")
	}
	// It must parse as JSON (Codex deserializes it) and not be a recognized event.
	if !strings.Contains(ka, "data: {") || !strings.Contains(ka, "proxy_keepalive") {
		t.Fatalf("keepalive frame malformed: %q", ka)
	}
	if isResponsesOutputEvent("response.proxy_keepalive") || isResponsesErrorFrame("response.proxy_keepalive") {
		t.Fatal("keepalive type must be neither output nor error")
	}
}

func TestResponsesKeepaliveEmitsEventNotComment(t *testing.T) {
	// With a gap between chunks past the keepalive window, the proxy commits and then
	// fires a keepalive during the silence. On the Responses wire that keepalive must
	// be a skippable EVENT (resets Codex's idle timer), never a comment.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	r := &gapReader{ctx: ctx, chunks: eventsOf(goodResponsesStream), gap: 60 * time.Millisecond}
	wrote, f := captureSSECore(ctx, cancel, rec, http.Header{}, r, nil, 25*time.Millisecond, false, true, newOpenAIWire(), time.Now().Add(25*time.Millisecond))
	if f != nil || !wrote {
		t.Fatalf("want committed live success, got wrote=%v f=%+v", wrote, f)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "response.proxy_keepalive") {
		t.Fatalf("expected a keepalive EVENT in live mode, got %q", body)
	}
	if strings.Contains(body, ": keepalive") {
		t.Fatalf("Responses wire must not emit a comment keepalive: %q", body)
	}
	if !strings.Contains(body, "response.completed") {
		t.Fatalf("stream must still complete: %q", body)
	}
}

func TestE2EResponsesHTTP503Retryable(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		io.WriteString(w, `{"error":{"message":"upstream unavailable"}}`)
	}))
	defer up.Close()
	setupForTest(up.URL)

	rec := doStreamResponses(`{"stream":true,"model":"gpt-5.6-sol"}`)
	if got := rec.Header().Get("X-Should-Retry"); got != "true" {
		t.Fatalf("HTTP 503 must be retryable; want true, got %q", got)
	}
}

// ─────────────────────────────────────────────── classification unit tests ──

func TestClassifyResponsesError(t *testing.T) {
	cases := []struct {
		name           string
		typ, code, msg string
		wantTransient  bool
		wantCode       string
	}{
		{"overload-by-code", "service_unavailable_error", "server_is_overloaded", "overloaded", true, "responses_overloaded"},
		{"overload-by-type", "overloaded_error", "", "", true, "responses_overloaded"},
		{"rate-limit", "rate_limit_error", "", "slow down", true, "responses_rate_limit"},
		{"rate-limit-code", "", "rate_limit_exceeded", "", true, "responses_rate_limit"},
		{"timeout-mislabeled-as-invalid-request", "invalid_request_error", "request_timeout", "stream error: stream disconnected before completion: stream closed before response.completed", true, "responses_timeout"},
		{"timeout-code-only", "", "request_timeout", "upstream timed out", true, "responses_timeout"},
		{"timeout-code-normalized", "invalid_request_error", " REQUEST_TIMEOUT ", "upstream timed out", true, "responses_timeout"},
		{"request-shape-type", "invalid_request_error", "", "bad tool schema", false, "responses_request_shape"},
		{"request-shape-msg", "api_error", "", "prompt is too long for context", false, "responses_request_shape"},
		{"request-shape-code-only", "", "context_length_exceeded", "input exceeds the context window", false, "responses_request_shape"},
		{"invalid-prompt-code", "", "invalid_prompt", "flagged", false, "responses_request_shape"},
		{"bio-policy-code", "", "bio_policy", "blocked", false, "responses_request_shape"},
		{"cyber-policy-code", "", "cyber_policy", "blocked", false, "responses_request_shape"},
		{"auth-rides-out", "authentication_error", "", "bad key", true, "responses_error"},
		{"unknown-rides-out", "server_error", "", "boom", true, "responses_error"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := classifyResponsesError(c.typ, c.code, c.msg)
			if f.transient != c.wantTransient {
				t.Errorf("transient=%v want %v (%+v)", f.transient, c.wantTransient, *f)
			}
			if f.code != c.wantCode {
				t.Errorf("code=%q want %q", f.code, c.wantCode)
			}
		})
	}
}

func TestResponsesEventType(t *testing.T) {
	// Codex dispatches on the JSON `type`, so it is authoritative when present —
	// even when a gateway's SSE event: name differs (e.g. a generic "message").
	if got := responsesEventType(event{name: "message", data: `{"type":"response.completed"}`}); got != "response.completed" {
		t.Errorf("json type must win over a differing event name: %q", got)
	}
	if got := responsesEventType(event{name: "", data: `{"type":"response.failed"}`}); got != "response.failed" {
		t.Errorf("json type used when name absent: %q", got)
	}
	// No JSON type -> "" (the SSE event: name is ignored, matching Codex, which
	// deserializes the data payload and skips a frame with no type).
	if got := responsesEventType(event{name: "response.failed", data: `{}`}); got != "" {
		t.Errorf("frame without a JSON type must be untyped, got %q", got)
	}
	if got := responsesEventType(event{name: "response.completed", data: `not json`}); got != "" {
		t.Errorf("unparseable data must be untyped, got %q", got)
	}
}

func TestResponsesGenericEventNamesStillComplete(t *testing.T) {
	// A gateway that labels every frame `event: message` but carries the real kind
	// in the JSON `type` (what Codex dispatches on) must still be read correctly:
	// response.completed is detected and the stream succeeds, instead of being
	// converted into a bogus truncated_stream. Guards the event-type precedence.
	generic := regexp.MustCompile(`(?m)^event: .*$`).ReplaceAllString(goodResponsesStream, "event: message")
	var st captureStats
	rec, wrote, f := captureResp(t, strings.NewReader(generic), &st, time.Hour, false)
	if f != nil || !wrote {
		t.Fatalf("generic-event-name stream must complete; wrote=%v f=%v", wrote, f)
	}
	if st.stop != "completed" {
		t.Fatalf("completion must be detected from the JSON type, got stop=%q", st.stop)
	}
	if rec.Body.String() != generic {
		t.Fatalf("buffered replay must be byte-for-byte identical")
	}
}

func TestErrorWriterForPath(t *testing.T) {
	rec := httptest.NewRecorder()
	errorWriterFor("/v1/responses")(rec, true, 503, "api_error", "steady-proxy: overloaded", 3, "responses_overloaded")
	if rec.Header().Get("X-Should-Retry") != "true" || rec.Header().Get("Retry-After") != "3" {
		t.Fatalf("retry headers missing: %v", rec.Header())
	}
	var body struct {
		Error struct{ Message, Type string } `json:"error"`
		Type  string                         `json:"type"` // Anthropic top-level wrapper — must be ABSENT for OpenAI
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("bad json: %v (%s)", err, rec.Body.String())
	}
	if body.Type == "error" {
		t.Fatalf("OpenAI error must not have the Anthropic top-level {type:error} wrapper: %s", rec.Body.String())
	}
	if body.Error.Message == "" || body.Error.Type != "api_error" {
		t.Fatalf("OpenAI error body malformed: %s", rec.Body.String())
	}
}

func TestKeepaliveWindow(t *testing.T) {
	// Every mode owns a distinct window. The values are deliberately all different so
	// the test proves each mode picks its OWN knob — in particular full-buffer Responses
	// uses responsesBufferMs, NOT keepaliveMs (decoupled from the Claude horizon).
	c := config{
		keepaliveMs:          600 * time.Second,
		wfKeepaliveMs:        10 * time.Second,
		responsesKeepaliveMs: 30 * time.Second,
		responsesBufferMs:    900 * time.Second,
	}
	// (isResp, earlyCommit, gated)
	if got := c.keepaliveWindow(true, true, false); got != c.responsesKeepaliveMs {
		t.Errorf("Responses early-commit = %v, want responsesKeepaliveMs (%v)", got, c.responsesKeepaliveMs)
	}
	if got := c.keepaliveWindow(true, false, false); got != c.responsesBufferMs {
		t.Errorf("Responses full-buffer = %v, want responsesBufferMs (%v) — must NOT inherit keepaliveMs (%v)", got, c.responsesBufferMs, c.keepaliveMs)
	}
	if got := c.keepaliveWindow(false, false, true); got != c.wfKeepaliveMs {
		t.Errorf("gated Messages = %v, want wfKeepaliveMs (%v)", got, c.wfKeepaliveMs)
	}
	if got := c.keepaliveWindow(false, false, false); got != c.keepaliveMs {
		t.Errorf("ordinary Messages = %v, want keepaliveMs (%v)", got, c.keepaliveMs)
	}
}

func TestPathHelpers(t *testing.T) {
	if !isResponsesPath("/v1/responses") || isResponsesPath("/v1/messages") {
		t.Fatal("isResponsesPath wrong")
	}
	if !isMessagesPath("/v1/messages") || isMessagesPath("/v1/messages/count_tokens") || isMessagesPath("/v1/responses") {
		t.Fatal("isMessagesPath wrong")
	}
	// Sub-resources match; look-alike paths at a non-boundary must NOT.
	if !isResponsesPath("/v1/responses/compact") || isResponsesPath("/v1/responses-legacy") || isResponsesPath("/v1/responsesfoo") {
		t.Fatal("isResponsesPath boundary matching wrong")
	}
	if !isMessagesPath("/v1/messages/batches") || isMessagesPath("/v1/messagesfoo") {
		t.Fatal("isMessagesPath boundary matching wrong")
	}
	if _, ok := wireFor("/v1/responses", false).(*openaiWire); !ok {
		t.Fatal("wireFor(/v1/responses) should be openaiWire")
	}
	if _, ok := wireFor("/v1/messages", false).(*anthropicWire); !ok {
		t.Fatal("wireFor(/v1/messages) should be anthropicWire")
	}
}

// ───────────────────────────────────────────────────────────── e2e (handle) ──

func doStreamResponses(body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handle(rec, req)
	return rec
}

func TestE2EResponsesStreamingSuccess(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodResponsesStream)
	}))
	defer up.Close()
	setupForTest(up.URL)

	rec := doStreamResponses(`{"stream":true,"model":"gpt-5.6-sol"}`)
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "response.completed") || !strings.Contains(rec.Body.String(), "Hi") {
		t.Fatalf("replayed body missing content: %q", rec.Body.String())
	}
}

func TestE2EResponsesAccessLogShowsReasoningEffortWhenSet(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodResponsesStream)
	}))
	defer up.Close()
	setupForTest(up.URL)

	logs := captureLogs(t, func() {
		rec := doStreamResponses(`{"stream":true,"model":"gpt-5.6-sol","reasoning":{"effort":"high"}}`)
		if rec.Code != 200 {
			t.Fatalf("want 200, got %d body=%s", rec.Code, rec.Body.String())
		}
	})
	if !strings.Contains(logs, "OK    gpt-5.6-sol/main  high  ") {
		t.Fatalf("access log missing reasoning effort:\n%s", logs)
	}

	logs = captureLogs(t, func() {
		rec := doStreamResponses(`{"stream":true,"model":"gpt-5.6-sol"}`)
		if rec.Code != 200 {
			t.Fatalf("want 200, got %d body=%s", rec.Code, rec.Body.String())
		}
	})
	if !strings.Contains(logs, "OK    gpt-5.6-sol/main  in=") {
		t.Fatalf("access log should omit unset reasoning effort:\n%s", logs)
	}
}

func TestE2EResponsesEarlyCommitDisabledBuffers(t *testing.T) {
	// PROXY_RESPONSES_EARLY_COMMIT=0: a healthy streaming Responses turn is buffered to
	// the terminal and replayed at once (mode=buffered), like /v1/messages, instead of
	// committing live. Verifies the config flag reaches the engine through handle().
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, goodResponsesStream)
	}))
	defer up.Close()
	setupForTest(up.URL)
	cfg.responsesEarlyCommit = false
	t.Cleanup(func() { cfg = loadConfig() }) // don't leak the override to later tests

	rec := doStreamResponses(`{"stream":true,"model":"gpt-5.6-sol"}`)
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if m := rec.Header().Get("X-Steady-Proxy-Mode"); m != "buffered" {
		t.Fatalf("early-commit disabled must buffer to terminal (mode=buffered), got %q", m)
	}
	if !strings.Contains(rec.Body.String(), "response.completed") {
		t.Fatalf("buffered replay missing terminal: %q", rec.Body.String())
	}
}

func TestE2EResponsesStartErrorBecomesRetryable(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"service_unavailable_error\",\"code\":\"server_is_overloaded\",\"message\":\"overloaded\"}}\n\n")
	}))
	defer up.Close()
	setupForTest(up.URL)

	rec := doStreamResponses(`{"stream":true,"model":"gpt-5.6-sol"}`)
	if rec.Code == 200 {
		t.Fatalf("in-band start error must not commit 200; body=%s", rec.Body.String())
	}
	if got := rec.Header().Get("X-Should-Retry"); got != "true" {
		t.Fatalf("want x-should-retry true on overload, got %q (code %d)", got, rec.Code)
	}
}

func TestE2EResponsesRequestShapeNotRetried(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"bad request\"}}}\n\n")
	}))
	defer up.Close()
	setupForTest(up.URL)

	rec := doStreamResponses(`{"stream":true,"model":"gpt-5.6-sol"}`)
	if got := rec.Header().Get("X-Should-Retry"); got != "false" {
		t.Fatalf("request-shape must NOT be retried; want false, got %q", got)
	}
}

func TestE2EResponsesRequestTimeoutRetries(t *testing.T) {
	// The gateway labeled a dropped stream as invalid_request_error despite its
	// request_timeout code. It must remain retryable on both HTTP and SSE paths.
	const errorJSON = `{"code":"request_timeout","message":"stream error: stream disconnected before completion: stream closed before response.completed","type":"invalid_request_error"}`
	const partial = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"discard this failed attempt\"}\n\n"
	for _, wire := range []struct {
		name   string
		status int
		body   string
	}{
		{"response.failed", 200, partial + "event: response.failed\ndata: {\"type\":\"response.failed\",\"sequence_number\":0,\"response\":{\"status\":\"failed\",\"error\":" + errorJSON + "}}\n\n"},
		{"error", 200, partial + "event: error\ndata: {\"type\":\"error\",\"error\":" + errorJSON + "}\n\n"},
		{"http", 400, `{"error":` + errorJSON + `}`},
	} {
		for _, outcome := range []struct {
			name         string
			localRetries int
			recover      bool
		}{
			{"local-recovery", 1, true},
			{"client-retry", 0, false},
			{"local-budget-exhausted", 1, false},
		} {
			t.Run(wire.name+"/"+outcome.name, func(t *testing.T) {
				const requestBody = `{"stream":true,"model":"gpt-5.6-sol"}`
				var attempts atomic.Int32
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					if err != nil || string(body) != requestBody {
						t.Errorf("retry must preserve request body: body=%q err=%v", body, err)
					}
					if attempts.Add(1) > 1 && outcome.recover {
						w.Header().Set("Content-Type", "text/event-stream")
						io.WriteString(w, goodResponsesStream)
						return
					}
					if wire.status == 200 {
						w.Header().Set("Content-Type", "text/event-stream")
					}
					w.WriteHeader(wire.status)
					io.WriteString(w, wire.body)
				}))
				defer up.Close()
				setupForTest(up.URL)
				t.Cleanup(func() { cfg = loadConfig() })
				cfg.responsesEarlyCommit = false
				cfg.responsesBufferMs = time.Hour
				cfg.txLocalRetries = outcome.localRetries
				cfg.localBackoffCap = time.Millisecond

				rec := doStreamResponses(requestBody)
				if got := int(attempts.Load()); got != outcome.localRetries+1 {
					t.Fatalf("upstream attempts=%d, want %d", got, outcome.localRetries+1)
				}
				if strings.Contains(rec.Body.String(), "discard this failed attempt") {
					t.Fatalf("failed attempt leaked to client: %q", rec.Body.String())
				}
				if outcome.recover {
					if rec.Code != 200 || rec.Body.String() != goodResponsesStream {
						t.Fatalf("want only the successful retry's stream: status=%d body=%q", rec.Code, rec.Body.String())
					}
					return
				}
				if rec.Code != 503 || rec.Header().Get("X-Should-Retry") != "true" || atoiSafe(rec.Header().Get("Retry-After")) < 1 {
					t.Fatalf("want retryable 503 with backoff: status=%d headers=%v body=%q", rec.Code, rec.Header(), rec.Body.String())
				}
				if !strings.Contains(rec.Body.String(), `"type":"api_error"`) || !strings.Contains(rec.Body.String(), "stream disconnected before completion") {
					t.Fatalf("retry response must normalize the type and preserve the cause: %q", rec.Body.String())
				}
			})
		}
	}
}
