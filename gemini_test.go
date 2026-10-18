package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

const goodGeminiStream = "data: {\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Hello\"}]}}],\"modelVersion\":\"gemini-test\"}\n\n" +
	"data: {\"candidates\":[{\"index\":0,\"finishReason\":\"STOP\"}]}\n\n" +
	"data: {\"usageMetadata\":{\"promptTokenCount\":100,\"cachedContentTokenCount\":25,\"candidatesTokenCount\":8,\"thoughtsTokenCount\":3,\"totalTokenCount\":111}}\n\n"

func geminiTestEvent(data string) event {
	return event{data: data, raw: []byte("data: " + data + "\n\n")}
}

func captureGemini(t *testing.T, body []byte, r io.Reader, st *captureStats, early bool) (*httptest.ResponseRecorder, bool, *failure) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	window := time.Hour
	wrote, f := captureSSECore(ctx, cancel, rec, http.Header{}, r, st, window, false, early, newGeminiWire(body), bufferDeadline(time.Now(), window))
	return rec, wrote, f
}

// Each entry is a separate transport read, so tests can exercise the buffered
// prefix/live boundary without timers. The final error is independent of any
// previously observed candidate finishReason.
type geminiTestReader struct {
	chunks []string
	err    error
}

func (r *geminiTestReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		return 0, io.EOF
	}
	n := copy(p, r.chunks[0])
	r.chunks[0] = r.chunks[0][n:]
	if r.chunks[0] == "" {
		r.chunks = r.chunks[1:]
	}
	return n, nil
}

// Readers may return bytes and EOF in the same call. A complete native response
// must replay on that EOF, but an unfinished SSE frame must still fail.
type geminiTestFinalReader struct {
	data []byte
}

func (r *geminiTestFinalReader) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, io.EOF
	}
	return n, nil
}

func TestGeminiCaptureBufferedWaitsForEOFAndTrailingUsage(t *testing.T) {
	for _, tc := range []struct {
		name string
		r    io.Reader
	}{
		{"single read", strings.NewReader(goodGeminiStream)},
		{"one byte reads", iotest1byte(goodGeminiStream)},
		{"data and EOF together", &geminiTestFinalReader{data: []byte(goodGeminiStream)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var st captureStats
			rec, wrote, f := captureGemini(t, nil, tc.r, &st, false)
			if !wrote || f != nil {
				t.Fatalf("complete stream: wrote=%v failure=%+v", wrote, f)
			}
			if rec.Body.String() != goodGeminiStream {
				t.Fatalf("response changed or trailing usage lost: %q", rec.Body.String())
			}
			if st.mode != "buffered" || rec.Header().Get("X-Steady-Proxy-Mode") != "buffered" {
				t.Fatalf("expected buffered replay: mode=%q headers=%v", st.mode, rec.Header())
			}
			if st.model != "gemini-test" || st.stop != "STOP" || st.inTok != 100 || st.inputTok != 75 || st.cacheReadTok != 25 || st.outTok != 11 {
				t.Fatalf("usage/model/stop mismatch: %+v", st)
			}
			if st.bytes != int64(len(goodGeminiStream)) {
				t.Fatalf("captured bytes=%d, want %d", st.bytes, len(goodGeminiStream))
			}
		})
	}
}

func TestGeminiControlFramesDoNotFailOrCompleteResponse(t *testing.T) {
	controls := ": keepalive\n\nid: 123\nretry: 1000\n\nevent: ping\n\ndata:\n\n"
	for _, early := range []bool{false, true} {
		t.Run(strconv.FormatBool(early), func(t *testing.T) {
			for _, complete := range []bool{false, true} {
				stream := controls
				if complete {
					stream += goodGeminiStream + controls
				}
				var st captureStats
				rec, wrote, f := captureGemini(t, nil, iotest1byte(stream), &st, early)
				if complete {
					if !wrote || f != nil || st.stop != "STOP" || st.outTok != 11 || !strings.Contains(rec.Body.String(), `"text":"Hello"`) {
						t.Fatalf("control frames broke a complete response: wrote=%v failure=%+v stats=%+v body=%q", wrote, f, st, rec.Body.String())
					}
				} else if wrote || f == nil || !f.transient || rec.Body.Len() != 0 {
					t.Fatalf("control frames completed an empty response: wrote=%v failure=%+v body=%q", wrote, f, rec.Body.String())
				}
			}
		})
	}
}

func TestGeminiWireCompletionTracksEveryRequestedCandidate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     string
		chunks   []string
		complete bool
	}{
		{"empty stream", "", nil, false},
		{"empty object", "", []string{`{}`}, false},
		{"no finish", "", []string{`{"candidates":[{"index":0,"content":{"parts":[{"text":"partial"}]}}]}`}, false},
		{"unspecified finish", "", []string{`{"candidates":[{"index":0,"finishReason":"FINISH_REASON_UNSPECIFIED"}]}`}, false},
		{"empty finish", "", []string{`{"candidates":[{"index":0,"finishReason":""}]}`}, false},
		{"default candidate count", "", []string{`{"candidates":[{"index":0,"finishReason":"STOP"}]}`}, true},
		{"omitted default index", `{}`, []string{`{"candidates":[{"finishReason":"STOP"}]}`}, true},
		{"explicit count one", `{"generationConfig":{"candidateCount":1}}`, []string{`{"candidates":[{"index":0,"finishReason":"STOP"}]}`}, true},
		{"two requested one finished", `{"generationConfig":{"candidateCount":2}}`, []string{`{"candidates":[{"index":0,"finishReason":"STOP"}]}`}, false},
		{"thinking enum encoding cannot mask missing candidate", `{"generationConfig":{"candidateCount":2,"thinkingConfig":{"thinkingLevel":2}}}`, []string{`{"candidates":[{"index":0,"finishReason":"STOP"}]}`}, false},
		{"only second finished", `{"generationConfig":{"candidateCount":2}}`, []string{`{"candidates":[{"index":1,"finishReason":"STOP"}]}`}, false},
		{"repeat finish cannot replace missing candidate", `{"generationConfig":{"candidateCount":2}}`, []string{`{"candidates":[{"index":0,"finishReason":"STOP"}]}`, `{"candidates":[{"index":0,"finishReason":"STOP"}]}`}, false},
		{"second remains unfinished", `{"generationConfig":{"candidateCount":2}}`, []string{`{"candidates":[{"index":0,"finishReason":"STOP"},{"index":1,"finishReason":"FINISH_REASON_UNSPECIFIED"}]}`}, false},
		{"two finish together", `{"generationConfig":{"candidateCount":2}}`, []string{`{"candidates":[{"index":0,"finishReason":"STOP"},{"index":1,"finishReason":"MAX_TOKENS"}]}`}, true},
		{"two finish out of order", `{"generationConfig":{"candidateCount":2}}`, []string{`{"candidates":[{"index":1,"finishReason":"SAFETY"}]}`, `{"candidates":[{"finishReason":"STOP"}]}`}, true},
		{"large request does not infer unseen candidates", `{"generationConfig":{"candidateCount":2147483647}}`, []string{`{"candidates":[{"index":0,"finishReason":"STOP"}]}`}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire := newGeminiWire([]byte(tc.body))
			for _, chunk := range tc.chunks {
				done, f := wire.buffered(geminiTestEvent(chunk), nil)
				if done || f != nil {
					t.Fatalf("event must stay buffered until EOF: done=%v failure=%+v", done, f)
				}
			}
			if got := wire.completeOnEOF(); got != tc.complete {
				t.Fatalf("completeOnEOF=%v, want %v", got, tc.complete)
			}
		})
	}
}

func TestGeminiProtoJSONCandidateCounts(t *testing.T) {
	useDefaultConfig(t)
	first := string(geminiTestEvent(`{"candidates":[{"index":0,"finishReason":"STOP"}]}`).raw)
	second := string(geminiTestEvent(`{"candidates":[{"index":1,"finishReason":"STOP"}]}`).raw)
	for _, body := range []string{
		`{"generation_config":{"candidate_count":2}}`,
		`{"generation_config":{"candidateCount":2}}`,
		`{"generationConfig":{"candidate_count":2}}`,
		`{"generationConfig":{"candidateCount":"2"}}`,
		`{"generationConfig":{"candidateCount":2.0}}`,
		`{"generationConfig":{"candidateCount":2e0}}`,
		`{"generationConfig":{"candidateCount":"2e0"}}`,
		`{"generationConfig":{"candidateCount":"2.0"}}`,
		`{"generationConfig":{"candidateCount":1,"candidate_count":2}}`,
		`{"generation_config":{"candidate_count":1},"generationConfig":{"candidateCount":2}}`,
	} {
		t.Run(body, func(t *testing.T) {
			_, wrote, f := captureGemini(t, []byte(body), strings.NewReader(first+second), nil, false)
			if !wrote || f != nil {
				t.Fatalf("valid two-candidate response rejected: wrote=%v failure=%+v", wrote, f)
			}
			rec, wrote, f := captureGemini(t, []byte(body), strings.NewReader(first), nil, false)
			if wrote || f == nil || !f.transient || rec.Body.Len() != 0 {
				t.Fatalf("missing second candidate accepted: wrote=%v failure=%+v body=%q", wrote, f, rec.Body.String())
			}
		})
	}
}

func TestGeminiNativeCandidateStopsAreDelivered(t *testing.T) {
	for _, reason := range []string{
		"STOP", "MAX_TOKENS", "SAFETY", "RECITATION", "LANGUAGE", "OTHER", "BLOCKLIST",
		"PROHIBITED_CONTENT", "SPII", "MALFORMED_FUNCTION_CALL", "UNEXPECTED_TOOL_CALL",
		"TOO_MANY_TOOL_CALLS", "IMAGE_SAFETY", "IMAGE_PROHIBITED_CONTENT", "IMAGE_OTHER",
		"NO_IMAGE", "CONTINUATION", "FUTURE_FINISH_REASON",
	} {
		t.Run(reason, func(t *testing.T) {
			data := `{"candidates":[{"index":0,"finishReason":` + strconv.Quote(reason) + `}]}`
			stream := string(geminiTestEvent(data).raw)
			var st captureStats
			rec, wrote, f := captureGemini(t, nil, strings.NewReader(stream), &st, false)
			if !wrote || f != nil || rec.Body.String() != stream || st.stop != reason {
				t.Fatalf("native stop was not preserved: wrote=%v failure=%+v stop=%q body=%q", wrote, f, st.stop, rec.Body.String())
			}
		})
	}
}

func TestGeminiPromptBlocksCompleteWithoutCandidates(t *testing.T) {
	for _, reason := range []string{"SAFETY", "OTHER", "BLOCKLIST", "PROHIBITED_CONTENT", "IMAGE_SAFETY", "FUTURE_BLOCK_REASON"} {
		t.Run(reason, func(t *testing.T) {
			stream := string(geminiTestEvent(`{"promptFeedback":{"blockReason":` + strconv.Quote(reason) + `},"usageMetadata":{"promptTokenCount":12}}`).raw)
			var st captureStats
			rec, wrote, f := captureGemini(t, []byte(`{"generationConfig":{"candidateCount":3}}`), strings.NewReader(stream), &st, false)
			if !wrote || f != nil || rec.Body.String() != stream || st.stop != reason || st.inTok != 12 {
				t.Fatalf("native prompt block was not preserved: wrote=%v failure=%+v stats=%+v body=%q", wrote, f, st, rec.Body.String())
			}
		})
	}
	for _, feedback := range []string{`{}`, `{"blockReason":""}`, `{"blockReason":"BLOCK_REASON_UNSPECIFIED"}`} {
		wire := newGeminiWire(nil)
		if done, f := wire.buffered(geminiTestEvent(`{"promptFeedback":`+feedback+`}`), nil); done || f != nil || wire.completeOnEOF() {
			t.Fatalf("non-blocking feedback must not complete: feedback=%s done=%v failure=%+v", feedback, done, f)
		}
	}
}

func TestGeminiPromptBlocksCannotHideCandidates(t *testing.T) {
	block := string(geminiTestEvent(`{"promptFeedback":{"blockReason":"SAFETY"}}`).raw)
	for _, finish := range []string{"", "STOP"} {
		candidate := string(geminiTestEvent(`{"candidates":[{"content":{"parts":[{"text":"candidate"}]},"finishReason":` + strconv.Quote(finish) + `}]}`).raw)
		for _, stream := range []string{block + candidate, candidate + block} {
			rec, wrote, f := captureGemini(t, nil, strings.NewReader(stream), nil, false)
			if wrote || f == nil || !f.transient || rec.Body.Len() != 0 {
				t.Fatalf("contradictory prompt block accepted: wrote=%v failure=%+v body=%q", wrote, f, rec.Body.String())
			}
		}
	}
}

func TestGeminiUsageSnapshotsDoNotDoubleCount(t *testing.T) {
	wire := newGeminiWire(nil)
	var st captureStats
	for _, data := range []string{
		`{"modelVersion":"gemini-first","usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":25,"candidatesTokenCount":4,"thoughtsTokenCount":3,"totalTokenCount":107}}`,
		`{"usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":25,"candidatesTokenCount":4,"thoughtsTokenCount":3,"totalTokenCount":107}}`,
	} {
		ev := geminiTestEvent(data)
		wire.scrape(ev, &st)
		if done, f := wire.buffered(ev, &st); done || f != nil {
			t.Fatalf("usage event: done=%v failure=%+v", done, f)
		}
		if st.inTok != 100 || st.inputTok != 75 || st.cacheReadTok != 25 || st.cacheCreationTok != 0 || st.outTok != 7 {
			t.Fatalf("repeated snapshot double-counted prompt/cache/output: %+v", st)
		}
	}
	for _, data := range []string{
		`{"modelVersion":"gemini-resolved","usageMetadata":{"candidatesTokenCount":6,"thoughtsTokenCount":5}}`,
		`{"usageMetadata":{"thoughtsTokenCount":7}}`,
	} {
		if _, f := wire.buffered(geminiTestEvent(data), &st); f != nil {
			t.Fatalf("trailing usage: %+v", f)
		}
	}
	if st.inTok != 100 || st.cacheReadTok != 25 || st.outTok != 13 || st.model != "gemini-resolved" {
		t.Fatalf("partial trailing usage must retain independent cumulative components: %+v", st)
	}
	if _, f := wire.buffered(geminiTestEvent(`{"usageMetadata":{"promptTokenCount":10,"cachedContentTokenCount":20}}`), &st); f != nil {
		t.Fatalf("cache clamp: %+v", f)
	}
	if st.inTok != 10 || st.inputTok != 0 || st.cacheReadTok != 10 || st.outTok != 13 {
		t.Fatalf("cache count must not exceed the prompt total: %+v", st)
	}
}

func TestGeminiMalformedEventsFailEvenWhenValidationDisabled(t *testing.T) {
	saved := cfg.validateJSON
	t.Cleanup(func() { cfg.validateJSON = saved })
	cases := map[string]string{
		"broken JSON":             `{"candidates":[`,
		"null":                    `null`,
		"array":                   `[]`,
		"string":                  `"not an object"`,
		"number":                  `12`,
		"boolean":                 `true`,
		"trailing JSON":           `{} {}`,
		"candidates object":       `{"candidates":{}}`,
		"candidate null":          `{"candidates":[null]}`,
		"candidate scalar":        `{"candidates":[12]}`,
		"candidate array":         `{"candidates":[[]]}`,
		"null index":              `{"candidates":[{"index":null,"finishReason":"STOP"}]}`,
		"negative index":          `{"candidates":[{"index":-1,"finishReason":"STOP"}]}`,
		"out of range index":      `{"candidates":[{"index":1,"finishReason":"STOP"}]}`,
		"fractional index":        `{"candidates":[{"index":0.5,"finishReason":"STOP"}]}`,
		"exponent index":          `{"candidates":[{"index":0e0,"finishReason":"STOP"}]}`,
		"string index":            `{"candidates":[{"index":"0","finishReason":"STOP"}]}`,
		"boolean index":           `{"candidates":[{"index":false,"finishReason":"STOP"}]}`,
		"object index":            `{"candidates":[{"index":{},"finishReason":"STOP"}]}`,
		"overflowing index":       `{"candidates":[{"index":999999999999999999999999,"finishReason":"STOP"}]}`,
		"duplicate index":         `{"candidates":[{"index":0,"finishReason":"STOP"},{"index":0,"finishReason":"STOP"}]}`,
		"duplicate omitted index": `{"candidates":[{"finishReason":"STOP"},{"finishReason":"STOP"}]}`,
		"invalid finish type":     `{"candidates":[{"finishReason":12}]}`,
		"content array":           `{"candidates":[{"content":[]}]}`,
		"parts object":            `{"candidates":[{"content":{"parts":{}}}]}`,
		"part null":               `{"candidates":[{"content":{"parts":[null]}}]}`,
		"part scalar":             `{"candidates":[{"content":{"parts":[1]}}]}`,
		"promptFeedback array":    `{"promptFeedback":[]}`,
		"usageMetadata array":     `{"usageMetadata":[]}`,
	}
	for _, validate := range []bool{false, true} {
		cfg.validateJSON = validate
		for name, data := range cases {
			t.Run(strconv.FormatBool(validate)+"/"+name, func(t *testing.T) {
				for _, live := range []bool{false, true} {
					wire := newGeminiWire(nil)
					if _, f := wire.buffered(geminiTestEvent(`{"candidates":[{"finishReason":"STOP"}]}`), nil); f != nil {
						t.Fatalf("initial finish: %+v", f)
					}
					ev := geminiTestEvent(data)
					var f *failure
					var done bool
					if live {
						var out [][]byte
						out, done, f = wire.live(ev, ev.raw, nil)
						if len(out) != 0 {
							t.Fatalf("malformed live data must not be forwarded: %q", out)
						}
					} else {
						done, f = wire.buffered(ev, nil)
					}
					if done || f == nil || !f.transient || f.code != "malformed_sse" || wire.completeOnEOF() {
						t.Fatalf("malformed event accepted: live=%v done=%v failure=%+v complete=%v", live, done, f, wire.completeOnEOF())
					}
				}
			})
		}
	}
}

func TestGeminiInBandErrorsFailBeforeAndAfterFinish(t *testing.T) {
	saved := cfg.validateJSON
	cfg.validateJSON = false
	t.Cleanup(func() { cfg.validateJSON = saved })
	for _, tc := range []struct {
		name      string
		data      string
		eventName string
		transient bool
	}{
		{"native unavailable", `{"error":{"code":503,"status":"UNAVAILABLE","message":"overloaded"}}`, "", true},
		{"native invalid argument", `{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"bad request"}}`, "", false},
		{"native not found", `{"error":{"code":404,"status":"NOT_FOUND","message":"model missing"}}`, "", false},
		{"named error", `{"error":{"code":503,"status":"UNAVAILABLE","message":"overloaded"}}`, "error", true},
		{"null error envelope", `{"error":null}`, "", true},
		{"missing named error envelope", `{}`, "error", true},
		{"empty named error", ``, "error", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, afterFinish := range []bool{false, true} {
				for _, live := range []bool{false, true} {
					wire := newGeminiWire(nil)
					if afterFinish {
						if _, f := wire.buffered(geminiTestEvent(`{"candidates":[{"index":0,"finishReason":"STOP"}]}`), nil); f != nil {
							t.Fatalf("initial finish: %+v", f)
						}
					}
					ev := geminiTestEvent(tc.data)
					ev.name = tc.eventName
					var done bool
					var f *failure
					if live {
						var out [][]byte
						out, done, f = wire.live(ev, ev.raw, nil)
						if len(out) != 0 {
							t.Fatalf("in-band error must not be forwarded: %q", out)
						}
					} else {
						done, f = wire.buffered(ev, nil)
					}
					if done || f == nil || f.transient != tc.transient || wire.completeOnEOF() {
						t.Fatalf("in-band error accepted or misclassified: live=%v afterFinish=%v done=%v failure=%+v complete=%v", live, afterFinish, done, f, wire.completeOnEOF())
					}
				}
			}
		})
	}
}

func TestGeminiCaptureRejectsTruncationAndNonCleanEOF(t *testing.T) {
	finish := string(geminiTestEvent(`{"candidates":[{"index":0,"finishReason":"STOP"}]}`).raw)
	for _, tc := range []struct {
		name   string
		body   string
		stream string
		err    error
	}{
		{"empty stream", "", "", nil},
		{"no finish", "", string(geminiTestEvent(`{"candidates":[{"index":0,"content":{"parts":[{"text":"unfinished"}]}}]}`).raw), nil},
		{"missing requested candidate", `{"generationConfig":{"candidateCount":2}}`, finish, nil},
		{"unfinished data frame", "", finish + "data: {\"usageMetadata\":{\"promptTokenCount\":9}}\n", nil},
		{"unfinished line", "", finish + "data: {\"usageMetadata\":", nil},
		{"unfinished frame with data and EOF together", "", finish + "data: {}", nil},
		{"unexpected EOF after finish", "", finish, io.ErrUnexpectedEOF},
		{"transport error after finish", "", finish, errors.New("connection reset")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var r io.Reader = &geminiTestReader{chunks: []string{tc.stream}, err: tc.err}
			if tc.name == "unfinished frame with data and EOF together" {
				r = &geminiTestFinalReader{data: []byte(tc.stream)}
			}
			rec, wrote, f := captureGemini(t, []byte(tc.body), r, nil, false)
			if wrote || f == nil || !f.transient || rec.Body.Len() != 0 {
				t.Fatalf("incomplete stream must remain uncommitted: wrote=%v failure=%+v body=%q", wrote, f, rec.Body.String())
			}
		})
	}
}

func TestGeminiCaptureLiveWaitsForCleanEOF(t *testing.T) {
	chunks := strings.SplitAfter(goodGeminiStream, "\n\n")
	chunks = chunks[:len(chunks)-1]
	var st captureStats
	rec, wrote, f := captureGemini(t, nil, &geminiTestReader{chunks: chunks}, &st, true)
	if !wrote || f != nil || st.mode != "live" || rec.Body.String() != goodGeminiStream || st.outTok != 11 {
		t.Fatalf("live response lost trailing usage: wrote=%v failure=%+v stats=%+v body=%q", wrote, f, st, rec.Body.String())
	}
}

func TestGeminiCaptureLiveRejectsErrorAfterFinish(t *testing.T) {
	prefix := string(geminiTestEvent(`{"candidates":[{"content":{"parts":[{"text":"prefix"}]},"finishReason":"STOP"}]}`).raw)
	for _, tc := range []struct {
		name string
		tail string
		err  error
	}{
		{"in-band error", string(geminiTestEvent(`{"error":{"code":503,"status":"UNAVAILABLE","message":"failed after finish"}}`).raw), nil},
		{"malformed event", "data: not-json\n\n", nil},
		{"trailing unfinished frame", "data: {}", nil},
		{"transport error", "", io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var st captureStats
			rec, wrote, f := captureGemini(t, nil, &geminiTestReader{chunks: []string{prefix, tc.tail}, err: tc.err}, &st, true)
			if !wrote || f == nil || !f.transient || st.mode != "live" || rec.Body.String() != prefix {
				t.Fatalf("post-commit failure must DROP without forwarding the bad tail: wrote=%v failure=%+v stats=%+v body=%q", wrote, f, st, rec.Body.String())
			}
		})
	}
}

func TestGeminiLongRetryInfoCannotFitRequestDeadline(t *testing.T) {
	useDefaultConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	f := classifyGeminiSSEError(`{"error":{"code":429,"details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"315576000000s"}]}}`)
	if f == nil || canWaitForLocalRetry(ctx, localRetryDelay(f.retryAfter, 0)) {
		t.Fatalf("native cooldown exceeded the request deadline but was accepted: %+v", f)
	}
}
