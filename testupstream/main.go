// testupstream — a fault-injecting mock of the Anthropic Messages API, used by
// the live docker tests. It returns valid Anthropic SSE for normal requests and
// deterministically reproduces the failure modes in docs/TROUBLESHOOTING.md.
//
// The desired fault is read from the LAST user message text via a directive:
//
//	[[FAULT=<kind>[:<arg>]]]
//
// kinds:
//
//	(none)            normal success (streams "MOCK_OK")
//	truncate          200 then close before message_stop (every time)
//	malformed         200 with a fully-framed event whose data is broken JSON
//	http500           500 Internal server error
//	overloaded        529 overloaded_error
//	permanent         400 invalid_request ("Missing Tool Result Block")
//	eof-then-ok[:N]    truncate the first N attempts (default 2), then succeed
//	overload-then-ok[:N] 529 the first N attempts (default 2), then succeed
//	slow:S             stream a valid response slowly over S seconds
//
// Attempts are counted per directive string so "…-then-ok" can recover. GET
// /__stats returns the counters; POST /__reset clears them.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	mu       sync.Mutex
	attempts = map[string]int{}
	faultRe  = regexp.MustCompile(`FAULT=([a-zA-Z0-9-]+?)(?::([0-9a-zA-Z]+))?\]\]`)
)

func main() {
	addr := os.Getenv("MOCK_ADDR")
	if addr == "" {
		addr = ":9099"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/__stats", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		json.NewEncoder(w).Encode(attempts)
	})
	mux.HandleFunc("/__reset", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts = map[string]int{}
		mu.Unlock()
		w.Write([]byte("ok"))
	})
	// /__count?q=<substr> -> total attempts across keys containing substr.
	mux.HandleFunc("/__count", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		mu.Lock()
		defer mu.Unlock()
		sum := 0
		for k, v := range attempts {
			if strings.Contains(k, q) {
				sum += v
			}
		}
		fmt.Fprintf(w, "%d", sum)
	})
	mux.HandleFunc("/", handle)
	fmt.Fprintf(os.Stderr, "testupstream listening on %s\n", addr)
	http.ListenAndServe(addr, mux)
}

func handle(w http.ResponseWriter, r *http.Request) {
	body := make([]byte, 0)
	buf := make([]byte, 4096)
	for {
		n, err := r.Body.Read(buf)
		body = append(body, buf[:n]...)
		if err != nil {
			break
		}
	}
	text := lastUserText(body)
	kind, arg := parseFault(text)
	n := bump(text)
	fmt.Fprintf(os.Stderr, "HIT path=%s fault=%q arg=%q attempt=%d x-stainless-timeout=%q x-stainless-retry-count=%q\n",
		r.URL.Path, kind, arg, n, r.Header.Get("X-Stainless-Timeout"), r.Header.Get("X-Stainless-Retry-Count"))

	switch kind {
	case "truncate":
		truncated(w)
	case "malformed":
		malformed(w)
	case "http500":
		httpErr(w, 500, "api_error", "Internal server error")
	case "overloaded":
		httpErr(w, 529, "overloaded_error", "Overloaded")
	case "permanent":
		httpErr(w, 400, "invalid_request_error", "Missing Tool Result Block")
	case "eof-then-ok":
		if n <= argOr(arg, 2) {
			truncated(w)
		} else {
			success(w)
		}
	case "overload-then-ok":
		if n <= argOr(arg, 2) {
			httpErr(w, 529, "overloaded_error", "Overloaded")
		} else {
			success(w)
		}
	case "malformed-then-ok":
		if n <= argOr(arg, 2) {
			malformed(w)
		} else {
			success(w)
		}
	case "slow":
		slow(w, argOr(arg, 305))
	default:
		success(w)
	}
}

// -------------------------------------------------------------- responses ----

func sseHead(w http.ResponseWriter) (http.Flusher, bool) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(200)
	fl, ok := w.(http.Flusher)
	if ok {
		fl.Flush()
	}
	return fl, ok
}

func ev(w http.ResponseWriter, fl http.Flusher, name, data string) {
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, data)
	if fl != nil {
		fl.Flush()
	}
}

func msgStart(w http.ResponseWriter, fl http.Flusher) {
	ev(w, fl, "message_start", `{"type":"message_start","message":{"id":"msg_mock","type":"message","role":"assistant","model":"mock","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}`)
	ev(w, fl, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
}

func msgEnd(w http.ResponseWriter, fl http.Flusher) {
	ev(w, fl, "content_block_stop", `{"type":"content_block_stop","index":0}`)
	ev(w, fl, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":1}}`)
	ev(w, fl, "message_stop", `{"type":"message_stop"}`)
}

func success(w http.ResponseWriter) {
	fl, _ := sseHead(w)
	msgStart(w, fl)
	ev(w, fl, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"MOCK_OK"}}`)
	msgEnd(w, fl)
}

func truncated(w http.ResponseWriter) {
	fl, _ := sseHead(w)
	msgStart(w, fl)
	ev(w, fl, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"MOCK_PAR"}}`)
	// return without message_stop -> the connection closes mid-stream
}

func malformed(w http.ResponseWriter) {
	fl, _ := sseHead(w)
	msgStart(w, fl)
	ev(w, fl, "content_block_delta", "{this is not valid json")
	msgEnd(w, fl)
}

func slow(w http.ResponseWriter, secs int) {
	fl, ok := sseHead(w)
	if !ok {
		return
	}
	msgStart(w, fl)
	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	i := 0
	for time.Now().Before(deadline) {
		ev(w, fl, "content_block_delta", fmt.Sprintf(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"tick%d "}}`, i))
		i++
		time.Sleep(2 * time.Second)
	}
	ev(w, fl, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"MOCK_OK"}}`)
	msgEnd(w, fl)
}

func httpErr(w http.ResponseWriter, status int, atype, msg string) {
	if status == 429 || status == 529 {
		w.Header().Set("Retry-After", "1")
	}
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"type": "error", "error": map[string]string{"type": atype, "message": msg}})
}

// ----------------------------------------------------------------- helpers ---

func bump(key string) int {
	mu.Lock()
	defer mu.Unlock()
	attempts[key]++
	return attempts[key]
}

func argOr(s string, d int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return d
}

func parseFault(text string) (string, string) {
	m := faultRe.FindStringSubmatch(text)
	if m == nil {
		return "", ""
	}
	return m[1], m[2]
}

func lastUserText(body []byte) string {
	var req struct {
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal(body, &req) != nil {
		return string(body)
	}
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role != "user" {
			continue
		}
		c := req.Messages[i].Content
		var s string
		if json.Unmarshal(c, &s) == nil {
			return s
		}
		var blocks []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(c, &blocks) == nil {
			out := ""
			for _, b := range blocks {
				out += b.Text + " "
			}
			return out
		}
	}
	return string(body)
}
