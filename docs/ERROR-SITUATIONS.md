# Error situations the proxy must cover

This catalog is derived from **real Claude Code session transcripts** on this
machine (1916 sessions under `~/.claude/projects`). Each row lists how the error
surfaces, its root cause, whether a blind retry can fix it, and what
`cc-retry-proxy` does. The Go tests in `*_test.go` target every row.

Legend for "Proxy action":
- **convert** → stamp `x-should-retry: true` so Claude Code's SDK re-sends.
- **pass-retryable** → upstream is already a retryable status; forward + stamp.
- **pass-permanent** → forward unchanged with `x-should-retry: false` (surfaces).

## 1. Truncation / dropped stream  → convert (the primary target)

The connection or stream dies before a complete `message_stop`. Claude's SDK has
already accepted `200 OK`, so it fails while parsing the body — these are *not*
retried natively, which is the bug we fix.

| Real message | Where it breaks | Proxy action |
|---|---|---|
| `API Error: JSON Parse error: Unexpected EOF` | `JSON.parse` on a truncated SSE `data:`/body (Bun phrasing) | **convert** — capture hits EOF before `message_stop` → `502 truncated_stream` + retry |
| `API Error: Request was aborted.` | stream aborted mid-flight | **convert** (truncated_stream / transport) |
| `API Error: The socket connection was closed unexpectedly.` | socket closed mid-body | **convert** |
| `API Error: Connection closed mid-response. The response above may be incomplete.` | upstream closed after partial content | **convert** |
| `API Error: Connection closed while thinking, before producing a response.` | upstream closed during reasoning, no content yet | **convert** |
| `API Error: stream error: stream ID 1; INTERNAL_ERROR; received from peer` | HTTP/2 `RST_STREAM` from the gateway | **convert** (stream_read_error) |

Because the proxy is **transactional** (buffers + validates the whole stream
before committing `200`), Claude never receives the partial body — so it never
hits the parse error. The truncation becomes a clean pre-body retryable error.

## 2. Malformed (but complete) stream  → convert

The stream is structurally complete (reaches `message_stop`) but an event's
payload is invalid JSON — a gateway/shim serialization bug, not a cut.

| Real message | Cause | Proxy action |
|---|---|---|
| `API Error: JSON Parse error: Unexpected identifier` | a fully-framed `data:` line with broken JSON | **convert** — per-event `json.Valid` check fails → `502 malformed_sse` + retry |

This is the edge case plain truncation-detection misses; the proxy validates the
JSON of every data event (toggle: `PROXY_VALIDATE_JSON=0` to disable).

## 3. Transient server errors  → pass-retryable / convert

Already-retryable upstream conditions. The proxy forwards them and (re)stamps
`x-should-retry` so they keep retrying until the budget/breaker stops them.

| Real message | Status | Proxy action |
|---|---|---|
| `API Error: Repeated N Overloaded errors` / `The API is at capacity` | 529 | **pass-retryable** (`overloaded_error`) |
| `API Error: N Internal server error` | 500 | **pass-retryable** |
| `API Error: Server is temporarily limiting requests … Rate limited` | 429 | **pass-retryable**, preserve `Retry-After` |
| `API Error: Request rejected (N) · temporary capacity issue` | 5xx | **pass-retryable** |
| mid-stream `event: error` with `overloaded_error` / `rate_limit_error` | after 200 | **convert** (mapped to pre-stream 529/429) |

## 4. Permanent / client errors  → pass-permanent (never loop)

Deterministically broken requests; retrying the identical request cannot help.
The proxy sets `x-should-retry: false` (explicit — this also stops the SDK's
default 5xx retry behavior) and surfaces the real error.

| Real message | Class | Proxy action |
|---|---|---|
| `API Error: N Invalid API key` | 401 auth | **pass-permanent** |
| `… authentication_error` / `This organization has been disabled.` | 401/403 | **pass-permanent** |
| `API Error: Missing Tool Result Block` | malformed conversation | **pass-permanent** |
| `… duplicate tool_use ID in conversation history` | malformed conversation | **pass-permanent** |
| `… unexpected tool_use_id found in tool_result blocks` | malformed conversation | **pass-permanent** |
| `… N due to tool use concurrency issues. Run /rewind …` | conversation state | **pass-permanent** |
| `… Extra inputs are not permitted … context_management` / `input_examples` | schema mismatch (shim) | **pass-permanent** |
| `… Unexpected value(s) for the anthropic-beta header` | header/beta mismatch | **pass-permanent** |
| `… max_tokens must be greater than thinking.budget_tokens` | invalid request | **pass-permanent** |
| `… thinking blocks … cannot be modified` | invalid request | **pass-permanent** |
| `… image dimensions exceed max allowed size` | invalid request | **pass-permanent** |
| `… prompt is too long` / context length exceeded | too large | **pass-permanent** |
| `API Error: Usage credits required for 1M context` | billing | **pass-permanent** |
| `… violate our Usage Policy` | content policy | **pass-permanent** |

A `400/422/4xx` with **no** permanent signature and **no** transient signature
defaults to **pass-permanent** ("don't guess"). If the in-house shim emits
`x-gateway-retryable: true|false`, that authoritative header overrides all
heuristics.

## 5. Normal situations  → pass through untouched

| Situation | Proxy action |
|---|---|
| Streaming success (`message_start … message_stop`) | capture, validate, replay `200` |
| Non-streaming success (`/v1/messages/count_tokens`, `stream:false`) | single-shot forward |
| Non-`/v1/messages` routes | single-shot forward |

## 6. Long generation  → transactional window (default up to ~600 s)

A long GPT-5/Sonnet turn where the proxy, buffering transactionally, has sent the
client *no bytes yet*.

**Measured constraint:** Claude aborts a request after its response-header ceiling
elapses with no response bytes — **default 60 s** (`CLAUDE_CODE_CONNECT_TIMEOUT_MS`;
`API_FORCE_IDLE_TIMEOUT=0` does **not** affect this pre-headers wait). A purely
transactional hold longer than that ceiling makes the client abort (and
abort-loop). So the **client** ceiling, not the proxy, caps the transactional
window — the two must move together.

How the proxy handles it (**tuned default: ~600 s**):
- It stays fully transactional up to `PROXY_KEEPALIVE_MS` (default **600 000 ms**).
  **Both** client-side abort timers must exceed that window, because the proxy
  sends no response headers during the hold: set
  **`CLAUDE_CODE_CONNECT_TIMEOUT_MS=660000`** (TTFB ceiling, default ~60 s) **and
  `API_TIMEOUT_MS=720000`** (hard per-attempt timeout, default 600 000 — equal to
  the window is a boundary race). Keep `API_FORCE_IDLE_TIMEOUT=0` (ON arms the
  no-bytes idle watchdog, which would kill the hold). Any turn that finishes — *or
  fails* — within the window is still uncommitted, so a late truncation /
  `JSON Parse error` converts to a clean automatic retry. **This is the case worth
  calling out: a 350 s turn that dies at ~340 s is fully recovered.**
- Only if a turn *exceeds* the window does the proxy **commit** (`200` + the
  buffered prefix) and stream the rest **live** with `: keepalive` pings, so the
  client survives indefinitely. A drop after commit can't be cleanly converted —
  it falls back to Claude's native dropped-stream retry.
- A genuinely **silent** upstream gap beyond `PROXY_UPSTREAM_BYTE_IDLE_MS`
  (default **600 000 ms**) is a wedged upstream → abort + convert (uncommitted) or
  end the stream (committed).
- Want a different ceiling? Move `PROXY_KEEPALIVE_MS` and **both** client timers
  (`CLAUDE_CODE_CONNECT_TIMEOUT_MS`, `API_TIMEOUT_MS`) together, keeping the client
  values above the window. (`CLAUDE_API_TIMEOUT` is *not* a real Claude Code var —
  it is ignored.)

Tested by `TestLongGenerationSlowDrip` (idle-reset), `TestKeepaliveCommitThenLive`
(commit→live path), and the live `long-gen` (>300 s) scenario.
