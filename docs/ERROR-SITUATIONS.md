# Error situations the proxy must cover

This catalog is derived from **real Claude Code session transcripts** on this
machine (1916 sessions under `~/.claude/projects`). Each row lists how the error
surfaces, its root cause, whether a blind retry can fix it, and what
`steady-proxy` does. The Go tests in `*_test.go` target every row.

**Design principle (blind stabilizer):** the upstream gateway owns retry
intelligence; this proxy only keeps the client alive. The policy is one rule —
*retry every failure (with a `Retry-After` backoff) except a deterministic
**request-shape** error that can never succeed as written.* So auth, billing,
policy, rate limits, capacity, and unknown `4xx` are all **retried**, not
surfaced (see the principle in `main.go`).

Legend for "Proxy action":
- **convert** → stamp `x-should-retry: true` (+ `Retry-After` backoff) so Claude
  Code's SDK re-sends.
- **surface** → forward with `x-should-retry: false` (request-shape only — it can
  never succeed, so we don't loop).

**Every retryable response is normalized to one generic shape — `503` +
`api_error`** (`surface()` in `classify.go`) — *never* its real identity like
`overloaded_error`/`529` or `rate_limit_error`/`429`. Claude Code handles those
specific shapes on dedicated paths that **ignore `x-should-retry`** and give up
after ~3 tries (e.g. `API Error: Repeated 529 Overloaded errors`, which never
increments `X-Stainless-Retry-Count`). Masking them as a plain `503` keeps every
retry inside the SDK loop the proxy drives. Claude Code 2.1.191 defaults to 10
retries and clamps `CLAUDE_CODE_MAX_RETRIES` to **15**; to amplify beyond that,
set `PROXY_TRANSACTIONAL_LOCAL_RETRIES=N`. The effective uncommitted
transactional upstream attempt ceiling is `(client retries + 1) * (N + 1)`.
The true cause is preserved in the access-log `code` field (e.g.
`sse_overloaded`), not the surfaced status.

When hidden transactional retries are enabled, the proxy waits before reissuing
the upstream request. If the upstream supplied `Retry-After`, that wait is
`Retry-After` plus the proxy's extra exponential delay, capped by
`PROXY_LOCAL_RETRY_EXTRA_BACKOFF_CAP_MS` (default 10 s).

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

For Codex's `/v1/responses`, the gateway can report a disconnect inside a
`response.failed` event instead of closing the connection. One observed error
carried `code: "request_timeout"`, `type: "invalid_request_error"`, and the message
`stream error: stream disconnected before completion: stream closed before response.completed`.
Known timeout, rate-limit, and overload codes take precedence over generic types
and message signatures: before commit, the
proxy retries locally or returns a retryable `503`; after commit, it drops the
stream for Codex's native retry. The same classification applies to HTTP errors.
Actual request-shape errors retain their existing non-retryable behavior.

Unfinished SSE lines/events are bounded before parsing so a malformed stream
cannot bypass the buffering cap. Disk-spooled buffers are released on every
exit, including EOF and live completion. Storage I/O errors retain their cause
and remain retryable; only an actual total-size overflow is `response_too_large`.
Responses completion frames with duplicate keys or invalid Unicode do not count
as successful terminals.

Non-streaming body-copy failures log `DROP` and abort the downstream HTTP
response. Error-body archiving observes byte-idle/request deadlines and records
`capture_error` when reading is interrupted.

## 2. Malformed (but complete) stream  → convert

The stream is structurally complete (reaches `message_stop`) but an event's
payload is invalid JSON — or a stream of valid events accumulates to invalid
tool/server-tool input JSON. These are gateway/shim serialization bugs, not cuts.

| Real message | Cause | Proxy action |
|---|---|---|
| `API Error: JSON Parse error: Unexpected identifier` | a fully-framed `data:` line with broken JSON | **convert** — per-event `json.Valid` check fails → `502 malformed_sse` + retry |
| `API Error: JSON Parse error: Unexpected EOF` | valid outer SSE events but incomplete accumulated tool/server-tool `input_json_delta` | **convert** — accumulated tool input validation fails before commit → `502 malformed_sse` + retry |

This is the edge case plain truncation-detection misses; the proxy validates the
JSON of every data event and each accumulated tool/server-tool input object (toggle:
`PROXY_VALIDATE_JSON=0` to disable validation and JSON-fragment normalization).
Downstream forwarding coalesces fragmented tool input JSON into one complete
delta (`PROXY_NORMALIZE_TOOL_JSON=0` disables only this normalization).

## 3. Transient server errors  → convert (normalized to a generic 503)

Already-retryable upstream conditions. The proxy stamps `x-should-retry` and
surfaces them all as a generic `503` + `api_error` (never their real status/type),
so they keep retrying inside Claude Code's SDK loop until the client's retry
budget is spent. The `Status seen by upstream` column is what we classify and log
via `code`; the client always sees `503`.

| Real message | Status seen by upstream | Proxy action |
|---|---|---|
| `API Error: Repeated N Overloaded errors` / `The API is at capacity` | 529 | **convert** → `503` (was surfacing `overloaded_error`/529, which tripped CC's give-up path after ~3) |
| `API Error: N Internal server error` | 500 | **convert** → `503` |
| `API Error: Server is temporarily limiting requests … Rate limited` | 429 | **convert** → `503`, preserve `Retry-After` |
| `API Error: Request rejected (N) · temporary capacity issue` | 5xx | **convert** → `503` |
| `steady-proxy: No available providers` and similar capacity 5xx | 5xx | **convert** → `503` |
| mid-stream `event: error` with `overloaded_error` / `rate_limit_error` | after 200 | **convert** → `503` (classified 529/429 for logs, masked on the wire) |

## 4. Request-shape errors  → surface (the ONLY things we don't retry)

The request itself can never succeed as written, so retrying the identical
request would loop forever. These are matched by `requestShapeSigs` in
`classify.go`; the proxy sets `x-should-retry: false` and surfaces the real error.

| Real message | Class | Proxy action |
|---|---|---|
| `API Error: Missing Tool Result Block` | malformed conversation | **surface** |
| `… duplicate tool_use ID in conversation history` | malformed conversation | **surface** |
| `… unexpected tool_use_id found in tool_result blocks` | malformed conversation | **surface** |
| `… N due to tool use concurrency issues. Run /rewind …` | conversation state | **surface** |
| `… Extra inputs are not permitted … context_management` / `input_examples` | schema mismatch (shim) | **surface** |
| `… Unexpected value(s) for the anthropic-beta header` | header/beta mismatch | **surface** |
| `… max_tokens must be greater than thinking.budget_tokens` | invalid request | **surface** |
| `… thinking blocks … cannot be modified` | invalid request | **surface** |
| `… image dimensions exceed max allowed size` | invalid request | **surface** |
| `… prompt is too long` / context length exceeded | too large | **surface** |

Any other `4xx` — including a bare `invalid_request_error` with no recognized
request-shape signature — **defaults to retry** (normalized to `502` +
`x-should-retry: true`), because "retry might work" and the upstream owns the
real verdict. An explicit `x-gateway-retryable: true|false` (or `x-should-retry`)
from the shim still overrides everything.

## 4b. Auth / billing / policy  → retry (was permanent, now ridden out)

These used to surface immediately. Under the blind-stabilizer policy they are
**retried** instead, because they are often *temporary* (a transient auth block,
a brief org disable, a rate/usage limit that resets). The trade-off: a genuinely
bad key or exhausted balance surfaces **late**, only after the retry budget is
spent.

| Real message | Class | Proxy action |
|---|---|---|
| `API Error: N Invalid API key` | 401 auth | **retry** |
| `… authentication_error` / `This organization has been disabled.` | 401/403 | **retry** |
| `API Error: Usage credits required for 1M context` | billing | **retry** |
| `… violate our Usage Policy` | content policy | **retry** |

If you'd rather these fail fast, add their signatures back to `requestShapeSigs`
in `classify.go`.

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
- It stays fully transactional up to `PROXY_KEEPALIVE_MS` (default **600 000 ms**),
  measured from client request start, including header waits and hidden retries.
  If headers or an error body are still pending at the end, it returns a retryable
  response. Backoff and retries cannot restart this deadline.
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
