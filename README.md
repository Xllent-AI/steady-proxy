# cc-retry-proxy

A tiny, transactional, self-healing reverse proxy that sits between **Claude Code**
or **Codex** and your **gateway**, so transient gateway failures never stop a turn
— no tmux, no terminal automation, and subagents are covered automatically.

```
Claude Code / Codex (+ subagents)  ──HTTP──▶  cc-retry-proxy (loopback)  ──HTTPS──▶  your gateway
```

It speaks both wire formats: the **Anthropic Messages** API (`POST /v1/messages`,
Claude Code) and the **OpenAI Responses** API (`POST /v1/responses`, Codex with
`wire_api = "responses"`). The transactional engine is shared; only the
stream interpretation (terminal event, in-band error shape, usage) differs per
wire — see `wireModel` in `wire.go`.

## Design principle: a blind stabilizer

This proxy is **not** a smart retry brain — your gateway already owns retry
intelligence (routing, provider selection, backoff). The proxy's only job is to
keep the client alive through outages, so the whole policy is one rule:

> **Buffer the full response. On *any* failure, tell the client to wait and retry.
> Only give up on a request that can never succeed as written.**

"Can never succeed" = a deterministic **request-shape** error: context too long,
malformed tool blocks, schema/validation, model-not-found. *Everything else is
retried on purpose* — network outages, `5xx`, rate limits, capacity ("no
available providers"), auth blocks, billing, and unknown `4xx`. A temporary block
is ridden out, not surfaced.

> Trade-off: a genuinely bad API key / exhausted billing now surfaces **late**
> (after the retry budget) instead of failing fast. That's the accepted cost of
> maximum steadiness. Claude Code 2.1.191 defaults to 10 retries and clamps
> `CLAUDE_CODE_MAX_RETRIES` to **15**; `PROXY_SDK_RETRY_CAP` is only a backstop.
> To amplify retry budget beyond the client clamp, opt in with
> `PROXY_TRANSACTIONAL_LOCAL_RETRIES`.

## What it does

- For `POST /v1/messages` it runs in **transactional mode**: it buffers and
  validates the *entire* Anthropic SSE stream and only writes `200 OK` +
  replays it once a complete, valid `message_stop` is captured. Downstream
  forwarding also coalesces tool/server-tool `input_json_delta` fragments into
  one complete JSON delta, avoiding client-side partial-JSON EOF failures.
- For `POST /v1/responses` (Codex) the same engine buffers the OpenAI Responses
  SSE stream, whose terminal is `response.completed`. A start-of-stream `error`
  or `response.failed` (overload / capacity / rate limit — which Codex otherwise
  treats as a **fatal turn error** and does not retry) is caught **pre-commit**
  and converted to a retry (or ridden out by hidden local retries). By default,
  once output appears it commits early and streams live, so streaming UX is
  preserved; a post-commit error truncates the stream so Codex's native
  stream-retry re-issues. Set `PROXY_RESPONSES_EARLY_COMMIT=0` to instead buffer
  the whole response like the Messages path (under the Responses-owned
  `PROXY_RESPONSES_BUFFER_MS` hold) — mid-stream errors then also become hidden
  retries, at the cost of requiring the client's `stream_idle_timeout_ms` to
  exceed that hold.
  Live bytes and non-text frames are forwarded verbatim. Buffered/prefix replay
  combines only adjacent compatible text deltas (bounded by
  `PROXY_RESPONSES_REPLAY_DELTA_BYTES`) so a completed buffered turn cannot burst
  thousands of tiny notifications into Codex. Codex's Responses parser is
  deliberately lenient (it tolerates missing/null fields and skips any frame it
  can't deserialize without failing the turn), so every other content frame is
  left alone; the proxy's guarantees are at the stream level (a parseable
  terminal, or a convert-to-retry). During a silent gap the keepalive is a **skippable Responses
  event** (not an SSE comment, which Codex's reader discards without resetting its
  idle timer). No tool-JSON coalescing; refusal-fallback and the Workflow stall
  watchdog are Anthropic-only and stay off. A **non-retryable** error (a
  deterministic request-shape) surfaces as **HTTP 400** — the one 4xx Codex treats
  as terminal; every other non-2xx it would retry regardless of `x-should-retry`.
- Any failure **before** that commit point — connection error, 5xx, a stalled or
  truncated stream, a mid-stream `error` event, any retryable status — is either
  retried inside the proxy when `PROXY_TRANSACTIONAL_LOCAL_RETRIES` is enabled,
  or converted into a *retryable* response by stamping **`x-should-retry: true`**
  plus a **`Retry-After` backoff** (exponential, capped ~30 s; the SDK waits then
  re-sends using Claude Code's own retry loop).
- Every retryable response is **normalized to one generic shape — `503` +
  `api_error`** — never its real identity like `overloaded_error`/`529` or
  `rate_limit_error`/`429`. Claude Code handles those specific shapes on dedicated
  paths that **ignore `x-should-retry`** and give up after ~3 tries (e.g.
  `Repeated 529 Overloaded errors`, which never increments the retry counter);
  masking them as a plain `503` keeps every retry inside the SDK loop above. The
  true cause is preserved in the access-log `code` (e.g. `sse_overloaded`).
- **Request-shape** errors pass through with **`x-should-retry: false`** so they
  surface instead of looping forever.
- The **client retry budget** is driven by the SDK's own
  `X-Stainless-Retry-Count`; `PROXY_SDK_RETRY_CAP` is a backstop (`0` disables
  conversion entirely). For extra budget under Claude Code's 15-retry clamp, set
  `PROXY_TRANSACTIONAL_LOCAL_RETRIES=N`: effective transactional upstream
  attempts are `(client retries + 1) * (N + 1)`, as long as the proxy has not
  committed bytes to the client. Hidden proxy retries wait for upstream
  `Retry-After` **plus** an extra exponential delay capped by
  `PROXY_LOCAL_RETRY_EXTRA_BACKOFF_CAP_MS` (default 10 s). There is **no circuit
  breaker** — a blind stabilizer never blocks the client; backoff prevents a
  tight hammer loop.
- **Long generations (tuned for turns up to ~600 s):** Claude aborts any request
  that reaches its response-header ceiling with no bytes — **default 60 s**
  (`CLAUDE_CODE_CONNECT_TIMEOUT_MS`, measured; `API_FORCE_IDLE_TIMEOUT=0` does
  *not* affect this pre-headers wait). To keep the stream **fully transactional**
  (so a late failure is still cleanly retryable) for long turns, this proxy
  defaults `PROXY_KEEPALIVE_MS` to **600 000** and you raise the **two client-side
  abort timers** above it — both matter because the proxy sends *no response
  headers* during the hold: **`CLAUDE_CODE_CONNECT_TIMEOUT_MS=660000`** (the TTFB /
  no-response-headers ceiling, default ~60 s) **and `API_TIMEOUT_MS=720000`** (the
  hard per-attempt request timeout, default 600 000 — left at 600 000 it *ties* the
  window and can abort at the boundary). Keep `API_FORCE_IDLE_TIMEOUT=0` (turning it
  on arms a no-bytes idle watchdog that would kill the transactional hold; note
  `CLAUDE_API_TIMEOUT` is not a real Claude Code var and is ignored). Now a turn
  that runs for minutes and fails near the end — e.g. a truncated stream /
  `JSON Parse error` — is still uncommitted, so it converts to an automatic retry.
  Only if a turn *exceeds* the window does the
  proxy **commit** the buffered prefix and switch to **live streaming with
  keepalive pings** (a post-commit drop then falls back to Claude's native
  dropped-stream retry). Want a different ceiling? Move all three together —
  `PROXY_KEEPALIVE_MS` and the two client timers — keeping the client values above
  the window.

The full situation catalog — derived from real session transcripts — and how each
is handled is in [docs/ERROR-SITUATIONS.md](docs/ERROR-SITUATIONS.md).

Trade-off of transactional mode: you lose live token-by-token streaming for turns
that complete within the grace window — the reply appears in a burst, then
completes. In exchange you get "complete reply or automatic retry, never a stuck
half-reply."

## Workflow agents

Claude Code's **Workflow** tool (`agent()` calls in an orchestration script) wraps
each agent in a **per-agent stall watchdog**: if the agent's stream produces no
assistant/user message for the stall budget — **default 180 s**, retried a few
times, then the agent hard-fails with *"agent stalled … no progress"* — it aborts
the request. That budget is **hardcoded** (there is no global env override;
`CLAUDE_ASYNC_AGENT_STALL_TIMEOUT_MS` drives a *different* path, and the only
per-agent override is the script-side `agent(prompt, {stallMs})`).

Full transactional buffering starves that watchdog: the proxy withholds every
byte until the upstream stream completes, so a single workflow turn that runs
longer than ~180 s emits nothing → the orchestrator sees no progress → it aborts
at 180 s (the proxy logs `client_gone`), retries, and the whole agent fails. This
hits **only workflow agents** — the interactive session and ordinary subagents
have no such watchdog.

The proxy fixes this transparently: it detects a workflow agent by the prologue
the Workflow runtime injects into its `system` prompt (*"You are a subagent
spawned by a workflow orchestration script"*) and gives **only those requests** a
**small** transactional window, `PROXY_WORKFLOW_KEEPALIVE_MS` (default 10 000). A
workflow turn that runs past the window commits its buffered prefix and then
streams the rest live — real events the watchdog counts as progress — so it clears
the *first-gap* stall (see below), while turns that finish inside the window stay
fully transactional (cleanly retryable). The detection is scoped to the `system`
field, so a *main* session that merely discusses workflows is never misclassified.
These requests show as `/wf` in the access log. Everything else keeps
`PROXY_KEEPALIVE_MS`.

**Why small, precisely.** The watchdog kills on a *forwarded-content-delta gap*:
it aborts when `stallMs` passes with no real delta reaching the client (keepalive
comments and `ping` do not count) — i.e. at `(last forwarded delta) + stallMs`.
While the proxy buffers it forwards nothing, so the **first gap** — query start to
the first forwarded delta — is roughly `window + upstream TTFB`. A small window
keeps that first gap under `stallMs`; that first-gap stall is the one this proxy
prevents. A large window (e.g. 120 000) is not *automatically* fatal — if the
upstream then streams steadily the turn can still survive — but it pushes the
first gap toward `stallMs` and delays going live, so small is the safe default.
**What the proxy cannot fix:** once past the first gap the stream is governed by
the upstream's own content-delta gaps, which the proxy cannot change — a genuine
mid-turn content-silent pause ≥ `stallMs` (server-side reasoning emitting only
pings, or a slow first byte under `effort:'high'`) will still stall. The only
remedy there is a larger per-agent `stallMs`.

> The robustness cost is small and targeted: pre-stream transient failures (5xx,
> `overloaded_error`, rate limits, capacity, connection errors) are classified
> *before* any bytes are captured, so they still convert to clean retries for
> workflow agents too. Only a mid-stream truncation on a turn already past the
> window degrades from a clean retry to a `DROP` (Claude's native dropped-stream
> retry). If a workflow sets a per-agent `stallMs` **below**
> `PROXY_WORKFLOW_KEEPALIVE_MS`, lower the window to match.

## Run (docker compose — recommended)

```bash
cp .env.example .env                  # set PROXY_UPSTREAM_URL to your real gateway
docker compose up -d --build          # proxy on 127.0.0.1:8789 -> your gateway
# or override inline instead of using .env:
PROXY_UPSTREAM_URL=https://your-gateway.example.com docker compose up -d --build
```

Then point Claude Code at it (next section). Logs: `docker compose logs -f proxy`.

## Build / test from source

```bash
cd cc-retry-proxy
make build                  # stamps VERSION + git commit + build date
./cc-retry-proxy --version
go test -race ./...          # unit + integration tests
./test/live.sh               # live: real `claude -p` -> proxy -> mock + real gateway
```

Run the binary directly instead of compose:

```bash
PROXY_UPSTREAM_URL=https://your-gateway.example.com PROXY_LISTEN_ADDR=127.0.0.1:8789 ./cc-retry-proxy
```

`docker compose up -d --build` stamps the binary with `VERSION`, the current Git
commit (from minimal `.git` metadata copied into the build context), and a build
timestamp. For fully explicit release builds, use:

```bash
make docker-build
```

To query a running proxy without reading process state:

```bash
curl -s http://127.0.0.1:8789/__version
```

Background / persistent (systemd user unit, survives logout):

```ini
# ~/.config/systemd/user/cc-retry-proxy.service
[Unit]
Description=cc-retry-proxy for Claude Code
After=network-online.target

[Service]
ExecStart=/opt/cc-retry-proxy/cc-retry-proxy
Environment=PROXY_UPSTREAM_URL=https://your-gateway.example.com
Environment=PROXY_KEEPALIVE_MS=600000
Environment=PROXY_LISTEN_ADDR=127.0.0.1:8789
Restart=always
RestartSec=2

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload && systemctl --user enable --now cc-retry-proxy
```

## Wire Claude Code to it

In `~/.claude/settings.json`, point the base URL at the proxy and let the proxy
hold the real upstream (the proxy forwards your `Authorization`/`anthropic-*`
headers unchanged):

```jsonc
"env": {
  "ANTHROPIC_BASE_URL": "http://127.0.0.1:8789",   // was: your real gateway URL
  "ANTHROPIC_AUTH_TOKEN": "…unchanged…",
  // Every client-side abort timer must EXCEED PROXY_KEEPALIVE_MS (600000): the
  // proxy sends no response headers during the transactional hold, so a timer
  // <= the window aborts the turn mid-hold.
  "CLAUDE_CODE_CONNECT_TIMEOUT_MS": "660000", // TTFB / "no response headers" ceiling (default ~60s)
  "API_TIMEOUT_MS": "720000",                 // hard per-attempt request timeout (default 600000)
  "API_FORCE_IDLE_TIMEOUT": "0"               // keep OFF; ON arms a no-bytes idle watchdog that kills the hold
  // (CLAUDE_API_TIMEOUT is NOT read by Claude Code — don't bother setting it.)
}
// and run the proxy with PROXY_UPSTREAM_URL set to your real gateway (via .env)
```

If Claude Code refuses a plain `http://` base URL, serve the proxy over TLS with a
local cert and set `NODE_EXTRA_CA_CERTS` — but loopback `http` is normally fine.

## Wire Codex to it

Point the Codex provider's `base_url` at the proxy (keep `/v1`) in
`~/.codex/config.toml`; the proxy holds the real upstream and forwards auth
unchanged:

```toml
[model_providers.crs]
base_url = "http://127.0.0.1:8789/v1"   # was: your real gateway .../v1
wire_api = "responses"
# Keep this strictly ABOVE the active proxy commit window: PROXY_RESPONSES_KEEPALIVE_MS
# (30s) in the default early-commit mode, or PROXY_RESPONSES_BUFFER_MS (600s) if you
# set PROXY_RESPONSES_EARLY_COMMIT=0. 660000 clears both with margin:
stream_idle_timeout_ms = 660000
# Codex's own retries still apply as a backstop when the proxy surfaces a retryable 503:
request_max_retries = 10
stream_max_retries = 10
```

Enable `PROXY_TRANSACTIONAL_LOCAL_RETRIES=N` so a start-of-stream overload is
ridden out inside the proxy and Codex never sees it. To extend the proxy's
hidden-retry protection past the first output event (mid-stream failures too), set
`PROXY_RESPONSES_EARLY_COMMIT=0` — the turn then buffers under
`PROXY_RESPONSES_BUFFER_MS` like the Messages path.

## Verify — reading the log

The proxy prints **one line per request** (always on; `PROXY_VERBOSE=1` only adds
extra internal retry chatter). Tail it with `docker compose logs -f proxy`:

```text
2026/06/21 16:34:00  cc-retry-proxy 0.1.0+a1b2c3d4e5f6 listening on http://0.0.0.0:8789 -> https://your-gateway.example.com  (transactional, keepalive=10m0s, wf-keepalive=10s, sdkRetryCap=100, txLocalRetries=6, refusalFallback=claude-opus-4-8; one log line per request)
2026/06/21 16:34:29  OK    claude-sonnet-4-6/main  high  in=1.2k out=437 tok  end_turn  buffered  3.41s
2026/06/21 16:34:30  OK    claude-haiku-4-5/sub    in=812 out=96 tok  end_turn  buffered  1.02s
2026/06/21 16:34:31  OK    gpt-5.6-sol/main  high  in=1.1k out=223 tok  completed  live  2.37s
   (timestamp prefix elided on the lines below for readability)
RETRY claude-sonnet-4-6/main  truncated_stream 502->503  retry-after=2s  0.9s
RETRY claude-haiku-4-5/main   sse_overloaded 529->503  retry-after=4s  0.2s  attempt=1
[local-retry] claude-opus-4-8/main http_503 503 wait=1s retry=1/6
OK    claude-sonnet-4-6/main  in=1.2k out=437 tok  end_turn  buffered  4.6s  proxy-retries=1
WARN  claude-fable-5/main  refusal -> retry with claude-opus-4-8  in=258.8k out=2.8k tok  40.7s
FAIL  claude-sonnet-4-6/main  request_shape 400  0.3s
DROP  claude-sonnet-4-6/main  truncated_stream -> committed, Claude retries natively  out=210 tok  61.0s
OK    /v1/messages/count_tokens  200  730B  2ms
```

Every line is prefixed by the logger with the date and time at second resolution
(`2026/06/21 16:34:29`).

Reading a line:
- **First column** = outcome — `OK` served · `RETRY` converted to an automatic
  retry (`x-should-retry: true` + `Retry-After` backoff, the SDK re-sends) ·
  `WARN` a `stop_reason: "refusal"` was intercepted and the request re-issued with
  `PROXY_REFUSAL_FALLBACK_MODEL` (`refusal -> retry with <model>`; a following `OK`
  line reports the fallback's result) · `FAIL` surfaced to you (request-shape
  error, or retry backstop hit) · `DROP` failed *after* committing a long turn, so
  Claude's native dropped-stream retry takes over.
- **`model/agent`** — the model called, and whether the caller is the `main` agent
  or a spawned `sub`agent. If the upstream stream echoes a different resolved
  model, success/drop lines show `requested->resolved/agent`.
- **Reasoning effort** — requests add the bare effort (for example **`high`**)
  as the next field when set via Messages `output_config.effort` or Responses
  `reasoning.effort`.
- Then only what varies: **`in=/out=` tokens** (`in` includes cache read/create
  input tokens), **stop reason**, **`buffered`/`live`** capture mode, and
  **duration**. Buffered GPT-compatible streams also normalize final input/cache
  usage into the replayed `message_start` event when the upstream sent zero
  placeholders there, leaving `message_delta` to carry output usage. Failures add
  the **`code`** and **`status`**, plus
  **`retry-after=Ns`** on a RETRY, **`attempt=N`** after a Claude Code retry, and
  **`proxy-retries=N`** when the proxy recovered or exhausted hidden local
  transactional retries. The **`code`** is the true cause (`sse_overloaded`,
  `truncated_stream`, …). The **`status`** is the real upstream status; when it
  was masked it reads **`orig->surfaced`**
  (e.g. `529->503`) — left of the arrow is what the upstream returned, right is what
  the client receives (every transient cause is masked to a generic `503`; see
  [normalization](#what-it-does)). A single number means it was not masked (a `503`
  that was already `503`, or a surfaced `FAIL` like `request_shape 400`).
- **`[local-retry]`** lines appear only with `PROXY_VERBOSE=1`. They are the
  proxy's hidden in-request retries and include the same `model/agent`, the true
  cause/status, wait time, hidden retry index, and SDK `attempt=N` when present.

Responses also carry headers: `X-CC-Retry-Proxy-Mode` (`buffered`/`live`) on
success, `X-CC-Retry-Proxy-Reason` on a synthesized error.

Live smoke test (sends one real request through the proxy to your gateway):

```bash
curl -sS http://127.0.0.1:8789/v1/messages \
  -H "authorization: Bearer $ANTHROPIC_AUTH_TOKEN" \
  -H "anthropic-version: 2023-06-01" -H "content-type: application/json" \
  -d '{"model":"<your-model>","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"say hi"}]}'
```

## Config (env)

| Var | Default | Meaning |
|---|---|---|
| `PROXY_LISTEN_ADDR` | `127.0.0.1:8789` | loopback bind (never expose publicly) |
| `PROXY_UPSTREAM_URL` | `https://your-gateway.example.com` | the real gateway (set via `.env`) |
| `PROXY_SDK_RETRY_CAP` | `100` | backstop only — stop converting once the SDK reports this many retries (`0` disables conversion). **Claude/Stainless-specific:** the count comes from the `X-Stainless-Retry-Count` header the Anthropic SDK sends. **Codex does not send it**, so on `/v1/responses` the count is always 0 and any positive cap is effectively unlimited — conversion is bounded instead by Codex's own `request_max_retries`/`stream_max_retries`. Set `0` to disable proxy conversion entirely for a Codex-only deployment |
| `PROXY_TRANSACTIONAL_LOCAL_RETRIES` | `0` | opt-in hidden retries per uncommitted transactional attempt (`/v1/messages` and `/v1/responses`). `1` means one extra upstream try before returning a retryable response to the client. For Codex this is the ideal path — an overload is ridden out and Codex never sees an error |
| `PROXY_LOCAL_RETRY_EXTRA_BACKOFF_CAP_MS` | `10000` | cap for the proxy's extra exponential wait between hidden local retries. If upstream sends `Retry-After`, the proxy waits `Retry-After + extra` |
| `PROXY_KEEPALIVE_MS` | `600000` | stay fully transactional up to this long, then commit + stream live; the client's `CLAUDE_CODE_CONNECT_TIMEOUT_MS` **must exceed it** (set `660000`); `0` = pure transactional |
| `PROXY_WORKFLOW_KEEPALIVE_MS` | `10000` | **workflow agents only** — a **small** window so the proxy commits *early* and streams live, feeding Claude Code's Workflow per-agent **stall** watchdog (default ~180 s, no global env override; kills on a forwarded-delta gap ≥ `stallMs`). Keep it small so the first gap (≈ window + TTFB) stays under `stallMs`; it can't fix a mid-turn content-silent pause ≥ `stallMs`. `0` disables (stall returns). Other callers keep `PROXY_KEEPALIVE_MS`. See [Workflow agents](#workflow-agents) |
| `PROXY_RESPONSES_KEEPALIVE_MS` | `30000` | **`/v1/responses` (Codex), early-commit mode only** — the stream commits *early* as soon as output appears, so this just bounds a **silent start** (reasoning with no output) before committing and streaming keepalive events. (With early-commit **off** the route buffers under `PROXY_RESPONSES_BUFFER_MS` instead.) Keep it below Codex's `stream_idle_timeout_ms` so the commit — after which the proxy emits Codex-visible keepalive events that reset Codex's idle timer — happens before Codex would idle out. Start-of-stream errors arrive before any output, so they're caught pre-commit regardless of this value |
| `PROXY_RESPONSES_BUFFER_MS` | `600000` | **`/v1/responses` (Codex), full-buffer mode only** (`PROXY_RESPONSES_EARLY_COMMIT=0`) — the max buffering hold before a safety-valve live commit: the `/v1/messages` policy applied to Responses. **Responses-owned** (independent of `PROXY_KEEPALIVE_MS`), so a proxy serving both Codex and Claude tunes the two transactional horizons separately. The Codex client's `stream_idle_timeout_ms` **must strictly exceed** it (nothing is forwarded while buffering) |
| `PROXY_RESPONSES_EARLY_COMMIT` | `1` | **`/v1/responses` (Codex) only** — `1` (default) commits as soon as the first output event is buffered, then streams live (streaming UX preserved). Set `0` to **buffer the whole response** to `response.completed` like `/v1/messages`, under the `PROXY_RESPONSES_BUFFER_MS` hold: this extends the proxy's hidden-retry protection to **mid-stream** errors (not just start-of-stream), but the proxy forwards nothing until the turn ends, so the Codex client's `stream_idle_timeout_ms` **must exceed that hold** |
| `PROXY_RESPONSES_REPLAY_DELTA_BYTES` | `65536` | **`/v1/responses` buffered/prefix replay only** — combine adjacent `response.output_text.delta` events for the same item/output/content route up to this many accumulated source-JSON bytes before replay. Counting the complete payload (including logprobs) keeps replay memory bounded as well as preventing full-buffer mode from dumping thousands of token-sized events into Codex's bounded app-server notification queue at completion. Text, routing fields, ordering barriers, and terminal/error events are preserved; normal live deltas are untouched. Events with non-empty logprobs and oversized, malformed, or ambiguous frames stay raw; `0` restores byte-for-byte replay |
| `PROXY_UPSTREAM_BYTE_IDLE_MS` | `600000` | abort + retry a silent/wedged upstream after this gap |
| `PROXY_VALIDATE_JSON` | `1` | per-event JSON plus accumulated tool/server-tool input JSON validation; `0` to disable validation and JSON-fragment normalization |
| `PROXY_NORMALIZE_TOOL_JSON` | `1` | coalesce tool/server-tool `input_json_delta` fragments into one complete JSON delta before downstream forwarding when JSON validation is enabled; `0` for byte-like upstream forwarding |
| `PROXY_REFUSAL_FALLBACK_MODEL` | `claude-opus-4-8` | when a request completes with `stop_reason: "refusal"` or Fable returns a pre-stream safeguards block, silently re-issue the same request with this model instead of returning the refusal (logs a `WARN`). Fires at most once per request (a refusal from the fallback model is delivered as-is) and only before anything is committed downstream; every non-model field is preserved. Set to `off`/`none`/empty to disable. Stream refusal detection still works with `PROXY_VALIDATE_JSON=0`; full JSON validation is still recommended |
| `PROXY_RESP_HEADER_TIMEOUT_MS` | `60000` | wait for the upstream status line |
| `PROXY_MAX_BUFFER_MEM_BYTES` | `1048576` | buffer in RAM up to this, then spill to an unlinked temp file |
| `PROXY_MAX_RESPONSE_BYTES` | `134217728` | hard cap on a single buffered response |
| `PROXY_DEADLINE_MARGIN_MS` | `25000` | finish before the client's own timeout |
| `PROXY_SPOOL_DIR` | `$TMPDIR` | where large responses spill (use tmpfs for sensitive prompts) |
| `PROXY_REQUEST_LOG_DIR` | off (`""`) | set a directory to save each request/response JSON archive there (see below) |
| `PROXY_VERBOSE` | off | set `1` for per-decision logs |

## Saving request/response data

The one-line access log tells you *what happened*; sometimes you need to see
*exactly what was sent and returned* — to debug a converted retry, a malformed
stream, or a surfaced request-shape 4xx. Set `PROXY_REQUEST_LOG_DIR` to a directory and the
proxy writes **one JSON archive per client request** into it:

```
PROXY_REQUEST_LOG_DIR=./logs PROXY_UPSTREAM_URL=… ./cc-retry-proxy
# ./logs/v1-messages-20260621t143005-a1b2c3d4.json   (a1b2c3d4 = the correlation id)
```

Each archive uses the versioned schema `cc-retry-proxy.payload.v2`. Bodies are
stored as JSON `data_base64` fields with `encoding`, byte `size`, and `sha256`, so
request and response payload bytes can be restored exactly, including binary or
image payloads. The archive records the original client request envelope, the
upstream URL with API secrets redacted, proxy build/version metadata, all upstream
attempts, each attempt's response or transport failure, retry wait/result, SSE
stats, and any model-swap decision.

- **Correlation id.** Every access-log line carries `id=<hex>`, and that same id is
  the archive filename suffix and top-level `id` — so a bad line pins straight to
  the payload: `ls logs/*<id>*.json`. The id only appears in the access log when
  `PROXY_REQUEST_LOG_DIR` is set (otherwise there's no archive to point at).
- **`prun` (ping-run).** The stats line reports the longest run of consecutive
  upstream `ping` events — a content-silent-gap tripwire. Healthy dense streams stay
  at `0`; a climbing `prun` on a `DROP` is the signature of a genuine mid-turn
  upstream pause (what a Workflow stall watchdog kills on).
- **API secrets are redacted.** API-secret carriers such as `Authorization`,
  `x-api-key`, `api-key`, and query params such as `api_key`, `key`,
  `api_token`, `auth_token`, `access_token`/`accessToken`, and
  `client_secret`/`clientSecret` are written redacted and listed in `redactions`.
  Non-forwarded `Proxy-Authorization` is also redacted. Other headers and query
  params are preserved for debugging/restoration.
- **Bodies are not truncated.** Full request and response bodies are archived for
  restoration. This can use significant disk for large requests or long streams.
- **Bodies are preserved verbatim.** The request body (your conversation) and
  response bodies land on disk unencrypted and are not inspected/redacted; point
  the dir at a tmpfs or a path you control, and keep it out of version control
  (`.gitignore` already excludes `/logs/`).
- For failures before a stream starts, the archive records the upstream HTTP
  status, response headers, and full upstream error body. Hidden local retries and
  model-fallback reissues appear as separate entries in `attempts`.

## Caveats

- **Server-side / remote MCP tools:** re-issuing a request is safe for
  *client-local* tool execution (Claude runs tools only after a complete reply),
  but it is **not** provider-level idempotency. If you enable server-side or
  remote side-effecting tools without their own idempotency keys, set
  `PROXY_SDK_RETRY_CAP=0` to disable conversion, or don't proxy those.
- **Live streaming is lost** for turns that finish within the grace window (by
  design). Turns longer than `PROXY_KEEPALIVE_MS` commit early and stream live but
  then can't cleanly convert a late drop. To keep live streaming *and* full
  robustness for long turns, add resumable responses in your in-house shim
  (OpenAI/Azure `background:true` + `starting_after=<sequence>`).
- Bind to loopback only; this is an unauthenticated local proxy.
