# cc-retry-proxy

A tiny, transactional, self-healing reverse proxy that sits between **Claude Code**
and your **gateway**, so transient gateway failures never stop a turn — no tmux,
no terminal automation, and subagents are covered automatically.

```
Claude Code (+ subagents)  ──HTTP──▶  cc-retry-proxy (loopback)  ──HTTPS──▶  your gateway
```

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
> maximum steadiness. The effective retry ceiling is your **client's**
> `maxRetries` (e.g. `API_MAX_RETRIES=30`); `PROXY_SDK_RETRY_CAP` is only a
> backstop.

## What it does

- For `POST /v1/messages` it runs in **transactional mode**: it buffers and
  validates the *entire* Anthropic SSE stream and only writes `200 OK` +
  replays it once a complete, valid `message_stop` is captured.
- Any failure **before** that commit point — connection error, 5xx, a stalled or
  truncated stream, a mid-stream `error` event, any retryable status — is
  converted into a *retryable* response by stamping **`x-should-retry: true`**
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
- The **retry budget** is driven by the SDK's own `X-Stainless-Retry-Count`; the
  client's `maxRetries` is the real ceiling and `PROXY_SDK_RETRY_CAP` is a
  backstop (`0` disables conversion entirely). There is **no circuit breaker** —
  a blind stabilizer never blocks the client; the `Retry-After` backoff is what
  prevents a tight hammer loop.
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
go build -o cc-retry-proxy .
go test -race ./...          # unit + integration tests
./test/live.sh               # live: real `claude -p` -> proxy -> mock + real gateway
```

Run the binary directly instead of compose:

```bash
PROXY_UPSTREAM_URL=https://your-gateway.example.com PROXY_LISTEN_ADDR=127.0.0.1:8789 ./cc-retry-proxy
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

## Verify — reading the log

The proxy prints **one line per request** (always on; `PROXY_VERBOSE=1` only adds
extra internal retry chatter). Tail it with `docker compose logs -f proxy`:

```text
2026/06/21 16:34:29  OK    claude-sonnet-4-6/main  in=1.2k out=437 tok  end_turn  buffered  3.41s
2026/06/21 16:34:30  OK    claude-haiku-4-5/sub    in=812 out=96 tok  end_turn  buffered  1.02s
   (timestamp prefix elided on the lines below for readability)
RETRY claude-sonnet-4-6/main  truncated_stream 502->503  retry-after=2s  0.9s
RETRY claude-haiku-4-5/main   sse_overloaded 529->503  retry-after=4s  0.2s  attempt=1
FAIL  claude-sonnet-4-6/main  request_shape 400  0.3s
DROP  claude-sonnet-4-6/main  truncated_stream -> committed, Claude retries natively  out=210 tok  61.0s
OK    /v1/messages/count_tokens  200  730B  2ms
```

Every line is prefixed by the logger with the date and time at second resolution
(`2026/06/21 16:34:29`).

Reading a line:
- **First column** = outcome — `OK` served · `RETRY` converted to an automatic
  retry (`x-should-retry: true` + `Retry-After` backoff, the SDK re-sends) ·
  `FAIL` surfaced to you (request-shape error, or retry backstop hit) · `DROP`
  failed *after* committing a long turn, so Claude's native dropped-stream retry
  takes over.
- **`model/agent`** — the model called, and whether the caller is the `main` agent
  or a spawned `sub`agent.
- Then only what varies: **`in=/out=` tokens** (`in` includes cache read/create
  input tokens), **stop reason**, **`buffered`/`live`** capture mode, and
  **duration**. Buffered GPT-compatible streams also normalize final input/cache
  usage into the replayed `message_start` event when the upstream sent zero
  placeholders there, leaving `message_delta` to carry output usage. Failures add
  the **`code`** and **`status`**, plus
  **`retry-after=Ns`** on a RETRY and **`attempt=N`** after a retry. The **`code`** is
  the true cause (`sse_overloaded`, `truncated_stream`, …). The **`status`** is the
  real upstream status; when it was masked it reads **`orig->surfaced`**
  (e.g. `529->503`) — left of the arrow is what the upstream returned, right is what
  the client receives (every transient cause is masked to a generic `503`; see
  [normalization](#what-it-does)). A single number means it was not masked (a `503`
  that was already `503`, or a surfaced `FAIL` like `request_shape 400`).

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
| `PROXY_SDK_RETRY_CAP` | `100` | backstop only — stop converting once the SDK reports this many retries (`0` disables conversion). Your client's `maxRetries` is the real ceiling; raise that, not this |
| `PROXY_KEEPALIVE_MS` | `600000` | stay fully transactional up to this long, then commit + stream live; the client's `CLAUDE_CODE_CONNECT_TIMEOUT_MS` **must exceed it** (set `660000`); `0` = pure transactional |
| `PROXY_UPSTREAM_BYTE_IDLE_MS` | `600000` | abort + retry a silent/wedged upstream after this gap |
| `PROXY_VALIDATE_JSON` | `1` | per-event JSON validation (catches malformed `data:` events); `0` to disable |
| `PROXY_RESP_HEADER_TIMEOUT_MS` | `60000` | wait for the upstream status line |
| `PROXY_MAX_BUFFER_MEM_BYTES` | `1048576` | buffer in RAM up to this, then spill to an unlinked temp file |
| `PROXY_MAX_RESPONSE_BYTES` | `134217728` | hard cap on a single buffered response |
| `PROXY_DEADLINE_MARGIN_MS` | `25000` | finish before the client's own timeout |
| `PROXY_SPOOL_DIR` | `$TMPDIR` | where large responses spill (use tmpfs for sensitive prompts) |
| `PROXY_REQUEST_LOG_DIR` | off (`""`) | set a directory to save the full request + response of each call to a file there (see below) |
| `PROXY_REQUEST_LOG_MAX_BYTES` | `10485760` | per-section cap (request body, response body) written to each saved file; the rest is truncated with a marker |
| `PROXY_VERBOSE` | off | set `1` for per-decision logs |

## Saving request/response data

The one-line access log tells you *what happened*; sometimes you need to see
*exactly what was sent and returned* — to debug a converted retry, a malformed
stream, or a surfaced request-shape 4xx. Set `PROXY_REQUEST_LOG_DIR` to a directory and the
proxy writes **one human-readable file per request** into it:

```
PROXY_REQUEST_LOG_DIR=./logs PROXY_UPSTREAM_URL=… ./cc-retry-proxy
# ./logs/v1-messages-20260621t143005-000001.log
```

Each file has a `=== REQUEST ===` section (method, path, headers, body) and a
`=== RESPONSE ===` section (outcome, token/stop/mode stats, headers, and the
captured SSE events — or the body for non-streaming routes). Notes:

- **Secrets are redacted.** `Authorization`, `x-api-key`, `cookie`, and similar
  headers are written as `***redacted (…last4)***`, never in full.
- **Bodies are capped** at `PROXY_REQUEST_LOG_MAX_BYTES` (default 10 MiB) each, so a
  huge stream can't exhaust RAM or disk; truncation is marked inline.
- **Prompts are written verbatim.** The request body (your conversation) lands on
  disk unencrypted — point the dir at a tmpfs or a path you control, and keep it out
  of version control (`.gitignore` already excludes `/logs/`).
- For failures *before* the stream starts (e.g. a pre-stream HTTP error), the file
  records the request and the outcome/code; the upstream error body itself is not
  separately captured.

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
