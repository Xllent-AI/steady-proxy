# Architecture

`steady-proxy` sits between an API client and an upstream gateway. Your gateway still handles model routing and provider selection; the proxy buffers streaming responses so transient upstream failures can be caught and retried before the client commits to a turn, and relays every final upstream answer unchanged.

## Client compatibility

The proxy selects a protocol interpreter based on the HTTP method, path, and request parameters:

- **Anthropic Messages:** `POST /v1/messages` with `"stream": true`
- **OpenAI Responses:** `POST /v1/responses` with `"stream": true`
- **Gemini GenerateContent:** `POST /{version}/models/{model}:streamGenerateContent?alt=sse` under `/v1`, `/v1beta`, `/v1alpha`, or `/v1beta1` (including Vertex AI `/projects/{project}/locations/{location}/publishers/google/models/{model}` paths)

All three streaming routes share one buffering and retry engine (`wireModel` in `wire.go`) and do not require a specific `User-Agent`. Other routes (`/v1/chat/completions`, `generateContent`, `countTokens`, `embedContent`, non-SSE `streamGenerateContent`, `/v1/models`, and `stream: false` requests) are forwarded as single-shot requests. A terminal upstream error is relayed with its original body and headers on every route. Errors the proxy synthesizes (retryable `503`s, proxy-side faults) use the route's native envelope: Google's `error.code` / `error.status` on Gemini model methods (`models/{model}:{method}`, including Vertex AI and Vertex express `publishers/google/models` paths), OpenAI's error object on `/v1/responses`, and Anthropic's `type: "error"` envelope on `/v1/messages` and all other routes.

| Requirement | Client expectation |
| --- | --- |
| **Transport & paths** | Standard HTTP + SSE. The proxy does not translate between protocols or support WebSocket streaming. |
| **Timeouts** | Header, idle, and per-attempt timeouts must exceed the active buffering window (and live keepalive interval); see [Timeouts](CONFIGURATION.md#timeouts). |
| **HTTP retries** | Retry HTTP `503` honoring `Retry-After`, and stop on `X-Should-Retry: false`. Any other error status is the upstream's own final answer. In-proxy retries (`PROXY_TRANSACTIONAL_LOCAL_RETRIES`) can also absorb pre-commit failures transparently. |
| **Post-commit recovery** | Handle interrupted streams after `200 OK` headers are sent. Once committed, the proxy cannot restart the stream on the client's behalf. |
| **Responses events** | Ignore unknown SSE event types (`response.proxy_keepalive`), which the proxy emits because SSE comments do not reset Codex's idle timer. |
| **Gemini events** | Accept standard SSE comment keepalives (`: keepalive`). Tool calls, thought signatures, and unknown JSON fields are preserved unchanged. |
| **Retry counter** | `PROXY_SDK_RETRY_CAP` reads `X-Stainless-Retry-Count`. Clients that do not send that header (such as Codex and Gemini SDKs) must enforce their own retry limit. |

## Buffering and the point of no return

Supported streaming requests start in **buffered mode**: the proxy reads upstream SSE frames into memory (spilling to disk beyond 1 MiB) without writing HTTP headers to the client.

- If the stream completes within the buffering window, the proxy sends `200 OK` and replays the validated buffer.
- If the stream fails retryably before headers are sent, the proxy discards the buffer and either retries upstream locally or returns a retryable `503` to the client. A terminal failure returns the upstream's error instead.
- A streaming request answered with a complete non-SSE `2xx` body is relayed unchanged (a broken body is retried like a truncated stream).

Writing `200 OK` response headers is the **commit point**. Once headers are sent, the proxy can no longer convert a failure into an HTTP error or replay the attempt. A terminal in-band error frame from the upstream (for example a permanent `event: error` or `response.failed`) is forwarded and ends the stream, logged `FAIL … delivered in-stream`. Any other failure aborts the downstream connection without a clean end, so no client mistakes a cut-off stream for a complete answer, and logs `DROP`.

| Request type | Default commit policy | Completion criterion |
| --- | --- | --- |
| **Anthropic Messages** | Buffer up to 240s (`PROXY_KEEPALIVE_MS`), then switch to live streaming. | Valid SSE stream ending in `message_stop`. |
| **Claude Code Workflow agent** | Buffer up to 10s (`PROXY_WORKFLOW_KEEPALIVE_MS`), then switch to live streaming once forwardable content arrives. | Valid SSE stream ending in `message_stop`. |
| **OpenAI Responses (default)** | Commit and stream live as soon as the first output event arrives; 30s (`PROXY_RESPONSES_KEEPALIVE_MS`) bounds a silent reasoning start. | Valid `response.completed` or `response.incomplete` event. |
| **OpenAI Responses (`PROXY_RESPONSES_EARLY_COMMIT=0`)** | Buffer up to 240s (`PROXY_RESPONSES_BUFFER_MS`), then switch to live streaming. | Valid `response.completed` or `response.incomplete` event. |
| **Gemini `streamGenerateContent?alt=sse`** | Buffer up to 240s (`PROXY_GEMINI_BUFFER_MS`), then switch to live streaming. | Clean transport EOF after every requested candidate has a `finishReason` (or a prompt block without candidates). |

### Gemini completion details

Because Gemini SSE has no dedicated terminal event, the proxy waits for clean EOF after all requested `candidateCount` candidates report a `finishReason`, preserving trailing usage-metadata frames (SSE control frames and empty data messages do not advance candidate completion). `generationConfig` / `generation_config` settings accept ProtoJSON camelCase or `snake_case` in JSON field order (later objects replace earlier ones), and candidate counts accept quoted integers, exponents, and `.0` fractions without mutating the request body.

Missing candidates, truncated frames, contradictory prompt blocks (a block accompanied by candidates), and read errors fail the attempt. Native `MAX_TOKENS`, safety stops, and standalone prompt blocks are valid completions and are delivered as-is.

### One window per client request

The buffering timer starts when the client request arrives. Upstream header waits, local retry backoffs, and model fallback attempts all share that single window.

When the window expires:
- A pending header or error-body read is cancelled and returned for client retry.
- An active SSE stream flushes its buffered prefix and switches to live streaming (Workflow requests wait until forwardable content is present).
- In live mode, silent gaps emit `: keepalive` comments on Messages/Gemini and `response.proxy_keepalive` events on Responses. Keepalives prevent client idle disconnects, but client cancellation, `PROXY_UPSTREAM_BYTE_IDLE_MS`, and the configured `PROXY_MAX_REQUEST_DURATION_MS` still apply. The total request limit is not inferred from client timeout headers.

## Retries

When an attempt fails before commit:

1. **Local retry:** If `PROXY_TRANSACTIONAL_LOCAL_RETRIES` has remaining budget and enough time remains in the buffering window and request deadline, the proxy waits and re-issues the upstream request transparently.
2. **Client retry:** Otherwise, the proxy returns HTTP `503` with `X-Should-Retry: true` and `Retry-After` (`api_error` envelope for Anthropic/Responses; `error.code: 503` / `UNAVAILABLE` for Gemini).

Masking all retryable failures—including `429` rate limits and `529` overloads—into a generic `503` prevents Claude Code from routing them into special-case handlers that ignore `X-Should-Retry` and abort after ~3 tries. The real failure code is preserved in the proxy logs.

Retried: connection errors, stalled or truncated streams, `408`/`409`/`429`/`5xx`, transient in-band errors, authoritative retry hints (`x-should-retry`, `x-gateway-retryable`), and a `4xx` whose error body explicitly names a transient fault (overloaded, rate limit, timeout). This is the official SDKs' rule. Everything else (auth, billing, permission, not-found, validation, an exhausted quota, any other `4xx`) is the upstream's final answer: an identical re-send fails the same way, so it is relayed once with its original body and headers plus `X-Should-Retry: false`. Responses answers such errors with `400` (or `429`) and Gemini maps `408`/`429`/`5xx` to `400`, because their SDKs retry other statuses regardless of `X-Should-Retry`. Redirects are not followed (credentials must not leave the configured upstream): a `3xx` returns `502` with reason `upstream_redirect_<status>` naming the target. See [Troubleshooting](TROUBLESHOOTING.md#invalid-requests).

### Retry budgets

With `PROXY_TRANSACTIONAL_LOCAL_RETRIES=N` (`N > 0`), a turn using the same model can make up to:

```text
(client retries + 1) × (N + 1) upstream attempts
```

bounded by the buffering window, total request deadline, and `PROXY_SDK_RETRY_CAP`. Local retries sleep for the upstream `Retry-After` **plus** an exponential backoff (1s, 2s, 4s, …, capped by `PROXY_LOCAL_RETRY_EXTRA_BACKOFF_CAP_MS`, default 10s).

When `N = 0` (the default), general local retries are off, except for a single fast retry (250 ms wait) when a connection error or selected HTTP status (`408`, `409`, `502`, `503` without `Retry-After`) arrives within 3 seconds of the attempt start.

`PROXY_SDK_RETRY_CAP` (default `100`) bounds retry conversion using the `X-Stainless-Retry-Count` header (it does not raise client limits; for example, Claude Code 2.1.191 defaults to 10 retries and clamps `CLAUDE_CODE_MAX_RETRIES` to 15). When the upstream provides no `Retry-After`, the synthesized client delay scales exponentially from 1s to 30s (+ up to 1s jitter) based on that header (or stays 1–2s when the header is absent). If retry conversion is disabled (`PROXY_SDK_RETRY_CAP=0`) or exhausted, Responses and transient Gemini failures return terminal HTTP `400` because their SDKs may retry `5xx`/`429` regardless of `X-Should-Retry: false`.

## Model fallback

On streaming `POST /v1/messages` requests for a Claude model (model name containing `claude`), if a buffered response finishes with `stop_reason: "refusal"` or fails pre-commit with a recognized, terminal Fable safeguard error, the proxy rewrites the top-level `"model"` field to `PROXY_REFUSAL_FALLBACK_MODEL` (default `claude-opus-5`) and re-issues the request once. Other vendors' models are never re-routed to a Claude fallback.

- All other JSON fields (`max_tokens`, `thinking`, `tools`, etc.) preserve their JSON values.
- The proxy logs `WARN` with the original and fallback models.
- Model substitution happens at most once per client request, gets a fresh local retry counter within the remaining request window, and remains enabled even when `PROXY_SDK_RETRY_CAP=0` or when a recognized safeguard error carries `x-gateway-retryable: false` / `x-should-retry: false`.
- The original answer is held while the fallback runs. If the fallback fails terminally (or the retry backstop is exhausted), the client receives the original refusal stream or safeguard error; a transient fallback failure is retried like any other.
- Set `PROXY_REFUSAL_FALLBACK_MODEL` to `off`, `none`, `disabled`, or `""` to disable it. Refusal fallback does not apply to Responses, Gemini, or non-streaming calls.

## Workflow agents

Claude Code's Workflow `agent()` runtime enforces a per-agent content-stall watchdog (180s by default; `CLAUDE_ASYNC_AGENT_STALL_TIMEOUT_MS` controls a different path) that only resets when actual assistant content is delivered—SSE pings and comments do not count.

When the proxy detects the Workflow system-prompt marker (`workflow orchestration script` inside `system`), it labels the request `/wf` in the logs and uses `PROXY_WORKFLOW_KEEPALIVE_MS` (default 10s) instead of the 4-minute window. Once 10s have elapsed and forwardable content is buffered, the proxy commits and streams live. If the upstream model itself goes silent for longer than the script's `stallMs`, raise `stallMs` in the workflow script (e.g., `agent(prompt, {stallMs: 300000})`).

## Stream validation and replay

- **Anthropic Messages:** Validates event sequence, event JSON, and accumulated `tool_use` / `server_tool_use` input JSON before replaying a buffered stream. Fragmented `input_json_delta` chunks are coalesced into a single valid JSON delta per tool block; tool input cut off by `stop_reason: "max_tokens"` is delivered as the upstream sent it. Zero-placeholder `message_start` token counts are backfilled per field from non-zero `message_delta` usage.
- **OpenAI Responses:** Requires a valid `response.completed` event (with non-empty string `id` and complete integer usage fields when present) or a `response.incomplete` event, which is delivered as the upstream's answer (an `interrupted` one must pass the `response.completed` check, because Codex parses it as a completion). During buffered/prefix replay, adjacent compatible `response.output_text.delta` frames are merged up to `PROXY_RESPONSES_REPLAY_DELTA_BYTES` (64 KiB) to avoid flooding the client queue; a merged frame carries the last merged frame's `sequence_number`, so the numbers skip, and frames with logprobs or mismatched metadata stay untouched.
- **Gemini SSE:** Validates JSON framing, candidate `finishReason` completion, and absence of in-band error objects, then replays each event's original bytes unchanged. Standalone SSE comment frames from the upstream are not replayed on any route; the proxy sends its own keepalives in live mode.

See [Stream processing](CONFIGURATION.md#stream-processing) to tune or disable these checks.

## Retry limits and side effects

Transactional buffering and local retries apply only to streaming `POST` requests on Messages, Responses, and Gemini SSE. Single-shot routes use one upstream attempt per client call; if a response body copy fails mid-transfer, the proxy aborts the connection and logs `DROP`.

Because retries re-send the upstream request, any server-side or remote MCP tool invoked by the provider may run more than once. Client-local tools are safe because the client only executes them after receiving a complete response. For remote tools with side effects, enforce idempotency at the tool layer or set `PROXY_SDK_RETRY_CAP=0` and `PROXY_REFUSAL_FALLBACK_MODEL=off`.
