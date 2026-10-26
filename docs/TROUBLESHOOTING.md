# Troubleshooting

Start by checking the outcome tag and failure code in the [access log](OPERATIONS.md#reading-the-logs). Even when the client receives a generic `503`, the log line records the underlying cause.

Failures **before commit** (before `200 OK` headers are sent to the client) can be retried locally or returned as a `503` client retry. Failures **after commit** cannot be replaced with an HTTP error: a terminal in-band error frame from the upstream is forwarded and logged `FAIL … delivered in-stream`; any other failure aborts the connection and logs `DROP`.

## Find the symptom

| Symptom | Section |
| --- | --- |
| EOF, socket reset, truncated reply, or JSON parse error | [Broken streams](#broken-streams) |
| Overload (`529`), rate limit (`429`), `5xx`, auth, billing, or redirect errors | [Service errors](#service-errors) |
| Invalid tool blocks, schema error, context overflow, or unknown model | [Invalid requests](#invalid-requests) |
| Request hangs, times out, or logs `client_gone` / `deadline` | [Long or silent requests](#long-or-silent-requests) |
| Claude Code Workflow agent aborts with “stalled … no progress” | [Workflow stalls](#workflow-stalls) |
| A different model answers after a refusal (`WARN`) | [Model fallback](#model-fallback) |
| `request_too_large`, `response_too_large`, `spool_error`, or missing archive | [Storage and payload capture](#storage-and-payload-capture) |

## Broken streams

Upstreams often fail mid-stream *after* sending HTTP `200`. Buffering catches these failures before the client sees them:

| Upstream failure | Proxy classification | Pre-commit action |
| --- | --- | --- |
| Stream closes before terminal event (`JSON Parse error: Unexpected EOF`, `Connection closed mid-response` / `while thinking`) | `truncated_stream` | Discard buffer and retry. |
| Socket reset, `Request was aborted.`, or HTTP/2 `INTERNAL_ERROR; received from peer` | Transport / stream-read error | Discard buffer and retry. |
| Invalid JSON in an Anthropic event (`JSON Parse error: Unexpected identifier`) or incomplete/invalid accumulated `tool_use` / `server_tool_use` JSON (unless `stop_reason` is `max_tokens`, which cuts tool input legitimately) | `malformed_sse` | Discard buffer and retry. |
| Non-SSE `2xx` body on a streaming call that breaks before its end | Transport / stream-read error | Discard buffer and retry. A complete non-SSE body is the upstream's answer and is relayed unchanged. |

### Protocol-specific validation

- **Anthropic Messages:** Keep `PROXY_VALIDATE_JSON=1` and `PROXY_NORMALIZE_TOOL_JSON=1` enabled so malformed events are retried and fragmented `input_json_delta` chunks are combined before replay.
- **OpenAI Responses:** Requires a valid `response.completed` event (non-empty string `id`, complete integer usage when present, no duplicate keys or invalid Unicode); a rejected one fails as `responses_invalid_completed`. `response.incomplete` is the upstream's answer (for example `max_output_tokens`) and is delivered unchanged; Codex 0.163+ treats it as terminal. Ordinary content events follow Codex's lenient parser.
- **Gemini `streamGenerateContent?alt=sse`:** Requires clean transport EOF after all requested candidates report a `finishReason` (or a standalone prompt block). Trailing usage frames are preserved. Missing candidates, missing finish reasons, partial frames, or malformed JSON trigger a retry before commit, and abort the HTTP transport after commit so the client does not mistake a drop for clean EOF. Native `MAX_TOKENS` and safety stops are valid completions and pass through unchanged.

If you see frequent `DROP` logs on `/v1/responses`, enable full buffering with `PROXY_RESPONSES_EARLY_COMMIT=0` and ensure the client's `stream_idle_timeout_ms` exceeds `PROXY_RESPONSES_BUFFER_MS`.

## Service errors

Recoverable upstream errors are normalized into a generic HTTP `503` with `X-Should-Retry: true` and `Retry-After` so clients like Claude Code stay on their standard retry loop instead of aborting early on `529` or `429`. Every other error is the upstream's final answer and is relayed unchanged.

| Upstream error | Handling |
| --- | --- |
| `529`, `overloaded_error`, “API is at capacity”, “No available providers” | Retry (`503`); original overload code logged. |
| `500`, `502`, `503`, `504`, `408`, `409` | Retry (`503`), preserving upstream `Retry-After`, unless overridden by [gateway headers](CONFIGURATION.md#optional-upstream-headers). |
| `429`, `rate_limit_error`, “Rate limited” | Retry (`503`), preserving upstream `Retry-After` or `RetryInfo`. |
| Exhausted quota: OpenAI `insufficient_quota`, Codex `usage_limit_reached` / `usage_not_included`, a Gemini `QuotaFailure` naming a per-day quota, or a `429` naming a request-shape problem | Final answer, relayed (`http_429`). |
| `401`, `402`, `403`, `404`, `413`, and every other `4xx` | Final answer, relayed with its original body and headers and `X-Should-Retry: false` after one upstream attempt (`http_<status>`). |
| `3xx` redirect | `502` with reason `upstream_redirect_<status>`; the message names the target without its query. Point `PROXY_UPSTREAM_URL` at the final URL. |
| In-stream SSE `error` / `response.failed` naming a transient fault (overload, timeout, rate limit, `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, unknown type) | Retry (`503`) before commit; abort the stream (`DROP`) after commit. |
| In-stream terminal error (invalid request, auth, billing, permission, not found, policy, exhausted quota) | Returned as the upstream's error before commit; forwarded in-stream after commit. |

### Special error-classification rules

- **Transient `4xx` bodies:** A `4xx` other than `408`/`409`/`429` is retried only when its error body explicitly names a transient fault (Anthropic `overloaded_error` / `rate_limit_error` / `timeout_error`, a Responses transient type or code, a Gemini transient canonical status). It is logged as `retryable_4xx_<status>`.
- **Responses transient codes:** If a `/v1/responses` error has `type: "invalid_request_error"` but carries a transient `code` (`request_timeout`, `rate_limit_exceeded`, `server_is_overloaded`, or `overloaded`), the transient code wins and the request is retried, waiting any delay the message names (`try again in 1m30s`, `20 seconds`). A `429` whose message says the tokens "must be reduced in order to run successfully" is permanent. Request-shape phrases are only matched on untyped errors, so a typed `server_error` stays retryable.
- **Anthropic SSE disconnect signature:** An Anthropic in-stream `event: error` with `type: "invalid_request_error"` is normally terminal, except for the exact gateway disconnect message:
  ```text
  stream error: stream disconnected before completion: stream closed before response.completed
  ```
  which is classified as `sse_stream_disconnect` and retried before commit.
- **Policy errors:** Responses `bio_policy` and `cyber_policy` codes are terminal.

A permanently invalid API key or empty balance fails on the first attempt with the upstream's own error. If your gateway returns such statuses for conditions that clear on their own, have it send `x-should-retry: true` or `x-gateway-retryable: true`; authoritative headers override the status rule. Clients that do not send `X-Stainless-Retry-Count` (Codex, Gemini SDKs) are not bounded by `PROXY_SDK_RETRY_CAP` and rely on their own retry limits.

## Invalid requests

Request errors cannot succeed on an identical retry. The proxy relays the upstream's error unchanged with `X-Should-Retry: false`, logged as `request_shape` (recognized signature) or `http_<status>`. Only the status changes where the client SDK would otherwise retry it: terminal Responses errors other than `400`/`429` become `400`, and terminal Gemini `408`/`429`/`5xx` become `400`.

| Error condition | Fix |
| --- | --- |
| Missing `tool_result`, duplicate `tool_use` ID, or unexpected `tool_use_id` | Rewind or repair the conversation's tool history. |
| Tool-use concurrency error (`Run /rewind`) | Rewind the session to a consistent state. |
| `Extra inputs are not permitted`, `context_management`, or `input_examples` | Remove unsupported fields for the target gateway/model. |
| Invalid `anthropic-beta` header | Use beta flags supported by your gateway. |
| `max_tokens must be greater than thinking.budget_tokens` | Raise `max_tokens` or lower `thinking.budget_tokens`. |
| Thinking blocks `cannot be modified` | Preserve historical thinking blocks unmodified. |
| Oversized image dimensions | Resize or remove the image. |
| `Prompt is too long` / context-length exceeded | Compact or shorten the conversation. |
| `model_not_found` | Configure a model supported by your gateway. |
| Responses `unknown_parameter`, `invalid_type`, or schema validation codes | Fix the rejected request field. |
| Gemini `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`, `NOT_FOUND`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, or `API_KEY_INVALID` | Fix the request, model name, key, billing, or region. |

### Protocol-specific validation details

- **Anthropic:** HTTP `4xx` other than `408`/`409`/`429` are terminal; a known request-shape signature labels them `request_shape`. In-stream SSE errors with a documented permanent type (`invalid_request_error`, `authentication_error`, `billing_error`, `permission_error`, `not_found_error`, `request_too_large`) are terminal (except the exact `sse_stream_disconnect` string above); `overloaded_error`, `rate_limit_error`, `api_error`, `timeout_error`, and unknown types retry.
- **Gemini:** Parses `error.code`, `error.status`, and `error.message` across HTTP and SSE errors (including JSON-array HTTP error bodies from non-SSE `streamGenerateContent`). `408`/`409`/`5xx` always retry. Otherwise the canonical status decides: `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`, `UNKNOWN`, `ABORTED`, and `RESOURCE_EXHAUSTED` retry, except a `RESOURCE_EXHAUSTED` whose `google.rpc.QuotaFailure` names a per-day quota (`quotaId` containing `PerDay`); `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `ALREADY_EXISTS`, and `UNIMPLEMENTED` are terminal. Other statuses (such as `CANCELLED`) follow the HTTP status rule. Native [`google.rpc.RetryInfo`](https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto) delays are honored when no positive `Retry-After` header is present (fractional seconds round up).
- **Interrupted error reads & HTTP/2 deadlines:** Truncated error bodies, malformed error JSON, and proxy buffering deadlines during HTTP/2 reads are treated as retryable transport/timeout failures—never as validation errors or `client_gone`.

### Codex context compaction

When a `/v1/responses` stream emits `response.failed` with `error.code: "context_length_exceeded"`, the proxy forwards that SSE event to the client instead of converting it into an HTTP error, allowing Codex's native auto-compaction to trigger. A top-level `error` event with the same code (or an HTTP context-length error) is returned as terminal HTTP `400` before commit; after commit, like every terminal in-band error, it is forwarded and logged `FAIL … delivered in-stream`.

## Long or silent requests

While buffering, the proxy sends no HTTP headers to the client. If the client's own timeout is shorter than the buffering window, the client will disconnect (`client_gone`).

| Symptom | Cause | Fix |
| --- | --- | --- |
| Claude Code aborts around ~60s with `client_gone` | Default response-header timeout (~60s) fired while buffering. | Set `CLAUDE_CODE_CONNECT_TIMEOUT_MS=660000`, `API_TIMEOUT_MS=720000`, and `API_FORCE_IDLE_TIMEOUT=0` in `~/.claude/settings.json`. |
| Client aborts at the buffering window with `client_gone` | Client timeout equals the configured window with no margin. | Keep client timeouts strictly above the window (default `240000` ms), including any explicit longer override. |
| `client_gone` after a `[local-retry]` line, before the window ends | The client's timeout is shorter than the buffering window; the failed attempt and the retry wait sent the client nothing. | Raise the client timeout above the window, or lower the window below it; see [Match the client to the window](CONFIGURATION.md#match-the-client-to-the-window). |
| Codex disconnects in full-buffer mode | `stream_idle_timeout_ms` is shorter than `PROXY_RESPONSES_BUFFER_MS`. | Set `stream_idle_timeout_ms = 660000` in `~/.codex/config.toml`. |
| `upstream_idle` in proxy log | No bytes received from gateway within `PROXY_UPSTREAM_BYTE_IDLE_MS`. | Check gateway health or increase `PROXY_UPSTREAM_BYTE_IDLE_MS`. |
| `deadline` before streaming starts | Header timeout (`PROXY_RESP_HEADER_TIMEOUT_MS`), buffering window, or total request deadline expired. | Compare the logged duration against your [timeout settings](CONFIGURATION.md#timeouts). |
| `DROP` after a long live stream | Upstream failed or a proxy limit expired after commitment. | Check the logged failure code; recovery depends on client stream retries. |
| `DROP … deadline` while upstream data is still arriving | `PROXY_MAX_REQUEST_DURATION_MS` expired; progress does not reset this explicit ceiling. | Set the maximum to the intended whole-request limit. `X-Stainless-Timeout` is forwarded metadata, not a proxy deadline; changing the client fingerprint or buffering window does not change this ceiling. |

## Workflow stalls

Claude Code Workflow `agent()` requests (logged as `/wf`) use `PROXY_WORKFLOW_KEEPALIVE_MS` (default 10s) so buffered content reaches the client before the Workflow runtime's 180s content-stall watchdog fires.

If a Workflow agent still stalls:
1. Verify `PROXY_WORKFLOW_KEEPALIVE_MS` is well below the script's `stallMs`.
2. Check the log for a high `prun=N` value, which indicates the upstream sent SSE pings without real content tokens. Because SSE pings do not reset the Workflow watchdog, increase `stallMs` in your workflow script (e.g., `agent(prompt, {stallMs: 300000})`).

## Model fallback

A `WARN` log with `refusal -> retry with <model>` or `safeguard -> retry with <model>` means the proxy intercepted a pre-commit refusal from a Claude model and re-issued the request with `PROXY_REFUSAL_FALLBACK_MODEL`. The next log line shows the fallback attempt's result; its model reads `<requested>~><fallback>`. Models whose name does not contain `claude` are never re-routed.

If the fallback fails terminally (for example your gateway does not serve that model), a second `WARN … fallback <model> failed: <code> -> delivering the original answer` line follows and the client receives the original refusal or safeguard error. Configure a model your gateway supports, or set `PROXY_REFUSAL_FALLBACK_MODEL=off` to pass refusals through unchanged. See [Model fallback](ARCHITECTURE.md#model-fallback).

## Storage and payload capture

- **`request_too_large`:** Client request body exceeded `PROXY_MAX_REQUEST_BYTES` (64 MiB default); rejected immediately without truncation.
- **`response_too_large`:** Buffered response, single SSE event, or pending tool-input data exceeded `PROXY_MAX_RESPONSE_BYTES` (128 MiB default). Before commit, this is a non-retryable error; after commit, the stream is aborted without replay.
- **`spool_error`:** Disk I/O failure in `PROXY_SPOOL_DIR` (check free space and permissions); treated as retryable.
- **Missing or partial payload archives:** Run with `PROXY_VERBOSE=1` and check for `[request-log]` errors, or inspect `capture_error` inside the JSON archive.

## Non-streaming calls

Non-streaming requests (`stream: false`), `count_tokens` / `countTokens`, and model listings bypass SSE buffering and local retries, using a single upstream attempt per call. Recoverable HTTP and connection failures still convert into `503` client retries; terminal errors are relayed unchanged. If a response body transfer fails mid-copy after `200 OK` headers are written, the proxy logs `DROP` and aborts the connection so the client does not accept a truncated JSON body.
