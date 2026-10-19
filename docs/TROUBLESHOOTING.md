# Troubleshooting

Start by checking the outcome tag and failure code in the [access log](OPERATIONS.md#reading-the-logs). Even when the client receives a generic `503`, the log line records the underlying cause.

Failures **before commit** (before `200 OK` headers are sent to the client) can be retried locally or returned as a `503` client retry. Failures **after commit** cannot be replaced with an HTTP error; the proxy closes the stream and logs `DROP`.

## Find the symptom

| Symptom | Section |
| --- | --- |
| EOF, socket reset, truncated reply, or JSON parse error | [Broken streams](#broken-streams) |
| Overload (`529`), rate limit (`429`), `5xx`, auth, or billing errors | [Service errors](#service-errors) |
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
| Invalid JSON in an Anthropic event (`JSON Parse error: Unexpected identifier`) or incomplete/invalid accumulated `tool_use` / `server_tool_use` JSON | `malformed_sse` | Discard buffer and retry. |
| HTML error page or non-SSE `Content-Type` on a streaming call | `unexpected_content_type` | Discard buffer and retry. |

### Protocol-specific validation

- **Anthropic Messages:** Keep `PROXY_VALIDATE_JSON=1` and `PROXY_NORMALIZE_TOOL_JSON=1` enabled so malformed events are retried and fragmented `input_json_delta` chunks are combined before replay.
- **OpenAI Responses:** Requires a valid `response.completed` event (duplicate keys or invalid Unicode in the terminal event are rejected; `response.incomplete` is treated as a retryable failure). Ordinary content events follow Codex's lenient parser.
- **Gemini `streamGenerateContent?alt=sse`:** Requires clean transport EOF after all requested candidates report a `finishReason` (or a standalone prompt block). Trailing usage frames are preserved. Missing candidates, missing finish reasons, partial frames, or malformed JSON trigger a retry before commit, and abort the HTTP transport after commit so the client does not mistake a drop for clean EOF. Native `MAX_TOKENS` and safety stops are valid completions and pass through unchanged.

If you see frequent `DROP` logs on `/v1/responses`, enable full buffering with `PROXY_RESPONSES_EARLY_COMMIT=0` and ensure the client's `stream_idle_timeout_ms` exceeds `PROXY_RESPONSES_BUFFER_MS`.

## Service errors

Recoverable upstream errors are normalized into a generic HTTP `503` with `X-Should-Retry: true` and `Retry-After` so clients like Claude Code stay on their standard retry loop instead of aborting early on `529` or `429`.

| Upstream error | Handling |
| --- | --- |
| `529`, `overloaded_error`, “API is at capacity”, “No available providers” | Retry (`503`); original overload code logged. |
| `500`, `502`, `503`, `504` | Retry (`503`), unless overridden by [gateway headers](CONFIGURATION.md#optional-upstream-headers). |
| `429`, `rate_limit_error`, “Rate limited” | Retry (`503`), preserving upstream `Retry-After` or `RetryInfo`. |
| In-stream SSE `error` / `response.failed` (overload, timeout, rate limit, `UNAVAILABLE`, `RESOURCE_EXHAUSTED`) | Retry (`503`) before commit; terminate stream (`DROP`) after commit. |
| Auth failure, billing block, or unknown `4xx` | Retried by default so temporary gateway/account blips do not abort a turn. |

### Special error-classification rules

- **Responses transient codes:** If a `/v1/responses` error has `type: "invalid_request_error"` but carries a transient `code` (`request_timeout`, `rate_limit_exceeded`, or `server_is_overloaded`), the transient code wins and the request is retried.
- **Anthropic SSE disconnect signature:** An Anthropic in-stream `event: error` with `type: "invalid_request_error"` is normally terminal, except for the exact gateway disconnect message:
  ```text
  stream error: stream disconnected before completion: stream closed before response.completed
  ```
  which is classified as `sse_stream_disconnect` and retried before commit.
- **Policy errors:** Generic policy errors retry by default, but Responses `bio_policy` and `cyber_policy` codes are terminal.

Because auth and billing failures are retried by default, a permanently invalid API key or empty balance will retry until the retry budget expires. Gateways that can distinguish permanent failures should return `x-gateway-retryable: false`.

## Invalid requests

Deterministic request-shape errors cannot succeed on an identical retry. The proxy returns them immediately with `X-Should-Retry: false` (and normalizes terminal Responses errors and non-retryable Gemini `408`/`429`/`5xx` errors to HTTP `400`).

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
| Gemini `INVALID_ARGUMENT`, `NOT_FOUND`, or bare HTTP `400`/`404` | Fix the request or model name (`API_KEY_INVALID` remains retryable). |

### Protocol-specific validation details

- **Anthropic:** HTTP `invalid_request_error` responses only fail permanently when they match a known request-shape signature; unrecognized HTTP `400`s remain retryable. In-stream SSE `invalid_request_error` events are terminal (except for the exact `sse_stream_disconnect` string above).
- **Gemini:** Parses `error.code`, `error.status`, and `error.message` across HTTP and SSE errors (including JSON-array HTTP error bodies from non-SSE `streamGenerateContent`). `FAILED_PRECONDITION` (often billing or region routing) and `google.rpc.ErrorInfo` with `reason: "API_KEY_INVALID"` remain retryable. Native [`google.rpc.RetryInfo`](https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto) delays are honored when no positive `Retry-After` header is present (fractional seconds round up).
- **Interrupted error reads & HTTP/2 deadlines:** Truncated error bodies, malformed error JSON, and proxy buffering deadlines during HTTP/2 reads are treated as retryable transport/timeout failures—never as validation errors or `client_gone`.

### Codex context compaction

When a `/v1/responses` stream emits `response.failed` with `error.code: "context_length_exceeded"`, the proxy forwards that SSE event to the client instead of converting it into an HTTP error, allowing Codex's native auto-compaction to trigger. A top-level `error` event with the same code (or an HTTP context-length error) is returned as terminal HTTP `400`.

## Long or silent requests

While buffering, the proxy sends no HTTP headers to the client. If the client's own timeout is shorter than the buffering window, the client will disconnect (`client_gone`).

| Symptom | Cause | Fix |
| --- | --- | --- |
| Claude Code aborts around ~60s with `client_gone` | Default response-header timeout (~60s) fired while buffering. | Set `CLAUDE_CODE_CONNECT_TIMEOUT_MS=660000`, `API_TIMEOUT_MS=720000`, and `API_FORCE_IDLE_TIMEOUT=0` in `~/.claude/settings.json`. |
| Client aborts right at ~600s with `client_gone` | Client timeout equals `PROXY_KEEPALIVE_MS` with no margin. | Keep client timeouts strictly above the buffering window (e.g., `660000` ms+). |
| Codex disconnects in full-buffer mode | `stream_idle_timeout_ms` is shorter than `PROXY_RESPONSES_BUFFER_MS`. | Set `stream_idle_timeout_ms = 660000` in `~/.codex/config.toml`. |
| `upstream_idle` in proxy log | No bytes received from gateway within `PROXY_UPSTREAM_BYTE_IDLE_MS`. | Check gateway health or increase `PROXY_UPSTREAM_BYTE_IDLE_MS`. |
| `deadline` before streaming starts | Header timeout (`PROXY_RESP_HEADER_TIMEOUT_MS`), buffering window, or total request deadline expired. | Compare the logged duration against your [timeout settings](CONFIGURATION.md#timeouts). |
| `DROP` after a long live stream | Upstream failed after the proxy committed to live output. | Check the logged failure code; recovery depends on client stream retries. |

## Workflow stalls

Claude Code Workflow `agent()` requests (logged as `/wf`) use `PROXY_WORKFLOW_KEEPALIVE_MS` (default 10s) so buffered content reaches the client before the Workflow runtime's 180s content-stall watchdog fires.

If a Workflow agent still stalls:
1. Verify `PROXY_WORKFLOW_KEEPALIVE_MS` is well below the script's `stallMs`.
2. Check the log for a high `prun=N` value, which indicates the upstream sent SSE pings without real content tokens. Because SSE pings do not reset the Workflow watchdog, increase `stallMs` in your workflow script (e.g., `agent(prompt, {stallMs: 300000})`).

## Model fallback

A `WARN` log with `refusal -> retry with <model>` or `safeguard -> retry with <model>` means the proxy intercepted a pre-commit Anthropic refusal and re-issued the request with `PROXY_REFUSAL_FALLBACK_MODEL`. The next log line shows the fallback attempt's result.

Ensure your gateway supports the configured fallback model, or set `PROXY_REFUSAL_FALLBACK_MODEL=off` to pass refusals through unchanged. See [Model fallback](ARCHITECTURE.md#model-fallback).

## Storage and payload capture

- **`request_too_large`:** Client request body exceeded `PROXY_MAX_REQUEST_BYTES` (64 MiB default); rejected immediately without truncation.
- **`response_too_large`:** Buffered response or single SSE event exceeded `PROXY_MAX_RESPONSE_BYTES` (128 MiB default); returned as a non-retryable error before commit.
- **`spool_error`:** Disk I/O failure in `PROXY_SPOOL_DIR` (check free space and permissions); treated as retryable.
- **Missing or partial payload archives:** Run with `PROXY_VERBOSE=1` and check for `[request-log]` errors, or inspect `capture_error` inside the JSON archive.

## Non-streaming calls

Non-streaming requests (`stream: false`), `count_tokens` / `countTokens`, and model listings bypass SSE buffering and local retries, using a single upstream attempt per call. Recoverable HTTP and connection failures still convert into `503` client retries. If a response body transfer fails mid-copy after `200 OK` headers are written, the proxy logs `DROP` and aborts the connection so the client does not accept a truncated JSON body.
