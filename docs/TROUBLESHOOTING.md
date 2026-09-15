# Troubleshooting

Start with the request's outcome and failure code in the
[access log](OPERATIONS.md#reading-the-logs). The code preserves the cause even
when the client receives a generic `503`.

Before the proxy sends response headers, a recoverable failure can be retried
locally or returned as a client retry. After headers have been sent, the proxy can
only end a failed stream and log `DROP`. The tables below assume the failure
happens **before commit** unless stated otherwise. Recovery still depends on the
remaining time and retry budgets.

## Find the symptom

| Symptom | Start here |
| --- | --- |
| EOF, socket closure, incomplete reply, or JSON parse error | [Broken streams](#broken-streams) |
| Repeated overload, rate-limit, authentication, or billing errors | [Service errors](#service-errors) |
| Invalid tools, context, schema, or model | [Invalid requests](#invalid-requests) |
| No visible output, timeout, or `client_gone` | [Long or silent requests](#long-or-silent-requests) |
| Workflow agent reports “stalled … no progress” | [Workflow stalls](#workflow-stalls) |
| A different model answers after a refusal | [Model fallback](#model-fallback) |
| Size-limit failure, missing archive, or disk error | [Storage and payload capture](#storage-and-payload-capture) |

## Broken streams

These failures often appear after an upstream has already returned HTTP `200`.
Holding the response lets the proxy detect the failure before the client accepts
that success status.

| Message or condition | What the proxy detects | Result before commit |
| --- | --- | --- |
| `JSON Parse error: Unexpected EOF` | Connection closes before the terminal event, or tool input ends as incomplete JSON. | Retry as `truncated_stream` or `malformed_sse`. |
| `Request was aborted.` or `The socket connection was closed unexpectedly.` | Interrupted upstream connection. | Retry a transport or stream-read failure. |
| `Connection closed mid-response` or `Connection closed while thinking` | Missing completion after partial output or reasoning. | Retry the incomplete stream. |
| HTTP/2 `INTERNAL_ERROR; received from peer` | Upstream resets a stream. | Retry as a stream-read or transport error. |
| `JSON Parse error: Unexpected identifier` | Invalid JSON in a complete Anthropic event. | Retry as `malformed_sse`. |
| Tool/server-tool fragments never form valid JSON | Invalid accumulated Anthropic tool input. | Retry as `malformed_sse`. |
| HTML or another non-SSE response to a streaming request | Unexpected content type. | Retry as `unexpected_content_type`. |

For Anthropic, leave `PROXY_VALIDATE_JSON=1` to catch malformed event and tool
payloads. `PROXY_NORMALIZE_TOOL_JSON=1` also combines fragmented tool input before
forwarding it. Turning validation off disables that normalization too.

For Responses streams, a valid `response.completed` is required for successful completion.
A malformed terminal, including duplicate keys or invalid Unicode, does not count
as success. Ordinary content frames are handled according to Codex's lenient
parser rather than Anthropic's validation rules. A `response.incomplete` event
also triggers a retry; it is not a successful completion.

If the log shows `DROP`, the client had already received headers. Default Responses
mode reaches that point as soon as output appears. Enable full buffering with
`PROXY_RESPONSES_EARLY_COMMIT=0` to protect more of the response, and set the
[client idle timeout](CONFIGURATION.md#match-the-client-to-the-window) above the
buffering window. For Messages, check whether the turn outlasted its window or
used the shorter Workflow window.

## Service errors

The proxy treats temporary service failures as retryable and renders them as a
generic `503` with retry headers. This keeps recognizable overload or rate-limit
shapes from triggering Claude Code's smaller special-case retry budgets.

| Upstream error | Expected handling |
| --- | --- |
| `529`, `overloaded_error`, “API is at capacity,” or “No available providers” | Retry; the log retains the overload or HTTP failure code. |
| `500`, `502`, `503`, or another server failure | Retry, subject to gateway override headers. |
| `429`, `rate_limit_error`, or “Rate limited” | Retry with the upstream delay when available. |
| An Anthropic SSE `error` reporting overload or rate limit | Retry before commit; end the stream after commit. |
| A Responses `error` or `response.failed` reporting overload, timeout, or rate limit | Retry before commit; end the stream after commit. |
| Invalid API key, disabled organization, billing block, or unknown `4xx` | Usually retry, since gateway access or capacity may recover. |

A Responses error can carry `type: "invalid_request_error"` while its more
specific `code` is `request_timeout`, `rate_limit_exceeded`, or
`server_is_overloaded`. The recognized transient code takes precedence. For
example, `request_timeout` with “stream disconnected before completion” remains
retryable instead of becoming an invalid-request failure.

If retries continue, check the gateway's credentials, account state, availability,
and rate limits. A permanently bad key or exhausted balance will not improve
without intervention, even though the proxy allows time for recovery. Generic
policy errors usually follow this retry policy; recognized Responses `bio_policy`
and `cyber_policy` codes are terminal.

Use [retry budgets](ARCHITECTURE.md#retry-budgets) to understand the remaining
attempts. Increasing the budget cannot fix a persistent configuration error.
A gateway that knows an error is permanent can use
[`x-gateway-retryable: false`](CONFIGURATION.md#optional-upstream-headers).

## Invalid requests

An identical retry cannot repair an invalid request. Recognized request errors
are returned with `X-Should-Retry: false`; terminal Responses errors use HTTP
`400` so Codex stops retrying them.

| Error or message | What to fix |
| --- | --- |
| Missing tool result, duplicate `tool_use` ID, or unexpected `tool_use_id` | Repair or rewind the conversation's tool history. |
| Tool-use concurrency error with “Run /rewind” | Restore a consistent conversation state. |
| “Extra inputs are not permitted,” `context_management`, or `input_examples` | Align the request schema with what the gateway accepts. |
| Unexpected `anthropic-beta` header value | Use a beta/header combination supported by the gateway. |
| `max_tokens must be greater than thinking.budget_tokens` | Increase the output limit or reduce the thinking budget. |
| Thinking blocks “cannot be modified” | Preserve the original thinking blocks in conversation history. |
| Image dimensions exceed the allowed size | Resize or replace the image. |
| “Prompt is too long” or context-length error | Compact or shorten the input. |
| Explicit `model_not_found` code | Choose a model the gateway supports. |
| Responses `unknown_parameter`, `invalid_type`, or another recognized validation code | Correct the named field or value. |

Anthropic HTTP errors use recognized message signatures. A bare HTTP
`invalid_request_error` without one can still retry; an Anthropic in-stream
`invalid_request_error` is terminal. Responses classification uses error codes,
types, and recognized messages, with known transient codes taking precedence over
generic invalid-request types. Gateway retry headers override HTTP heuristics.

### Codex context compaction

A Responses `response.failed` event whose error code is exactly
`context_length_exceeded` is forwarded to Codex so its native compaction path can
handle it. This is an intentional exception to converting errors into HTTP
responses. A top-level `error` event with the same code, or another recognized
context-error shape, is returned as terminal HTTP `400` instead.

## Long or silent requests

Silence while buffering is expected. The proxy has not sent response headers
because it is still waiting for a complete response. If the client cancels during
that wait, the proxy cannot keep the abandoned call running for it.

| Log or timing | Likely cause | Next step |
| --- | --- | --- |
| Claude aborts around 60 seconds with `client_gone` | Its response-header timeout is shorter than the buffering window. | Apply both Claude timeout settings from the [README](../README.md#claude-code). |
| Claude aborts near 600 seconds | Its per-attempt timeout may equal the default window. | Keep `API_TIMEOUT_MS` strictly above the window. |
| Codex idles out in full-buffer mode | `stream_idle_timeout_ms` is too short. | Set it above `PROXY_RESPONSES_BUFFER_MS`. |
| `upstream_idle` | No upstream bytes arrived within the byte-idle limit. | Check the gateway for a stalled generation or connection. |
| `deadline` before streaming begins | A header timeout, buffering-window deadline, or total request deadline elapsed. | Compare the duration with the [timeout settings](CONFIGURATION.md#timeouts). |
| `DROP` after a long live stream | The response failed after the proxy had committed. | Check the cause code and the client's stream recovery. |

The buffering window includes header waits and all hidden retries. More local
retries do not extend it. Keepalive messages help an open client connection, but
cannot override its absolute timeout or make an upstream produce useful content.

## Workflow stalls

Workflow `agent()` calls have a separate watchdog based on real forwarded content.
The proxy's default 10-second Workflow window reduces buffering delay before the
first useful output. A workflow should appear as `/wf` in the log.

If it still stalls, keep `PROXY_WORKFLOW_KEEPALIVE_MS` below its `stallMs`, then
check for a long gap in actual upstream content. Pings do not reset the Workflow
watchdog; a high `prun` value can help identify such a gap. A larger script-side
`stallMs` is needed when the upstream itself remains content-silent too long.
See [Workflow behavior](ARCHITECTURE.md#workflow-agents) for detection and limits.

## Model fallback

A `WARN` line with `refusal -> retry with <model>` or
`safeguard -> retry with <model>` records an Anthropic model substitution. The
following outcome is for the fallback attempt. Ensure that the configured model
is available through your gateway.

The substitution only works before commit and happens at most once per client
request. It does not apply to Responses requests. To keep the original refusal response, set
`PROXY_REFUSAL_FALLBACK_MODEL=off`; see [model fallback](ARCHITECTURE.md#model-fallback).

## Storage and payload capture

`request_too_large` means the incoming body exceeded `PROXY_MAX_REQUEST_BYTES`.
It is rejected without truncation. `response_too_large` means a buffered response
or an individual SSE event exceeded `PROXY_MAX_RESPONSE_BYTES`; it is returned as
a non-retryable failure before commit. Reduce the payload or deliberately adjust
the relevant limit.

`spool_error` means a storage operation failed, not that the size cap was reached.
Check permissions and free space for `PROXY_SPOOL_DIR`. Storage I/O failures remain
retryable, and temporary response buffers are released when attempts end.

For a missing or incomplete payload archive, enable `PROXY_VERBOSE=1` and look for
`[request-log]` errors. A `capture_error` in an archive indicates a read problem.
Archives have no automatic retention; see
[payload capture and data handling](OPERATIONS.md#saving-request-and-response-data).

## Non-streaming calls

Successful non-streaming requests, token counting, and model listing are forwarded
without full response buffering. Their HTTP or connection errors can still become
client retries, but there are no hidden transactional retries. If a successful
body copy fails, the proxy logs `DROP` and aborts the HTTP response so a truncated
body does not look like clean success.
