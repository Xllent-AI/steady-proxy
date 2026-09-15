# Architecture

steady-proxy handles the connection between an API client and its gateway. The gateway
still owns provider routing and availability. The proxy holds responses long
enough to catch failures, then either delivers the response or makes another
attempt possible within the configured budgets.

## Client compatibility

The proxy selects stream behavior from the HTTP method, API path, and request's
`stream` field. It does not require a Claude Code or Codex user agent. Streaming
`POST` requests on the Anthropic Messages and OpenAI Responses routes use the same
buffering and retry engine, with a separate parser for each protocol.

Other agents, SDKs, and applications can use these routes when they support the
proxy's timeout, retry, and stream behavior. The integration tests exercise both
protocols using ordinary HTTP requests. The live CLI harness uses Claude Code;
it does not establish compatibility with every client.

| Client behavior | Requirement or effect |
| --- | --- |
| Base URL and transport | Send HTTP requests to the proxy using the expected API paths and SSE for streaming. The proxy does not translate between APIs or implement WebSocket streaming. |
| Timeouts | Allow the initial buffered wait and the chosen live keepalive interval; see [timeouts](CONFIGURATION.md#timeouts). |
| HTTP retries | Retry HTTP `503` with `Retry-After`. Honor `X-Should-Retry: false` where supported. Local proxy retries can recover some failures even if the client does not retry. |
| Stream recovery | Handle an incomplete response after headers have been sent. The proxy cannot restart a committed response for the client. |
| Responses events | Tolerate the additional `response.proxy_keepalive` event. Completion checks and content handling follow Codex's parser expectations. |
| Retry counter | Without `X-Stainless-Retry-Count`, the proxy treats the client retry count as zero; a positive `PROXY_SDK_RETRY_CAP` cannot bound those client retries. |

Some compatibility choices apply to every request on a protocol's routes.
Responses errors classified as non-retryable become HTTP `400`, and a
`response.failed` event with `context_length_exceeded` is forwarded for the client
to handle. Codex uses that event for compaction; other clients need their own
handling. The shorter Workflow window specifically depends on Claude Code's
system-prompt marker.

Other routes, including `/v1/chat/completions`, are forwarded with one upstream
attempt per client call. They do not receive stream validation, transactional
buffering, or model fallback. HTTP errors can still be normalized for retry, using
the Anthropic-style error envelope on non-Responses paths.

## Buffering and the point of no return

A supported streaming request starts in **buffered mode**. The proxy reads the upstream
server-sent events (SSE) without sending response headers or content to the client.
If the response completes, it sends `200 OK` and replays the buffered events.
If the attempt fails, it can discard the buffer and retry without exposing a
partial reply.

The moment it sends response headers is the **commit point**. From then on, it
cannot replace the response with an HTTP error. If a stream fails after that
point, the proxy ends it and logs `DROP`; recovery depends on the client's stream
retry support and remaining budget.

The two APIs choose that point differently:

| Request | Default behavior | Successful completion |
| --- | --- | --- |
| Anthropic Messages | Buffer for up to 600 seconds, then switch to live output. | A valid stream ending in `message_stop`. |
| Claude Code Workflow agent | Use a 10-second window, then switch once real content is available. | The same Messages completion. |
| OpenAI Responses | Switch to live output as soon as an output event arrives; a 30-second window bounds a silent start. | A valid `response.completed`. |
| Responses with `PROXY_RESPONSES_EARLY_COMMIT=0` | Buffer for up to 600 seconds, then switch to live output. | A valid `response.completed`. |

Buffering trades immediate output for a chance to recover before the client sees
anything. Choose full buffering when that protection matters more than seeing
tokens arrive live.

### One window per client request

The buffering window starts when the client request reaches the proxy. Waiting
for upstream headers, retry delays, extra attempts, and model fallback all use
that same window. None of them restart its clock.

When the window ends, a pending header or error-body read is cancelled and becomes
eligible for a client retry. An active SSE capture switches to live output;
Workflow requests also wait for real forwardable content. In live mode, keepalives
help keep the client connection open. Messages clients receive SSE comments;
Responses clients receive a `response.proxy_keepalive` event. This event is
skippable in Codex, whose SSE reader discards comments without resetting its idle
timer; other Responses clients must also tolerate it.

Keepalives cannot extend an absolute request deadline or make a silent upstream
produce content. The separate upstream idle timeout still applies. See
[timeouts](CONFIGURATION.md#timeouts) for the settings that must move together.

## Retries

Before commit, a recoverable failure has two possible paths:

1. If extra local retries are enabled and there is enough time, the proxy waits
   and sends the request again. The client continues waiting on its original call.
2. Otherwise, the proxy returns HTTP `503` with an `api_error`,
   `X-Should-Retry: true`, and `Retry-After`. The client can then retry the call.

Using one generic error shape matters for Claude Code. Recognizable overload and
rate-limit errors can enter special client handlers with smaller retry budgets.
The proxy keeps those failures in the ordinary retry path and records the original
cause in its logs.

Connection failures, truncation, overload, rate limits, and most temporary service
errors are retryable. Authentication, billing, and unknown errors also usually
retry, since a gateway may recover from them. A truly invalid key or empty balance
therefore takes longer to surface.

Recognized invalid requests are returned without a retry request. Explicit
[gateway retry headers](CONFIGURATION.md#optional-upstream-headers) can override
HTTP error classification. The Responses path also has exceptions designed for
Codex's native context compaction and terminal policy errors; see the
[error guide](TROUBLESHOOTING.md#invalid-requests).

### Retry budgets

`PROXY_TRANSACTIONAL_LOCAL_RETRIES=N` allows up to `N` extra attempts for a
recoverable streaming failure before commit. With `N > 0`, a request that keeps
using the same model can make at most:

```text
(client retries + 1) × (N + 1) upstream attempts
```

The actual number may be lower because attempts and backoff must fit within the
buffering window, request deadline, and proxy retry cap. Local retries wait for
upstream `Retry-After` **plus** an exponential delay starting at one second. The
extra delay is capped at 10 seconds by default; that cap does not shorten an
upstream-requested wait.

The default `N=0` disables the general local retry budget. It still permits one
short retry for selected fast connection or HTTP failures: the failure must arrive
within three seconds, and the proxy waits 250 ms before trying again. The formula
above does not describe that default path.

`PROXY_SDK_RETRY_CAP` is a separate backstop based on the Anthropic SDK's
`X-Stainless-Retry-Count` header. It does not increase the client's own budget.
Claude Code 2.1.191 was observed to default to 10 retries and clamp
`CLAUDE_CODE_MAX_RETRIES` to 15; client versions can change these limits. Codex does
not send the Stainless counter, so any positive proxy cap leaves its retry limit
to `request_max_retries` and `stream_max_retries`. Other clients without that
header likewise need to enforce their own retry limit.

The same counter determines the `Retry-After` sent to the client when the upstream
provides no delay. It grows exponentially from one second to 30 seconds, plus
0–1 seconds of jitter. Without the counter, the synthesized delay remains
1–2 seconds on each client request. Local retries use the proxy's own attempt
counter for their backoff.

There is no circuit breaker. Retries are controlled by backoff, time limits, and
attempt budgets.

## Model fallback

For streaming Anthropic requests, a complete response with
`stop_reason: "refusal"` can trigger a new attempt using
`PROXY_REFUSAL_FALLBACK_MODEL` instead. The default model is `claude-opus-5`.
Recognized Fable safeguards errors can trigger the same substitution before
commit, even without a complete response stream.

Only the request's top-level `model` value changes; the other fields retain their
values. The proxy logs `WARN` with the original and fallback model. Choose a model
your gateway supports, or set the variable to `off`, `none`, `disabled`, or an
empty value to disable fallback.

The substitution happens at most once per client request. A refusal from the
fallback model is delivered normally, as is a refusal that arrives after commit.
The fallback receives a fresh local retry budget, but it shares the original
request's time limits. Setting `PROXY_SDK_RETRY_CAP=0` does not disable this
separate model substitution.

Recognized safeguards errors can also trigger fallback when the upstream sends
`x-gateway-retryable: false` or `x-should-retry: false`. Those headers govern retry
classification; disable fallback separately with `PROXY_REFUSAL_FALLBACK_MODEL=off`
if the original safeguards error should reach the client.

Fallback does not apply to OpenAI Responses or non-streaming requests. It detects
the structured refusal and recognized safeguards messages; it does not inspect
ordinary assistant text for a refusal.

## Workflow agents

Claude Code's Workflow `agent()` calls have a per-agent stall watchdog. In the
observed runtime, its default budget is 180 seconds without forwarded content.
A 600-second buffering window could therefore hide an otherwise healthy response
until the watchdog aborts it.

The proxy recognizes the Workflow runtime marker, `workflow orchestration script`,
in the request's `system` field and uses `PROXY_WORKFLOW_KEEPALIVE_MS` instead.
The default is 10 seconds. Once that window has elapsed and real content is
available, it forwards the buffered prefix and streams live. Interactive sessions
and ordinary subagents retain their usual window. Workflow requests appear as
`/wf` in the log.

Keep this window below the agent's `stallMs`. Keepalive comments and ping events
do not count as Workflow progress, so the proxy cannot fix a long gap in actual
upstream content. A slow first output or a mid-turn pause that exceeds `stallMs`
needs a larger script-side budget, such as `agent(prompt, {stallMs: 300000})`.
`CLAUDE_ASYNC_AGENT_STALL_TIMEOUT_MS` controls a different path; there is no global
override for this watchdog in the observed runtime.

## Stream validation and replay

For Anthropic, the proxy checks stream order, event JSON, and accumulated tool or
server-tool input JSON before delivering a buffered success. It combines tool
`input_json_delta` fragments into one complete JSON delta so the client does not
try to parse incomplete fragments. Buffered replay can also fill initial input
and cache usage from final usage when a gateway supplied zero placeholders.

For Responses, validation focuses on completion and error events. Other content
frames are handled to match Codex's lenient parser. During buffered or prefix replay,
adjacent compatible text deltas can be combined to avoid flooding the client's
notification queue. Live text and non-text frames are preserved. Frames with
non-empty logprobs, ambiguous structure, or incompatible routing stay separate.
Responses tool arguments are not combined by the proxy.

The [stream settings](CONFIGURATION.md#stream-processing) control these behaviors.
Response buffers spill from memory to temporary files and are released when the
attempt finishes. Size limits also bound unfinished SSE events before parsing.

## Retry limits and side effects

The full buffering and local retry engine applies to streaming `POST` requests
on the Messages and Responses routes. Other calls, including `count_tokens`, model
listing, and `stream: false`, use one upstream attempt per client request. Their
HTTP and connection failures can still be converted into client retries. If copying
a successful body fails, the proxy aborts the response and logs `DROP`.

Retrying an upstream request may repeat work already performed by server-side or
remote tools. Buffering prevents delivery of a failed attempt to the client; it
does not provide provider-level idempotency. Use idempotency protection for those
tools or keep such requests outside the proxy. To disable proxy retry conversion
and local retries, set `PROXY_SDK_RETRY_CAP=0`; disable model substitution
separately with `PROXY_REFUSAL_FALLBACK_MODEL=off`. Clients may still apply their
own recovery after a committed stream fails.

The proxy does not resume a partially delivered response. It cannot combine full
response validation with immediate streaming for the same attempt without support
from the gateway.
