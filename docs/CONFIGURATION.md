# Configuration

Start with the [quick start](../README.md#quick-start). Most installations only
need a gateway URL and the matching client timeouts. This reference covers the
remaining settings when you need to change the defaults.

## Applying settings

Docker Compose reads `.env` and passes the variables declared in
[`docker-compose.yml`](../docker-compose.yml) to the container. After editing
`.env`, run `docker compose up -d` to apply the changes. Exported shell values take
precedence over `.env`.

The binary reads its process environment; it does not load `.env` itself. Set
variables when launching it or in your service configuration. See
[running from source](DEVELOPMENT.md#build-and-run).

Tables below show **binary defaults**. Compose changes three of them:

| Variable | Binary | Compose |
| --- | --- | --- |
| `PROXY_LISTEN_ADDR` | `127.0.0.1:8789` | Fixed to `0.0.0.0:8789` inside the container, with the host port bound to `127.0.0.1`. |
| `PROXY_RESP_HEADER_TIMEOUT_MS` | `60000` | `660000`, to allow gateways that wait for model output before sending headers. |
| `PROXY_VERBOSE` | Off | `1`. |

Compose does not currently forward the storage-limit variables,
`PROXY_SPOOL_DIR`, `PROXY_MAX_REQUEST_DURATION_MS`, or `PROXY_DEADLINE_MARGIN_MS`.
To set one of those in Docker, add it to the service's `environment` mapping;
putting it in `.env` alone has no effect. Mount any custom storage directory too.

All `_MS` settings use integer milliseconds, and `_BYTES` settings use integer
bytes. Invalid numeric values or values below the permitted minimum stop startup
with an error naming the offending variables. Empty numeric settings use their
defaults. Use `0` and `1` for feature switches; refusal fallback has its own
disable values below.

## Connection

| Variable | Default | Purpose |
| --- | --- | --- |
| `PROXY_UPSTREAM_URL` | Required | Gateway URL: `http(s)://host[:port][/prefix]`. Query strings and fragments are rejected. |
| `PROXY_LISTEN_ADDR` | `127.0.0.1:8789` | Address to listen on. Keep access restricted to loopback. |

The proxy appends the incoming request path to the upstream URL. For example,
`https://gateway.example.com/api` plus `/v1/responses` becomes
`https://gateway.example.com/api/v1/responses`. Authentication stays in the client;
its authorization headers are forwarded to this gateway.

## Timeouts

A **buffering window** is how long the proxy may withhold response headers and
content before switching to live output. It starts at client request arrival and
includes upstream waits, retry delays, and fallback attempts. See
[the buffering lifecycle](ARCHITECTURE.md#buffering-and-the-point-of-no-return).

| Variable | Default | Purpose |
| --- | --- | --- |
| `PROXY_KEEPALIVE_MS` | `600000` (10 min) | Messages buffering window. `0` disables the timed switch to live output. |
| `PROXY_WORKFLOW_KEEPALIVE_MS` | `10000` (10 s) | Workflow agent window; live output also requires real content. `0` keeps these requests buffered without a timed switch. |
| `PROXY_RESPONSES_KEEPALIVE_MS` | `30000` (30 s) | Responses silent-start window when early commit is on. `0` disables this timer; output can still trigger early commit. |
| `PROXY_RESPONSES_BUFFER_MS` | `600000` (10 min) | Responses window when early commit is off. `0` disables the timed switch to live output. |
| `PROXY_RESP_HEADER_TIMEOUT_MS` | `60000` (1 min) | Wait for upstream response headers. `0` disables this timeout, but other deadlines still apply. |
| `PROXY_UPSTREAM_BYTE_IDLE_MS` | `600000` (10 min) | Maximum gap without upstream bytes during SSE capture or error-body reads. Must be at least `1`. |
| `PROXY_MAX_REQUEST_DURATION_MS` | `1500000` (25 min) | Total request deadline, including local retries. Must be at least `1`. |
| `PROXY_DEADLINE_MARGIN_MS` | `25000` (25 s) | Time reserved before a client's `X-Stainless-Timeout` deadline. `0` removes the margin. |

On streaming Messages and Responses requests, `X-Stainless-Timeout` is read in
**seconds**. The effective duration is the smaller of the configured request
ceiling and the client timeout minus the margin. For short client timeouts, the
margin is limited to half their budget. Other routes use the configured ceiling.

### Match the client to the window

For any client, keep response-header, idle, and total request timeouts long enough
to cover the active buffering window. Also allow for keepalive intervals after
the response goes live. The setting names depend on the client or SDK.

For Claude Code with the default 600,000 ms Messages window, set:

| Claude Code setting | Value | Why |
| --- | --- | --- |
| `CLAUDE_CODE_CONNECT_TIMEOUT_MS` | `660000` | Allows the proxy to wait before sending response headers. |
| `API_TIMEOUT_MS` | `720000` | Keeps the per-attempt timeout above the buffering window. |
| `API_FORCE_IDLE_TIMEOUT` | `0` | Prevents the optional idle watchdog from aborting the buffered wait. |

Both timeouts must be strictly above the window. Matching them exactly risks an
abort at the boundary. The response-header timeout was observed to default to
about 60 seconds in Claude Code; disabling its idle watchdog does not change
that limit. `CLAUDE_API_TIMEOUT` is not a recognized setting for this purpose.

For Codex, keep `stream_idle_timeout_ms` above `PROXY_RESPONSES_KEEPALIVE_MS` in
early-commit mode, or above `PROXY_RESPONSES_BUFFER_MS` in full-buffer mode. The
README's `660000` covers either default. A zero window does not disable the
client's timeouts or the proxy's request deadline.

Claude Code Workflow agents have an additional content stall timer. Keep their proxy window
small and below the script's `stallMs`; see [Workflow agents](ARCHITECTURE.md#workflow-agents).

## Retries and fallback

| Variable | Default | Purpose |
| --- | --- | --- |
| `PROXY_TRANSACTIONAL_LOCAL_RETRIES` | `0` | Extra attempts for recoverable streaming failures before commit. `0` retains only the limited fast retry path. |
| `PROXY_LOCAL_RETRY_EXTRA_BACKOFF_CAP_MS` | `10000` | Maximum extra delay added to upstream `Retry-After` between local retries. `0` removes the extra delay. |
| `PROXY_SDK_RETRY_CAP` | `100` | Stop retry conversion when the Stainless retry counter reaches this value. `0` disables conversion and local retries. |
| `PROXY_REFUSAL_FALLBACK_MODEL` | `claude-opus-5` | Alternate model for an Anthropic refusal before commit. `off`, `none`, `disabled`, or an explicit empty value disables substitution. |

Local retries need enough time left in the current request and buffering window.
The SDK cap uses the Stainless counter; it does not raise client retry limits.
Clients without that header, including Codex, must enforce their own retry limits
when the proxy cap is positive. With a zero cap, Responses failures are returned
as terminal HTTP `400` because Codex does not honor `X-Should-Retry: false` alone.

Model fallback has a separate budget and remains enabled when the SDK cap is
zero. See [retry budgets](ARCHITECTURE.md#retry-budgets) and
[model fallback](ARCHITECTURE.md#model-fallback) for the exact scope.

## Stream processing

| Variable | Default | Purpose |
| --- | --- | --- |
| `PROXY_RESPONSES_EARLY_COMMIT` | `1` | Stream Responses output as soon as it arrives. `0` enables full buffering under `PROXY_RESPONSES_BUFFER_MS`. |
| `PROXY_RESPONSES_REPLAY_DELTA_BYTES` | `65536` (64 KiB) | Limit for combining adjacent compatible Responses text deltas during replay. `0` preserves raw replay. |
| `PROXY_VALIDATE_JSON` | `1` | Validate Anthropic event and accumulated tool-input JSON. `0` disables these checks and tool-JSON normalization. |
| `PROXY_NORMALIZE_TOOL_JSON` | `1` | Combine Anthropic tool and server-tool input fragments into complete JSON deltas. Requires JSON validation. `0` disables this combination only. |

The Responses replay limit counts source JSON bytes, not just visible text. It
bounds grouped payloads and reduces bursts of tiny notifications when replaying a
buffered response. Live deltas are unchanged. Incompatible, oversized, malformed,
or ambiguous frames and frames with non-empty logprobs remain raw.

Responses completion/error detection and Anthropic structured refusal detection
remain active even when `PROXY_VALIDATE_JSON=0`.

## Storage and diagnostics

| Variable | Default | Purpose |
| --- | --- | --- |
| `PROXY_MAX_BUFFER_MEM_BYTES` | `1048576` (1 MiB) | Response buffer memory threshold before spilling to a temporary file. `0` spills immediately. |
| `PROXY_MAX_RESPONSE_BYTES` | `134217728` (128 MiB) | Maximum buffered response size; also bounds each SSE event, including unfinished lines. Must be positive. |
| `PROXY_MAX_REQUEST_BYTES` | `67108864` (64 MiB) | Maximum client request body. Larger bodies are rejected with `413`, or `400` on Responses. Must be positive. |
| `PROXY_SPOOL_DIR` | OS temporary directory | Location for response spill files; typically `$TMPDIR` or `/tmp`. |
| `PROXY_REQUEST_LOG_DIR` | Off | Directory for full request/response JSON archives. Empty or unset disables archiving. |
| `PROXY_VERBOSE` | Off | `1` adds internal retry and archive diagnostic messages. Request outcome logs remain enabled regardless. |

Archives contain unencrypted payloads and have no automatic retention. See
[saving request and response data](OPERATIONS.md#saving-request-and-response-data)
for their format and data-handling limits.

## Optional upstream headers

A gateway can set these headers on a non-2xx response to override error
classification:

| Header | Effect |
| --- | --- |
| `x-gateway-retryable: true` | Mark the failure retryable, subject to the remaining budget. |
| `x-gateway-retryable: false` | Mark the failure non-retryable. |
| `x-should-retry: true` or `false` | Apply the same decision if `x-gateway-retryable` did not provide one. |
| `Retry-After` | Supply the retry delay in seconds or as an HTTP date. |

`x-gateway-retryable` takes precedence over `x-should-retry`, which takes precedence
over status and body heuristics. A terminal Responses error is rendered as HTTP
`400`; other routes retain the classified non-retryable status.

These headers control retry classification. Recognized Anthropic safeguards
errors can still trigger a separate [model fallback](ARCHITECTURE.md#model-fallback),
even with a `false` verdict. Set `PROXY_REFUSAL_FALLBACK_MODEL=off` to disable that
substitution too.

The proxy strips `x-gateway-retryable`, `x-gateway-error-stage`, and
`x-gateway-error-code` from client requests, so clients cannot supply these gateway
hints themselves.
