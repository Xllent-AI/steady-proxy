# Configuration

Most setups only need `PROXY_UPSTREAM_URL` in `.env` and matching client timeouts (see the [README quick start](../README.md#quick-start)). This page documents all environment variables and upstream gateway headers.

## How settings are loaded

- **Docker Compose** reads `.env` and passes the variables declared in `docker-compose.yml` into the container. Run `docker compose up -d` after editing `.env`. Shell environment variables take precedence over `.env`.
- **Native binary** reads its process environment directly and does not read `.env`.

### Binary vs. Compose defaults

The tables below list **binary defaults**. `docker-compose.yml.example` overrides three values for container use:

| Variable | Binary default | Compose default | Reason |
| --- | --- | --- | --- |
| `PROXY_LISTEN_ADDR` | `127.0.0.1:8789` | `0.0.0.0:8789` | Listens inside the container network while publishing `127.0.0.1:8789` on the host. |
| `PROXY_RESP_HEADER_TIMEOUT_MS` | `60000` | `660000` | Accommodates gateways that withhold HTTP headers until generation starts. |
| `PROXY_VERBOSE` | `0` | `1` | Enables local-retry and archive diagnostic logs by default in Docker. |

Storage limits (`PROXY_MAX_*_BYTES`), `PROXY_SPOOL_DIR`, `PROXY_MAX_REQUEST_DURATION_MS`, and `PROXY_DEADLINE_MARGIN_MS` are not listed in `docker-compose.yml.example`—add them under `environment:` in `docker-compose.yml` if you need to override them in Docker.

All `_MS` variables take integer milliseconds; `_BYTES` variables take integer bytes; boolean switches use `1` and `0`. Startup fails immediately if `PROXY_UPSTREAM_URL` is missing/invalid or if any numeric variable is malformed or below its minimum.

## Connection

| Variable | Default | Description |
| --- | --- | --- |
| `PROXY_UPSTREAM_URL` | *Required* | Upstream gateway root (`http(s)://host[:port][/prefix]`). Query strings and fragments are rejected. Incoming request paths are appended verbatim. |
| `PROXY_LISTEN_ADDR` | `127.0.0.1:8789` | Bind address. Keep restricted to loopback (`127.0.0.1`). |

## Timeouts

A **buffering window** is how long the proxy holds a streaming request before switching to live output. It starts when the client request arrives and covers header waits, local retries, backoff, and model fallback.

| Variable | Default | Description |
| --- | --- | --- |
| `PROXY_KEEPALIVE_MS` | `600000` (10m) | Anthropic Messages buffering window. `0` disables the timed switch to live output. |
| `PROXY_WORKFLOW_KEEPALIVE_MS` | `10000` (10s) | Claude Code Workflow agent buffering window (commits once forwardable content is present). `0` disables the timed switch. |
| `PROXY_RESPONSES_KEEPALIVE_MS` | `30000` (30s) | OpenAI Responses silent-start window when `PROXY_RESPONSES_EARLY_COMMIT=1`. `0` disables the timer. |
| `PROXY_RESPONSES_BUFFER_MS` | `600000` (10m) | OpenAI Responses full-buffering window when `PROXY_RESPONSES_EARLY_COMMIT=0`. `0` disables the timed switch. |
| `PROXY_GEMINI_BUFFER_MS` | `600000` (10m) | Gemini `streamGenerateContent?alt=sse` buffering window. `0` disables the timed switch. |
| `PROXY_RESP_HEADER_TIMEOUT_MS` | `60000` (1m) | Max wait for upstream HTTP response headers (`660000` in Compose). `0` disables this timer. |
| `PROXY_UPSTREAM_BYTE_IDLE_MS` | `600000` (10m) | Max gap without upstream bytes during SSE streaming or error reads (minimum `1`). |
| `PROXY_MAX_REQUEST_DURATION_MS` | `1500000` (25m) | Hard ceiling for an entire client request, including local retries (minimum `1`). |
| `PROXY_DEADLINE_MARGIN_MS` | `25000` (25s) | Margin subtracted from the client's `X-Stainless-Timeout` (in seconds) so the proxy can return a clean retry before the client aborts. Capped at half the client timeout for short requests; `0` disables the margin. |

### Match the client to the window

Client response-header, stream-idle, and per-attempt timeouts must be strictly greater than the active buffering window:

- **Claude Code (`~/.claude/settings.json`):** Set `CLAUDE_CODE_CONNECT_TIMEOUT_MS=660000`, `API_TIMEOUT_MS=720000`, and `API_FORCE_IDLE_TIMEOUT=0` for the default 600,000 ms window (`CLAUDE_API_TIMEOUT` has no effect).
- **Codex (`~/.codex/config.toml`):** Set `stream_idle_timeout_ms = 660000` to cover both the 30s early-commit window and the 600s full-buffer window.
- **Gemini SDKs:** Set HTTP/stream timeouts to at least `660000` ms to cover `PROXY_GEMINI_BUFFER_MS`.
- **Workflow agents:** Keep `PROXY_WORKFLOW_KEEPALIVE_MS` below the workflow script's `stallMs`; see [Workflow agents](ARCHITECTURE.md#workflow-agents).

## Retries and fallback

| Variable | Default | Description |
| --- | --- | --- |
| `PROXY_TRANSACTIONAL_LOCAL_RETRIES` | `0` | Extra in-proxy upstream attempts before commit on recoverable streaming failures. `0` keeps only a single fast retry (<3s failure, 250ms delay). |
| `PROXY_LOCAL_RETRY_EXTRA_BACKOFF_CAP_MS` | `10000` (10s) | Max exponential delay added to upstream `Retry-After` between local retries. `0` adds no extra delay. |
| `PROXY_SDK_RETRY_CAP` | `100` | Stops converting errors into `503` retries once `X-Stainless-Retry-Count` reaches this value. `0` disables retry conversion and local retries. |
| `PROXY_REFUSAL_FALLBACK_MODEL` | `claude-opus-5` | Fallback model for pre-commit Anthropic refusals and recognized Fable safeguard errors. Set to `off`, `none`, `disabled`, or `""` to disable. |

See [Retries](ARCHITECTURE.md#retries) and [Model fallback](ARCHITECTURE.md#model-fallback) for behavior details.

## Stream processing

| Variable | Default | Description |
| --- | --- | --- |
| `PROXY_RESPONSES_EARLY_COMMIT` | `1` | Commit and stream `/v1/responses` as soon as the first output event arrives. Set `0` for full buffering under `PROXY_RESPONSES_BUFFER_MS`. |
| `PROXY_RESPONSES_REPLAY_DELTA_BYTES` | `65536` (64 KiB) | Max source JSON bytes when merging adjacent compatible `response.output_text.delta` events on replay. `0` replays raw frames. |
| `PROXY_VALIDATE_JSON` | `1` | Validate Anthropic SSE event JSON and accumulated tool-input JSON. `0` disables Anthropic JSON validation and tool-JSON normalization (terminal/error detection on Responses, Gemini, and Anthropic refusals stays active). |
| `PROXY_NORMALIZE_TOOL_JSON` | `1` | Coalesce fragmented Anthropic `input_json_delta` chunks into a single valid JSON delta on buffered replay (requires `PROXY_VALIDATE_JSON=1`). |

## Storage and diagnostics

| Variable | Default | Description |
| --- | --- | --- |
| `PROXY_MAX_BUFFER_MEM_BYTES` | `1048576` (1 MiB) | In-memory buffer limit per request before spilling to disk in `PROXY_SPOOL_DIR`. `0` spills immediately. |
| `PROXY_MAX_RESPONSE_BYTES` | `134217728` (128 MiB) | Max buffered response size and max single SSE event size (minimum `1`). |
| `PROXY_MAX_REQUEST_BYTES` | `67108864` (64 MiB) | Max client request body size (minimum `1`). Oversized bodies fail with `413` (`400` on Responses). |
| `PROXY_SPOOL_DIR` | OS temp dir | Directory for temporary response spill files (unlinked immediately on release). |
| `PROXY_REQUEST_LOG_DIR` | *Off* | Directory for full request/response JSON archives. Empty or unset disables archiving; see [Saving request and response data](OPERATIONS.md#saving-request-and-response-data). |
| `PROXY_VERBOSE` | `0` | Set `1` to log internal `[local-retry]` and `[request-log]` diagnostics (`1` in Compose). |

## Optional upstream headers

Your gateway can attach these headers to non-2xx HTTP responses to override the proxy's error heuristics:

| Upstream header | Effect |
| --- | --- |
| `x-gateway-retryable: true` / `false` | Force the error to be treated as retryable or terminal (highest precedence). |
| `x-should-retry: true` / `false` | Same effect when `x-gateway-retryable` is not set. |
| `Retry-After` | Delay in seconds or HTTP-date format for local and client retries. |

Non-retryable Responses errors normalize to HTTP `400`. Non-retryable Gemini `408`, `429`, and `5xx` errors also normalize to `400` so Gemini SDKs do not auto-retry them by status code.

The proxy strips `x-gateway-retryable`, `x-gateway-error-stage`, and `x-gateway-error-code` from incoming client requests so clients cannot spoof gateway hints.
