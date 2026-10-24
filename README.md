# steady-proxy

`steady-proxy` is a local reverse proxy for AI agents and API clients. It sits between your client and API gateway, buffering streaming responses so dropped connections, truncated streams, rate limits, and gateway 5xx errors can be retried before the client sees a failed turn.

```text
Agents / SDKs / applications → steady-proxy (127.0.0.1:8789) → your gateway
```

### Key features

- **Transactional buffering:** Holds SSE responses until completion (or a configurable time window) so broken streams can be discarded and retried cleanly.
- **Retry normalization:** Converts recoverable HTTP and pre-commit stream failures into standard `503` responses with `Retry-After` (plus optional in-proxy retries). Final upstream answers (auth, billing, validation, and other `4xx`) are relayed unchanged.
- **Stream validation and repair:** Validates Anthropic event order and tool JSON, coalesces fragmented `input_json_delta` chunks, and merges bursty Responses text deltas on replay.
- **Refusal fallback:** Re-issues Claude refusals against a fallback model (default `claude-opus-5`, can be disabled) before committing the response, delivering the original refusal if the fallback cannot answer.
- **Workflow watchdog support:** Uses a short buffering window for Claude Code Workflow agents so their content-stall timer does not trip.
- **Diagnostics:** Logs one-line request outcomes and optionally archives full request/response payloads for debugging.

### Supported routes

| API | Route | Default behavior |
| --- | --- | --- |
| **Anthropic Messages** | `POST /v1/messages` (`stream: true`) | Buffers up to 10 minutes (`600000` ms) until `message_stop`, then replays. Workflow agents switch to live streaming after 10 seconds once content is ready. |
| **OpenAI Responses** | `POST /v1/responses` (`stream: true`) | Catches start-of-stream failures before commit, then streams live on first output. Set `PROXY_RESPONSES_EARLY_COMMIT=0` for 10-minute full buffering. |
| **Gemini GenerateContent** | `POST /{version}/models/{model}:streamGenerateContent?alt=sse` (including Vertex AI publisher and express paths) | Buffers up to 10 minutes through clean EOF after all requested candidates finish. |
| **Other routes** | `/v1/chat/completions`, `generateContent`, `countTokens`, `embedContent`, non-SSE `streamGenerateContent`, `/v1/models`, non-streaming calls | Forwarded single-shot; recoverable HTTP and connection errors still normalize for client retry. |

See [Architecture](docs/ARCHITECTURE.md#client-compatibility) for client compatibility details.

## Quick start

Requires Docker with Compose v2 (or Go 1.23+ to [build from source](docs/DEVELOPMENT.md)).

```bash
git clone https://github.com/Xllent-AI/steady-proxy
cd steady-proxy
cp .env.example .env
cp docker-compose.yml.example docker-compose.yml
```

Set your gateway URL in `.env`:

```dotenv
PROXY_UPSTREAM_URL=https://your-gateway.example.com
```

Use the gateway root (or required prefix) without a trailing `/v1`—the proxy appends the incoming request path (`/v1/messages`, `/v1/responses`, etc.) verbatim.

Start the proxy and verify it responds:

```bash
docker compose up -d --build
curl -fsS http://127.0.0.1:8789/__version
```

`/__version` checks local proxy health only. The proxy forwards your client's existing API keys and auth headers to the gateway.

### Prebuilt container image

Linux `amd64` and `arm64` images are published to GitHub Container Registry:

```bash
docker pull ghcr.io/xllent-ai/steady-proxy:latest
```

`latest` tracks `main`; version tags (such as `v0.1.1`) pin a release. To run the prebuilt image with Compose, set `image: ghcr.io/xllent-ai/steady-proxy:latest` in `docker-compose.yml`, remove the `build:` block, and run `docker compose pull && docker compose up -d`. See [Releasing](docs/RELEASING.md#container-images).

## Client setup

### Claude Code

Merge this `env` block into `~/.claude/settings.json` alongside your existing settings and credentials:

```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "http://127.0.0.1:8789",
    "CLAUDE_CODE_CONNECT_TIMEOUT_MS": "660000",
    "API_TIMEOUT_MS": "720000",
    "API_FORCE_IDLE_TIMEOUT": "0"
  }
}
```

Because the proxy sends no HTTP headers while buffering, client timeouts must exceed the 600,000 ms buffering window and the idle watchdog must be disabled. If you change `PROXY_KEEPALIVE_MS`, update these client timeouts to match; see [Timeouts](docs/CONFIGURATION.md#timeouts).

### Codex

Update your active provider section in `~/.codex/config.toml` (keep `/v1` in `base_url` and keep your auth settings):

```toml
[model_providers.your_provider]
base_url = "http://127.0.0.1:8789/v1"
wire_api = "responses"
stream_idle_timeout_ms = 660000
request_max_retries = 10
stream_max_retries = 10
```

To enable full 10-minute buffering for Responses and up to 3 in-proxy retries per client attempt, add to `.env` and run `docker compose up -d`:

```dotenv
PROXY_RESPONSES_EARLY_COMMIT=0
PROXY_TRANSACTIONAL_LOCAL_RETRIES=3
```

Full buffering delays output until the turn completes (or the window expires). Local retries apply across Messages, Responses, and Gemini SSE within the active buffering window; see [Retry budgets](docs/ARCHITECTURE.md#retry-budgets).

### Gemini

Point your Gemini SDK or client base URL at `http://127.0.0.1:8789` and set `PROXY_UPSTREAM_URL` to your Gemini gateway (or `https://generativelanguage.googleapis.com`). The proxy forwards `x-goog-api-key`, Bearer tokens, and query parameters unchanged.

Example streaming call:

```bash
curl -N --max-time 720 \
  "http://127.0.0.1:8789/v1beta/models/${GEMINI_MODEL}:streamGenerateContent?alt=sse" \
  -H "x-goog-api-key: ${GEMINI_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"contents":[{"parts":[{"text":"Say hello."}]}]}'
```

Set client timeouts above `PROXY_GEMINI_BUFFER_MS` (default 600,000 ms) and configure bounded retries on HTTP `503` honoring `Retry-After`. Failed upstream attempts and local-retry waits count toward that silent window; for a client whose first-byte timeout is shorter, lower `PROXY_GEMINI_BUFFER_MS` below it (see [Match the client to the window](docs/CONFIGURATION.md#match-the-client-to-the-window)). If a Gemini stream fails after committing to live output, the proxy aborts the HTTP transport so the client does not mistake a truncated stream for clean EOF.

### Other API clients

Point Messages clients at `http://127.0.0.1:8789/v1/messages` and Responses clients at `http://127.0.0.1:8789/v1/responses` (watch whether your SDK appends `/v1` automatically). Enable `stream: true`, set client timeouts above the buffering window, and retry HTTP `503` with `Retry-After`. See [Client compatibility](docs/ARCHITECTURE.md#client-compatibility).

### Verify traffic

Send a test prompt and tail the proxy logs:

```bash
docker compose logs -f proxy
```

Each request logs a single line starting with `OK` (delivered), `RETRY` (client asked to retry), `WARN` (model fallback), `FAIL` (terminal error), or `DROP` (stream failed after headers were already sent). See [Reading the logs](docs/OPERATIONS.md#reading-the-logs).

## Security and operational notes

- **Bind to loopback only:** The proxy has no authentication of its own; anyone who can reach its port can relay requests through your gateway. Compose binds `127.0.0.1:8789` by default.
- **Payload archives are unencrypted:** `PROXY_REQUEST_LOG_DIR` is off by default. When enabled, it writes full prompts and responses to disk without encryption or automatic retention; see [Data handling](docs/OPERATIONS.md#data-handling).
- **Retries repeat upstream work:** Buffering hides failed attempts from the client, but cannot undo side effects already executed by server-side or remote tools. Ensure remote tools are idempotent; see [Retry limits and side effects](docs/ARCHITECTURE.md#retry-limits-and-side-effects).

## Documentation

| Guide | Contents |
| --- | --- |
| [Architecture](docs/ARCHITECTURE.md) | Buffering lifecycle, commit points, retry budgets, refusal fallback, and Workflow agents. |
| [Configuration](docs/CONFIGURATION.md) | Environment variables, client timeout matching, and upstream gateway headers. |
| [Operations](docs/OPERATIONS.md) | Compose workflow, log format, payload archiving, and `systemd` service setup. |
| [Troubleshooting](docs/TROUBLESHOOTING.md) | Symptom lookup, error classification rules, and timeout debugging. |
| [Development](docs/DEVELOPMENT.md) | Building from source, running unit and live tests, and code layout. |
| [Releasing](docs/RELEASING.md) | Local release packaging, tag publishing, and GHCR container images. |

## License

[MIT](LICENSE) © Xllent AI.
