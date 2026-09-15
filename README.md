# steady-proxy

steady-proxy exists so agents can work continuously through common API and gateway
disruptions, with fewer interruptions that need you to restart a turn or type
“continue.” It runs between your **agent or API client** and its gateway,
protecting requests from dropped connections, broken streams, overload, and
temporary errors.

It provides:

- **Buffering:** hold responses until they are complete, so a broken reply can be
  retried before the agent sees it.
- **Retries and backoff:** turn recoverable failures into automatic client retries,
  with optional extra retries inside the proxy.
- **Model fallback:** retry Anthropic refusals with a configured fallback model.
- **Stream validation and repair:** catch malformed Anthropic streams, combine
  fragmented tool input, and smooth buffered Responses text replay.
- **Long-turn support:** use configurable buffering windows and keepalives, with
  a shorter window for Claude Code Workflow agents.
- **Diagnostics:** log request outcomes and optionally archive full request and
  response data for debugging.

```text
Agents / SDKs / applications → steady-proxy on localhost → your gateway
```

Stream protection is built around **Anthropic Messages** (`/v1/messages`) and
**OpenAI Responses** (`/v1/responses`). Claude Code and Codex are supported clients;
other agents and SDK-based applications can use these routes too, with compatible
timeout, retry, and SSE handling. Subagents using the same API settings are covered
without terminal automation. See [client compatibility](docs/ARCHITECTURE.md#client-compatibility)
for the requirements and behavior of other routes.

## What to expect

By default, Messages streams are buffered for up to 10 minutes. A response that
finishes within that window appears all at once. Responses streams go live as soon
as output arrives; you can enable full buffering to protect them from later stream
failures.

Before the proxy sends a response, it can discard a failed attempt and retry.
After it starts streaming to the client, recovery depends on the client's own
stream retry behavior. Invalid requests, exhausted retry budgets, and client
timeouts can still stop a turn. See the [architecture guide](docs/ARCHITECTURE.md) for the
behavior and limits of each mode.

## Quick start

You need Docker with Compose v2 and a gateway that supports your client's API.
For a native installation, see [building and running from source](docs/DEVELOPMENT.md).

```bash
git clone https://github.com/Xllent-AI/steady-proxy
cd steady-proxy
cp .env.example .env
```

Edit `.env` and set your gateway URL:

```dotenv
PROXY_UPSTREAM_URL=https://your-gateway.example.com
```

Use the gateway root, or its required path prefix. The proxy appends the request
path: `/v1/messages` becomes `https://your-gateway.example.com/v1/messages`.
Do not add another `/v1` unless your gateway requires that extra prefix.

Start the proxy:

```bash
docker compose up -d --build
curl -fsS http://127.0.0.1:8789/__version
```

The version endpoint confirms that the proxy is running; it does not contact your
gateway. Next, configure the client you use below. Keep your existing API
credentials: the proxy forwards them to the gateway.

### Claude Code

Merge this `env` block into `~/.claude/settings.json`, keeping your other settings
and authentication values:

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

The proxy sends no response headers while buffering. Both client timeouts must
exceed the default 600,000 ms buffering window, and the idle watchdog must stay
off. If you change that window, adjust the client timers with it; see
[timeouts](docs/CONFIGURATION.md#timeouts).

### Codex

In `~/.codex/config.toml`, update your **active provider's** section. Replace
`your_provider` below with its existing name and preserve its authentication
settings:

```toml
[model_providers.your_provider]
base_url = "http://127.0.0.1:8789/v1"
wire_api = "responses"
stream_idle_timeout_ms = 660000
request_max_retries = 10
stream_max_retries = 10
```

Keep `/v1` in the client URL. The idle timeout above covers both the default
streaming mode and the optional 10-minute buffering mode.

For full buffering and up to three extra upstream attempts per client request,
add these settings to `.env`, then run `docker compose up -d`:

```dotenv
PROXY_RESPONSES_EARLY_COMMIT=0
PROXY_TRANSACTIONAL_LOCAL_RETRIES=3
```

Full buffering delays visible output until completion or the buffering window
ends. Extra retries apply to both supported streaming APIs and must fit within
the active window. See [retry budgets](docs/ARCHITECTURE.md#retry-budgets).

### Other API clients

Set your client's base URL so it sends Messages requests to
`http://127.0.0.1:8789/v1/messages` or Responses requests to
`http://127.0.0.1:8789/v1/responses`. Some SDKs append `/v1` themselves; configure
the base URL to produce exactly one `/v1` in the request path.

Keep authentication in the client and use `stream: true` for stream protection.
Set client timeouts above the active buffering window and configure retries for
HTTP `503`, respecting `Retry-After`. Extra proxy retries can absorb failures
before a response starts, but recovery after streaming begins belongs to the
client. Check the [compatibility requirements](docs/ARCHITECTURE.md#client-compatibility),
especially for Responses clients with strict event parsing.

### Check a request

Start a new client session and send a short prompt while watching:

```bash
docker compose logs -f proxy
```

`OK` means the request was delivered. `RETRY` means the proxy asked the client to
retry, and `FAIL` means it returned an error without requesting another attempt.
See [reading the logs](docs/OPERATIONS.md#reading-the-logs) for examples and the
other outcomes.

## Before you use it

Keep the proxy on **loopback**. It has no authentication of its own; anyone who
can reach its port can send requests through it. Compose publishes the port only
on `127.0.0.1`.

Full payload archives are **off by default**. Enabling them saves prompts and
responses unencrypted, without automatic retention. See
[data handling](docs/OPERATIONS.md#data-handling) before enabling them.

Retries repeat upstream work. Server-side or remote tools with side effects need
their own idempotency protection. Buffering does not undo work already performed
by the provider; see [retry limits and side effects](docs/ARCHITECTURE.md#retry-limits-and-side-effects).

## Documentation

| Guide | Use it to… |
| --- | --- |
| [Architecture](docs/ARCHITECTURE.md) | Understand client compatibility, buffering, retries, fallback, and Workflow agents. |
| [Configuration](docs/CONFIGURATION.md) | Tune timeouts, retry budgets, storage limits, and gateway hints. |
| [Operations](docs/OPERATIONS.md) | Read logs, capture payloads, and run the proxy persistently. |
| [Troubleshooting](docs/TROUBLESHOOTING.md) | Match an error or symptom to its cause and next step. |
| [Development](docs/DEVELOPMENT.md) | Build, test, and find the relevant source code. |

## License

[MIT](LICENSE) © Xllent AI.
