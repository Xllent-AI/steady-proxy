# Operations

See the [README quick start](../README.md#quick-start) for initial setup. This guide covers day-to-day operation, log inspection, payload archiving, and running `steady-proxy` as a `systemd` service.

## Docker Compose

After copying `.env.example` to `.env` and `docker-compose.yml.example` to `docker-compose.yml`:

```bash
docker compose up -d --build   # build and start
docker compose logs -f proxy   # tail request logs
docker compose up -d           # reload .env changes
docker compose down            # stop
```

The container runs as `${UID:-1000}:${GID:-1000}` so payload archives in `./logs` are owned by your host user. If your host UID/GID is not `1000`, check `id -u` and `id -g` and set `UID` and `GID` in `.env` (Bash does not export `UID` by default and has no `GID` variable). The host `./logs` directory is mounted at `/logs` with container working directory `/`, so `PROXY_REQUEST_LOG_DIR=./logs` writes directly to `./logs`.

## Check the running version

```bash
curl -fsS http://127.0.0.1:8789/__version
```

`/__version` returns build metadata locally without contacting your gateway. To test the end-to-end gateway path with a real (billed) request:

```bash
curl -sS http://127.0.0.1:8789/v1/messages \
  -H "authorization: Bearer $ANTHROPIC_AUTH_TOKEN" \
  -H "anthropic-version: 2023-06-01" \
  -H "content-type: application/json" \
  -d '{"model":"<your-model>","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"say hi"}]}'
```

## Reading the logs

Every proxied request logs one summary line. Setting `PROXY_VERBOSE=1` adds internal `[local-retry]` and `[request-log]` lines.

```text
OK    claude-sonnet-4-6/main  high  in=1.2k out=437 tok  end_turn  buffered  3.41s
OK    gpt-5.6/main  high  in=1.1k out=223 tok  completed  live  2.37s
RETRY claude-sonnet-4-6/main  truncated_stream 502->503  retry-after=2s  0.9s
OK    claude-sonnet-4-6/main  in=1.2k out=437 tok  end_turn  buffered  4.6s  proxy-retries=1
WARN  claude-fable-5/main  refusal -> retry with claude-opus-5  in=258.8k out=2.8k tok  40.7s
OK    claude-fable-5~>claude-opus-5/main  in=258.8k out=3.1k tok  end_turn  buffered  52.0s
FAIL  claude-sonnet-4-6/main  request_shape 400  0.3s
FAIL  claude-sonnet-4-6/main  http_401 401  0.1s
```

### Outcomes

| Tag | Meaning |
| --- | --- |
| `OK` | Response delivered (`buffered`, `live`, or `non-sse` for a non-stream `2xx` relayed as-is). |
| `RETRY` | Recoverable pre-commit failure; returned HTTP `503` with `Retry-After` so the client retries. |
| `WARN` | Pre-commit Claude refusal or safeguard error triggered model fallback (a subsequent line logs the fallback outcome), or the fallback failed and the original answer was delivered. |
| `FAIL` | Non-retryable error (the upstream's final answer, relayed unchanged, or an exhausted retry cap) returned to the client, or a terminal in-band error forwarded after commit (`delivered in-stream`). |
| `DROP` | Stream or body transfer failed *after* `200 OK` headers were already sent; the proxy aborted the connection and the client must handle recovery. |

### Log fields

| Field | Meaning |
| --- | --- |
| `model/main`, `model/sub`, `model/wf` | Requested model and caller type (`requested->resolved` if the upstream reports a different model ID). Gemini model names come from the URL path. |
| `high`, `medium`, etc. | Explicit effort level from Messages `output_config.effort`, Responses `reasoning.effort`, or Gemini `generationConfig.thinkingConfig.thinkingLevel` (`minimal`/`low`/`medium`/`high` in camelCase or `snake_case`, lowercased; never inferred from `thinkingBudget` or model defaults). |
| `in=… out=… tok` | Input (including cache read/creation) and output token counts. |
| `end_turn`, `completed`, `incomplete:max_output_tokens`, etc. | Upstream stop/finish reason. |
| `truncated_stream`, `http_401`, `request_shape`, etc. | Classified failure code before status normalization. `retryable_4xx_<status>` marks a `4xx` whose body named a transient fault; `upstream_redirect_<status>` an unfollowed redirect. |
| `529->503` | Upstream HTTP status `->` status sent to the client (one number when unchanged). |
| `retry-after=2s` | Backoff sent to the client in `Retry-After`. |
| `attempt=N` | Client retry index from `X-Stainless-Retry-Count` (omitted when 0). |
| `proxy-retries=N` | Extra pre-commit attempts made inside the proxy (`PROXY_TRANSACTIONAL_LOCAL_RETRIES`). |
| `prun=N` | Longest run of consecutive upstream SSE pings (high values indicate a content-silent gap). |
| `id=<hex>` | Archive correlation ID when `PROXY_REQUEST_LOG_DIR` is enabled. |

Caller suffixes (`/main`, `/sub`, `/wf`) are derived from Claude Code headers and the Workflow system-prompt marker; other clients appear as `/main`.

With `PROXY_VERBOSE=1`, `[local-retry]` lines log each internal retry cause, wait duration, and skips (such as `skip=not-enough-time`). Responses also carry `X-Steady-Proxy-Mode` on streamed deliveries and `X-Steady-Proxy-Reason` on error responses, including relayed upstream errors. A streaming request answered with a non-SSE `2xx` logs `non-sse <content-type>`; a terminal in-band error forwarded after commit logs `FAIL … delivered in-stream`.

## Saving request and response data

To capture full request and response payloads for debugging, set:

```dotenv
PROXY_REQUEST_LOG_DIR=./logs
```

The proxy writes one JSON archive per client request (e.g., `v1-messages-20260621t143005-a1b2c3d4.json`, matching `id=a1b2c3d4` in the access log). Local retries and model fallback attempts are recorded inside the same file's `attempts` array.

Archives use schema `steady-proxy.payload.v2` and include the client request envelope, redacted upstream URL, build metadata, per-attempt headers/bodies, retry delays, stream stats, and final outcome. Payloads are stored lossless in `data_base64` with `encoding`, `size`, and `sha256` metadata (unchanged retry bodies reference `request_body_ref`). Interrupted reads record `capture_error`.

Archiving is best-effort and never fails a proxied call; set `PROXY_VERBOSE=1` to see `[request-log]` write errors.

### Data handling

- **Network exposure:** The proxy has no authentication. Keep `PROXY_LISTEN_ADDR` bound to loopback (`127.0.0.1`).
- **Forwarded credentials & proxies:** Client auth headers (`Authorization`, `x-api-key`, `x-goog-api-key`, etc.) and query parameters (`key`) are forwarded to `PROXY_UPSTREAM_URL`. Upstream redirects are not followed or passed to the client (`502 upstream_redirect_<status>`); conditional GET/HEAD `304 Not Modified` responses are relayed with their cache metadata. Standard `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY` environment variables are honored. Upstream URL usernames/passwords are redacted in startup logs and archives.
- **Spool files:** Request bodies stay in memory; buffered responses over `PROXY_MAX_BUFFER_MEM_BYTES` spill to temporary files in `PROXY_SPOOL_DIR` and are unlinked immediately after the attempt. Point `PROXY_SPOOL_DIR` at `tmpfs` if spill files must not touch disk.
- **Archive sensitivity & retention:** Archives are **unencrypted**. Known credential headers and query params (`Authorization`, `x-api-key`, `x-goog-api-key`, `Proxy-Authorization`, `key`, `api_key`, `api_token`, `access_token`, `client_secret`) are redacted and listed under `redactions`, but **request and response bodies are stored raw** (base64-encoded). There is no automatic rotation or retention—disable `PROXY_REQUEST_LOG_DIR` once debugging is complete.

## Run as a user service

For a native Linux install, copy the `steady-proxy` binary to `/opt/steady-proxy/steady-proxy` and create `~/.config/systemd/user/steady-proxy.service`:

```ini
[Unit]
Description=steady-proxy for coding agents
After=network-online.target

[Service]
ExecStart=/opt/steady-proxy/steady-proxy
Environment=PROXY_UPSTREAM_URL=https://your-gateway.example.com
Environment=PROXY_LISTEN_ADDR=127.0.0.1:8789
Environment=PROXY_KEEPALIVE_MS=240000
Restart=always
RestartSec=2

[Install]
WantedBy=default.target
```

Enable and follow the service logs:

```bash
systemctl --user daemon-reload
systemctl --user enable --now steady-proxy
journalctl --user -u steady-proxy -f
```

To keep the service running when logged out, enable lingering: `loginctl enable-linger "$USER"`.
