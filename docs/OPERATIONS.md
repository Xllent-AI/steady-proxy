# Running and observing steady-proxy

The [quick start](../README.md#quick-start) covers installation and client setup.
This guide covers routine operation, request logs, and payload capture.

## Docker Compose

Run these commands from the repository directory:

```bash
docker compose up -d --build
docker compose logs -f proxy
```

After changing `.env`, apply it with `docker compose up -d`. To stop the service,
run `docker compose down`.

Compose runs the container as `${UID:-1000}:${GID:-1000}` so archived files can be
owned by your host user. If your IDs differ, run `id -u` and `id -g`, then add
their numeric results as `UID` and `GID` in `.env`. Bash does not export its `UID`
variable by default and has no built-in `GID` variable. Exported values take
precedence over `.env`.

The host's `./logs` directory is mounted at `/logs` in the container. The container
working directory is `/`, so `PROXY_REQUEST_LOG_DIR=./logs` uses that mount.

## Check the running version

```bash
curl -fsS http://127.0.0.1:8789/__version
```

This returns build metadata without making a gateway request. It confirms that
the proxy is reachable, not that gateway authentication or model routing works.

To check the full Messages path, send a short prompt from Claude Code, or run the
following after exporting `ANTHROPIC_AUTH_TOKEN` and replacing `<your-model>` with
a model supported by your gateway. This makes a real, potentially billed request.

```bash
curl -sS http://127.0.0.1:8789/v1/messages \
  -H "authorization: Bearer $ANTHROPIC_AUTH_TOKEN" \
  -H "anthropic-version: 2023-06-01" \
  -H "content-type: application/json" \
  -d '{"model":"<your-model>","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"say hi"}]}'
```

## Reading the logs

Request outcome logs are always enabled. `PROXY_VERBOSE=1` adds internal retry
decisions and archive diagnostics. Each line has a date and time prefix; these
illustrative examples omit it:

```text
OK    claude-sonnet-4-6/main  high  in=1.2k out=437 tok  end_turn  buffered  3.41s
OK    gpt-5.6/main  high  in=1.1k out=223 tok  completed  live  2.37s
RETRY claude-sonnet-4-6/main  truncated_stream 502->503  retry-after=2s  0.9s
OK    claude-sonnet-4-6/main  in=1.2k out=437 tok  end_turn  buffered  4.6s  proxy-retries=1
WARN  claude-fable-5/main  refusal -> retry with claude-opus-5  in=258.8k out=2.8k tok  40.7s
FAIL  claude-sonnet-4-6/main  request_shape 400  0.3s
```

| Outcome | Meaning |
| --- | --- |
| `OK` | The proxy delivered the response. `buffered` or `live` tells you how. |
| `RETRY` | It returned a retryable error with a delay for the client. This does not prove the client retried. |
| `WARN` | It switched to the fallback model; the later outcome reports that attempt's result. |
| `FAIL` | It returned an error without requesting another attempt, usually because of an invalid request or the retry backstop. |
| `DROP` | The response had already started when delivery failed. The client must handle the incomplete stream. |

The fields after the outcome explain the request:

| Field | Meaning |
| --- | --- |
| `model/main`, `model/sub`, `model/wf` | Requested model and detected caller kind. `requested->resolved` shows a different model reported by the upstream. |
| `high` or another effort value | Reasoning effort requested through Messages `output_config.effort` or Responses `reasoning.effort`. |
| `in=… out=… tok` | Input and output token counts. Input includes cache reads and creation. |
| `end_turn`, `completed`, etc. | The reported stop reason. |
| `truncated_stream`, `sse_overloaded`, etc. | The classified failure code; use it to identify the actual cause. |
| `502->503` | Original or classified failure status followed by the status sent to the client. A single value means no status change. |
| `retry-after=2s` | Delay requested from the client. |
| `attempt=N` | Client retry count from `X-Stainless-Retry-Count`, shown when positive. |
| `proxy-retries=N` | Extra retries made inside the proxy under the configured local budget. |
| `prun=N` | Longest sequence of consecutive upstream ping events; a large value can indicate a gap in useful content. |
| `id=<hex>` | Correlation ID, present when payload archiving is enabled. |

Caller labels use Claude Code headers and the Workflow system-prompt marker.
Requests without those signals appear as `main`, including requests from other
applications; the label does not identify or restrict the client software.

Verbose `[local-retry]` lines include the cause, wait, and retry index.
`skip=not-enough-time` explains why another attempt could not fit the time budget.
The response headers also expose `X-Steady-Proxy-Mode` on streaming delivery and
`X-Steady-Proxy-Reason` on synthesized errors.

## Saving request and response data

Enable payload capture only when you need the exact requests and upstream replies:

```dotenv
PROXY_REQUEST_LOG_DIR=./logs
```

For Compose, add this to `.env` and run `docker compose up -d`. For the binary,
set it in the launch environment. The proxy attempts to write one JSON archive per
proxied client request, including non-streaming routes. Hidden retries and model
fallback appear as separate entries in the same archive's `attempts` array.

An archive filename looks like `v1-messages-20260621t143005-a1b2c3d4.json`. Its
suffix matches both the log's `id=a1b2c3d4` and the archive's top-level `id`. To
locate it, use `ls logs/*a1b2c3d4*.json` with the ID from your log.

Archives use schema `steady-proxy.payload.v2`. They contain the original client
request envelope, redacted upstream URL, proxy build metadata, upstream attempts,
response or transport failures, retry waits, stream statistics, model changes,
and final outcome. Bodies use base64 in `data_base64`, with `encoding`, byte
`size`, and `sha256` metadata. Repeated request bodies may be referenced through
`request_body_ref` instead of duplicated.

Captured bytes can be restored exactly, including binary and image payloads.
A failed or interrupted read may only capture part of a response; a
`capture_error` field records a read interruption when available. Error-body
capture obeys the upstream idle and request deadlines.

Archiving is best effort. It does not fail the proxied request if the directory
cannot be created or an archive cannot be written. Enable `PROXY_VERBOSE=1` to
see `[request-log]` errors when an expected archive is missing.

### Data handling

The proxy has no authentication of its own. Keep its listening port on loopback;
Compose's published port already is. Anyone with access to the port can relay
requests with credentials they supply and read `/__version`.

Client authorization and Anthropic headers are forwarded to `PROXY_UPSTREAM_URL`.
The proxy does not follow redirects. Its HTTP transport honors
`HTTP_PROXY`/`HTTPS_PROXY` and `NO_PROXY` in the proxy process environment, so a
configured forward proxy is also part of the connection path.

Request bodies are held in memory. Buffered responses spill beyond the configured
memory threshold to temporary files in `PROXY_SPOOL_DIR`; those files are unlinked
and closed when released. Use a tmpfs-backed spool directory if payloads must stay
off persistent disk.

Payload archives are different: they are persistent and **unencrypted**. Known
credential headers and query parameters are redacted, including `Authorization`,
`x-api-key`, `Proxy-Authorization`, `api_key`, `api_token`, `access_token`, and
`client_secret`. Redacted names are listed in `redactions`. Other headers and
parameters remain, and **bodies are not inspected or redacted**. Base64 is an
encoding, not encryption.

There is no archive rotation, size cap, or retention policy. Turn capture off
after reproducing a problem, or manage retention yourself. Keep archives in a
directory you control and out of version control; `/logs/` is already ignored.

## Run as a user service

For a native installation, place a built binary at the path used below, then
create `~/.config/systemd/user/steady-proxy.service`:

```ini
[Unit]
Description=steady-proxy for coding agents
After=network-online.target

[Service]
ExecStart=/opt/steady-proxy/steady-proxy
Environment=PROXY_UPSTREAM_URL=https://your-gateway.example.com
Environment=PROXY_LISTEN_ADDR=127.0.0.1:8789
Environment=PROXY_KEEPALIVE_MS=600000
Restart=always
RestartSec=2

[Install]
WantedBy=default.target
```

Replace the binary path and gateway URL for your installation, and configure the
client as shown in the README. Enable the service and read its logs with:

```bash
systemctl --user daemon-reload
systemctl --user enable --now steady-proxy
journalctl --user -u steady-proxy -f
```

To keep a user service running after logout, the account needs systemd lingering
enabled: `loginctl enable-linger "$USER"`. This may require administrator
authorization on your host.
