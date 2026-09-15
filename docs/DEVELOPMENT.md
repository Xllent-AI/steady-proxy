# Development

steady-proxy requires Go 1.23 or later and uses only the standard library.
Docker is optional for native development; the live CLI harness requires it.

## Build and run

From a checkout of the repository:

```bash
make build
./steady-proxy --version
PROXY_UPSTREAM_URL=https://your-gateway.example.com ./steady-proxy
```

The binary listens on `127.0.0.1:8789` by default. It reads environment variables
directly and does not load `.env`. The README includes setup for
[Claude Code](../README.md#claude-code), [Codex](../README.md#codex), and
[other API clients](../README.md#other-api-clients).

`make build` stamps the binary with `VERSION`, the Git commit, build date, and
working-tree status. To build a versioned Docker image with explicit build
metadata, run:

```bash
make docker-build
```

This tags both `steady-proxy:<VERSION>` and `steady-proxy:latest`. Ordinary
`docker compose up -d --build` also records the version, commit from the minimal
Git metadata in its build context, and build timestamp. The running proxy exposes
that information at `/__version`.

For version bumps, local release previews, and tag-triggered publishing, see
[releasing](RELEASING.md).

## Tests and checks

```bash
go vet ./...
go build ./...
make test
```

`make test` runs `go test -race ./...`, including unit and integration tests.
CI also checks `gofmt` and builds the Docker image. Use focused tests for the
behavior you change, then run the relevant broader checks before submitting it.

### Live client harness

[`test/live.sh`](../test/live.sh) runs a real `claude -p` client through a
dockerized proxy against a mock gateway with controlled failures. It requires
Docker with Compose v2, `curl`, `claude`, and GNU `timeout` on `PATH`. On macOS,
install GNU coreutils and add its `gnubin` directory to `PATH` so the command is
available as `timeout`. The default run takes about 10 minutes, including a
generation longer than 300 seconds:

```bash
./test/live.sh
```

To skip the long-generation case:

```bash
DO_LONG=0 ./test/live.sh
```

The harness uses a separate Compose project and test image, random loopback ports,
and isolated client settings. By default, it makes no real model requests. It
removes its test project on exit or interruption and retains failed-run logs in
the temporary directory it prints.

To add one real, potentially billed gateway call, export `ANTHROPIC_AUTH_TOKEN`
and run:

```bash
DO_REAL=1 PROXY_UPSTREAM_URL=https://your-gateway.example.com ./test/live.sh
```

## Finding the code

Read the [architecture guide](ARCHITECTURE.md) for the request lifecycle and
[troubleshooting](TROUBLESHOOTING.md) for expected failure behavior.

| File | Responsibility |
| --- | --- |
| [`main.go`](../main.go) | Configuration, routing, deadlines, local retries, fallback, and access logs. |
| [`wire.go`](../wire.go) | Shared protocol interface and Anthropic stream behavior. |
| [`responses.go`](../responses.go) | OpenAI Responses completion, errors, keepalives, and replay. |
| [`sse.go`](../sse.go) | SSE capture, buffering, validation, tool JSON, and the transition to live output. |
| [`classify.go`](../classify.go) | Failure classification and client retry responses. |
| [`transport.go`](../transport.go) | Upstream deadline and byte-idle handling. |
| [`requestlog.go`](../requestlog.go) | Payload archives, body capture, and credential redaction. |
| [`version.go`](../version.go) | CLI and HTTP version reporting. |
| [`testupstream/main.go`](../testupstream/main.go) | Mock gateway used by the live harness. |

Protocol implementations share the buffering and retry engine through `wireModel`.
When changing one, check both buffered and live delivery: an error before commit
can become an HTTP retry, while an error after commit must terminate the stream.
