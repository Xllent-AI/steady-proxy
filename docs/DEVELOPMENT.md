# Development

`steady-proxy` requires Go 1.23+ and uses only the Go standard library (`go.mod` has no external dependencies). Docker with Compose v2 is only needed for container builds and the live CLI test harness.

## Build and run

```bash
make build
./steady-proxy --version
PROXY_UPSTREAM_URL=https://your-gateway.example.com ./steady-proxy
```

The binary listens on `127.0.0.1:8789` by default and reads `PROXY_*` variables directly from the process environment (it does not load `.env`). See the [README](../README.md#client-setup) for client configuration.

`make build` embeds `VERSION`, the Git commit, build timestamp, and working-tree status into the binary (exposed via `--version` and `GET /__version`). To build a Docker image with version metadata:

```bash
make docker-build
```

This tags both `steady-proxy:<VERSION>` and `steady-proxy:latest`. For release packaging and publishing, see [Releasing](RELEASING.md).

## Tests and checks

```bash
go vet ./...
go build ./...
make test
```

`make test` runs `go test -race ./...` across all unit and integration suites. CI also enforces `gofmt`, runs `make release-test`, and verifies the Docker build.

### Live client harness

[`test/live.sh`](../test/live.sh) exercises a real `claude -p` CLI client through a containerized proxy against a fault-injecting mock gateway (`testupstream`). It requires Docker with Compose v2, `curl`, `claude`, and GNU `timeout` (on macOS, install `coreutils` and add `gnubin` to `PATH`).

```bash
./test/live.sh              # full suite (~10m, includes a >300s long turn)
DO_LONG=0 ./test/live.sh    # skip the 300s+ long-turn case
```

The harness runs in an isolated Compose project on random loopback ports, makes no real upstream API calls by default, and cleans up containers on exit (retaining logs in a temporary directory on failure).

To include one live (billed) gateway test, export `ANTHROPIC_AUTH_TOKEN` and run:

```bash
DO_REAL=1 PROXY_UPSTREAM_URL=https://your-gateway.example.com ./test/live.sh
```

## Finding the code

All production code lives in `package main`:

| File | Responsibility |
| --- | --- |
| [`main.go`](../main.go) | Config parsing, HTTP routing, request deadlines, local retry loop, refusal fallback, and access logging. |
| [`wire.go`](../wire.go) | Shared `wireModel` protocol interface, route matching, and Anthropic stream interpretation. |
| [`responses.go`](../responses.go) | OpenAI Responses stream completion, error handling, `response.proxy_keepalive`, and delta coalescing. |
| [`gemini.go`](../gemini.go) | Native Gemini SSE candidate tracking, usage extraction, and replay. |
| [`gemini_errors.go`](../gemini_errors.go) | Google numeric/canonical error classification, `RetryInfo` parsing, and error envelopes. |
| [`sse.go`](../sse.go) | Shared SSE capture engine (`captureSSECore`), memory/disk spooling, JSON validation, tool-input normalization, and live cutover. |
| [`classify.go`](../classify.go) | Error classification and `503` retry response rendering. |
| [`transport.go`](../transport.go) | Upstream HTTP transport, header timeouts, and byte-idle reader. |
| [`requestlog.go`](../requestlog.go) | Opt-in JSON payload archiving and credential redaction. |
| [`version.go`](../version.go) | `--version` flag and `/__version` endpoint. |
| [`testupstream/main.go`](../testupstream/main.go) | Deterministic fault-injecting mock gateway used by `test/live.sh`. |

When modifying protocol behavior, check both buffered (`buffered()`) and post-commit (`live()`) paths: pre-commit failures can be discarded and retried as HTTP `503`, whereas post-commit failures must terminate the stream cleanly.
