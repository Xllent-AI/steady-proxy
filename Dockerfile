# Build both the proxy and the test mock as static binaries.
FROM golang:1.26 AS build
WORKDIR /src
COPY . .
ARG VERSION
ARG COMMIT
ARG BUILD_DATE
ARG DIRTY
RUN set -eu; \
    v="${VERSION:-$(cat VERSION 2>/dev/null || printf dev)}"; \
    c="${COMMIT:-}"; \
    if [ -z "$c" ] && [ -f .git/HEAD ]; then \
      head="$(cat .git/HEAD)"; \
      case "$head" in \
        ref:*) \
          ref="${head#ref: }"; \
          c="$(cat ".git/$ref" 2>/dev/null || true)"; \
          if [ -z "$c" ] && [ -f .git/packed-refs ]; then \
            c="$(awk -v r="$ref" '$1 !~ /^#/ && $2 == r {print $1; exit}' .git/packed-refs)"; \
          fi; \
          ;; \
        *) c="$head" ;; \
      esac; \
    fi; \
    d="${BUILD_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"; \
    dirty_flag="${DIRTY:-unknown}"; \
    ldflags="-s -w -X main.version=$v -X main.commit=$c -X main.buildDate=$d -X main.dirty=$dirty_flag"; \
    CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags "$ldflags" -o /out/steady-proxy .; \
    CGO_ENABLED=0 go build -trimpath -buildvcs=false -o /out/testupstream ./testupstream

# Minimal runtime; distroless/static ships CA certs (needed for upstream TLS).
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/steady-proxy /steady-proxy
COPY --from=build /out/testupstream /testupstream
COPY --from=build /src/LICENSE /LICENSE
# Relative paths in env (e.g. PROXY_REQUEST_LOG_DIR=./logs) resolve against /,
# where docker-compose.yml bind-mounts ./logs.
WORKDIR /
# Never run as root: `docker run` without --user gets distroless's nonroot
# (uid 65532); docker-compose.yml overrides this with the host user so
# PROXY_REQUEST_LOG_DIR files are owned by you. /tmp (the default spool dir)
# is world-writable in this image.
USER nonroot:nonroot
EXPOSE 8789
ENTRYPOINT ["/steady-proxy"]
