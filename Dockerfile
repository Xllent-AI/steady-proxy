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
    CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags "$ldflags" -o /out/cc-retry-proxy .; \
    CGO_ENABLED=0 go build -trimpath -buildvcs=false -o /out/testupstream ./testupstream

# Minimal runtime; distroless/static ships CA certs (needed for upstream TLS).
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/cc-retry-proxy /cc-retry-proxy
COPY --from=build /out/testupstream /testupstream
EXPOSE 8789
ENTRYPOINT ["/cc-retry-proxy"]
