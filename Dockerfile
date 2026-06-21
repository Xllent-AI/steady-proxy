# Build both the proxy and the test mock as static binaries.
FROM golang:1.26 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/cc-retry-proxy . \
 && CGO_ENABLED=0 go build -trimpath -o /out/testupstream ./testupstream

# Minimal runtime; distroless/static ships CA certs (needed for upstream TLS).
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/cc-retry-proxy /cc-retry-proxy
COPY --from=build /out/testupstream /testupstream
EXPOSE 8789
ENTRYPOINT ["/cc-retry-proxy"]
