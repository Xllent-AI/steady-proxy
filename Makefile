VERSION ?= $(shell sed -n '1p' VERSION 2>/dev/null || printf dev)
COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || printf unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
DIRTY ?= $(shell test -z "$$(git status --porcelain 2>/dev/null)" && printf false || printf true)

LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildDate=$(BUILD_DATE) -X main.dirty=$(DIRTY)

.PHONY: build docker-build test version

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o cc-retry-proxy .

docker-build:
	docker build \
		--build-arg VERSION="$(VERSION)" \
		--build-arg COMMIT="$(COMMIT)" \
		--build-arg BUILD_DATE="$(BUILD_DATE)" \
		--build-arg DIRTY="$(DIRTY)" \
		-t cc-retry-proxy:$(VERSION) \
		-t cc-retry-proxy:latest .

test:
	go test ./...

version: build
	./cc-retry-proxy --version
