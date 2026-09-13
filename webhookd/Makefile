GO ?= go

.PHONY: build test test-unit test-integration vet run receiver vectors tidy

build:
	$(GO) build ./...

test: test-unit test-integration

test-unit:
	$(GO) test ./internal/signature/ ./internal/retry/ ./internal/ssrf/

test-integration:
	$(GO) test ./internal/integration/ -timeout 300s

vet:
	$(GO) vet ./...

run:
	DATABASE_URL?=postgres://postgres:postgres@localhost:5432/webhookd?sslmode=disable \
	ALLOW_PRIVATE_HOSTS=127.0.0.1/32 \
	$(GO) run ./cmd/server

receiver:
	WEBHOOK_SECRET?=whsec_devsecret PORT?=9090 $(GO) run ./cmd/receiver

vectors:
	$(GO) run ./scripts/genvectors > internal/signature/testdata/vectors.json

tidy:
	$(GO) mod tidy
