# go-events — typed in-process pub/sub bus.
# Run `make help` for the self-documenting target list.

.DEFAULT_GOAL := help

help: ## list available targets with descriptions
.PHONY: help
help:
	@awk -F':.*## ' '/^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

BIN_DIR := bin

build: ## compile the library and put the example binary into bin/
.PHONY: build
build:
	go build ./...
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/basic ./examples/basic

test: ## unit tests, fail-fast (includes example compilation)
.PHONY: test
test:
	go test -failfast ./...

coverage: ## coverage gate (>= 80%) on the library package
.PHONY: coverage
coverage:
	mkdir -p coverage
	go test -failfast -coverprofile=coverage/coverage.out .
	go tool cover -func=coverage/coverage.out | tail -1
	@go tool cover -func=coverage/coverage.out | awk -F'\t' '/^total:/ { gsub(/%/,"",$$NF); if ($$NF+0 < 80) { print "FAIL: coverage below 80% floor"; exit 1 } }'
