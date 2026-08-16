# Contributing to events

Thanks for helping out. This is a small library, so the bar is simple:
small, focused changes with tests.

## Getting started

```sh
git clone git@github.com:go-warehouse/events.git
cd events
make help   # build · test · coverage
```

Go 1.26+ is required. There are no dependencies and the tests use no
external services — everything runs offline.

## Making changes

1. Open an issue describing the change first if it is more than a typo —
   discuss before coding anything architectural.
2. Branch from `main` with a short descriptive name (`fix/drop-log-id`,
   `feat/telemetry`).
3. Follow test-driven development: write the failing test, watch it fail,
   make it pass. Every behavior change ships with a test.
4. Match the existing style: stdlib only, small functions, `%w` error
   wrapping, table-driven tests, no package-level mutable globals.

## Gates (every commit must pass)

```sh
gofmt -l .                      # empty
go vet ./...
go test -race -failfast ./...   # race-clean
make coverage                   # >= 80% floor
golangci-lint run               # 0 issues
```

The test suite uses only real code — no mocks of the bus itself. The
concurrency tests must stay deterministic: no unbounded waits, and any
test that could block a handler must size its channels so `Close`/`Wait`
can always join.

## PR checklist

- Conventional commit title (`fix:`, `feat:`, `docs:`, …)
- Summary of the change and why
- Tests updated or added
- README updated if the public API or semantics changed
- All gates green

## API policy

The public API is `New`, `Subscribe`, `Publish`, `Close`, `Wait`, `Event`,
the two options, and `ErrClosed`. Changes to signatures or semantics are
breaking — they need an issue first and a clear migration note in the
README.

## Releases

Cutting a release is tagging and pushing — see
[RELEASING.md](RELEASING.md).
