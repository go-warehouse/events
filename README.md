# events

A small, typed, in-process publish/subscribe event bus Go library — stdlib
only, no dependencies.

Subscribers receive events of their **exact runtime type**, wrapped in an
`Event` carrying the ID the bus assigned at publish time. Delivery is
asynchronous, FIFO per subscriber, and bounded: queues never grow without
limit.

## Installation

```sh
go get github.com/go-warehouse/events
```

```go
import events "github.com/go-warehouse/events"
```

## Examples

`examples/basic/` demonstrates the cross-package wiring the bus is designed
for: the bus is created in `main`, a subscriber registers from its own
package, and a producer publishes from another — with `Close` + `Wait`
guaranteeing the final delivery. Run it with:

```sh
go run ./examples/basic
# or: make build && ./bin/basic
```

## Quick start

```go
package main

import (
	"context"
	"fmt"

	events "github.com/go-warehouse/events"
)

type NoteChanged struct{ Path string }

func main() {
	bus := events.New()
	ctx := context.Background()

	// Subscribe[T] is a package function — Go methods cannot carry type
	// parameters. T is inferred from the handler.
	if err := events.Subscribe(bus, ctx, func(e events.Event[NoteChanged]) {
		fmt.Println("changed:", e.Data.Path, "event id:", e.ID, "event name:", e.Name)
	}); err != nil {
		panic(err)
	}

	if err := bus.Publish(ctx, NoteChanged{Path: "meetings/standup.md"}); err != nil {
		panic(err)
	}

	// Close stops accepting publishes; Wait delivers any remaining queued
	// events and joins the subscriber goroutines.
	bus.Close()
	if err := bus.Wait(ctx); err != nil {
		panic(err)
	}
}
```

## Event IDs and names

`Publish` generates a UUIDv4 (via `crypto/rand`) for each event. Every
subscriber of the same publish receives the **same** ID, so fan-out
deliveries can be correlated.

`Name` is the fully qualified runtime type name of the published data
(`github.com/acme/app.NoteChanged`; bare `"int"` for builtins), stamped per
subscription at zero per-publish cost. It is the dispatch token for
consumers that can't see Go types — a wire protocol just writes
`{id, name, data}` into its frame. It is fully qualified so two packages
can both define a `Message` type without their events colliding on the
wire. Pointer/slice/map data falls back to the reflect string form.

## Semantics

| Rule | Behavior |
| --- | --- |
| Matching | Subscribers receive events whose runtime type is **exactly** `T`. Pointer and value types are distinct. Subscribe with the concrete type you publish — subscribing an interface type is rejected with an error. |
| Delivery | Asynchronous, on a per-subscription bus-owned goroutine — never on the publisher's goroutine. Handlers may publish reentrantly. |
| Ordering | FIFO per subscriber in acceptance order. No ordering across subscribers. |
| Fan-out | Every subscriber of the type receives each event. Duplicate subscriptions allowed. |
| Backpressure | `Publish` never blocks on subscriber queues: a full queue drops its **oldest** event and logs a warning naming the dropped event's id. |
| Bounds | Per-subscriber queue capacity defaults to 64 (`WithBufferSize`); values below 1 clamp to 1. No unbounded growth. |
| Lifecycle | Every event accepted by `Publish` is delivered — a subscription ctx cancel and bus `Close` both drain the queue before the goroutine exits. After `Close`, `Subscribe` and `Publish` return `ErrClosed`. |
| Panics | A panicking handler is recovered and logged; the panicked event is dropped and the subscription continues. |
| Close | `Close` stops accepting publishes and signals subscribers to drain and exit; it **never blocks on handlers**, so a handler may shut the bus down from the inside. `Wait(ctx)` joins the subscriber goroutines, bounded by its ctx. Both are idempotent. |
| Guards | `Subscribe` rejects nil ctx, nil handler, and interface type parameters; `Publish` rejects nil data and nil ctx; `Wait` rejects nil ctx. `WithLogger(nil)` falls back to the default logger. |

## Lifecycle

`New` → `Subscribe` (starts a goroutine) → `Publish` (fan-out) → cancel the
subscription ctx (unsubscribe, after draining queued events) or
`Close` + `Wait` (drain + join everything).

Unlike join-first designs, `Close` never waits for handlers — a handler
that calls `b.Close()` to shut the app down from the inside returns
immediately, and its own goroutine then drains and exits.

## Options

- `WithBufferSize(n int)` — per-subscriber queue capacity (default 64).
- `WithLogger(l *slog.Logger)` — logger for drop warnings and recovered
  handler panics (default `slog.Default()`).

## Non-goals

No persistence, replay, retry, cross-process transport, or topic strings.
For multiple processes or brokers, use Watermill or similar.

## Contributing

Small, focused changes with tests — see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
