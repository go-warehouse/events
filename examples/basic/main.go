// Basic demonstrates the cross-package wiring the bus is designed for:
// the bus is created in main, a subscriber registers from its own package,
// and a producer publishes from another. Close drains, so the final
// delivery is guaranteed without any sleeps.
package main

import (
	"context"
	"log"

	events "github.com/go-warehouse/events"

	"github.com/go-warehouse/events/examples/basic/producer"
	"github.com/go-warehouse/events/examples/basic/subscriber"
)

func main() {
	ctx := context.Background()
	bus := events.New()

	if err := subscriber.Register(ctx, bus); err != nil {
		log.Fatal(err)
	}

	if err := producer.Emit(ctx, bus, "meetings/standup.md"); err != nil {
		log.Fatal(err)
	}

	// Close stops accepting publishes; Wait delivers any remaining queued
	// event and joins the subscriber goroutine — the print happens before
	// this returns.
	bus.Close()
	if err := bus.Wait(ctx); err != nil {
		log.Fatal(err)
	}
}
