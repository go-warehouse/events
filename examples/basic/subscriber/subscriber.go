// Package subscriber registers a handler for the producer's event type.
package subscriber

import (
	"context"
	"fmt"

	events "github.com/go-warehouse/events"

	"github.com/go-warehouse/events/examples/basic/producer"
)

// Register subscribes to NoteChanged events on the bus.
func Register(ctx context.Context, b *events.Bus) error {
	return events.Subscribe(b, ctx, func(e events.Event[producer.NoteChanged]) {
		fmt.Printf("subscriber received: name=%s id=%s path=%s\n", e.Name, e.ID, e.Data.Path)
	})
}
