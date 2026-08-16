// Package producer owns an event type and publishes it onto the bus.
package producer

import (
	"context"

	events "github.com/go-warehouse/events"
)

// NoteChanged reports that a wiki note was created or updated.
type NoteChanged struct{ Path string }

// Emit publishes a NoteChanged event.
func Emit(ctx context.Context, b *events.Bus, path string) error {
	return b.Publish(ctx, NoteChanged{Path: path})
}
