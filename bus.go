// Package events is a typed, in-process publish/subscribe bus. Subscribers
// receive events of their exact runtime type wrapped in Event with the ID
// the bus assigned at publish time. Delivery is asynchronous, FIFO per
// subscriber, and bounded. Stdlib only.
package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
)

// ErrClosed is returned by Subscribe and Publish after the bus is closed.
var ErrClosed = errors.New("events: bus closed")

// Event wraps published data with the metadata the bus assigned at publish
// time. Every subscriber of the same publish receives the same ID, so
// fan-out deliveries can be correlated. Name is the fully qualified
// runtime type name of T (e.g. "github.com/acme/app.NoteChanged") — the
// unambiguous dispatch token for consumers that cannot see Go types.
type Event[T any] struct {
	ID   string
	Name string
	Data T
}

// Bus is a typed, in-process publish/subscribe bus.
type Bus struct {
	mu     sync.Mutex
	subs   map[reflect.Type]map[*subscriber]struct{}
	opts   options
	closed bool
}

// New creates an open Bus.
func New(opts ...Option) *Bus {
	o := options{bufferSize: defaultBufferSize, logger: slog.Default(), id: uuid4}
	for _, opt := range opts {
		opt(&o)
	}
	if o.logger == nil {
		o.logger = slog.Default()
	}
	return &Bus{subs: make(map[reflect.Type]map[*subscriber]struct{}), opts: o}
}

// Subscribe registers h for events whose runtime type is exactly T,
// delivered asynchronously on a bus-owned goroutine that dies with ctx.
// Methods cannot carry type parameters, so Subscribe is a package function.
// T must be a concrete type — an interface type would never match a
// published value.
func Subscribe[T any](b *Bus, ctx context.Context, h func(Event[T])) error {
	if ctx == nil {
		return errors.New("events: subscribe: nil ctx")
	}
	if h == nil {
		return errors.New("events: subscribe: nil handler")
	}
	key := reflect.TypeFor[T]()
	if key.Kind() == reflect.Interface {
		return fmt.Errorf("events: subscribe: %s is an interface — subscribe with the concrete event type", key)
	}
	s := newSubscriber(key, ctx, h, b.opts.bufferSize, b.opts.logger)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	byType, ok := b.subs[s.key]
	if !ok {
		byType = make(map[*subscriber]struct{})
		b.subs[s.key] = byType
	}
	byType[s] = struct{}{}
	go s.run(b)
	return nil
}

// Publish assigns data a fresh event ID and delivers it to every
// subscriber of its exact runtime type. The id is generated before the bus
// lock is taken, so id generation never serializes other publishers.
func (b *Bus) Publish(ctx context.Context, data any) error {
	if data == nil {
		return errors.New("events: cannot publish nil event")
	}
	if ctx == nil {
		return errors.New("events: publish: nil ctx")
	}
	id, err := b.opts.id()
	if err != nil {
		return fmt.Errorf("events: generate event id: %w", err)
	}
	env := envelope{id: id, data: data}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	subs := b.subs[reflect.TypeOf(data)]
	if len(subs) == 0 {
		return nil
	}
	for _, d := range b.fanOutLocked(env, subs) {
		b.opts.logger.Warn("events: dropping oldest event", "type", d.key.String(), "id", d.id)
	}
	return nil
}

// drop records an evicted envelope so the warning can name what was
// actually lost.
type drop struct {
	key reflect.Type
	id  string
}

// fanOutLocked pushes env to every subscriber; callers hold b.mu. It
// returns the evicted envelopes so drops are logged outside the lock —
// a handler must never run under it, and neither should a user's logger.
func (b *Bus) fanOutLocked(env envelope, subs map[*subscriber]struct{}) []drop {
	drops := make([]drop, 0, len(subs))
	for s := range subs {
		evicted, dropped := s.push(env)
		if dropped {
			drops = append(drops, drop{key: s.key, id: evicted.id})
		}
	}
	return drops
}

// Close stops accepting publishes and signals every subscriber to drain
// its queue and exit. It never blocks on handlers, so a handler may call
// Close to shut the bus down from the inside — pair it with Wait to join.
// Idempotent; concurrent-safe with Subscribe and Publish.
func (b *Bus) Close() {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		for _, byType := range b.subs {
			for s := range byType {
				close(s.stop)
			}
		}
	}
	b.mu.Unlock()
}

// Wait blocks until every subscriber goroutine has drained its queue and
// exited. Progress is bounded by ctx — if it expires, Wait returns
// ctx.Err() while the bus still shuts down (a handler that never returns
// cannot be killed in Go; its goroutine exits once the handler does).
// Idempotent; concurrent-safe with Close.
func (b *Bus) Wait(ctx context.Context) error {
	if ctx == nil {
		return errors.New("events: wait: nil ctx")
	}
	b.mu.Lock()
	subs := make([]*subscriber, 0, len(b.subs))
	for _, byType := range b.subs {
		for s := range byType {
			subs = append(subs, s)
		}
	}
	b.mu.Unlock()

	for _, s := range subs {
		select {
		case <-s.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// remove unregisters a subscriber; called by its own goroutine on exit.
func (b *Bus) remove(s *subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	byType := b.subs[s.key]
	delete(byType, s)
	if len(byType) == 0 {
		delete(b.subs, s.key)
	}
}
