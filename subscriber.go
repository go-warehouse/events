package events

import (
	"context"
	"log/slog"
	"reflect"
	"sync"
)

// envelope is the internal queue payload: the event ID assigned by Publish
// plus the published data.
type envelope struct {
	id   string
	data any
}

// subscriber is one handler registration. A bus-owned goroutine drains its
// queue and invokes the handler outside any bus lock, so handlers may
// publish freely.
type subscriber struct {
	key  reflect.Type
	h    func(envelope)
	log  *slog.Logger
	ctx  context.Context
	q    *queue
	sig  chan struct{} // buffered-1 wake token
	stop chan struct{} // closed by Close
	done chan struct{} // closed when the goroutine exits
}

func newSubscriber[T any](key reflect.Type, ctx context.Context, h func(Event[T]), buf int, log *slog.Logger) *subscriber {
	name := typeName(key)
	return &subscriber{
		key: key,
		h: func(env envelope) {
			h(Event[T]{ID: env.id, Name: name, Data: env.data.(T)})
		},
		log:  log,
		ctx:  ctx,
		q:    newQueue(buf),
		sig:  make(chan struct{}, 1),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

// typeName returns an unambiguous name for t: the fully qualified form for
// named types ("github.com/acme/app.NoteChanged"), the bare name for
// builtins ("int"), and the reflect string form for everything else
// (pointer/slice/map types have no package path).
func typeName(t reflect.Type) string {
	if name := t.Name(); name != "" {
		if pkg := t.PkgPath(); pkg != "" {
			return pkg + "." + name
		}
		return name
	}
	return t.String()
}

// queue is a fixed-capacity FIFO ring of envelopes. push evicts the oldest
// element when full and returns it with dropped=true.
type queue struct {
	mu    sync.Mutex
	buf   []envelope
	head  int
	count int
}

func newQueue(cap int) *queue {
	if cap < 1 {
		cap = 1
	}
	return &queue{buf: make([]envelope, cap)}
}

func (q *queue) push(v envelope) (evicted envelope, dropped bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.count == len(q.buf) {
		evicted = q.buf[q.head]
		q.buf[q.head] = v
		q.head = (q.head + 1) % len(q.buf)
		return evicted, true
	}
	q.buf[(q.head+q.count)%len(q.buf)] = v
	q.count++
	return envelope{}, false
}

func (q *queue) pop() (envelope, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.count == 0 {
		return envelope{}, false
	}
	v := q.buf[q.head]
	q.buf[q.head] = envelope{}
	q.head = (q.head + 1) % len(q.buf)
	q.count--
	return v, true
}

// run delivers queued events until ctx is cancelled (unsubscribe) or stop
// is closed (bus Close). Both exit paths drain first: every event accepted
// by Publish is delivered, no matter which path ends the goroutine.
func (s *subscriber) run(b *Bus) {
	defer close(s.done)
	for {
		select {
		case <-s.sig:
			s.drain()
		case <-s.ctx.Done():
			s.drain()
			b.remove(s)
			return
		case <-s.stop:
			s.drain()
			b.remove(s)
			return
		}
	}
}

// drain pops and delivers until the queue is empty, so a single wake token
// covers a whole burst of publishes.
func (s *subscriber) drain() {
	for {
		v, ok := s.q.pop()
		if !ok {
			return
		}
		s.deliver(v)
	}
}

// deliver invokes the handler, recovering panics so one bad handler cannot
// kill the subscription's goroutine. The panicked event is dropped.
func (s *subscriber) deliver(v envelope) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("events: handler panicked", "type", s.key.String(), "panic", r)
		}
	}()
	s.h(v)
}

// push appends an event and wakes the goroutine if it is idle. When the
// queue is full it returns the evicted envelope with dropped=true.
func (s *subscriber) push(v envelope) (evicted envelope, dropped bool) {
	evicted, dropped = s.q.push(v)
	select {
	case s.sig <- struct{}{}:
	default:
	}
	return evicted, dropped
}
