package events

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- helpers ----------------------------------------------------------------

// newTestBus returns a bus that is closed when the test ends, so no
// subscriber goroutine outlives its test.
func newTestBus(t *testing.T, opts ...Option) *Bus {
	t.Helper()
	b := New(opts...)
	t.Cleanup(func() { b.Close() })
	return b
}

func mustSubscribe[T any](t *testing.T, b *Bus, ctx context.Context, h func(Event[T])) {
	t.Helper()
	if err := Subscribe(b, ctx, h); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
}

func mustPublish(t *testing.T, b *Bus, data any) {
	t.Helper()
	if err := b.Publish(context.Background(), data); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func mustRecv[T any](t *testing.T, ch <-chan T, want T, msg string) {
	t.Helper()
	select {
	case v := <-ch:
		if !reflect.DeepEqual(v, want) {
			t.Fatalf("%s: got %v, want %v", msg, v, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: timed out waiting for %v", msg, want)
	}
}

func mustNoRecv[T any](t *testing.T, ch <-chan T, msg string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("%s: unexpected delivery %v", msg, v)
	default:
	}
}

func mustNotPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()
	f()
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}

func validUUIDv4(t *testing.T, s string) bool {
	t.Helper()
	parts := strings.Split(s, "-")
	if len(parts) != 5 || len(s) != 36 {
		return false
	}
	for i, p := range parts {
		if p == "" {
			t.Fatalf("malformed UUID %q: empty group %d", s, i)
		}
		for _, r := range p {
			if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
				return false
			}
		}
	}
	return parts[2][0] == '4' &&
		(parts[3][0] == '8' || parts[3][0] == '9' || parts[3][0] == 'a' || parts[3][0] == 'b')
}

// --- core behavior ----------------------------------------------------------

// TestSubscribePublish checks a single round trip: a subscribed handler
// receives the published value wrapped in an Event.
func TestSubscribePublish(t *testing.T) {
	b := newTestBus(t)
	got := make(chan int, 1)
	mustSubscribe(t, b, context.Background(), func(e Event[int]) { got <- e.Data })
	mustPublish(t, b, 42)
	mustRecv(t, got, 42, "round trip")
}

// TestAutoID checks the bus assigns a UUIDv4 id per publish and every
// subscriber of that event sees the same id (fan-out shares the id).
func TestAutoID(t *testing.T) {
	b := newTestBus(t)
	ids := make(chan string, 2)
	for range 2 {
		mustSubscribe(t, b, context.Background(), func(e Event[string]) { ids <- e.ID })
	}
	mustPublish(t, b, "ping")
	first := <-ids
	if !validUUIDv4(t, first) {
		t.Fatalf("id %q is not a UUIDv4", first)
	}
	mustRecv(t, ids, first, "fan-out id")
}

// TestEventName checks the envelope carries the fully qualified runtime
// type name of the published data, so consumers (and future wire
// protocols) can dispatch on it unambiguously — including two types that
// share an unqualified name across packages.
func TestEventName(t *testing.T) {
	type noteChanged struct{ Path string }

	b := newTestBus(t)
	notes := make(chan Event[noteChanged], 1)
	reminders := make(chan Event[int], 1)
	mustSubscribe(t, b, context.Background(), func(e Event[noteChanged]) { notes <- e })
	mustSubscribe(t, b, context.Background(), func(e Event[int]) { reminders <- e })
	mustPublish(t, b, noteChanged{Path: "a.md"})
	mustPublish(t, b, 1)

	e := <-notes
	if e.Name != "github.com/go-warehouse/events.noteChanged" {
		t.Fatalf("name %q, want fully qualified package name", e.Name)
	}
	if e.Data.Path != "a.md" {
		t.Fatalf("data %+v, want a.md", e.Data)
	}
	e2 := <-reminders
	if e2.Name != "int" || e2.Data != 1 {
		t.Fatalf("builtin event %+v, want name int data 1", e2)
	}
}

// TestIDGenerationError checks a failing id generator aborts the publish
// and delivers nothing.
func TestIDGenerationError(t *testing.T) {
	b := newTestBus(t)
	b.opts.id = func() (string, error) { return "", errors.New("entropy exhausted") }
	got := make(chan Event[int], 1)
	mustSubscribe(t, b, context.Background(), func(e Event[int]) { got <- e })
	if err := b.Publish(context.Background(), 1); err == nil {
		t.Fatal("publish with failing id generator: expected error")
	}
	mustNoRecv(t, got, "id failure")
}

// TestUUID4 checks the format and uniqueness of generated ids.
func TestUUID4(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for range 1000 {
		id, err := uuid4()
		if err != nil {
			t.Fatalf("uuid4: %v", err)
		}
		if !validUUIDv4(t, id) {
			t.Fatalf("id %q is not a UUIDv4", id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = struct{}{}
	}
}

// TestTypeIsolation checks subscribers only receive events of their exact
// type. Close + Wait drain everything, so emptiness is deterministic — no
// sleep-based assertion.
func TestTypeIsolation(t *testing.T) {
	b := newTestBus(t)
	got := make(chan int, 1)
	mustSubscribe(t, b, context.Background(), func(e Event[int]) { got <- e.Data })
	mustPublish(t, b, "not an int")
	b.Close()
	if err := b.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	mustNoRecv(t, got, "type isolation")
}

// TestFanOut checks every subscriber of a type receives each event.
func TestFanOut(t *testing.T) {
	b := newTestBus(t)
	chs := []chan int{make(chan int, 1), make(chan int, 1)}
	for _, ch := range chs {
		mustSubscribe(t, b, context.Background(), func(e Event[int]) { ch <- e.Data })
	}
	mustPublish(t, b, 7)
	for _, ch := range chs {
		mustRecv(t, ch, 7, "fan-out")
	}
}

// TestReentrantPublish checks a handler may publish without deadlocking the
// bus — delivery must never run synchronously on the publisher's goroutine.
func TestReentrantPublish(t *testing.T) {
	b := newTestBus(t)
	pong := make(chan string, 1)
	mustSubscribe(t, b, context.Background(), func(e Event[int]) {
		if err := b.Publish(context.Background(), "pong"); err != nil {
			t.Errorf("reentrant publish: %v", err)
		}
	})
	mustSubscribe(t, b, context.Background(), func(e Event[string]) { pong <- e.Data })
	mustPublish(t, b, 1)
	mustRecv(t, pong, "pong", "reentrant publish")
}

// TestFIFOOrder checks a single subscriber receives events in publish order.
func TestFIFOOrder(t *testing.T) {
	for _, n := range []int{1, 5, 128} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			// Buffer >= n so no event is dropped: this test isolates FIFO
			// ordering from the drop policy.
			b := newTestBus(t, WithBufferSize(n))
			got := make(chan int, n)
			mustSubscribe(t, b, context.Background(), func(e Event[int]) { got <- e.Data })
			for i := range n {
				mustPublish(t, b, i+1)
			}
			for want := range n {
				mustRecv(t, got, want+1, fmt.Sprintf("position %d", want+1))
			}
		})
	}
}

// TestQueueDropOldest checks the ring drops the oldest element when full
// and reports the evicted envelope.
func TestQueueDropOldest(t *testing.T) {
	tests := []struct {
		name    string
		cap     int
		push    []int
		evicted []string // "" = no drop
		popped  []int
	}{
		{"cap1", 1, []int{1, 2, 3}, []string{"", "id-1", "id-2"}, []int{3}},
		{"cap2", 2, []int{1, 2, 3, 4}, []string{"", "", "id-1", "id-2"}, []int{3, 4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := newQueue(tt.cap)
			for i, v := range tt.push {
				evicted, dropped := q.push(envelope{id: fmt.Sprintf("id-%d", v), data: v})
				if (evicted.id != "") != dropped {
					t.Fatalf("push %d: evicted %q, dropped %v", v, evicted.id, dropped)
				}
				if evicted.id != tt.evicted[i] {
					t.Fatalf("push %d: evicted %q, want %q", v, evicted.id, tt.evicted[i])
				}
			}
			for i, want := range tt.popped {
				v, ok := q.pop()
				if !ok {
					t.Fatalf("pop %d: queue empty, want %d", i, want)
				}
				if v.data != want {
					t.Fatalf("pop %d: got %d, want %d", i, v.data, want)
				}
			}
			if _, ok := q.pop(); ok {
				t.Fatal("queue not empty after expected pops")
			}
		})
	}
}

// TestFanOutReportsEvictedID checks the drop bookkeeping names the EVICTED
// event, not the surviving one — the drop warning must identify what was
// actually lost.
func TestFanOutReportsEvictedID(t *testing.T) {
	b := New()
	s := newSubscriber(reflect.TypeFor[int](), context.Background(), func(e Event[int]) {}, 1, slog.Default())
	subs := map[*subscriber]struct{}{s: {}}
	if drops := b.fanOutLocked(envelope{id: "id-A", data: 1}, subs); len(drops) != 0 {
		t.Fatalf("first push: unexpected drops %v", drops)
	}
	drops := b.fanOutLocked(envelope{id: "id-B", data: 2}, subs)
	if len(drops) != 1 || drops[0].id != "id-A" {
		t.Fatalf("evicted %v, want id-A", drops)
	}
}

// TestDropWarnsAndBounded checks a saturated bus drops oldest events with a
// warning and delivers an order-preserving subsequence. The handler is
// gated so the queue fills deterministically; exact values depend on how
// many events are in flight, so only the invariant is asserted.
func TestDropWarnsAndBounded(t *testing.T) {
	var logs bytes.Buffer
	b := newTestBus(t, WithBufferSize(1), WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	release := make(chan struct{})
	got := make(chan int, 300)
	mustSubscribe(t, b, context.Background(), func(e Event[int]) { <-release; got <- e.Data })
	const total = 200
	for i := range total {
		mustPublish(t, b, i+1)
	}
	close(release)
	delivered := make([]int, 0, total)
	for {
		select {
		case v := <-got:
			delivered = append(delivered, v)
			continue
		case <-time.After(500 * time.Millisecond):
		}
		break
	}
	if len(delivered) == 0 {
		t.Fatal("nothing delivered")
	}
	if len(delivered) > 2 { // cap 1 in queue + at most 1 in flight
		t.Fatalf("delivered %d events with buffer 1: %v", len(delivered), delivered)
	}
	for i := 1; i < len(delivered); i++ {
		if delivered[i] <= delivered[i-1] {
			t.Fatalf("delivery out of order: %v", delivered)
		}
	}
	if !strings.Contains(logs.String(), "dropping oldest event") {
		t.Fatal("no drop warning logged")
	}
}

// TestHandlerPanicRecovered checks a panicking handler is recovered, logged,
// and does not stop later deliveries.
func TestHandlerPanicRecovered(t *testing.T) {
	var logs bytes.Buffer
	b := newTestBus(t, WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	got := make(chan int, 1)
	calls := 0
	mustSubscribe(t, b, context.Background(), func(e Event[int]) {
		calls++
		if calls == 1 {
			panic("boom")
		}
		got <- e.Data
	})
	mustPublish(t, b, 1)
	mustPublish(t, b, 2)
	mustRecv(t, got, 2, "after panic")
	if !strings.Contains(logs.String(), "handler panicked") {
		t.Fatal("panic not logged")
	}
}

// TestSubscriberCtxCancel checks cancelling the subscription ctx unregisters
// the subscriber and stops NEW deliveries.
func TestSubscriberCtxCancel(t *testing.T) {
	b := newTestBus(t)
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan int, 1)
	mustSubscribe(t, b, ctx, func(e Event[int]) { got <- e.Data })
	mustPublish(t, b, 1)
	mustRecv(t, got, 1, "first delivery")
	cancel()
	waitFor(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.subs) == 0
	})
	mustPublish(t, b, 2)
	b.Close()
	if err := b.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	mustNoRecv(t, got, "after cancel")
}

// TestRunDrainsQueuedOnCancel checks the cancel exit path drains queued
// events before the goroutine ends — every event accepted by Publish is
// delivered, even when the subscription ctx fires. Enqueues bypass the
// wake token so only ctx.Done can be ready in run's select, making the
// cancel path deterministic.
func TestRunDrainsQueuedOnCancel(t *testing.T) {
	b := New()
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan int, 8)
	s := newSubscriber(reflect.TypeFor[int](), ctx, func(e Event[int]) { got <- e.Data }, 8, slog.Default())
	b.mu.Lock()
	b.subs[s.key] = map[*subscriber]struct{}{s: {}}
	b.mu.Unlock()
	s.q.push(envelope{id: "id-1", data: 1})
	s.q.push(envelope{id: "id-2", data: 2})
	s.q.push(envelope{id: "id-3", data: 3})
	cancel()
	go s.run(b)
	waitFor(t, func() bool {
		select {
		case <-s.done:
			return true
		default:
			return false
		}
	})
	for want := 1; want <= 3; want++ {
		mustRecv(t, got, want, "drain on cancel")
	}
	waitFor(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.subs) == 0
	})
}

// TestBufferSizeClamp checks sizes below 1 behave as 1: the bus never
// buffers unboundedly. With an unbounded (or default-sized) queue all three
// events would be delivered; a clamped buffer 1 can deliver at most 2
// (one in flight, one queued).
func TestBufferSizeClamp(t *testing.T) {
	for _, size := range []int{0, -5} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			b := newTestBus(t, WithBufferSize(size))
			release := make(chan struct{})
			got := make(chan int, 3)
			mustSubscribe(t, b, context.Background(), func(e Event[int]) { <-release; got <- e.Data })
			for i := range 3 {
				mustPublish(t, b, i+1)
			}
			close(release)
			delivered, last := 0, 0
			for {
				select {
				case v := <-got:
					if v <= last {
						t.Fatalf("delivery out of order: %d after %d", v, last)
					}
					last = v
					delivered++
					continue
				case <-time.After(500 * time.Millisecond):
				}
				break
			}
			if delivered == 0 || delivered > 2 {
				t.Fatalf("delivered %d of 3 events with clamped buffer 1", delivered)
			}
		})
	}
}

// --- guards -----------------------------------------------------------------

func TestPublishNil(t *testing.T) {
	b := newTestBus(t)
	if err := b.Publish(context.Background(), nil); err == nil {
		t.Fatal("publish nil: expected error")
	}
}

func TestPublishNilCtx(t *testing.T) {
	b := newTestBus(t)
	var nilCtx context.Context // deliberately nil — the guard must reject it
	var err error
	mustNotPanic(t, func() { err = b.Publish(nilCtx, 1) })
	if err == nil {
		t.Fatal("publish with nil ctx: expected error")
	}
}

func TestSubscribeNilCtx(t *testing.T) {
	b := newTestBus(t)
	var nilCtx context.Context // deliberately nil — the guard must reject it
	var err error
	mustNotPanic(t, func() { err = Subscribe(b, nilCtx, func(Event[int]) {}) })
	if err == nil {
		t.Fatal("subscribe with nil ctx: expected error")
	}
}

func TestSubscribeNilHandler(t *testing.T) {
	b := newTestBus(t)
	if err := Subscribe[int](b, context.Background(), nil); err == nil {
		t.Fatal("subscribe with nil handler: expected error")
	}
}

func TestSubscribeInterfaceRejected(t *testing.T) {
	b := newTestBus(t)
	err := Subscribe(b, context.Background(), func(e Event[fmt.Stringer]) {})
	if err == nil {
		t.Fatal("subscribe with interface type: expected error")
	}
	if !strings.Contains(err.Error(), "interface") {
		t.Fatalf("error %q does not explain the interface problem", err)
	}
}

func TestWaitNilCtx(t *testing.T) {
	b := New()
	b.Close()
	var nilCtx context.Context // deliberately nil — the guard must reject it
	var err error
	mustNotPanic(t, func() { err = b.Wait(nilCtx) })
	if err == nil {
		t.Fatal("wait with nil ctx: expected error")
	}
	_ = b.Wait(context.Background())
}

func TestWithLoggerNilDefaults(t *testing.T) {
	b := New(WithLogger(nil))
	if b.opts.logger == nil {
		t.Fatal("nil logger was kept; a panicking handler would crash the process")
	}
	b.Close()
}

// --- close ------------------------------------------------------------------

func TestPublishAfterClose(t *testing.T) {
	b := newTestBus(t)
	b.Close()
	if err := b.Publish(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after close: got %v, want ErrClosed", err)
	}
}

// TestPublishClosedBusWithCancelledCtx checks the closed state wins over a
// cancelled ctx: callers detecting shutdown via ErrClosed must not be
// misled by context.Canceled.
func TestPublishClosedBusWithCancelledCtx(t *testing.T) {
	b := newTestBus(t)
	b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Publish(ctx, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after close with cancelled ctx: got %v, want ErrClosed", err)
	}
}

func TestSubscribeAfterClose(t *testing.T) {
	b := newTestBus(t)
	b.Close()
	if err := Subscribe(b, context.Background(), func(Event[int]) {}); !errors.Is(err, ErrClosed) {
		t.Fatalf("subscribe after close: got %v, want ErrClosed", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	b := newTestBus(t)
	for range 2 {
		b.Close()
	}
}

func TestCloseDrains(t *testing.T) {
	b := New(WithBufferSize(32))
	release := make(chan struct{})
	got := make(chan int, 10)
	mustSubscribe(t, b, context.Background(), func(e Event[int]) { <-release; got <- e.Data })
	const total = 10
	for i := range total {
		mustPublish(t, b, i+1)
	}
	waited := make(chan error, 1)
	go func() {
		b.Close()
		waited <- b.Wait(context.Background())
	}()
	close(release)
	if err := <-waited; err != nil {
		t.Fatalf("wait: %v", err)
	}
	for want := range total {
		mustRecv(t, got, want+1, fmt.Sprintf("drained position %d", want+1))
	}
	b.mu.Lock()
	remaining := len(b.subs)
	b.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("%d subscribers remain after close", remaining)
	}
}

func TestWaitCtxExpiry(t *testing.T) {
	b := New()
	release := make(chan struct{})
	mustSubscribe(t, b, context.Background(), func(e Event[int]) { <-release })
	mustPublish(t, b, 1)
	b.Close()
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := b.Wait(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait with expired ctx: got %v, want DeadlineExceeded", err)
	}
	close(release)
	if err := b.Wait(context.Background()); err != nil {
		t.Fatalf("second wait: %v", err)
	}
}

// TestHandlerMayClose checks a handler can shut the bus down from the
// inside: Close never waits for handlers, so the subscriber goroutine that
// runs this handler is not joined by its own call.
func TestHandlerMayClose(t *testing.T) {
	b := New()
	called := make(chan struct{})
	mustSubscribe(t, b, context.Background(), func(e Event[int]) {
		b.Close()
		close(called)
	})
	mustPublish(t, b, 1)
	mustRecv(t, called, struct{}{}, "handler close")
	if err := b.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if err := b.Publish(context.Background(), 2); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after handler close: got %v, want ErrClosed", err)
	}
}

// TestCloseNotBlockedByIDGeneration checks Publish never holds the bus lock
// while generating an id: Close must complete even while a publish is
// stuck generating one.
func TestCloseNotBlockedByIDGeneration(t *testing.T) {
	b := New()
	release := make(chan struct{})
	b.opts.id = func() (string, error) { <-release; return "x", nil }
	done := make(chan error, 1)
	go func() { done <- b.Publish(context.Background(), 1) }()
	b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := b.Wait(ctx); err != nil {
		t.Fatalf("wait blocked behind id generation: %v", err)
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after close: got %v, want ErrClosed", err)
	}
}

func TestPublishCancelledCtx(t *testing.T) {
	b := newTestBus(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Publish(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("publish with cancelled ctx: got %v, want Canceled", err)
	}
}

func TestPublishNoSubscribers(t *testing.T) {
	b := newTestBus(t)
	if err := b.Publish(context.Background(), "unsubscribed"); err != nil {
		t.Fatalf("publish with no subscribers: %v", err)
	}
}

// --- concurrency ------------------------------------------------------------

func TestConcurrentPublishRace(t *testing.T) {
	const pub, per = 8, 250

	t.Run("buffer enough", func(t *testing.T) {
		b := New(WithBufferSize(pub * per))
		got := make(chan int, pub*per)
		mustSubscribe(t, b, context.Background(), func(e Event[int]) { got <- e.Data })
		var wg sync.WaitGroup
		for g := range pub {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := range per {
					if err := b.Publish(context.Background(), g*per+i+1); err != nil {
						t.Errorf("publish %d/%d: %v", g, i+1, err)
						return
					}
				}
			}(g)
		}
		wg.Wait()
		b.Close()
		if err := b.Wait(context.Background()); err != nil {
			t.Fatalf("wait: %v", err)
		}
		close(got)
		seen := make(map[int]struct{}, pub*per)
		for v := range got {
			seen[v] = struct{}{}
		}
		if len(seen) != pub*per {
			t.Fatalf("delivered %d of %d events", len(seen), pub*per)
		}
	})

	t.Run("buffer small", func(t *testing.T) {
		b := New(WithBufferSize(16))
		// Sized for every event: the handler must never block, or Close
		// joins a handler waiting on this channel and the test deadlocks.
		got := make(chan int, pub*per)
		mustSubscribe(t, b, context.Background(), func(e Event[int]) { got <- e.Data })
		var wg sync.WaitGroup
		for g := range pub {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := range per {
					_ = b.Publish(context.Background(), g*per+i+1)
				}
			}(g)
		}
		wg.Wait()
		b.Close()
		if err := b.Wait(context.Background()); err != nil {
			t.Fatalf("wait: %v", err)
		}
		close(got)
		last := make(map[int]int, pub)
		for v := range got {
			g := (v - 1) / per
			if v <= last[g] {
				t.Fatalf("publisher %d out of order: %d after %d", g, v, last[g])
			}
			last[g] = v
		}
	})

	t.Run("publish vs close", func(t *testing.T) {
		b := New()
		mustSubscribe(t, b, context.Background(), func(Event[int]) {})
		results := make(chan error, pub+1)
		go func() {
			b.Close()
			results <- b.Wait(context.Background())
		}()
		for range pub {
			go func() { results <- b.Publish(context.Background(), 1) }()
		}
		for range pub + 1 {
			select {
			case err := <-results:
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Fatalf("unexpected error: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("publish or wait blocked")
			}
		}
	})
}
