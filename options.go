package events

import "log/slog"

// defaultBufferSize is the per-subscriber queue capacity when WithBufferSize
// is not given.
const defaultBufferSize = 64

// options holds the bus configuration applied by New. Immutable after New.
type options struct {
	bufferSize int
	logger     *slog.Logger
	id         func() (string, error)
}

// Option configures a Bus; applied by New.
type Option func(*options)

// WithBufferSize sets the per-subscriber queue capacity (default 64).
// Values below 1 are clamped to 1 — the bus never buffers unboundedly.
func WithBufferSize(n int) Option {
	return func(o *options) { o.bufferSize = n }
}

// WithLogger sets the logger for drop warnings and recovered handler
// panics (default slog.Default()).
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}
