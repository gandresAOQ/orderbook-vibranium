// Package memlog is the in-memory driven adapter for the port.EventLog: the
// ordered stream between the matching engine (producer) and downstream
// consumers (settlement, projections).
//
// In-memory it is a buffered channel; in production Publish would append to a
// partition (keyed by symbol to preserve per-instrument ordering) and consumers
// would read from a committed offset. The port interface is intentionally tiny
// so this can be swapped for Kafka/Kinesis without touching the core.
package memlog

import "github.com/meli/orderbook/internal/core/domain"

// Log is an in-process port.EventLog backed by a buffered channel.
type Log struct {
	ch chan domain.Event
}

// New creates an in-memory log with the given buffer capacity. The buffer
// absorbs bursts so the engine is never blocked by a slow consumer.
func New(buffer int) *Log {
	if buffer <= 0 {
		buffer = 1 << 16
	}
	return &Log{ch: make(chan domain.Event, buffer)}
}

// Publish appends an event to the stream.
func (l *Log) Publish(ev domain.Event) { l.ch <- ev }

// Events returns the read side of the stream.
func (l *Log) Events() <-chan domain.Event { return l.ch }

// Close closes the underlying channel.
func (l *Log) Close() { close(l.ch) }
