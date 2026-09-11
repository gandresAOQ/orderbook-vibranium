package memstore

import (
	"context"
	"sync"
)

// Journal is an in-memory port.EventJournal: a set of already-applied event IDs.
//
// With the in-process channel log an event is delivered exactly once, so this
// journal never actually rejects anything. It exists so the settlement service
// has a single code path, and it becomes meaningful the moment the log is backed
// by Kafka/Redpanda (at-least-once delivery). Being in-memory, it is reset on
// restart — the durable equivalent is the Postgres journal.
type Journal struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewJournal creates an empty journal.
func NewJournal() *Journal {
	return &Journal{seen: map[string]struct{}{}}
}

// MarkProcessed records eventID, reporting whether it is new.
func (j *Journal) MarkProcessed(_ context.Context, eventID string) (bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, dup := j.seen[eventID]; dup {
		return false, nil
	}
	j.seen[eventID] = struct{}{}
	return true, nil
}
