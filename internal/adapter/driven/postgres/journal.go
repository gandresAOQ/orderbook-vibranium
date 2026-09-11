package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Journal is the durable port.EventJournal: the settlement idempotency ledger.
//
// It is what makes an at-least-once log safe for money. Redpanda/Kafka can
// redeliver an event (consumer restart, rebalance, uncommitted offset); the
// PRIMARY KEY on processed_events turns the second delivery into a no-op that
// settlement then skips.
//
// Unlike the in-memory journal this survives restarts, so a consumer that comes
// back up and re-reads from its last committed offset will not re-apply events
// it had already settled.
type Journal struct {
	pool *pgxpool.Pool
}

// NewJournal builds a Postgres-backed settlement journal.
func NewJournal(pool *pgxpool.Pool) *Journal {
	return &Journal{pool: pool}
}

// MarkProcessed records eventID, reporting whether it is new.
//
// The INSERT ... ON CONFLICT DO NOTHING is a single atomic statement: exactly
// one concurrent caller can insert a given event ID, and everyone else sees
// zero rows affected. That makes the check-and-set race-free without an
// explicit lock.
func (j *Journal) MarkProcessed(ctx context.Context, eventID string) (bool, error) {
	tag, err := j.pool.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`,
		eventID)
	if err != nil {
		return false, fmt.Errorf("mark event %s processed: %w", eventID, err)
	}
	return tag.RowsAffected() == 1, nil
}
