// Package port declares the interfaces (ports) that connect the application
// core to the outside world, following the hexagonal (ports & adapters) style.
//
//   - Outbound (driven) ports: what the core NEEDS from infrastructure
//     (matching, persistence, messaging). Implemented by adapters in
//     internal/adapter/driven/*.
//   - Inbound (driving) ports: what the core OFFERS to the outside
//     (place/cancel orders, read market data, manage wallets). Implemented by
//     the application services and called by adapters in
//     internal/adapter/driving/*.
//
// The core depends only on these interfaces, never on concrete infrastructure.
//
// Repository methods take a context and return an error because the adapters
// behind them may be remote (Postgres, Redis): an I/O failure on a money path
// must be surfaced, never silently swallowed.
package port

import (
	"context"

	"github.com/meli/orderbook/internal/core/domain"
)

// MatchingEngine serializes and matches orders against a single order book.
// The concrete adapter owns the domain.OrderBook and runs the single-goroutine
// loop that guarantees deterministic ordering.
type MatchingEngine interface {
	// Place submits an already-validated, already-reserved order and blocks
	// until matched, returning the taker's resulting snapshot.
	Place(ctx context.Context, o *domain.Order) (*domain.Order, error)
	// Cancel removes a resting order by ID.
	Cancel(ctx context.Context, orderID string) (*domain.Order, error)
	// Depth returns a snapshot of the top `depth` price levels per side.
	Depth(ctx context.Context, depth int) (domain.BookSnapshot, error)
}

// EventPublisher appends events to the ordered log. Called only by the single
// engine goroutine so sequence order is preserved.
type EventPublisher interface {
	Publish(domain.Event)
}

// EventLog is the full ordered event stream: publish + subscribe + close.
// In-memory it is a buffered channel; in production it is a partitioned,
// durable log (e.g. Kafka/Redpanda keyed by symbol).
type EventLog interface {
	EventPublisher
	// Events returns the channel consumers range over.
	Events() <-chan domain.Event
	// Close signals that no more events will be published and flushes.
	Close()
}

// EventJournal records which events have already been applied by settlement.
// It is what turns an at-least-once event log into effectively-once money
// movement: a redelivered event is recognized and skipped.
type EventJournal interface {
	// MarkProcessed records eventID as handled. It returns true when the event
	// is new (and must be applied) and false when it was already recorded
	// (a duplicate redelivery that must be skipped).
	MarkProcessed(ctx context.Context, eventID string) (bool, error)
}

// WalletRepository persists user balances (the reserve/settle/release ledger).
type WalletRepository interface {
	// Get returns a copy of the wallet.
	Get(ctx context.Context, userID string) (domain.Wallet, error)
	// Upsert creates or replaces a wallet (used for seeding).
	Upsert(ctx context.Context, w domain.Wallet) error
	// Update applies fn to the wallet atomically. fn mutates in place; if it
	// returns an error the change is rolled back. Implementations must
	// serialize concurrent updates to the same wallet (in-process lock, or a
	// row-level lock in the database).
	Update(ctx context.Context, userID string, fn func(*domain.Wallet) error) error
	// List returns copies of all wallets (used for reconciliation).
	List(ctx context.Context) ([]domain.Wallet, error)
}

// TradeRepository is the append-only trade history — the traceability backbone.
type TradeRepository interface {
	Append(ctx context.Context, t domain.Trade) error
	// Recent returns up to `limit` most recent trades (newest first).
	// limit <= 0 returns all.
	Recent(ctx context.Context, limit int) ([]domain.Trade, error)
	Count(ctx context.Context) (int, error)
}

// OrderRepository is the order-status projection, queried by clients without
// touching the engine's hot in-memory book.
type OrderRepository interface {
	Upsert(ctx context.Context, o domain.Order) error
	Get(ctx context.Context, orderID string) (domain.Order, bool, error)
}
