package port

import (
	"context"
	"time"

	"github.com/meli/orderbook/internal/core/domain"
)

// Metrics is the outbound port for observability.
//
// Why a port instead of calling OpenTelemetry directly
// ----------------------------------------------------
// The core speaks in domain events ("an order was placed", "a trade settled"),
// not in instrument types ("increment counter orderbook_orders_total"). Keeping
// that vocabulary in a port has three practical benefits:
//
//  1. The core stays free of vendor SDKs, exactly like it stays free of HTTP and
//     SQL. Swapping OpenTelemetry for anything else touches one adapter.
//  2. Tests run against the no-op implementation with zero setup.
//  3. The metric names and label cardinality are decided in ONE place (the
//     adapter), which is what keeps a high-throughput system from accidentally
//     exploding a time-series database.
//
// Implementations must be safe for concurrent use and must never block: they are
// called from the request path and from the single-writer engine goroutine, where
// a slow observer would become a throughput problem.
type Metrics interface {
	// OrderPlaced records the outcome of an order submission, including the
	// latency of the whole accept path (validation, reservation, matching).
	OrderPlaced(ctx context.Context, side domain.Side, status domain.OrderStatus, d time.Duration)

	// OrderRejected records an order refused before it reached the book, with a
	// low-cardinality reason (insufficient funds, validation, unavailable).
	OrderRejected(ctx context.Context, reason string)

	// TradeExecuted records a matched trade and the value it moved.
	TradeExecuted(ctx context.Context, t domain.Trade)

	// SettlementApplied records the result of applying one event, plus its lag:
	// the delay between the engine emitting it and settlement applying it. Lag is
	// the health signal for the asynchronous half of the pipeline.
	SettlementApplied(ctx context.Context, evType domain.EventType, lag time.Duration, err error)

	// MoneySupply reports the total COP and Vibranium across all wallets
	// (available + locked). The order book must conserve value, so these gauges
	// are expected to be FLAT: any movement means money was created or destroyed
	// and is the single most important alert in the system.
	MoneySupply(ctx context.Context, cop, vibranium int64)
}
