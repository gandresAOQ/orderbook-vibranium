// Package telemetry is the driven adapter for port.Metrics, backed by
// OpenTelemetry. It also provides a no-op implementation so observability can be
// switched off (or ignored in tests) without the core knowing.
package telemetry

import (
	"context"
	"time"

	"github.com/meli/orderbook/internal/core/domain"
)

// Noop discards every measurement. It is the default when telemetry is
// disabled, which keeps the call sites in the core unconditional: they never
// need a nil check or an `if enabled` branch.
type Noop struct{}

// NewNoop returns a metrics sink that does nothing.
func NewNoop() Noop { return Noop{} }

func (Noop) OrderPlaced(context.Context, domain.Side, domain.OrderStatus, time.Duration) {}
func (Noop) OrderRejected(context.Context, string)                                       {}
func (Noop) TradeExecuted(context.Context, domain.Trade)                                 {}
func (Noop) SettlementApplied(context.Context, domain.EventType, time.Duration, error)   {}
func (Noop) MoneySupply(context.Context, int64, int64)                                   {}
