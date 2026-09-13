// Package service holds the application core's use cases. Services implement
// the inbound (driving) ports and orchestrate the domain plus the outbound
// (driven) ports. They contain no transport or storage concerns.
package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/meli/orderbook/internal/core/domain"
	"github.com/meli/orderbook/internal/core/port"
)

// Trading implements port.TradingService.
//
// It owns the critical orchestration that keeps concurrent bursts safe:
// reserve funds atomically (the risk check that prevents double-spend) BEFORE
// the order is allowed into the matching engine, and run a compensating
// release if the engine cannot accept it.
type Trading struct {
	symbol  string
	engine  port.MatchingEngine
	wallets port.WalletRepository
	orders  port.OrderRepository
	metrics port.Metrics
	now     func() time.Time
}

// NewTrading builds the trading use case. `metrics` may be a no-op sink; it is
// never nil, so the call sites below stay unconditional.
func NewTrading(
	symbol string,
	engine port.MatchingEngine,
	wallets port.WalletRepository,
	orders port.OrderRepository,
	metrics port.Metrics,
) *Trading {
	return &Trading{
		symbol: symbol, engine: engine, wallets: wallets,
		orders: orders, metrics: metrics, now: time.Now,
	}
}

// PlaceOrder validates the command, reserves funds and submits to the engine.
func (t *Trading) PlaceOrder(ctx context.Context, cmd port.PlaceOrderCommand) (*domain.Order, error) {
	started := t.now()

	order := &domain.Order{
		ID:        generateID("ord"),
		UserID:    cmd.UserID,
		Symbol:    t.symbol,
		Side:      cmd.Side,
		Price:     cmd.Price,
		Quantity:  cmd.Quantity,
		CreatedAt: t.now(),
	}
	if err := order.Validate(); err != nil {
		t.metrics.OrderRejected(ctx, "validation")
		return nil, err
	}

	// Risk check: reserve funds atomically. The wallet update is the
	// serialization point that prevents double-spend across concurrent orders.
	if err := t.reserve(ctx, order); err != nil {
		switch {
		case errors.Is(err, domain.ErrInsufficientCOP), errors.Is(err, domain.ErrInsufficientVibranium):
			// Return the rejected order alongside the balance error so the
			// caller can render it (HTTP 422 with the order body).
			order.Status = domain.StatusRejected
			t.metrics.OrderRejected(ctx, "insufficient_funds")
			return order, err
		case errors.Is(err, domain.ErrWalletNotFound):
			t.metrics.OrderRejected(ctx, "wallet_not_found")
			return nil, err
		default:
			// Infrastructure failure (e.g. the wallet store is down). Tag it so
			// the adapter reports the real cause instead of blaming the engine.
			t.metrics.OrderRejected(ctx, "wallet_store_unavailable")
			return nil, fmt.Errorf("%w: %w", ErrReservationFailed, err)
		}
	}

	result, err := t.engine.Place(ctx, order)
	if err != nil {
		// Engine unreachable/cancelled: undo the reservation so funds are freed.
		t.release(ctx, order)
		t.metrics.OrderRejected(ctx, "engine_unavailable")
		return nil, err
	}

	// Latency of the whole accept path, which is what the client actually waits on.
	t.metrics.OrderPlaced(ctx, result.Side, result.Status, t.now().Sub(started))
	return result, nil
}

// CancelOrder removes a resting order by ID.
func (t *Trading) CancelOrder(ctx context.Context, orderID string) (*domain.Order, error) {
	return t.engine.Cancel(ctx, orderID)
}

// GetOrder returns the projected status of an order.
func (t *Trading) GetOrder(ctx context.Context, orderID string) (domain.Order, bool, error) {
	return t.orders.Get(ctx, orderID)
}

// reserve locks the funds backing a new order.
func (t *Trading) reserve(ctx context.Context, o *domain.Order) error {
	return t.wallets.Update(ctx, o.UserID, func(w *domain.Wallet) error {
		if o.Side == domain.Buy {
			return w.ReserveCOP(o.ReservedCOP())
		}
		return w.ReserveVibranium(o.ReservedVibranium())
	})
}

// release refunds a reservation (compensating action on engine failure).
//
// It deliberately uses a fresh, detached context: the caller's context may
// already be cancelled (that is often WHY the engine call failed), and the
// compensation must still run or the user's funds stay locked forever.
func (t *Trading) release(_ context.Context, o *domain.Order) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 5*time.Second)
	defer cancel()
	_ = t.wallets.Update(ctx, o.UserID, func(w *domain.Wallet) error {
		if o.Side == domain.Buy {
			return w.ReleaseCOP(o.ReservedCOP())
		}
		return w.ReleaseVibranium(o.ReservedVibranium())
	})
}

// generateID returns a random 128-bit hex identifier.
func generateID(prefix string) string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}
