package service

import (
	"context"
	"log/slog"

	"github.com/meli/orderbook/internal/core/domain"
	"github.com/meli/orderbook/internal/core/port"
)

// Settlement consumes the engine's event stream and applies its monetary
// effects to wallets, records trade history, and maintains the order
// projection. It is deliberately the ONLY component that moves settled money,
// keeping the matching engine free of financial state.
//
// Ordering guarantee: events arrive in engine sequence order on a single
// stream, so per-trade wallet mutations and the subsequent release events are
// applied consistently.
type Settlement struct {
	log     port.EventLog
	journal port.EventJournal
	wallets port.WalletRepository
	trades  port.TradeRepository
	orders  port.OrderRepository
	logger  *slog.Logger
}

// NewSettlement creates the settlement use case. `journal` provides idempotency
// against redelivered events; it is required (use the in-memory journal when
// running on the in-process log, where redelivery cannot happen).
func NewSettlement(
	log port.EventLog,
	journal port.EventJournal,
	wallets port.WalletRepository,
	trades port.TradeRepository,
	orders port.OrderRepository,
	logger *slog.Logger,
) *Settlement {
	if logger == nil {
		logger = slog.Default()
	}
	return &Settlement{log: log, journal: journal, wallets: wallets, trades: trades, orders: orders, logger: logger}
}

// Run consumes events until the stream closes or ctx is cancelled. Run in its
// own goroutine: `go settlement.Run(ctx)`.
func (s *Settlement) Run(ctx context.Context) {
	events := s.log.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			s.handle(ctx, ev)
		}
	}
}

// handle guards the event against redelivery, then applies it.
//
// The journal is consulted BEFORE applying. On a durable, at-least-once log
// (Kafka/Redpanda) the same event can be delivered more than once — after a
// consumer restart, a rebalance, or an uncommitted offset. Without this guard a
// redelivered TRADE would debit and credit the wallets twice.
//
// Failure policy: if the journal itself errors we skip the event rather than
// risk double-settling. That fails closed on money, which is the safe side for
// a ledger; in production the event would be dead-lettered and alerted on.
func (s *Settlement) handle(ctx context.Context, ev domain.Event) {
	fresh, err := s.journal.MarkProcessed(ctx, ev.ID)
	if err != nil {
		s.logger.Error("settlement journal unavailable, skipping event",
			"event", ev.ID, "type", ev.Type, "err", err)
		return
	}
	if !fresh {
		s.logger.Debug("duplicate event skipped", "event", ev.ID, "type", ev.Type)
		return
	}
	s.apply(ctx, ev)
}

func (s *Settlement) apply(ctx context.Context, ev domain.Event) {
	switch ev.Type {
	case domain.EventTrade:
		s.applyTrade(ctx, ev.Trade)
	case domain.EventOrderUpdated:
		s.applyOrderUpdate(ctx, ev)
	case domain.EventOrderAccepted:
		if ev.Order != nil {
			if err := s.orders.Upsert(ctx, *ev.Order); err != nil {
				s.logger.Error("order projection upsert failed", "order", ev.Order.ID, "err", err)
			}
		}
	}
}

// applyTrade exchanges value between buyer and seller and records the trade.
func (s *Settlement) applyTrade(ctx context.Context, t *domain.Trade) {
	if t == nil {
		return
	}
	notional := t.Notional() // price * quantity

	// Buyer: spend locked COP, receive Vibranium.
	if err := s.wallets.Update(ctx, t.BuyerID, func(w *domain.Wallet) error {
		return w.SettleBuy(notional, t.Quantity)
	}); err != nil {
		// In production: dead-letter + alert. A settlement failure here means a
		// reservation invariant was violated upstream.
		s.logger.Error("settle buy failed", "trade", t.ID, "buyer", t.BuyerID, "err", err)
	}

	// Seller: deliver locked Vibranium, receive COP.
	if err := s.wallets.Update(ctx, t.SellerID, func(w *domain.Wallet) error {
		return w.SettleSell(t.Quantity, notional)
	}); err != nil {
		s.logger.Error("settle sell failed", "trade", t.ID, "seller", t.SellerID, "err", err)
	}

	if err := s.trades.Append(ctx, *t); err != nil {
		s.logger.Error("trade history append failed", "trade", t.ID, "err", err)
	}
}

// applyOrderUpdate refreshes the order projection and returns any funds the
// engine flagged for release (buy over-reservation or cancelled remainder).
func (s *Settlement) applyOrderUpdate(ctx context.Context, ev domain.Event) {
	if ev.Order == nil {
		return
	}
	o := ev.Order
	if err := s.orders.Upsert(ctx, *o); err != nil {
		s.logger.Error("order projection upsert failed", "order", o.ID, "err", err)
	}

	if ev.ReleaseCOP > 0 {
		if err := s.wallets.Update(ctx, o.UserID, func(w *domain.Wallet) error {
			return w.ReleaseCOP(ev.ReleaseCOP)
		}); err != nil {
			s.logger.Error("release COP failed", "order", o.ID, "user", o.UserID, "err", err)
		}
	}
	if ev.ReleaseVibranium > 0 {
		if err := s.wallets.Update(ctx, o.UserID, func(w *domain.Wallet) error {
			return w.ReleaseVibranium(ev.ReleaseVibranium)
		}); err != nil {
			s.logger.Error("release Vibranium failed", "order", o.ID, "user", o.UserID, "err", err)
		}
	}
}
