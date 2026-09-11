package port

import (
	"context"

	"github.com/meli/orderbook/internal/core/domain"
)

// PlaceOrderCommand is the transport-agnostic input to place a limit order.
// Driving adapters (e.g. HTTP) map their request DTOs into this command.
type PlaceOrderCommand struct {
	UserID   string
	Side     domain.Side
	Price    int64
	Quantity int64
}

// TradingService places and cancels orders. It orchestrates the risk check
// (fund reservation) and submission to the matching engine.
type TradingService interface {
	// PlaceOrder validates, reserves funds and submits the order. On
	// insufficient funds it returns the rejected order together with the
	// balance error so adapters can render it.
	PlaceOrder(ctx context.Context, cmd PlaceOrderCommand) (*domain.Order, error)
	// CancelOrder removes a resting order, releasing its reserved funds.
	CancelOrder(ctx context.Context, orderID string) (*domain.Order, error)
	// GetOrder returns the projected status of an order.
	GetOrder(ctx context.Context, orderID string) (domain.Order, bool, error)
}

// MarketDataService exposes read-only market data.
type MarketDataService interface {
	Book(ctx context.Context, depth int) (domain.BookSnapshot, error)
	RecentTrades(ctx context.Context, limit int) ([]domain.Trade, error)
	TradeCount(ctx context.Context) (int, error)
}

// WalletService seeds and reads wallets. Registration is out of scope for the
// MVP, so Seed doubles as create/replace for testing.
type WalletService interface {
	Seed(ctx context.Context, userID string, cop, vibranium int64) (domain.Wallet, error)
	Get(ctx context.Context, userID string) (domain.Wallet, error)
	List(ctx context.Context) ([]domain.Wallet, error)
}
