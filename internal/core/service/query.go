package service

import (
	"context"
	"errors"

	"github.com/meli/orderbook/internal/core/domain"
	"github.com/meli/orderbook/internal/core/port"
)

// MarketData implements port.MarketDataService: read-only views of the book and
// the trade history.
type MarketData struct {
	engine port.MatchingEngine
	trades port.TradeRepository
}

// NewMarketData builds the market-data use case.
func NewMarketData(engine port.MatchingEngine, trades port.TradeRepository) *MarketData {
	return &MarketData{engine: engine, trades: trades}
}

// Book returns aggregated depth for the top `depth` levels of each side.
func (m *MarketData) Book(ctx context.Context, depth int) (domain.BookSnapshot, error) {
	return m.engine.Depth(ctx, depth)
}

// RecentTrades returns the newest `limit` trades.
func (m *MarketData) RecentTrades(ctx context.Context, limit int) ([]domain.Trade, error) {
	return m.trades.Recent(ctx, limit)
}

// TradeCount returns the total number of settled trades.
func (m *MarketData) TradeCount(ctx context.Context) (int, error) {
	return m.trades.Count(ctx)
}

// ErrInvalidBalance is returned when seeding a wallet with negative balances.
var ErrInvalidBalance = errors.New("balances must be non-negative")

// ErrReservationFailed wraps an infrastructure failure that prevented the fund
// reservation (as opposed to the user simply not having enough balance).
// It lets adapters report "wallet store unavailable" instead of blaming the
// matching engine.
var ErrReservationFailed = errors.New("could not reserve funds")

// Wallets implements port.WalletService.
type Wallets struct {
	wallets port.WalletRepository
}

// NewWallets builds the wallet use case.
func NewWallets(wallets port.WalletRepository) *Wallets {
	return &Wallets{wallets: wallets}
}

// Seed creates or replaces a wallet (registration is out of scope for the MVP).
func (s *Wallets) Seed(ctx context.Context, userID string, cop, vibranium int64) (domain.Wallet, error) {
	if userID == "" {
		return domain.Wallet{}, domain.ErrMissingUser
	}
	if cop < 0 || vibranium < 0 {
		return domain.Wallet{}, ErrInvalidBalance
	}
	w := domain.Wallet{UserID: userID, COPAvailable: cop, VibraniumAvailable: vibranium}
	if err := s.wallets.Upsert(ctx, w); err != nil {
		return domain.Wallet{}, err
	}
	return w, nil
}

// Get returns a single wallet.
func (s *Wallets) Get(ctx context.Context, userID string) (domain.Wallet, error) {
	return s.wallets.Get(ctx, userID)
}

// List returns all wallets (reconciliation).
func (s *Wallets) List(ctx context.Context) ([]domain.Wallet, error) {
	return s.wallets.List(ctx)
}
