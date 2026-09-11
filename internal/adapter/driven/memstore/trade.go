package memstore

import (
	"context"
	"sync"

	"github.com/meli/orderbook/internal/core/domain"
)

// TradeStore is a concurrency-safe in-memory port.TradeRepository.
// In production this is a high-volume append store (partitioned Postgres,
// DynamoDB, or a data lake) fed from the trades topic.
type TradeStore struct {
	mu     sync.RWMutex
	trades []domain.Trade
}

// NewTradeStore creates an empty trade store.
func NewTradeStore() *TradeStore {
	return &TradeStore{}
}

// Append records an executed trade.
func (s *TradeStore) Append(_ context.Context, t domain.Trade) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trades = append(s.trades, t)
	return nil
}

// Recent returns the newest trades first, up to limit (<=0 means all).
func (s *TradeStore) Recent(_ context.Context, limit int) ([]domain.Trade, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := len(s.trades)
	if limit <= 0 || limit > n {
		limit = n
	}
	out := make([]domain.Trade, 0, limit)
	for i := n - 1; i >= n-limit; i-- {
		out = append(out, s.trades[i])
	}
	return out, nil
}

// Count returns the total number of recorded trades.
func (s *TradeStore) Count(_ context.Context) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.trades), nil
}
