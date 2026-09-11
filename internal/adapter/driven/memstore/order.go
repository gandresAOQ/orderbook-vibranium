package memstore

import (
	"context"
	"sync"

	"github.com/meli/orderbook/internal/core/domain"
)

// OrderStore is a concurrency-safe in-memory port.OrderRepository. It lets
// clients query the current state of an order without touching the engine's
// hot in-memory book.
type OrderStore struct {
	mu     sync.RWMutex
	orders map[string]domain.Order
}

// NewOrderStore creates an empty order store.
func NewOrderStore() *OrderStore {
	return &OrderStore{orders: map[string]domain.Order{}}
}

// Upsert inserts or updates the projected order state. The latest write wins,
// which is correct because the engine emits updates in sequence order.
func (s *OrderStore) Upsert(_ context.Context, o domain.Order) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orders[o.ID] = o
	return nil
}

// Get returns the projected order state.
func (s *OrderStore) Get(_ context.Context, orderID string) (domain.Order, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.orders[orderID]
	return o, ok, nil
}
