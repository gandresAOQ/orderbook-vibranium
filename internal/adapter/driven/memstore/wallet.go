// Package memstore holds in-memory driven adapters for the persistence ports
// (wallets, trades, orders, settlement journal). They are the MVP stand-ins for
// real infrastructure (Postgres) and can be swapped without touching the core,
// since callers depend only on the port interfaces.
package memstore

import (
	"context"
	"sort"
	"sync"

	"github.com/meli/orderbook/internal/core/domain"
)

// WalletStore is a concurrency-safe in-memory port.WalletRepository.
//
// A single RWMutex is adequate for the MVP. To scale writes you would shard by
// userID (a lock per shard) or move wallets into Postgres with row-level locks
// (see the postgres adapter, which does exactly that).
type WalletStore struct {
	mu      sync.RWMutex
	wallets map[string]*domain.Wallet
}

// NewWalletStore creates an empty store.
func NewWalletStore() *WalletStore {
	return &WalletStore{wallets: map[string]*domain.Wallet{}}
}

// Get returns a copy of the wallet.
func (s *WalletStore) Get(_ context.Context, userID string) (domain.Wallet, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.wallets[userID]
	if !ok {
		return domain.Wallet{}, domain.ErrWalletNotFound
	}
	return *w, nil
}

// Upsert creates or replaces a wallet.
func (s *WalletStore) Upsert(_ context.Context, w domain.Wallet) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := w
	s.wallets[w.UserID] = &cp
	return nil
}

// Update mutates a wallet atomically under the write lock.
func (s *WalletStore) Update(_ context.Context, userID string, fn func(*domain.Wallet) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.wallets[userID]
	if !ok {
		return domain.ErrWalletNotFound
	}
	snapshot := *w // rollback copy
	if err := fn(w); err != nil {
		*w = snapshot
		return err
	}
	return nil
}

// List returns copies of all wallets, sorted by userID for stable output.
func (s *WalletStore) List(_ context.Context) ([]domain.Wallet, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.Wallet, 0, len(s.wallets))
	for _, w := range s.wallets {
		out = append(out, *w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UserID < out[j].UserID })
	return out, nil
}
