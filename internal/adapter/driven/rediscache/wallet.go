// Package rediscache adds a Redis read cache in front of another
// port.WalletRepository.
//
// It is a DECORATOR, not a replacement: it implements the same port and wraps
// the durable repository. That is the payoff of the hexagonal design — caching
// is composed in at the composition root and neither the core nor Postgres knows
// it exists.
//
// Consistency model
// -----------------
// Reads are cached for a short TTL; every write path (Upsert/Update) INVALIDATES
// the key rather than trying to update it. Invalidate-on-write is the safe
// choice for money: a stale balance is never written back, and the worst case is
// an extra database read.
//
// Critically, the cache is NEVER on the authoritative path. Update() delegates
// straight to the wrapped repository, so the reserve/settle logic still runs
// against Postgres row locks. The cache only serves GET /wallets/{id} style
// reads, which is where the hot traffic is.
package rediscache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/meli/orderbook/internal/core/domain"
	"github.com/meli/orderbook/internal/core/port"
)

const keyPrefix = "wallet:"

// WalletCache decorates a port.WalletRepository with a Redis read cache.
type WalletCache struct {
	inner  port.WalletRepository
	rdb    *redis.Client
	ttl    time.Duration
	logger *slog.Logger
}

// Connect opens a Redis client and verifies reachability, retrying up to `wait`.
func Connect(ctx context.Context, url string, wait time.Duration) (*redis.Client, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	rdb := redis.NewClient(opts)

	deadline := time.Now().Add(wait)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = rdb.Ping(pingCtx).Err()
		cancel()
		if err == nil {
			return rdb, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			_ = rdb.Close()
			return nil, fmt.Errorf("redis unreachable after %s: %w", wait, err)
		}
		select {
		case <-ctx.Done():
			_ = rdb.Close()
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// NewWalletCache wraps inner with a Redis read cache.
func NewWalletCache(inner port.WalletRepository, rdb *redis.Client, ttl time.Duration, logger *slog.Logger) *WalletCache {
	if logger == nil {
		logger = slog.Default()
	}
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &WalletCache{inner: inner, rdb: rdb, ttl: ttl, logger: logger}
}

// Get serves from cache when possible, falling back to the wrapped repository.
//
// Cache errors are logged and ignored: Redis being down degrades latency, never
// correctness, because the durable repository remains the source of truth.
func (c *WalletCache) Get(ctx context.Context, userID string) (domain.Wallet, error) {
	key := keyPrefix + userID

	if raw, err := c.rdb.Get(ctx, key).Bytes(); err == nil {
		var w domain.Wallet
		if err := json.Unmarshal(raw, &w); err == nil {
			return w, nil
		}
		// Corrupt entry: drop it and fall through to the source of truth.
		c.rdb.Del(ctx, key)
	} else if !errors.Is(err, redis.Nil) {
		c.logger.Warn("wallet cache read failed", "user", userID, "err", err)
	}

	w, err := c.inner.Get(ctx, userID)
	if err != nil {
		return domain.Wallet{}, err
	}
	if raw, err := json.Marshal(w); err == nil {
		if err := c.rdb.Set(ctx, key, raw, c.ttl).Err(); err != nil {
			c.logger.Warn("wallet cache write failed", "user", userID, "err", err)
		}
	}
	return w, nil
}

// Upsert writes through to the repository and invalidates the cached entry.
func (c *WalletCache) Upsert(ctx context.Context, w domain.Wallet) error {
	if err := c.inner.Upsert(ctx, w); err != nil {
		return err
	}
	c.invalidate(ctx, w.UserID)
	return nil
}

// Update delegates to the repository (which holds the authoritative lock) and
// then invalidates the cached entry.
func (c *WalletCache) Update(ctx context.Context, userID string, fn func(*domain.Wallet) error) error {
	err := c.inner.Update(ctx, userID, fn)
	// Invalidate even on failure: a rolled-back attempt may still have raced
	// with a concurrent read that populated the cache.
	c.invalidate(ctx, userID)
	return err
}

// List always goes to the source of truth.
//
// Reconciliation must never read a cache — the whole point of GET /wallets is to
// prove the money invariant, so a stale answer would be worse than a slow one.
func (c *WalletCache) List(ctx context.Context) ([]domain.Wallet, error) {
	return c.inner.List(ctx)
}

func (c *WalletCache) invalidate(ctx context.Context, userID string) {
	if err := c.rdb.Del(ctx, keyPrefix+userID).Err(); err != nil && !errors.Is(err, redis.Nil) {
		c.logger.Warn("wallet cache invalidation failed", "user", userID, "err", err)
	}
}
