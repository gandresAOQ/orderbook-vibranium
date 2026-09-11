// Package postgres holds the durable driven adapters for the persistence ports
// (wallets, trades, orders, settlement journal), backed by Postgres via pgx.
//
// Why Postgres for wallets: money needs ACID. Every balance mutation runs in a
// transaction that takes a row-level lock (SELECT ... FOR UPDATE) on the wallet,
// so concurrent orders from the same user are serialized by the database
// instead of by an in-process mutex. That is what lets the API tier scale
// horizontally without losing the no-double-spend guarantee.
package postgres

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// Connect opens a pooled connection and waits until the database answers.
// Containers frequently start out of order, so it retries until `wait` elapses.
func Connect(ctx context.Context, url string, maxConns int32, wait time.Duration) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	deadline := time.Now().Add(wait)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = pool.Ping(pingCtx)
		cancel()
		if err == nil {
			return pool, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			pool.Close()
			return nil, fmt.Errorf("postgres unreachable after %s: %w", wait, err)
		}
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Migrate applies the embedded schema. It is idempotent (CREATE TABLE IF NOT
// EXISTS), so it is safe to run on every boot and needs no migration container.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}
