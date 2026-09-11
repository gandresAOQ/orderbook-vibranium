package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/meli/orderbook/internal/core/domain"
)

// OrderStore is the durable port.OrderRepository (order status projection).
type OrderStore struct {
	pool *pgxpool.Pool
}

// NewOrderStore builds a Postgres-backed order repository.
func NewOrderStore(pool *pgxpool.Pool) *OrderStore {
	return &OrderStore{pool: pool}
}

const orderColumns = `id, user_id, symbol, side, price, quantity, status,
                      filled_quantity, sequence, created_at, updated_at`

// Upsert inserts or updates the projected order state.
//
// The guard on `sequence` keeps the projection monotonic: an out-of-order or
// redelivered update can never overwrite newer state with older state.
func (s *OrderStore) Upsert(ctx context.Context, o domain.Order) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO orders (`+orderColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (id) DO UPDATE SET
			status          = EXCLUDED.status,
			filled_quantity = EXCLUDED.filled_quantity,
			sequence        = EXCLUDED.sequence,
			updated_at      = EXCLUDED.updated_at
		WHERE orders.sequence <= EXCLUDED.sequence`,
		o.ID, o.UserID, o.Symbol, string(o.Side), o.Price, o.Quantity,
		string(o.Status), o.FilledQuantity, o.Sequence, o.CreatedAt, o.UpdatedAt)
	if err != nil {
		return fmt.Errorf("upsert order %s: %w", o.ID, err)
	}
	return nil
}

// Get returns the projected order state.
func (s *OrderStore) Get(ctx context.Context, orderID string) (domain.Order, bool, error) {
	var o domain.Order
	var side, status string

	err := s.pool.QueryRow(ctx,
		`SELECT `+orderColumns+` FROM orders WHERE id = $1`, orderID,
	).Scan(&o.ID, &o.UserID, &o.Symbol, &side, &o.Price, &o.Quantity,
		&status, &o.FilledQuantity, &o.Sequence, &o.CreatedAt, &o.UpdatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Order{}, false, nil
	}
	if err != nil {
		return domain.Order{}, false, fmt.Errorf("get order %s: %w", orderID, err)
	}
	o.Side = domain.Side(side)
	o.Status = domain.OrderStatus(status)
	return o, true, nil
}
