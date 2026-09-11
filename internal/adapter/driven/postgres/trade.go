package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/meli/orderbook/internal/core/domain"
)

// TradeStore is the durable, append-only port.TradeRepository (traceability).
type TradeStore struct {
	pool *pgxpool.Pool
}

// NewTradeStore builds a Postgres-backed trade repository.
func NewTradeStore(pool *pgxpool.Pool) *TradeStore {
	return &TradeStore{pool: pool}
}

// Append records an executed trade.
//
// ON CONFLICT DO NOTHING makes the write idempotent on the trade's primary key,
// so a redelivered event can never duplicate history even if the settlement
// journal is bypassed.
func (s *TradeStore) Append(ctx context.Context, t domain.Trade) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO trades (id, symbol, price, quantity, buy_order_id, sell_order_id,
		                    buyer_id, seller_id, sequence, executed_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (id) DO NOTHING`,
		t.ID, t.Symbol, t.Price, t.Quantity, t.BuyOrderID, t.SellOrderID,
		t.BuyerID, t.SellerID, t.Sequence, t.ExecutedAt)
	if err != nil {
		return fmt.Errorf("append trade %s: %w", t.ID, err)
	}
	return nil
}

// Recent returns the newest trades first, up to limit (<=0 means all).
func (s *TradeStore) Recent(ctx context.Context, limit int) ([]domain.Trade, error) {
	query := `SELECT id, symbol, price, quantity, buy_order_id, sell_order_id,
	                 buyer_id, seller_id, sequence, executed_at
	          FROM trades ORDER BY sequence DESC`
	args := []any{}
	if limit > 0 {
		query += ` LIMIT $1`
		args = append(args, limit)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("recent trades: %w", err)
	}
	defer rows.Close()

	out := []domain.Trade{}
	for rows.Next() {
		var t domain.Trade
		if err := rows.Scan(&t.ID, &t.Symbol, &t.Price, &t.Quantity,
			&t.BuyOrderID, &t.SellOrderID, &t.BuyerID, &t.SellerID,
			&t.Sequence, &t.ExecutedAt); err != nil {
			return nil, fmt.Errorf("scan trade: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate trades: %w", err)
	}
	return out, nil
}

// Count returns the total number of recorded trades.
func (s *TradeStore) Count(ctx context.Context) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM trades`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count trades: %w", err)
	}
	return n, nil
}
