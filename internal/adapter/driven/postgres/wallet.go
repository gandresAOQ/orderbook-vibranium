package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/meli/orderbook/internal/core/domain"
)

// WalletStore is the durable port.WalletRepository backed by Postgres.
type WalletStore struct {
	pool *pgxpool.Pool
}

// NewWalletStore builds a Postgres-backed wallet repository.
func NewWalletStore(pool *pgxpool.Pool) *WalletStore {
	return &WalletStore{pool: pool}
}

const walletColumns = `cop_available, cop_locked, vib_available, vib_locked`

// Get returns the wallet, or domain.ErrWalletNotFound.
func (s *WalletStore) Get(ctx context.Context, userID string) (domain.Wallet, error) {
	w := domain.Wallet{UserID: userID}
	err := s.pool.QueryRow(ctx,
		`SELECT `+walletColumns+` FROM wallets WHERE user_id = $1`, userID,
	).Scan(&w.COPAvailable, &w.COPLocked, &w.VibraniumAvailable, &w.VibraniumLocked)

	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Wallet{}, domain.ErrWalletNotFound
	}
	if err != nil {
		return domain.Wallet{}, fmt.Errorf("get wallet %s: %w", userID, err)
	}
	return w, nil
}

// Upsert creates or replaces a wallet (seeding).
func (s *WalletStore) Upsert(ctx context.Context, w domain.Wallet) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO wallets (user_id, cop_available, cop_locked, vib_available, vib_locked, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (user_id) DO UPDATE SET
			cop_available = EXCLUDED.cop_available,
			cop_locked    = EXCLUDED.cop_locked,
			vib_available = EXCLUDED.vib_available,
			vib_locked    = EXCLUDED.vib_locked,
			updated_at    = now()`,
		w.UserID, w.COPAvailable, w.COPLocked, w.VibraniumAvailable, w.VibraniumLocked)
	if err != nil {
		return fmt.Errorf("upsert wallet %s: %w", w.UserID, err)
	}
	return nil
}

// Update applies fn to the wallet inside a transaction that holds a row-level
// lock on it (SELECT ... FOR UPDATE).
//
// This is the durable equivalent of the in-memory mutex, and it is what makes
// the no-double-spend guarantee survive horizontal scaling of the API tier:
// concurrent reservations for the SAME user are serialized by Postgres, while
// different users proceed in parallel. If fn returns an error the transaction is
// rolled back, so a rejected reservation leaves no trace.
func (s *WalletStore) Update(ctx context.Context, userID string, fn func(*domain.Wallet) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin wallet tx: %w", err)
	}
	// Rollback is a no-op once the tx is committed.
	defer func() { _ = tx.Rollback(ctx) }()

	w := domain.Wallet{UserID: userID}
	err = tx.QueryRow(ctx,
		`SELECT `+walletColumns+` FROM wallets WHERE user_id = $1 FOR UPDATE`, userID,
	).Scan(&w.COPAvailable, &w.COPLocked, &w.VibraniumAvailable, &w.VibraniumLocked)

	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrWalletNotFound
	}
	if err != nil {
		return fmt.Errorf("lock wallet %s: %w", userID, err)
	}

	// Domain rules decide whether the mutation is allowed.
	if err := fn(&w); err != nil {
		return err // tx rolled back by the deferred call
	}

	if _, err := tx.Exec(ctx, `
		UPDATE wallets SET
			cop_available = $2, cop_locked = $3,
			vib_available = $4, vib_locked = $5,
			updated_at    = now()
		WHERE user_id = $1`,
		userID, w.COPAvailable, w.COPLocked, w.VibraniumAvailable, w.VibraniumLocked,
	); err != nil {
		return fmt.Errorf("update wallet %s: %w", userID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit wallet %s: %w", userID, err)
	}
	return nil
}

// List returns all wallets ordered by user, for reconciliation.
func (s *WalletStore) List(ctx context.Context) ([]domain.Wallet, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_id, `+walletColumns+` FROM wallets ORDER BY user_id`)
	if err != nil {
		return nil, fmt.Errorf("list wallets: %w", err)
	}
	defer rows.Close()

	out := []domain.Wallet{}
	for rows.Next() {
		var w domain.Wallet
		if err := rows.Scan(&w.UserID, &w.COPAvailable, &w.COPLocked,
			&w.VibraniumAvailable, &w.VibraniumLocked); err != nil {
			return nil, fmt.Errorf("scan wallet: %w", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wallets: %w", err)
	}
	return out, nil
}
