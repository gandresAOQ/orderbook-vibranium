package domain

import "errors"

// Wallet holds a user's balances. Each asset is split into:
//   - Available: free to spend / to back new orders.
//   - Locked   : reserved to back OPEN orders resting in the book.
//
// The available/locked split is the heart of order-book correctness: it
// prevents double-spend when a user (or their bot) fires many orders in the
// same millisecond. Funds are moved available -> locked at order acceptance,
// and locked -> settled (or released) as trades execute or orders cancel.
//
// Wallet methods are NOT safe for concurrent use on their own; the repository
// that owns them is responsible for synchronization.
type Wallet struct {
	UserID string `json:"userId"`

	COPAvailable int64 `json:"copAvailable"`
	COPLocked    int64 `json:"copLocked"`

	VibraniumAvailable int64 `json:"vibraniumAvailable"`
	VibraniumLocked    int64 `json:"vibraniumLocked"`
}

// Balance errors.
var (
	ErrInsufficientCOP       = errors.New("insufficient COP available")
	ErrInsufficientVibranium = errors.New("insufficient Vibranium available")
	ErrInsufficientLocked    = errors.New("insufficient locked balance to settle/release")
	ErrNegativeAmount        = errors.New("amount must be non-negative")
)

// ErrWalletNotFound is returned when a wallet does not exist.
var ErrWalletNotFound = errors.New("wallet not found")

// --- Reservation (order acceptance) ---

// ReserveCOP moves COP from available to locked. Used when a BUY is accepted.
func (w *Wallet) ReserveCOP(amount int64) error {
	if amount < 0 {
		return ErrNegativeAmount
	}
	if w.COPAvailable < amount {
		return ErrInsufficientCOP
	}
	w.COPAvailable -= amount
	w.COPLocked += amount
	return nil
}

// ReserveVibranium moves Vibranium from available to locked. Used for SELL.
func (w *Wallet) ReserveVibranium(amount int64) error {
	if amount < 0 {
		return ErrNegativeAmount
	}
	if w.VibraniumAvailable < amount {
		return ErrInsufficientVibranium
	}
	w.VibraniumAvailable -= amount
	w.VibraniumLocked += amount
	return nil
}

// --- Release (cancel / over-reservation refund) ---

// ReleaseCOP returns locked COP to available (cancel or over-reservation).
func (w *Wallet) ReleaseCOP(amount int64) error {
	if amount < 0 {
		return ErrNegativeAmount
	}
	if w.COPLocked < amount {
		return ErrInsufficientLocked
	}
	w.COPLocked -= amount
	w.COPAvailable += amount
	return nil
}

// ReleaseVibranium returns locked Vibranium to available.
func (w *Wallet) ReleaseVibranium(amount int64) error {
	if amount < 0 {
		return ErrNegativeAmount
	}
	if w.VibraniumLocked < amount {
		return ErrInsufficientLocked
	}
	w.VibraniumLocked -= amount
	w.VibraniumAvailable += amount
	return nil
}

// --- Settlement (trade execution) ---

// SettleBuy applies a fill to the BUYER: locked COP is spent and Vibranium is
// credited. `spent` is quantity * executionPrice.
func (w *Wallet) SettleBuy(spent, vibranium int64) error {
	if spent < 0 || vibranium < 0 {
		return ErrNegativeAmount
	}
	if w.COPLocked < spent {
		return ErrInsufficientLocked
	}
	w.COPLocked -= spent
	w.VibraniumAvailable += vibranium
	return nil
}

// SettleSell applies a fill to the SELLER: locked Vibranium is delivered and
// COP is credited. `received` is quantity * executionPrice.
func (w *Wallet) SettleSell(vibranium, received int64) error {
	if vibranium < 0 || received < 0 {
		return ErrNegativeAmount
	}
	if w.VibraniumLocked < vibranium {
		return ErrInsufficientLocked
	}
	w.VibraniumLocked -= vibranium
	w.COPAvailable += received
	return nil
}

// TotalCOP returns available + locked COP (for reconciliation/invariants).
func (w *Wallet) TotalCOP() int64 { return w.COPAvailable + w.COPLocked }

// TotalVibranium returns available + locked Vibranium.
func (w *Wallet) TotalVibranium() int64 { return w.VibraniumAvailable + w.VibraniumLocked }
