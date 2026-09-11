// Package domain contains the core business types and rules of the order book.
// It is the innermost hexagon: pure logic with no dependencies on transport,
// storage or messaging. Everything else (ports, services, adapters) depends on
// this package; this package depends on nothing but the standard library.
//
// Money & quantity representation
// -------------------------------
// All amounts are integers to keep arithmetic EXACT (no floating point drift):
//   - Price    : int64 COP per 1 unit of Vibranium.
//   - Quantity : int64 units of Vibranium.
//   - COP      : int64 pesos.
//
// In production you would use a decimal type (e.g. Postgres NUMERIC) to support
// fractional units and remove the int64 ceiling. Integers are used here because
// correctness of credits/debits is the priority for the MVP.
package domain

import (
	"errors"
	"time"
)

// Side is the direction of an order.
type Side string

const (
	// Buy purchases Vibranium paying COP.
	Buy Side = "BUY"
	// Sell sells Vibranium receiving COP.
	Sell Side = "SELL"
)

// Valid reports whether s is a known side.
func (s Side) Valid() bool { return s == Buy || s == Sell }

// Opposite returns the matching side for this order's side.
func (s Side) Opposite() Side {
	if s == Buy {
		return Sell
	}
	return Buy
}

// OrderStatus is the lifecycle state of an order.
type OrderStatus string

const (
	// StatusOpen means the order is resting in the book, not (fully) filled.
	StatusOpen OrderStatus = "OPEN"
	// StatusPartiallyFilled means part matched, remainder still resting.
	StatusPartiallyFilled OrderStatus = "PARTIALLY_FILLED"
	// StatusFilled means fully matched.
	StatusFilled OrderStatus = "FILLED"
	// StatusCancelled means the resting remainder was cancelled by the user.
	StatusCancelled OrderStatus = "CANCELLED"
	// StatusRejected means the order never entered the book (e.g. no balance).
	StatusRejected OrderStatus = "REJECTED"
)

// Terminal reports whether the status is final (no further transitions).
func (s OrderStatus) Terminal() bool {
	return s == StatusFilled || s == StatusCancelled || s == StatusRejected
}

// Order is a limit order for a single instrument.
type Order struct {
	ID       string `json:"id"`
	UserID   string `json:"userId"`
	Symbol   string `json:"symbol"`
	Side     Side   `json:"side"`
	Price    int64  `json:"price"`    // COP per unit
	Quantity int64  `json:"quantity"` // total units requested

	Status         OrderStatus `json:"status"`
	FilledQuantity int64       `json:"filledQuantity"`

	// Sequence is a monotonic value assigned by the engine on acceptance.
	// It establishes time priority deterministically, independent of clocks.
	Sequence int64 `json:"sequence"`

	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Remaining returns the still-unmatched quantity.
func (o *Order) Remaining() int64 { return o.Quantity - o.FilledQuantity }

// ReservedCOP is the COP locked when a BUY order is accepted (at limit price).
// Zero for SELL orders.
func (o *Order) ReservedCOP() int64 {
	if o.Side == Buy {
		return o.Price * o.Quantity
	}
	return 0
}

// ReservedVibranium is the Vibranium locked when a SELL order is accepted.
// Zero for BUY orders.
func (o *Order) ReservedVibranium() int64 {
	if o.Side == Sell {
		return o.Quantity
	}
	return 0
}

// CloneOrder returns a shallow copy of o. Used to hand immutable snapshots to
// adapters/consumers without aliasing the live pointer held by the book.
func CloneOrder(o *Order) *Order {
	c := *o
	return &c
}

// Validation errors returned by Validate.
var (
	ErrInvalidSide     = errors.New("invalid side: must be BUY or SELL")
	ErrInvalidPrice    = errors.New("invalid price: must be a positive integer")
	ErrInvalidQuantity = errors.New("invalid quantity: must be a positive integer")
	ErrMissingUser     = errors.New("invalid order: userId is required")
	ErrMissingSymbol   = errors.New("invalid order: symbol is required")
)

// ErrOrderNotFound is returned when cancelling/looking up an unknown or
// no-longer-resting order.
var ErrOrderNotFound = errors.New("order not found or no longer resting")

// Validate checks the order's invariants before it is accepted.
func (o *Order) Validate() error {
	if !o.Side.Valid() {
		return ErrInvalidSide
	}
	if o.UserID == "" {
		return ErrMissingUser
	}
	if o.Symbol == "" {
		return ErrMissingSymbol
	}
	if o.Price <= 0 {
		return ErrInvalidPrice
	}
	if o.Quantity <= 0 {
		return ErrInvalidQuantity
	}
	return nil
}
