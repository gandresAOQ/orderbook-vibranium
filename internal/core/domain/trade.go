package domain

import "time"

// Trade is an executed match between a resting (maker) order and an
// incoming (taker) order. It is the atomic unit of settlement and the
// backbone of traceability: the trade stream is an append-only audit log.
type Trade struct {
	ID     string `json:"id"`
	Symbol string `json:"symbol"`

	// Price is the execution price = the resting (maker) order's price.
	Price    int64 `json:"price"`
	Quantity int64 `json:"quantity"`

	BuyOrderID  string `json:"buyOrderId"`
	SellOrderID string `json:"sellOrderId"`
	BuyerID     string `json:"buyerId"`
	SellerID    string `json:"sellerId"`

	// Sequence is a monotonic per-symbol counter for ordering/traceability.
	Sequence int64 `json:"sequence"`

	ExecutedAt time.Time `json:"executedAt"`
}

// Notional is the total COP value exchanged in the trade.
func (t *Trade) Notional() int64 { return t.Price * t.Quantity }
