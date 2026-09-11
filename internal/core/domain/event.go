package domain

import "time"

// EventType discriminates the events emitted by the matching engine.
type EventType string

const (
	// EventOrderAccepted is emitted when an order enters the engine.
	// (Funds were already reserved synchronously before submission.)
	EventOrderAccepted EventType = "ORDER_ACCEPTED"
	// EventTrade is emitted for every executed match. Drives settlement.
	EventTrade EventType = "TRADE"
	// EventOrderUpdated is emitted when an order's status/fill changes,
	// including terminal transitions (FILLED / CANCELLED).
	EventOrderUpdated EventType = "ORDER_UPDATED"
)

// Event is a single item on the engine's output stream (the "log").
//
// This mirrors the real architecture: the engine is a deterministic function
// over an ordered input of commands, producing an ordered output of events.
// Settlement, projections and the trade history are all built by consuming it.
type Event struct {
	// ID uniquely identifies this event across redeliveries. It is
	// "<producerID>-<sequence>", where producerID is unique per engine
	// instance, so a replay of the SAME event always carries the SAME ID while
	// two different engine instances can never collide. This is the
	// idempotency key used by the settlement journal to guarantee that an
	// at-least-once log (Kafka) never double-settles money.
	ID string `json:"id"`

	Type     EventType `json:"type"`
	Sequence int64     `json:"sequence"`
	At       time.Time `json:"at"`

	// Trade is set when Type == EventTrade.
	Trade *Trade `json:"trade,omitempty"`

	// Order is a snapshot set when Type is EventOrderAccepted / EventOrderUpdated.
	Order *Order `json:"order,omitempty"`

	// ReleaseCOP / ReleaseVibranium are set on TERMINAL order updates so the
	// settlement layer can return funds locked beyond what was actually traded:
	//   - BUY terminal : over-reservation (limit price minus execution price)
	//                    plus any unfilled remainder on cancel.
	//   - SELL terminal: the unsold reserved Vibranium.
	ReleaseCOP       int64 `json:"releaseCop,omitempty"`
	ReleaseVibranium int64 `json:"releaseVibranium,omitempty"`
}
