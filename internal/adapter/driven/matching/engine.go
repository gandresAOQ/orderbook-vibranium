// Package matching is the driven adapter that implements port.MatchingEngine.
//
// It owns exactly one domain.OrderBook and processes all commands on a single
// goroutine. Serializing every mutation through one channel is what makes the
// book correct under massive concurrency without a single lock: ordering is
// decided once, at the channel, then applied deterministically. Event sequence
// numbers are assigned inside this goroutine so the emitted stream is ordered.
package matching

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/meli/orderbook/internal/core/domain"
	"github.com/meli/orderbook/internal/core/port"
)

type commandType int

const (
	cmdPlace commandType = iota
	cmdCancel
	cmdSnapshot
)

// command is a unit of work for the engine goroutine.
type command struct {
	kind    commandType
	order   *domain.Order
	orderID string
	depth   int
	reply   chan commandReply
}

type commandReply struct {
	order    *domain.Order
	snapshot domain.BookSnapshot
	err      error
}

// Engine is the single-writer matching adapter. It satisfies
// port.MatchingEngine and publishes events through a port.EventPublisher.
type Engine struct {
	book *domain.OrderBook
	sink port.EventPublisher
	in   chan command
	seq  int64 // global event-log sequence

	// producerID is unique per engine instance. Combined with seq it forms a
	// globally unique event ID, so redeliveries from an at-least-once log are
	// detectable while two engine instances can never collide.
	producerID string

	now func() time.Time
}

// NewEngine builds an engine for one symbol. `buffer` sizes the command channel
// and acts as the backpressure buffer for bursts of orders.
func NewEngine(symbol string, sink port.EventPublisher, buffer int) *Engine {
	if buffer <= 0 {
		buffer = 1 << 16
	}
	return &Engine{
		book:       domain.NewOrderBook(symbol),
		sink:       sink,
		in:         make(chan command, buffer),
		producerID: newProducerID(),
		now:        time.Now,
	}
}

// newProducerID returns a random 64-bit hex identifier for this engine instance.
func newProducerID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// Fall back to a timestamp; uniqueness matters, secrecy does not.
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}

// Symbol returns the traded instrument.
func (e *Engine) Symbol() string { return e.book.Symbol() }

// Run consumes commands until ctx is cancelled or the input channel drains.
// Call it in its own goroutine: `go engine.Run(ctx)`.
func (e *Engine) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case cmd, ok := <-e.in:
			if !ok {
				return
			}
			e.handle(cmd)
		}
	}
}

func (e *Engine) handle(cmd command) {
	switch cmd.kind {
	case cmdPlace:
		e.handlePlace(cmd)
	case cmdCancel:
		e.handleCancel(cmd)
	case cmdSnapshot:
		cmd.reply <- commandReply{snapshot: e.book.Snapshot(cmd.depth)}
	}
}

func (e *Engine) handlePlace(cmd command) {
	res := e.book.Place(cmd.order)

	for _, f := range res.Fills {
		e.publish(domain.Event{Type: domain.EventTrade, Trade: f.Trade})
		e.publish(domain.Event{Type: domain.EventOrderUpdated, Order: f.Maker})
	}
	takerSnapshot := domain.CloneOrder(cmd.order)
	e.publish(domain.Event{
		Type:       domain.EventOrderUpdated,
		Order:      takerSnapshot,
		ReleaseCOP: res.TakerReleaseCOP,
	})

	if cmd.reply != nil {
		cmd.reply <- commandReply{order: takerSnapshot}
	}
}

func (e *Engine) handleCancel(cmd command) {
	res, ok := e.book.Cancel(cmd.orderID)
	if !ok {
		if cmd.reply != nil {
			cmd.reply <- commandReply{err: domain.ErrOrderNotFound}
		}
		return
	}
	snapshot := domain.CloneOrder(res.Order)
	e.publish(domain.Event{
		Type:             domain.EventOrderUpdated,
		Order:            snapshot,
		ReleaseCOP:       res.ReleaseCOP,
		ReleaseVibranium: res.ReleaseVibranium,
	})
	if cmd.reply != nil {
		cmd.reply <- commandReply{order: snapshot}
	}
}

func (e *Engine) publish(ev domain.Event) {
	e.seq++
	ev.Sequence = e.seq
	ev.ID = e.producerID + "-" + strconv.FormatInt(e.seq, 10)
	ev.At = e.now()
	e.sink.Publish(ev)
}

// --- port.MatchingEngine ---

// Place submits an already-validated, already-reserved order and blocks until
// the engine has processed it, returning the resulting order snapshot.
func (e *Engine) Place(ctx context.Context, o *domain.Order) (*domain.Order, error) {
	reply := make(chan commandReply, 1)
	select {
	case e.in <- command{kind: cmdPlace, order: o, reply: reply}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case r := <-reply:
		return r.order, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Cancel removes a resting order by ID.
func (e *Engine) Cancel(ctx context.Context, orderID string) (*domain.Order, error) {
	reply := make(chan commandReply, 1)
	select {
	case e.in <- command{kind: cmdCancel, orderID: orderID, reply: reply}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case r := <-reply:
		return r.order, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Depth returns a snapshot of the top `depth` price levels per side.
func (e *Engine) Depth(ctx context.Context, depth int) (domain.BookSnapshot, error) {
	reply := make(chan commandReply, 1)
	select {
	case e.in <- command{kind: cmdSnapshot, depth: depth, reply: reply}:
	case <-ctx.Done():
		return domain.BookSnapshot{}, ctx.Err()
	}
	select {
	case r := <-reply:
		return r.snapshot, r.err
	case <-ctx.Done():
		return domain.BookSnapshot{}, ctx.Err()
	}
}
