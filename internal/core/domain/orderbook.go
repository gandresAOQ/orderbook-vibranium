package domain

// OrderBook is the pure, deterministic core of the matching engine: an
// in-memory limit order book. Given the same sequence of Place/Cancel calls it
// always produces the same trades.
//
// It is intentionally free of concurrency primitives. All serialization is the
// responsibility of the driven "matching" adapter (one goroutine owns one
// OrderBook), so the book itself needs no locks. That is the core answer to the
// challenge's constraint: "the order book does not support concurrency."

import (
	"sort"
	"strconv"
	"time"
)

// priceLevel is a FIFO queue of resting orders at a single price.
// FIFO ordering is what gives us *time* priority within a price level.
type priceLevel struct {
	price int64
	queue []*Order
}

// bookSide holds all resting orders for one side (bids or asks).
type bookSide struct {
	isBid  bool
	levels map[int64]*priceLevel
	// prices is kept sorted: descending for bids, ascending for asks, so the
	// best price is always prices[0].
	prices []int64
}

func newBookSide(isBid bool) *bookSide {
	return &bookSide{isBid: isBid, levels: map[int64]*priceLevel{}}
}

func (s *bookSide) best() (int64, bool) {
	if len(s.prices) == 0 {
		return 0, false
	}
	return s.prices[0], true
}

func (s *bookSide) add(o *Order) {
	lvl, ok := s.levels[o.Price]
	if !ok {
		lvl = &priceLevel{price: o.Price}
		s.levels[o.Price] = lvl
		s.insertPrice(o.Price)
	}
	lvl.queue = append(lvl.queue, o)
}

// insertPrice inserts p into the sorted prices slice, keeping best-first order.
func (s *bookSide) insertPrice(p int64) {
	var i int
	if s.isBid {
		// descending: first index whose price is smaller than p
		i = sort.Search(len(s.prices), func(i int) bool { return s.prices[i] < p })
	} else {
		// ascending: first index whose price is greater than p
		i = sort.Search(len(s.prices), func(i int) bool { return s.prices[i] > p })
	}
	s.prices = append(s.prices, 0)
	copy(s.prices[i+1:], s.prices[i:])
	s.prices[i] = p
}

func (s *bookSide) removePrice(p int64) {
	for i, v := range s.prices {
		if v == p {
			s.prices = append(s.prices[:i], s.prices[i+1:]...)
			return
		}
	}
}

// popFront removes the front order of a level once it is fully filled.
func (s *bookSide) popFront(lvl *priceLevel) {
	lvl.queue = lvl.queue[1:]
	if len(lvl.queue) == 0 {
		delete(s.levels, lvl.price)
		s.removePrice(lvl.price)
	}
}

// remove deletes a specific resting order (used by Cancel).
func (s *bookSide) remove(o *Order) bool {
	lvl, ok := s.levels[o.Price]
	if !ok {
		return false
	}
	for i, ord := range lvl.queue {
		if ord.ID == o.ID {
			lvl.queue = append(lvl.queue[:i], lvl.queue[i+1:]...)
			if len(lvl.queue) == 0 {
				delete(s.levels, lvl.price)
				s.removePrice(lvl.price)
			}
			return true
		}
	}
	return false
}

// Fill pairs an executed trade with the maker order's post-fill snapshot.
type Fill struct {
	Trade *Trade
	Maker *Order
}

// PlaceResult is the deterministic outcome of placing one order.
type PlaceResult struct {
	Taker *Order
	Fills []Fill
	// TakerReleaseCOP is the BUY over-reservation to refund: when a buy taker
	// executes against cheaper resting asks it locked more than it spent.
	TakerReleaseCOP int64
}

// CancelResult is the outcome of cancelling a resting order.
type CancelResult struct {
	Order            *Order
	ReleaseCOP       int64
	ReleaseVibranium int64
}

// OrderBook is a single-instrument limit order book.
type OrderBook struct {
	symbol   string
	bids     *bookSide
	asks     *bookSide
	orders   map[string]*Order // resting orders, by ID (for cancel)
	orderSeq int64
	tradeSeq int64
	now      func() time.Time
}

// NewOrderBook creates an empty book for the given symbol.
func NewOrderBook(symbol string) *OrderBook {
	return &OrderBook{
		symbol: symbol,
		bids:   newBookSide(true),
		asks:   newBookSide(false),
		orders: map[string]*Order{},
		now:    time.Now,
	}
}

// Symbol returns the instrument this book trades.
func (b *OrderBook) Symbol() string { return b.symbol }

// crosses reports whether a maker at makerPrice crosses the taker's limit.
func crosses(takerSide Side, takerPrice, makerPrice int64) bool {
	if takerSide == Buy {
		return makerPrice <= takerPrice // willing to buy at or below limit
	}
	return makerPrice >= takerPrice // willing to sell at or above limit
}

// Place matches an incoming order against the book and rests any remainder.
// It mutates o (status, filledQuantity, sequence) and returns the result.
func (b *OrderBook) Place(o *Order) PlaceResult {
	b.orderSeq++
	o.Sequence = b.orderSeq
	o.Status = StatusOpen
	o.UpdatedAt = b.now()

	res := PlaceResult{Taker: o}
	opposite := b.asks
	if o.Side == Sell {
		opposite = b.bids
	}

	for o.Remaining() > 0 {
		bestPrice, ok := opposite.best()
		if !ok || !crosses(o.Side, o.Price, bestPrice) {
			break
		}
		lvl := opposite.levels[bestPrice]
		maker := lvl.queue[0]

		qty := min64(o.Remaining(), maker.Remaining())
		execPrice := maker.Price // price-time priority: maker sets the price

		maker.FilledQuantity += qty
		o.FilledQuantity += qty

		b.tradeSeq++
		trade := &Trade{
			ID:         b.symbol + "-" + itoa(b.tradeSeq),
			Symbol:     b.symbol,
			Price:      execPrice,
			Quantity:   qty,
			Sequence:   b.tradeSeq,
			ExecutedAt: b.now(),
		}
		if o.Side == Buy {
			trade.BuyOrderID, trade.BuyerID = o.ID, o.UserID
			trade.SellOrderID, trade.SellerID = maker.ID, maker.UserID
			// Buy taker locked at its own (higher/equal) limit; refund the gap.
			res.TakerReleaseCOP += (o.Price - execPrice) * qty
		} else {
			trade.SellOrderID, trade.SellerID = o.ID, o.UserID
			trade.BuyOrderID, trade.BuyerID = maker.ID, maker.UserID
		}

		maker.UpdatedAt = b.now()
		if maker.Remaining() == 0 {
			maker.Status = StatusFilled
			opposite.popFront(lvl)
			delete(b.orders, maker.ID)
		} else {
			maker.Status = StatusPartiallyFilled
		}

		res.Fills = append(res.Fills, Fill{Trade: trade, Maker: CloneOrder(maker)})
	}

	switch {
	case o.Remaining() == 0:
		o.Status = StatusFilled
	case o.FilledQuantity > 0:
		o.Status = StatusPartiallyFilled
		b.rest(o)
	default:
		o.Status = StatusOpen
		b.rest(o)
	}
	o.UpdatedAt = b.now()
	return res
}

func (b *OrderBook) rest(o *Order) {
	b.orders[o.ID] = o
	if o.Side == Buy {
		b.bids.add(o)
	} else {
		b.asks.add(o)
	}
}

// Cancel removes a resting order and reports the funds to release. The bool is
// false if the order is unknown (already filled, never existed, or not resting).
func (b *OrderBook) Cancel(orderID string) (CancelResult, bool) {
	o, ok := b.orders[orderID]
	if !ok {
		return CancelResult{}, false
	}
	remaining := o.Remaining()
	if o.Side == Buy {
		b.bids.remove(o)
	} else {
		b.asks.remove(o)
	}
	delete(b.orders, orderID)
	o.Status = StatusCancelled
	o.UpdatedAt = b.now()

	res := CancelResult{Order: o}
	if o.Side == Buy {
		res.ReleaseCOP = remaining * o.Price
	} else {
		res.ReleaseVibranium = remaining
	}
	return res, true
}

// Snapshot returns aggregated depth for the top n levels of each side.
func (b *OrderBook) Snapshot(depth int) BookSnapshot {
	return BookSnapshot{
		Symbol: b.symbol,
		Bids:   b.bids.levelsView(depth),
		Asks:   b.asks.levelsView(depth),
	}
}

// BookSnapshot is a read-only view of the book's depth.
type BookSnapshot struct {
	Symbol string      `json:"symbol"`
	Bids   []LevelView `json:"bids"`
	Asks   []LevelView `json:"asks"`
}

// LevelView is aggregated resting quantity at a price.
type LevelView struct {
	Price    int64 `json:"price"`
	Quantity int64 `json:"quantity"`
	Orders   int   `json:"orders"`
}

func (s *bookSide) levelsView(depth int) []LevelView {
	out := []LevelView{}
	for i, p := range s.prices {
		if depth > 0 && i >= depth {
			break
		}
		lvl := s.levels[p]
		var q int64
		for _, o := range lvl.queue {
			q += o.Remaining()
		}
		out = append(out, LevelView{Price: p, Quantity: q, Orders: len(lvl.queue)})
	}
	return out
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
