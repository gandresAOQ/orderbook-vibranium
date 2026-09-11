package domain_test

import (
	"testing"

	"github.com/meli/orderbook/internal/core/domain"
)

func order(id, user string, side domain.Side, price, qty int64) *domain.Order {
	return &domain.Order{ID: id, UserID: user, Symbol: "VIB", Side: side, Price: price, Quantity: qty}
}

func TestNoCrossRestsBothSides(t *testing.T) {
	b := domain.NewOrderBook("VIB")
	b.Place(order("s1", "seller", domain.Sell, 100, 10))
	res := b.Place(order("b1", "buyer", domain.Buy, 90, 10))

	if len(res.Fills) != 0 {
		t.Fatalf("expected no fills, got %d", len(res.Fills))
	}
	if res.Taker.Status != domain.StatusOpen {
		t.Fatalf("expected OPEN, got %s", res.Taker.Status)
	}
	snap := b.Snapshot(10)
	if len(snap.Bids) != 1 || len(snap.Asks) != 1 {
		t.Fatalf("expected one level per side, got bids=%d asks=%d", len(snap.Bids), len(snap.Asks))
	}
}

func TestFullMatchAtMakerPrice(t *testing.T) {
	b := domain.NewOrderBook("VIB")
	b.Place(order("s1", "seller", domain.Sell, 100, 10))
	res := b.Place(order("b1", "buyer", domain.Buy, 100, 10))

	if len(res.Fills) != 1 {
		t.Fatalf("expected 1 fill, got %d", len(res.Fills))
	}
	f := res.Fills[0]
	if f.Trade.Price != 100 || f.Trade.Quantity != 10 {
		t.Fatalf("unexpected trade %+v", f.Trade)
	}
	if res.Taker.Status != domain.StatusFilled {
		t.Fatalf("expected FILLED, got %s", res.Taker.Status)
	}
	if res.TakerReleaseCOP != 0 {
		t.Fatalf("expected no over-reservation, got %d", res.TakerReleaseCOP)
	}
}

func TestBuyOverReservationRefund(t *testing.T) {
	b := domain.NewOrderBook("VIB")
	// Resting ask cheaper than the incoming buy's limit.
	b.Place(order("s1", "seller", domain.Sell, 90, 10))
	res := b.Place(order("b1", "buyer", domain.Buy, 100, 10))

	if res.Fills[0].Trade.Price != 90 {
		t.Fatalf("expected execution at maker price 90, got %d", res.Fills[0].Trade.Price)
	}
	// Buyer locked 100*10=1000 but spent 90*10=900 -> refund 100.
	if res.TakerReleaseCOP != 100 {
		t.Fatalf("expected refund 100, got %d", res.TakerReleaseCOP)
	}
}

func TestPriceTimePriority(t *testing.T) {
	b := domain.NewOrderBook("VIB")
	// Two asks at the same price; s1 arrives first so it must fill first.
	b.Place(order("s1", "sellerA", domain.Sell, 100, 6))
	b.Place(order("s2", "sellerB", domain.Sell, 100, 6))
	res := b.Place(order("b1", "buyer", domain.Buy, 100, 9))

	if len(res.Fills) != 2 {
		t.Fatalf("expected 2 fills, got %d", len(res.Fills))
	}
	if res.Fills[0].Trade.SellOrderID != "s1" || res.Fills[0].Trade.Quantity != 6 {
		t.Fatalf("first fill should exhaust s1: %+v", res.Fills[0].Trade)
	}
	if res.Fills[1].Trade.SellOrderID != "s2" || res.Fills[1].Trade.Quantity != 3 {
		t.Fatalf("second fill should partially hit s2: %+v", res.Fills[1].Trade)
	}
	if res.Fills[1].Maker.Status != domain.StatusPartiallyFilled {
		t.Fatalf("s2 should be PARTIALLY_FILLED, got %s", res.Fills[1].Maker.Status)
	}
}

func TestPartialFillRemainderRests(t *testing.T) {
	b := domain.NewOrderBook("VIB")
	b.Place(order("s1", "seller", domain.Sell, 100, 4))
	res := b.Place(order("b1", "buyer", domain.Buy, 100, 10))

	if res.Taker.Status != domain.StatusPartiallyFilled {
		t.Fatalf("expected PARTIALLY_FILLED, got %s", res.Taker.Status)
	}
	if res.Taker.FilledQuantity != 4 {
		t.Fatalf("expected 4 filled, got %d", res.Taker.FilledQuantity)
	}
	snap := b.Snapshot(10)
	if len(snap.Bids) != 1 || snap.Bids[0].Quantity != 6 {
		t.Fatalf("expected 6 resting on the bid, got %+v", snap.Bids)
	}
}

func TestBestPriceMatchedFirst(t *testing.T) {
	b := domain.NewOrderBook("VIB")
	b.Place(order("s1", "sellerA", domain.Sell, 105, 10))
	b.Place(order("s2", "sellerB", domain.Sell, 100, 10)) // better (lower) ask
	res := b.Place(order("b1", "buyer", domain.Buy, 110, 5))

	if res.Fills[0].Trade.SellOrderID != "s2" {
		t.Fatalf("expected best ask s2@100 matched first, got %s", res.Fills[0].Trade.SellOrderID)
	}
	if res.Fills[0].Trade.Price != 100 {
		t.Fatalf("expected exec price 100, got %d", res.Fills[0].Trade.Price)
	}
}

func TestCancelReleasesRemainder(t *testing.T) {
	b := domain.NewOrderBook("VIB")
	b.Place(order("b1", "buyer", domain.Buy, 100, 10))
	res, ok := b.Cancel("b1")
	if !ok {
		t.Fatal("expected cancel to succeed")
	}
	if res.Order.Status != domain.StatusCancelled {
		t.Fatalf("expected CANCELLED, got %s", res.Order.Status)
	}
	if res.ReleaseCOP != 1000 { // 100 * 10
		t.Fatalf("expected release 1000 COP, got %d", res.ReleaseCOP)
	}
	if _, ok := b.Cancel("b1"); ok {
		t.Fatal("expected second cancel to fail")
	}
}

func TestCancelSellReleasesVibranium(t *testing.T) {
	b := domain.NewOrderBook("VIB")
	b.Place(order("s1", "seller", domain.Sell, 100, 7))
	res, _ := b.Cancel("s1")
	if res.ReleaseVibranium != 7 {
		t.Fatalf("expected release 7 Vibranium, got %d", res.ReleaseVibranium)
	}
}
