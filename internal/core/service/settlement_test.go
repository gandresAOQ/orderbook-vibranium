package service_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/meli/orderbook/internal/adapter/driven/matching"
	"github.com/meli/orderbook/internal/adapter/driven/memlog"
	"github.com/meli/orderbook/internal/adapter/driven/memstore"
	"github.com/meli/orderbook/internal/core/domain"
	"github.com/meli/orderbook/internal/core/port"
	"github.com/meli/orderbook/internal/core/service"
)

// harness wires the full pipeline (engine -> log -> settlement) through the
// application services, exactly as cmd/orderbook does, so tests exercise real
// behavior across the hexagon.
type harness struct {
	trading *service.Trading
	wallets *memstore.WalletStore
	trades  *memstore.TradeStore
	orders  *memstore.OrderStore
	log     *memlog.Log
	cancel  context.CancelFunc
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	log := memlog.New(1 << 16)
	wallets := memstore.NewWalletStore()
	trades := memstore.NewTradeStore()
	orders := memstore.NewOrderStore()
	eng := matching.NewEngine("VIB", log, 1<<16)
	trading := service.NewTrading("VIB", eng, wallets, orders)
	settler := service.NewSettlement(log, memstore.NewJournal(), wallets, trades, orders, nil)

	ctx, cancel := context.WithCancel(context.Background())
	go eng.Run(ctx)
	go settler.Run(ctx)

	return &harness{trading: trading, wallets: wallets, trades: trades, orders: orders, log: log, cancel: cancel}
}

func (h *harness) close() { h.cancel() }

func (h *harness) seed(user string, cop, vib int64) {
	_ = h.wallets.Upsert(context.Background(),
		domain.Wallet{UserID: user, COPAvailable: cop, VibraniumAvailable: vib})
}

func (h *harness) tradeCount() int {
	n, _ := h.trades.Count(context.Background())
	return n
}

// place goes through the trading service, which reserves funds then submits.
func (h *harness) place(t *testing.T, user string, side domain.Side, price, qty int64) {
	t.Helper()
	_, err := h.trading.PlaceOrder(context.Background(), port.PlaceOrderCommand{
		UserID: user, Side: side, Price: price, Quantity: qty,
	})
	if err != nil {
		t.Fatalf("place failed for %s: %v", user, err)
	}
}

// waitForTrades blocks until at least n trades have settled or timeout.
func (h *harness) waitForTrades(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.tradeCount() >= n {
			time.Sleep(20 * time.Millisecond) // let trailing release events apply
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d trades, got %d", n, h.tradeCount())
}

func TestEndToEndSettlement(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	h.seed("buyer", 1_000_000, 0)
	h.seed("seller", 0, 100)

	h.place(t, "seller", domain.Sell, 100, 50)
	h.place(t, "buyer", domain.Buy, 100, 50)
	h.waitForTrades(t, 1)

	buyer, _ := h.wallets.Get(context.Background(), "buyer")
	seller, _ := h.wallets.Get(context.Background(), "seller")

	// buyer spent 50*100 = 5000 COP, received 50 Vibranium.
	if buyer.COPAvailable != 995_000 || buyer.COPLocked != 0 || buyer.VibraniumAvailable != 50 {
		t.Fatalf("unexpected buyer wallet: %+v", buyer)
	}
	// seller received 5000 COP, delivered 50 Vibranium.
	if seller.COPAvailable != 5_000 || seller.VibraniumLocked != 0 || seller.VibraniumAvailable != 50 {
		t.Fatalf("unexpected seller wallet: %+v", seller)
	}
}

func TestBuyOverReservationRefundedEndToEnd(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	h.seed("buyer", 1_000, 0)
	h.seed("seller", 0, 10)

	h.place(t, "seller", domain.Sell, 90, 10) // cheaper ask
	h.place(t, "buyer", domain.Buy, 100, 10)  // willing to pay more
	h.waitForTrades(t, 1)

	buyer, _ := h.wallets.Get(context.Background(), "buyer")
	// Locked 1000, spent 900, refund 100 -> available 100, locked 0, vib 10.
	if buyer.COPAvailable != 100 || buyer.COPLocked != 0 || buyer.VibraniumAvailable != 10 {
		t.Fatalf("over-reservation not refunded: %+v", buyer)
	}
}

// TestConcurrentPlacementConservesValue fires many concurrent, fully-matching
// orders and verifies the fundamental invariant: total COP and total Vibranium
// across all users are conserved (no money created or destroyed).
func TestConcurrentPlacementConservesValue(t *testing.T) {
	h := newHarness(t)
	defer h.close()

	const pairs = 500
	const price = 100
	const qty = 1

	// Each pair: one buyer with enough COP, one seller with enough Vibranium.
	for i := 0; i < pairs; i++ {
		h.seed(fmt.Sprintf("buyer-%d", i), price*qty, 0)
		h.seed(fmt.Sprintf("seller-%d", i), 0, qty)
	}

	initialCOP, initialVib := h.totals()

	var wg sync.WaitGroup
	for i := 0; i < pairs; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			h.place(t, fmt.Sprintf("seller-%d", i), domain.Sell, price, qty)
		}(i)
		go func(i int) {
			defer wg.Done()
			h.place(t, fmt.Sprintf("buyer-%d", i), domain.Buy, price, qty)
		}(i)
	}
	wg.Wait()
	h.waitForTrades(t, pairs)

	finalCOP, finalVib := h.totals()
	if finalCOP != initialCOP {
		t.Fatalf("COP not conserved: initial=%d final=%d", initialCOP, finalCOP)
	}
	if finalVib != initialVib {
		t.Fatalf("Vibranium not conserved: initial=%d final=%d", initialVib, finalVib)
	}
	if h.tradeCount() != pairs {
		t.Fatalf("expected %d trades, got %d", pairs, h.tradeCount())
	}
}

func (h *harness) totals() (cop, vib int64) {
	wallets, err := h.wallets.List(context.Background())
	if err != nil {
		panic(err)
	}
	for _, w := range wallets {
		cop += w.TotalCOP()
		vib += w.TotalVibranium()
	}
	return cop, vib
}
