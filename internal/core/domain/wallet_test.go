package domain_test

import (
	"errors"
	"testing"

	"github.com/meli/orderbook/internal/core/domain"
)

func TestReserveCOP(t *testing.T) {
	w := &domain.Wallet{UserID: "u", COPAvailable: 1000}
	if err := w.ReserveCOP(600); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.COPAvailable != 400 || w.COPLocked != 600 {
		t.Fatalf("unexpected balances: %+v", w)
	}
	if err := w.ReserveCOP(500); !errors.Is(err, domain.ErrInsufficientCOP) {
		t.Fatalf("expected ErrInsufficientCOP, got %v", err)
	}
}

func TestReserveVibranium(t *testing.T) {
	w := &domain.Wallet{UserID: "u", VibraniumAvailable: 10}
	if err := w.ReserveVibranium(4); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.VibraniumAvailable != 6 || w.VibraniumLocked != 4 {
		t.Fatalf("unexpected balances: %+v", w)
	}
	if err := w.ReserveVibranium(7); !errors.Is(err, domain.ErrInsufficientVibranium) {
		t.Fatalf("expected ErrInsufficientVibranium, got %v", err)
	}
}

func TestSettleBuyConservesTotals(t *testing.T) {
	// Buyer reserved 1000 COP to buy 10 @ 100.
	w := &domain.Wallet{UserID: "buyer", COPAvailable: 0, COPLocked: 1000}
	if err := w.SettleBuy(1000, 10); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.COPLocked != 0 || w.VibraniumAvailable != 10 {
		t.Fatalf("unexpected balances: %+v", w)
	}
}

func TestSettleSellConservesTotals(t *testing.T) {
	w := &domain.Wallet{UserID: "seller", VibraniumLocked: 10}
	if err := w.SettleSell(10, 1000); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.VibraniumLocked != 0 || w.COPAvailable != 1000 {
		t.Fatalf("unexpected balances: %+v", w)
	}
}

func TestReleaseReturnsLockedToAvailable(t *testing.T) {
	w := &domain.Wallet{UserID: "u", COPLocked: 500, VibraniumLocked: 5}
	if err := w.ReleaseCOP(500); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := w.ReleaseVibranium(5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.COPAvailable != 500 || w.COPLocked != 0 {
		t.Fatalf("unexpected COP: %+v", w)
	}
	if w.VibraniumAvailable != 5 || w.VibraniumLocked != 0 {
		t.Fatalf("unexpected Vibranium: %+v", w)
	}
}

func TestSettleGuardsAgainstNegative(t *testing.T) {
	w := &domain.Wallet{UserID: "u", COPLocked: 100}
	if err := w.SettleBuy(200, 1); !errors.Is(err, domain.ErrInsufficientLocked) {
		t.Fatalf("expected ErrInsufficientLocked, got %v", err)
	}
}
