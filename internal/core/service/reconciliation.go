package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/meli/orderbook/internal/core/port"
)

// Reconciliation periodically proves the system's most important invariant:
// trading moves value between users but never creates or destroys it, so the
// total COP and Vibranium across all wallets (available + locked) must stay
// constant.
//
// The load test asserts this at the end of a run. This service asserts it
// continuously, which is what you actually want in production: a silent
// accounting bug is far more dangerous than a loud crash, and a flat gauge that
// suddenly moves is the earliest possible signal.
//
// It only reads, so it can never be the cause of a discrepancy.
type Reconciliation struct {
	wallets  port.WalletRepository
	metrics  port.Metrics
	logger   *slog.Logger
	interval time.Duration

	// baseline is the first observation; later observations are compared to it.
	baselineCOP int64
	baselineVib int64
	haveBase    bool
}

// NewReconciliation builds the reconciliation job. A zero interval disables it.
func NewReconciliation(
	wallets port.WalletRepository,
	metrics port.Metrics,
	logger *slog.Logger,
	interval time.Duration,
) *Reconciliation {
	if logger == nil {
		logger = slog.Default()
	}
	return &Reconciliation{wallets: wallets, metrics: metrics, logger: logger, interval: interval}
}

// Run samples the money supply until ctx is cancelled. Run it in its own
// goroutine: `go reconciliation.Run(ctx)`.
func (r *Reconciliation) Run(ctx context.Context) {
	if r.interval <= 0 {
		return
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.sample(ctx)
		}
	}
}

// sample reads every wallet and reports the totals.
//
// Note the deliberate limitation: this is a non-transactional read, so a sample
// taken mid-settlement can catch a trade with one leg applied and see a
// transient discrepancy. That is why the log below is a warning and not an
// alert — a real alert should fire on a discrepancy that PERSISTS across
// consecutive samples, which is what a Prometheus alert rule with a `for:`
// clause expresses naturally.
func (r *Reconciliation) sample(ctx context.Context) {
	wallets, err := r.wallets.List(ctx)
	if err != nil {
		r.logger.Error("reconciliation: cannot read wallets", "err", err)
		return
	}

	var cop, vib int64
	for _, w := range wallets {
		cop += w.TotalCOP()
		vib += w.TotalVibranium()
	}
	r.metrics.MoneySupply(ctx, cop, vib)

	if !r.haveBase {
		r.baselineCOP, r.baselineVib, r.haveBase = cop, vib, true
		r.logger.Info("reconciliation baseline",
			"wallets", len(wallets), "totalCOP", cop, "totalVibranium", vib)
		return
	}

	// Seeding wallets legitimately changes the totals (it is a deposit, not a
	// trade), so a change is only reported, and the baseline moves with it.
	if cop != r.baselineCOP || vib != r.baselineVib {
		r.logger.Warn("reconciliation: money supply changed",
			"copBefore", r.baselineCOP, "copNow", cop,
			"vibraniumBefore", r.baselineVib, "vibraniumNow", vib,
			"note", "expected only when wallets are seeded or credited externally")
		r.baselineCOP, r.baselineVib = cop, vib
	}
}
