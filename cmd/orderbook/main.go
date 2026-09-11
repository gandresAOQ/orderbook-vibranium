// Command orderbook is the entrypoint and composition root for the Vibranium
// order book service. It wires the hexagonal pieces together:
//
//	Driving adapter (REST) ──▶ inbound ports (services) ──▶ outbound ports
//	                                                          │
//	  ┌───────────────────────────────────────────────────────┤
//	  ▼                     ▼                    ▼              ▼
//	MatchingEngine     WalletRepository    TradeRepository   EventLog
//	(matching)         (memory|postgres)   (memory|postgres) (memory|kafka)
//
// Every boundary is a port interface, so the in-memory adapters and the real
// infrastructure (Redpanda/Postgres/Redis) are interchangeable through
// environment variables alone — see internal/bootstrap.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/meli/orderbook/internal/adapter/driven/matching"
	"github.com/meli/orderbook/internal/adapter/driving/rest"
	"github.com/meli/orderbook/internal/bootstrap"
	"github.com/meli/orderbook/internal/config"
	"github.com/meli/orderbook/internal/core/service"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe the local /health endpoint and exit")
	flag.Parse()

	cfg := config.Load()

	if *healthcheck {
		os.Exit(runHealthcheck("127.0.0.1:" + cfg.Port))
	}
	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	// --- Driven adapters, selected by configuration ---
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), cfg.InfraWaitTimeout+30*time.Second)
	infra, err := bootstrap.Build(startupCtx, cfg, logger)
	cancelStartup()
	if err != nil {
		logger.Error("infrastructure unavailable, refusing to start", "err", err)
		os.Exit(1)
	}
	defer infra.Close()

	eng := matching.NewEngine(cfg.Symbol, infra.Log, cfg.EngineBuffer)

	// --- Application core (inbound ports) ---
	trading := service.NewTrading(cfg.Symbol, eng, infra.Wallets, infra.Orders)
	market := service.NewMarketData(eng, infra.Trades)
	walletSvc := service.NewWallets(infra.Wallets)
	settler := service.NewSettlement(infra.Log, infra.Journal, infra.Wallets, infra.Trades, infra.Orders, logger)

	// --- Driving adapter (REST) ---
	server := rest.NewServer(cfg.Symbol, trading, market, walletSvc, logger)

	// Engine and settlement run as long-lived goroutines.
	engineCtx, stopEngine := context.WithCancel(context.Background())
	settleCtx, stopSettle := context.WithCancel(context.Background())
	go eng.Run(engineCtx)
	go settler.Run(settleCtx)

	httpServer := &http.Server{
		Addr:              cfg.Addr(),
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("orderbook listening",
			"addr", cfg.Addr(), "symbol", cfg.Symbol, "adapters", infra.Describe)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server error", "err", err)
			os.Exit(1)
		}
	}()

	// --- Graceful shutdown ---
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down")

	// 1) Stop accepting new HTTP requests.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown error", "err", err)
	}

	// 2) Stop the engine, then close the log so settlement drains remaining
	//    events (the log flushes in-flight events to the broker first).
	stopEngine()
	infra.Log.Close()

	// 3) Give settlement a moment to finish, then stop it.
	time.Sleep(500 * time.Millisecond)
	stopSettle()
	logger.Info("bye")
}

// runHealthcheck probes /health and returns an exit code (0 healthy, 1 not).
// Used by the container healthcheck since the distroless image has no shell.
func runHealthcheck(addr string) int {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/health")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
