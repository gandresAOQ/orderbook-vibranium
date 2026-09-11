// Package bootstrap is the assembly layer of the composition root: it reads the
// configuration and builds the concrete driven adapters that satisfy the core's
// outbound ports.
//
// This is the ONLY place in the codebase that knows both "Postgres" and
// "in-memory" exist. The core, the services and the REST adapter depend purely
// on port interfaces, which is what makes the swap a configuration change rather
// than a code change.
package bootstrap

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/meli/orderbook/internal/adapter/driven/kafkalog"
	"github.com/meli/orderbook/internal/adapter/driven/memlog"
	"github.com/meli/orderbook/internal/adapter/driven/memstore"
	"github.com/meli/orderbook/internal/adapter/driven/postgres"
	"github.com/meli/orderbook/internal/adapter/driven/rediscache"
	"github.com/meli/orderbook/internal/config"
	"github.com/meli/orderbook/internal/core/port"
)

// Infrastructure is the assembled set of driven adapters plus the resources
// that must be released on shutdown.
type Infrastructure struct {
	Log     port.EventLog
	Journal port.EventJournal
	Wallets port.WalletRepository
	Trades  port.TradeRepository
	Orders  port.OrderRepository

	// Describe reports the chosen drivers, for the startup log line.
	Describe map[string]string

	closers []io.Closer
	pool    *pgxpool.Pool
	rdb     *redis.Client
}

// Build wires the adapters selected by cfg.
//
// It fails fast: if a driver is configured but its backing service is
// unreachable, startup returns an error instead of silently falling back to
// memory. Silently degrading a ledger to non-durable storage would be worse than
// not starting.
func Build(ctx context.Context, cfg config.Config, logger *slog.Logger) (*Infrastructure, error) {
	infra := &Infrastructure{Describe: map[string]string{}}

	// --- Postgres (shared by wallets / trades / orders / journal) ---
	if cfg.UsesPostgres() {
		pool, err := postgres.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns, cfg.InfraWaitTimeout)
		if err != nil {
			return nil, err
		}
		if err := postgres.Migrate(ctx, pool); err != nil {
			pool.Close()
			return nil, err
		}
		infra.pool = pool
		logger.Info("postgres ready", "maxConns", cfg.DBMaxConns)
	}

	// --- Wallets ---
	switch cfg.WalletStoreDriver {
	case config.DriverPostgres:
		infra.Wallets = postgres.NewWalletStore(infra.pool)
		infra.Describe["wallets"] = "postgres"
	case config.DriverMemory:
		infra.Wallets = memstore.NewWalletStore()
		infra.Describe["wallets"] = "memory"
	default:
		return nil, fmt.Errorf("unknown WALLET_STORE_DRIVER %q", cfg.WalletStoreDriver)
	}

	// --- Trades ---
	switch cfg.TradeStoreDriver {
	case config.DriverPostgres:
		infra.Trades = postgres.NewTradeStore(infra.pool)
		infra.Describe["trades"] = "postgres"
	case config.DriverMemory:
		infra.Trades = memstore.NewTradeStore()
		infra.Describe["trades"] = "memory"
	default:
		return nil, fmt.Errorf("unknown TRADE_STORE_DRIVER %q", cfg.TradeStoreDriver)
	}

	// --- Orders ---
	switch cfg.OrderStoreDriver {
	case config.DriverPostgres:
		infra.Orders = postgres.NewOrderStore(infra.pool)
		infra.Describe["orders"] = "postgres"
	case config.DriverMemory:
		infra.Orders = memstore.NewOrderStore()
		infra.Describe["orders"] = "memory"
	default:
		return nil, fmt.Errorf("unknown ORDER_STORE_DRIVER %q", cfg.OrderStoreDriver)
	}

	// --- Settlement journal (idempotency) ---
	// Durable whenever Postgres is available: the journal is only meaningful
	// across restarts, which is exactly when redelivery happens.
	if infra.pool != nil {
		infra.Journal = postgres.NewJournal(infra.pool)
		infra.Describe["journal"] = "postgres"
	} else {
		infra.Journal = memstore.NewJournal()
		infra.Describe["journal"] = "memory"
	}

	// --- Redis read cache (optional decorator over wallets) ---
	if cfg.UsesRedis() {
		rdb, err := rediscache.Connect(ctx, cfg.RedisURL, cfg.InfraWaitTimeout)
		if err != nil {
			infra.Close()
			return nil, err
		}
		infra.rdb = rdb
		infra.Wallets = rediscache.NewWalletCache(infra.Wallets, rdb, cfg.RedisTTL, logger)
		infra.Describe["cache"] = "redis"
		logger.Info("redis cache ready", "ttl", cfg.RedisTTL)
	} else {
		infra.Describe["cache"] = "none"
	}

	// --- Event log ---
	switch cfg.EventLogDriver {
	case config.DriverKafka:
		kl, err := kafkalog.New(ctx, kafkalog.Config{
			Brokers:       cfg.KafkaBrokers,
			Topic:         cfg.KafkaEventsTopic,
			ConsumerGroup: cfg.KafkaConsumerGroup,
			Buffer:        cfg.EngineBuffer,
		}, cfg.InfraWaitTimeout, logger)
		if err != nil {
			infra.Close()
			return nil, err
		}
		infra.Log = kl
		infra.Describe["eventLog"] = "kafka:" + cfg.KafkaEventsTopic
		logger.Info("kafka event log ready",
			"brokers", cfg.KafkaBrokers, "topic", cfg.KafkaEventsTopic, "group", cfg.KafkaConsumerGroup)
	case config.DriverMemory:
		infra.Log = memlog.New(cfg.EngineBuffer)
		infra.Describe["eventLog"] = "memory"
	default:
		infra.Close()
		return nil, fmt.Errorf("unknown EVENT_LOG_DRIVER %q", cfg.EventLogDriver)
	}

	return infra, nil
}

// Close releases the pooled/networked resources. The event log is closed
// separately during shutdown, because settlement must drain it first.
func (i *Infrastructure) Close() {
	if i.rdb != nil {
		_ = i.rdb.Close()
		i.rdb = nil
	}
	if i.pool != nil {
		i.pool.Close()
		i.pool = nil
	}
	for _, c := range i.closers {
		_ = c.Close()
	}
	i.closers = nil
}
