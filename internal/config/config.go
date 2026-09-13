// Package config loads runtime configuration from environment variables with
// sensible defaults, so the service runs with zero setup (`make run`) and can
// be pointed at real infrastructure purely through the environment.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Driver names for the swappable driven adapters.
const (
	// DriverMemory keeps everything in-process (default, zero infrastructure).
	DriverMemory = "memory"
	// DriverPostgres persists wallets/trades/orders in Postgres.
	DriverPostgres = "postgres"
	// DriverKafka publishes/consumes the event log through Kafka/Redpanda.
	DriverKafka = "kafka"
	// DriverRedis fronts wallet reads with a Redis cache.
	DriverRedis = "redis"
	// DriverNone disables an optional component.
	DriverNone = "none"
)

// Config holds all runtime settings.
type Config struct {
	Host     string
	Port     string
	LogLevel string
	Symbol   string

	EngineBuffer int

	// Driver selectors. "memory" (default) runs fully in-process; the other
	// values wire the real adapters.
	EventLogDriver    string // memory | kafka
	WalletStoreDriver string // memory | postgres
	TradeStoreDriver  string // memory | postgres
	OrderStoreDriver  string // memory | postgres
	CacheDriver       string // none   | redis

	// Kafka / Redpanda
	KafkaBrokers       []string
	KafkaEventsTopic   string
	KafkaConsumerGroup string

	// Postgres
	DatabaseURL      string
	DBMaxConns       int32
	DBConnectTimeout time.Duration

	// Redis
	RedisURL string
	RedisTTL time.Duration

	// InfraWaitTimeout bounds how long startup waits for dependencies to
	// become reachable (containers often start out of order).
	InfraWaitTimeout time.Duration

	// --- Observability (OpenTelemetry) ---

	// TelemetryEnabled turns metrics and tracing on. When false the core still
	// calls the Metrics port, but it is wired to a no-op sink.
	TelemetryEnabled bool
	ServiceName      string
	ServiceVersion   string
	// OTLPEndpoint receives traces (e.g. "jaeger:4317"). Empty leaves tracing
	// off while metrics stay on, since metrics are scraped, not pushed.
	OTLPEndpoint     string
	TraceSampleRatio float64
	// ReconcileInterval is how often the money-supply invariant is sampled.
	// Zero disables the job.
	ReconcileInterval time.Duration
}

// Load reads configuration from the environment.
func Load() Config {
	return Config{
		Host:         getEnv("HOST", "0.0.0.0"),
		Port:         getEnv("PORT", "3000"),
		LogLevel:     getEnv("LOG_LEVEL", "info"),
		Symbol:       getEnv("SYMBOL", "VIB"),
		EngineBuffer: getEnvInt("ENGINE_BUFFER", 1<<16),

		EventLogDriver:    getEnv("EVENT_LOG_DRIVER", DriverMemory),
		WalletStoreDriver: getEnv("WALLET_STORE_DRIVER", DriverMemory),
		TradeStoreDriver:  getEnv("TRADE_STORE_DRIVER", DriverMemory),
		OrderStoreDriver:  getEnv("ORDER_STORE_DRIVER", DriverMemory),
		CacheDriver:       getEnv("CACHE_DRIVER", DriverNone),

		KafkaBrokers:       getEnvList("KAFKA_BROKERS", []string{"localhost:9092"}),
		KafkaEventsTopic:   getEnv("KAFKA_EVENTS_TOPIC", "orderbook.events"),
		KafkaConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "settlement"),

		DatabaseURL:      getEnv("DATABASE_URL", "postgres://orderbook:orderbook@localhost:5432/orderbook?sslmode=disable"),
		DBMaxConns:       int32(getEnvInt("DB_MAX_CONNS", 20)),
		DBConnectTimeout: getEnvDuration("DB_CONNECT_TIMEOUT", 5*time.Second),

		RedisURL: getEnv("REDIS_URL", "redis://localhost:6379"),
		RedisTTL: getEnvDuration("REDIS_TTL", 2*time.Second),

		InfraWaitTimeout: getEnvDuration("INFRA_WAIT_TIMEOUT", 60*time.Second),

		TelemetryEnabled:  getEnvBool("TELEMETRY_ENABLED", true),
		ServiceName:       getEnv("OTEL_SERVICE_NAME", "orderbook"),
		ServiceVersion:    getEnv("SERVICE_VERSION", "0.1.0"),
		OTLPEndpoint:      getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		TraceSampleRatio:  getEnvFloat("OTEL_TRACES_SAMPLER_RATIO", 0.05),
		ReconcileInterval: getEnvDuration("RECONCILE_INTERVAL", 15*time.Second),
	}
}

// Addr returns the host:port the HTTP server binds to.
func (c Config) Addr() string { return c.Host + ":" + c.Port }

// UsesPostgres reports whether any repository is backed by Postgres.
func (c Config) UsesPostgres() bool {
	return c.WalletStoreDriver == DriverPostgres ||
		c.TradeStoreDriver == DriverPostgres ||
		c.OrderStoreDriver == DriverPostgres
}

// UsesKafka reports whether the event log is backed by Kafka/Redpanda.
func (c Config) UsesKafka() bool { return c.EventLogDriver == DriverKafka }

// UsesRedis reports whether the Redis cache decorator is enabled.
func (c Config) UsesRedis() bool { return c.CacheDriver == DriverRedis }

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func getEnvFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// getEnvList parses a comma-separated list (e.g. KAFKA_BROKERS=a:9092,b:9092).
func getEnvList(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}
