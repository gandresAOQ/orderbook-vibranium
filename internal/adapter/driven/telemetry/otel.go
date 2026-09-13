package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/meli/orderbook/internal/core/domain"
)

// Config describes how telemetry is exported.
type Config struct {
	ServiceName    string
	ServiceVersion string
	Symbol         string

	// OTLPEndpoint receives traces (e.g. "jaeger:4317"). Empty disables tracing
	// while leaving metrics on, because metrics are useful with no collector at
	// all — they are scraped straight off /metrics.
	OTLPEndpoint string

	// TraceSampleRatio in [0,1]. Tracing every order at thousands per second is
	// expensive and rarely more informative than a sample.
	TraceSampleRatio float64
}

// Telemetry is the OpenTelemetry implementation of port.Metrics plus the
// provider lifecycle.
//
// Metrics are exposed in Prometheus format on an HTTP handler (pull model),
// which is the simplest thing that works locally: no collector to run, and the
// endpoint is inspectable with curl during a demo. Traces are pushed over OTLP
// because there is nothing to pull them from.
type Telemetry struct {
	logger *slog.Logger

	meterProvider *sdkmetric.MeterProvider
	traceProvider *sdktrace.TracerProvider
	registry      *prometheus.Registry

	// Instruments. Attribute sets are deliberately narrow: side, status, event
	// type and a coarse reason. No order IDs or user IDs — those are unbounded
	// and would turn every distinct value into a new time series.
	orders      metric.Int64Counter
	orderErrors metric.Int64Counter
	acceptTime  metric.Float64Histogram

	trades      metric.Int64Counter
	tradedQty   metric.Int64Counter
	tradedValue metric.Int64Counter

	settled     metric.Int64Counter
	settleFails metric.Int64Counter
	settleLag   metric.Float64Histogram

	copSupply metric.Int64Gauge
	vibSupply metric.Int64Gauge

	symbolAttr attribute.KeyValue
}

// New builds the providers and instruments. The returned Telemetry is ready to
// use; call Shutdown to flush on exit.
func New(ctx context.Context, cfg Config, logger *slog.Logger) (*Telemetry, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "orderbook"
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
	))
	if err != nil {
		return nil, fmt.Errorf("build otel resource: %w", err)
	}

	// --- Metrics: Prometheus pull exporter ---
	registry := prometheus.NewRegistry()
	promExporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		return nil, fmt.Errorf("build prometheus exporter: %w", err)
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(promExporter),
	)
	otel.SetMeterProvider(meterProvider)

	t := &Telemetry{
		logger:        logger,
		meterProvider: meterProvider,
		registry:      registry,
		symbolAttr:    attribute.String("symbol", cfg.Symbol),
	}
	if err := t.initInstruments(meterProvider.Meter("github.com/meli/orderbook")); err != nil {
		return nil, err
	}

	// --- Traces: OTLP push exporter (optional) ---
	if cfg.OTLPEndpoint != "" {
		exp, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("build OTLP trace exporter: %w", err)
		}
		ratio := cfg.TraceSampleRatio
		if ratio <= 0 {
			ratio = 0.05
		}
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithBatcher(exp),
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		)
		t.traceProvider = tp
		otel.SetTracerProvider(tp)
		logger.Info("otel tracing enabled", "endpoint", cfg.OTLPEndpoint, "sampleRatio", ratio)
	}

	// W3C propagation, so a trace can be continued across process boundaries.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return t, nil
}

func (t *Telemetry) initInstruments(m metric.Meter) error {
	var err error
	fail := func(name string, e error) error {
		return fmt.Errorf("create instrument %s: %w", name, e)
	}

	if t.orders, err = m.Int64Counter("orderbook.orders",
		metric.WithDescription("Orders accepted into the engine"),
		metric.WithUnit("{order}")); err != nil {
		return fail("orders", err)
	}
	if t.orderErrors, err = m.Int64Counter("orderbook.orders.rejected",
		metric.WithDescription("Orders refused before entering the book"),
		metric.WithUnit("{order}")); err != nil {
		return fail("orders.rejected", err)
	}
	if t.acceptTime, err = m.Float64Histogram("orderbook.order.accept.duration",
		metric.WithDescription("Latency of the accept path: validate, reserve funds, match"),
		metric.WithUnit("ms")); err != nil {
		return fail("order.accept.duration", err)
	}

	if t.trades, err = m.Int64Counter("orderbook.trades",
		metric.WithDescription("Trades executed"),
		metric.WithUnit("{trade}")); err != nil {
		return fail("trades", err)
	}
	if t.tradedQty, err = m.Int64Counter("orderbook.traded.quantity",
		metric.WithDescription("Vibranium units traded"),
		metric.WithUnit("{unit}")); err != nil {
		return fail("traded.quantity", err)
	}
	// No WithUnit here: "COP" is not a UCUM unit, so the Prometheus exporter
	// would append a literal `_COP` suffix to the series name. The currency is
	// already in the metric name, and a predictable name matters because the
	// alert rules in deploy/rules.yml reference it.
	if t.tradedValue, err = m.Int64Counter("orderbook.traded.notional",
		metric.WithDescription("COP value exchanged")); err != nil {
		return fail("traded.notional", err)
	}

	if t.settled, err = m.Int64Counter("orderbook.settlement.events",
		metric.WithDescription("Events applied by settlement"),
		metric.WithUnit("{event}")); err != nil {
		return fail("settlement.events", err)
	}
	if t.settleFails, err = m.Int64Counter("orderbook.settlement.failures",
		metric.WithDescription("Events settlement could not apply"),
		metric.WithUnit("{event}")); err != nil {
		return fail("settlement.failures", err)
	}
	if t.settleLag, err = m.Float64Histogram("orderbook.settlement.lag",
		metric.WithDescription("Delay between the engine emitting an event and settlement applying it"),
		metric.WithUnit("ms")); err != nil {
		return fail("settlement.lag", err)
	}

	if t.copSupply, err = m.Int64Gauge("orderbook.supply.cop",
		metric.WithDescription("Total COP across all wallets (available + locked); must stay flat")); err != nil {
		return fail("supply.cop", err)
	}
	if t.vibSupply, err = m.Int64Gauge("orderbook.supply.vibranium",
		metric.WithDescription("Total Vibranium across all wallets (available + locked); must stay flat")); err != nil {
		return fail("supply.vibranium", err)
	}
	return nil
}

// MetricsHandler serves the Prometheus scrape endpoint.
func (t *Telemetry) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(t.registry, promhttp.HandlerOpts{})
}

// Tracer returns a tracer for instrumenting spans.
func (t *Telemetry) Tracer(name string) trace.Tracer { return otel.Tracer(name) }

// Shutdown flushes pending telemetry. Traces are batched, so without this the
// last spans of a run are lost.
func (t *Telemetry) Shutdown(ctx context.Context) {
	if t.traceProvider != nil {
		if err := t.traceProvider.Shutdown(ctx); err != nil {
			t.logger.Error("otel trace shutdown", "err", err)
		}
	}
	if t.meterProvider != nil {
		if err := t.meterProvider.Shutdown(ctx); err != nil {
			t.logger.Error("otel meter shutdown", "err", err)
		}
	}
}

// --- port.Metrics ---

func (t *Telemetry) OrderPlaced(ctx context.Context, side domain.Side, status domain.OrderStatus, d time.Duration) {
	attrs := metric.WithAttributes(
		t.symbolAttr,
		attribute.String("side", string(side)),
		attribute.String("status", string(status)),
	)
	t.orders.Add(ctx, 1, attrs)
	t.acceptTime.Record(ctx, float64(d.Microseconds())/1000.0, attrs)
}

func (t *Telemetry) OrderRejected(ctx context.Context, reason string) {
	t.orderErrors.Add(ctx, 1, metric.WithAttributes(
		t.symbolAttr, attribute.String("reason", reason)))
}

func (t *Telemetry) TradeExecuted(ctx context.Context, tr domain.Trade) {
	attrs := metric.WithAttributes(t.symbolAttr)
	t.trades.Add(ctx, 1, attrs)
	t.tradedQty.Add(ctx, tr.Quantity, attrs)
	t.tradedValue.Add(ctx, tr.Notional(), attrs)
}

func (t *Telemetry) SettlementApplied(ctx context.Context, evType domain.EventType, lag time.Duration, err error) {
	attrs := metric.WithAttributes(
		t.symbolAttr, attribute.String("event.type", string(evType)))
	if err != nil {
		t.settleFails.Add(ctx, 1, attrs)
		return
	}
	t.settled.Add(ctx, 1, attrs)
	if lag > 0 {
		t.settleLag.Record(ctx, float64(lag.Microseconds())/1000.0, attrs)
	}
}

func (t *Telemetry) MoneySupply(ctx context.Context, cop, vibranium int64) {
	attrs := metric.WithAttributes(t.symbolAttr)
	t.copSupply.Record(ctx, cop, attrs)
	t.vibSupply.Record(ctx, vibranium, attrs)
}
