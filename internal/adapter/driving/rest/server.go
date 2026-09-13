// Package rest is the driving adapter that exposes the application core over
// HTTP using only the standard library (net/http with Go 1.22+ method+pattern
// routing). It is stateless and horizontally scalable: it maps request DTOs to
// inbound-port commands, delegates to the services, and maps results/errors to
// HTTP responses. All business orchestration lives in the core, not here.
package rest

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/meli/orderbook/internal/core/port"
)

// Server holds the inbound ports the HTTP handlers delegate to.
type Server struct {
	symbol  string
	trading port.TradingService
	market  port.MarketDataService
	wallets port.WalletService
	logger  *slog.Logger

	// metricsHandler serves the Prometheus scrape endpoint. Nil disables the
	// route, so the API is identical whether or not telemetry is enabled.
	metricsHandler http.Handler
	// tracing wraps the router with OpenTelemetry HTTP instrumentation.
	tracing bool
}

// Option configures optional server behaviour.
type Option func(*Server)

// WithMetricsEndpoint exposes h at GET /metrics.
func WithMetricsEndpoint(h http.Handler) Option {
	return func(s *Server) { s.metricsHandler = h }
}

// WithTracing wraps the router in otelhttp so every request becomes a span and
// incoming W3C traceparent headers are honoured.
func WithTracing() Option {
	return func(s *Server) { s.tracing = true }
}

// NewServer builds an API server wired to the application services.
func NewServer(
	symbol string,
	trading port.TradingService,
	market port.MarketDataService,
	wallets port.WalletService,
	logger *slog.Logger,
	opts ...Option,
) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{symbol: symbol, trading: trading, market: market, wallets: wallets, logger: logger}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Handler returns the configured HTTP handler (router).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", s.handleHealth)

	// Orders
	mux.HandleFunc("POST /orders", s.handlePlaceOrder)
	mux.HandleFunc("DELETE /orders/{id}", s.handleCancelOrder)
	mux.HandleFunc("GET /orders/{id}", s.handleGetOrder)

	// Wallets (seeding is for testing only; real registration is out of scope)
	mux.HandleFunc("POST /wallets", s.handleUpsertWallet)
	mux.HandleFunc("GET /wallets", s.handleListWallets)
	mux.HandleFunc("GET /wallets/{id}", s.handleGetWallet)

	// Market data & traceability
	mux.HandleFunc("GET /book", s.handleGetBook)
	mux.HandleFunc("GET /trades", s.handleGetTrades)

	// Observability: Prometheus scrape endpoint (pull model).
	if s.metricsHandler != nil {
		mux.Handle("GET /metrics", s.metricsHandler)
	}

	var h http.Handler = logging(s.logger, mux)
	if s.tracing {
		// otelhttp reads the incoming traceparent, starts a server span and
		// injects it into the request context, so everything the handlers call
		// downstream is automatically part of the same trace.
		//
		// The span name comes from the ROUTE pattern, not the raw path: naming
		// spans after "/orders/ord-9f3a..." would create unbounded span names.
		h = otelhttp.NewHandler(h, "orderbook",
			otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
				if pattern := routePattern(r); pattern != "" {
					return r.Method + " " + pattern
				}
				return r.Method
			}),
			// Scraping our own metrics endpoint would generate a span per scrape.
			otelhttp.WithFilter(func(r *http.Request) bool {
				return r.URL.Path != "/metrics" && r.URL.Path != "/health"
			}),
		)
	}
	return h
}

// routePattern collapses identifiers into the matched route so span names stay
// low-cardinality (e.g. "DELETE /orders/{id}" rather than one name per order).
func routePattern(r *http.Request) string {
	path := r.URL.Path
	switch {
	case path == "/orders":
		return "/orders"
	case strings.HasPrefix(path, "/orders/"):
		return "/orders/{id}"
	case path == "/wallets":
		return "/wallets"
	case strings.HasPrefix(path, "/wallets/"):
		return "/wallets/{id}"
	case path == "/book", path == "/trades":
		return path
	default:
		return ""
	}
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

// logging is a minimal access-log middleware.
func logging(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		logger.Debug("http", "method", r.Method, "path", r.URL.Path, "status", sw.status)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
