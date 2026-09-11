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

	"github.com/meli/orderbook/internal/core/port"
)

// Server holds the inbound ports the HTTP handlers delegate to.
type Server struct {
	symbol  string
	trading port.TradingService
	market  port.MarketDataService
	wallets port.WalletService
	logger  *slog.Logger
}

// NewServer builds an API server wired to the application services.
func NewServer(
	symbol string,
	trading port.TradingService,
	market port.MarketDataService,
	wallets port.WalletService,
	logger *slog.Logger,
) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{symbol: symbol, trading: trading, market: market, wallets: wallets, logger: logger}
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

	return logging(s.logger, mux)
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
