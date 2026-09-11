package rest

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/meli/orderbook/internal/core/domain"
	"github.com/meli/orderbook/internal/core/port"
	"github.com/meli/orderbook/internal/core/service"
)

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	count, err := s.market.TradeCount(r.Context())
	if err != nil {
		// Health must reflect dependency failure: if the trade store is
		// unreachable the service is degraded, not "ok".
		s.logger.Error("health: trade count failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "degraded",
			"symbol": s.symbol,
			"error":  err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"symbol": s.symbol,
		"trades": count,
	})
}

// --- Orders ---

type placeOrderRequest struct {
	UserID          string `json:"userId"`
	Side            string `json:"side"`
	Price           int64  `json:"price"`
	Quantity        int64  `json:"quantity"`
	ClientRequestID string `json:"clientRequestId,omitempty"`
}

func (s *Server) handlePlaceOrder(w http.ResponseWriter, r *http.Request) {
	var req placeOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	cmd := port.PlaceOrderCommand{
		UserID:   req.UserID,
		Side:     domain.Side(req.Side),
		Price:    req.Price,
		Quantity: req.Quantity,
	}

	result, err := s.trading.PlaceOrder(r.Context(), cmd)
	if err != nil {
		s.writePlaceError(w, result, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

// writePlaceError maps a PlaceOrder error to the right HTTP status, preserving
// the API contract (422 returns the rejected order body).
func (s *Server) writePlaceError(w http.ResponseWriter, order *domain.Order, err error) {
	switch {
	case errors.Is(err, domain.ErrInsufficientCOP), errors.Is(err, domain.ErrInsufficientVibranium):
		writeJSON(w, http.StatusUnprocessableEntity, order)
	case errors.Is(err, domain.ErrWalletNotFound):
		writeError(w, http.StatusNotFound, "wallet not found for user")
	case errors.Is(err, service.ErrReservationFailed):
		writeError(w, http.StatusServiceUnavailable, "wallet store unavailable: "+err.Error())
	case errors.Is(err, domain.ErrInvalidSide),
		errors.Is(err, domain.ErrInvalidPrice),
		errors.Is(err, domain.ErrInvalidQuantity),
		errors.Is(err, domain.ErrMissingUser),
		errors.Is(err, domain.ErrMissingSymbol):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusServiceUnavailable, "engine unavailable: "+err.Error())
	}
}

func (s *Server) handleCancelOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, err := s.trading.CancelOrder(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrOrderNotFound) {
			writeError(w, http.StatusNotFound, "order not found or no longer resting")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "engine unavailable: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	o, found, err := s.trading.GetOrder(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "order store unavailable: "+err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "order not found")
		return
	}
	writeJSON(w, http.StatusOK, o)
}

// --- Wallets ---

type upsertWalletRequest struct {
	UserID             string `json:"userId"`
	COPAvailable       int64  `json:"copAvailable"`
	VibraniumAvailable int64  `json:"vibraniumAvailable"`
}

func (s *Server) handleUpsertWallet(w http.ResponseWriter, r *http.Request) {
	var req upsertWalletRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	wal, err := s.wallets.Seed(r.Context(), req.UserID, req.COPAvailable, req.VibraniumAvailable)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrMissingUser):
			writeError(w, http.StatusBadRequest, "userId is required")
		case errors.Is(err, service.ErrInvalidBalance):
			writeError(w, http.StatusBadRequest, "balances must be non-negative")
		default:
			writeError(w, http.StatusServiceUnavailable, "wallet store unavailable: "+err.Error())
		}
		return
	}
	writeJSON(w, http.StatusCreated, wal)
}

func (s *Server) handleGetWallet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	wal, err := s.wallets.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrWalletNotFound) {
			writeError(w, http.StatusNotFound, "wallet not found")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "wallet store unavailable: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, wal)
}

func (s *Server) handleListWallets(w http.ResponseWriter, r *http.Request) {
	wallets, err := s.wallets.List(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "wallet store unavailable: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, wallets)
}

// --- Market data ---

func (s *Server) handleGetBook(w http.ResponseWriter, r *http.Request) {
	depth := queryInt(r, "depth", 10)
	snap, err := s.market.Book(r.Context(), depth)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "engine unavailable: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleGetTrades(w http.ResponseWriter, r *http.Request) {
	limit := queryInt(r, "limit", 50)
	trades, err := s.market.RecentTrades(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "trade store unavailable: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, trades)
}

func queryInt(r *http.Request, key string, def int) int {
	if v := r.URL.Query().Get(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
