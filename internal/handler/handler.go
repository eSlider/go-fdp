package handler

import (
	"encoding/json"
	"net/http"
	"sync-v3/internal/domain"
	"sync-v3/internal/service"
	"sync-v3/pkg/data"

	"github.com/go-playground/validator/v10"
	"github.com/go-viper/mapstructure/v2"
	"github.com/gorilla/mux"
)

type MarketHandler struct {
	service  *service.MarketService
	validate *validator.Validate
}

func NewMarketHandler(service *service.MarketService) *MarketHandler {
	return &MarketHandler{
		service:  service,
		validate: validator.New(),
	}
}

func (h *MarketHandler) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/v1/data", h.GetMarketHistory).Methods("GET")
	r.HandleFunc("/v1/aggtrades", h.GetAggTrades).Methods("GET")
	r.HandleFunc("/v1/symbols", h.GetSymbols).Methods("GET")
	r.HandleFunc("/v1/markets", h.GetMarkets).Methods("GET")
}

// AssetRequestDTO represents query parameters for /v1/data
type AssetRequestDTO struct {
	From       int64  `validate:"required"`              // Millisecond timestamp
	To         int64  `validate:"required,gtfield=From"` // Millisecond timestamp
	Market     string `validate:"required"`
	Exchange   string `validate:"omitempty"`
	MarketType string `validate:"omitempty,oneof=spot futures options"`
	Frame      string `validate:"omitempty,oneof=1s 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M"`
	Indicator  string `validate:"omitempty,oneof=klines aggTrades"`
}

// AggTradesRequestDTO represents query parameters for /v1/aggtrades
type AggTradesRequestDTO struct {
	From       int64  `validate:"required"`              // Millisecond timestamp
	To         int64  `validate:"required,gtfield=From"` // Millisecond timestamp
	Market     string `validate:"required"`
	Exchange   string `validate:"omitempty"`
	MarketType string `validate:"omitempty,oneof=spot futures options"`
}

// AggTradesLast24hRequestDTO represents query parameters for /v1/aggtrades/last24h
type AggTradesLast24hRequestDTO struct {
	Market     string `validate:"required"`
	Exchange   string `validate:"omitempty"`
	MarketType string `validate:"omitempty,oneof=spot futures options"`
}

func (h *MarketHandler) GetMarketHistory(w http.ResponseWriter, r *http.Request) {
	// Default values
	dto := &AssetRequestDTO{
		Exchange:   "binance",
		MarketType: "spot",
		Frame:      "1m",
		Indicator:  "klines",
	}

	if err := h.bindAndValidate(r, dto); err != nil {
		h.writeError(w, err, http.StatusBadRequest)
		return
	}

	req := domain.MarketDataRequest{
		From:       *data.AnyTimestampToTime(dto.From),
		To:         *data.AnyTimestampToTime(dto.To),
		Market:     dto.Market,
		Exchange:   dto.Exchange,
		MarketType: domain.NewMarketType(dto.MarketType),
		Frame:      domain.NewFrame(dto.Frame),
		Indicator:  domain.Indicator(dto.Indicator),
	}

	candles, err := h.service.GetMarketHistory(r.Context(), req)
	if err != nil {
		h.writeError(w, err, http.StatusInternalServerError)
		return
	}

	h.writeJSON(w, candles)
}

func (h *MarketHandler) GetAggTrades(w http.ResponseWriter, r *http.Request) {
	// Default values
	dto := &AggTradesRequestDTO{
		Exchange:   "binance",
		MarketType: "spot",
	}

	if err := h.bindAndValidate(r, dto); err != nil {
		h.writeError(w, err, http.StatusBadRequest)
		return
	}

	req := domain.MarketDataRequest{
		From:       *data.AnyTimestampToTime(dto.From),
		To:         *data.AnyTimestampToTime(dto.To),
		Market:     dto.Market,
		Exchange:   dto.Exchange,
		MarketType: domain.NewMarketType(dto.MarketType),
		Frame:      domain.NoFrame, // AggTrades don't have frames
		Indicator:  domain.AggTrades,
	}

	aggTrades, err := h.service.GetAggTrades(r.Context(), req)
	if err != nil {
		h.writeError(w, err, http.StatusInternalServerError)
		return
	}

	h.writeJSON(w, aggTrades)
}

func (h *MarketHandler) GetMarkets(w http.ResponseWriter, r *http.Request) {
	markets, err := h.service.GetMarkets(r.Context())
	if err != nil {
		h.writeError(w, err, http.StatusInternalServerError)
		return
	}
	response := make([]map[string]string, len(markets))
	for i, v := range markets {
		response[i] = map[string]string{"name": v}
	}
	h.writeJSON(w, response)
}

func (h *MarketHandler) GetSymbols(w http.ResponseWriter, r *http.Request) {
	symbols, err := h.service.GetSymbols(r.Context())
	if err != nil {
		h.writeError(w, err, http.StatusInternalServerError)
		return
	}
	response := make([]map[string]string, len(symbols))
	for i, v := range symbols {
		response[i] = map[string]string{"symbol": v}
	}
	h.writeJSON(w, response)
}

func (h *MarketHandler) bindAndValidate(r *http.Request, dest any) error {
	values := r.URL.Query()
	m := make(map[string]any)
	for k, v := range values {
		if len(v) > 0 {
			m[k] = v[0]
		}
	}

	if err := mapstructure.WeakDecode(m, dest); err != nil {
		return err
	}

	return h.validate.Struct(dest)
}

func (h *MarketHandler) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (h *MarketHandler) writeError(w http.ResponseWriter, err error, code int) {
	w.WriteHeader(code)
	h.writeJSON(w, map[string]string{"error": err.Error()})
}
