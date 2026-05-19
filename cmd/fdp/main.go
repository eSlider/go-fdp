package main

import (
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/eslider/go-fdp/internal/handler"
	"github.com/eslider/go-fdp/internal/market"
	"github.com/eslider/go-fdp/internal/store"
	"github.com/eslider/go-fdp/pkg/binance"
	"github.com/eslider/go-fdp/pkg/etl"
	"github.com/eslider/go-fdp/pkg/etl/bitfinex"
	"github.com/eslider/go-fdp/pkg/etl/polymarket"
	"github.com/eslider/go-fdp/pkg/integrity"
	"github.com/gorilla/mux"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	port := flag.Int("port", 8082, "port to listen on")
	flag.Parse()

	st, err := store.NewStore()
	if err != nil {
		slog.Error("store", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	ctx := context.Background()
	consumer, err := binance.NewHistoryConsumer(ctx)
	if err != nil {
		slog.Error("history consumer", "error", err)
		os.Exit(1)
	}

	binanceSource := binance.NewSource(consumer)
	router := etl.NewRouter(
		map[etl.Source]etl.BulkLoader{etl.SourceBinance: binanceSource},
		map[etl.Source]etl.LiveSeries{etl.SourceBinance: binanceSource},
	)
	_ = bitfinex.NewStub()
	_ = polymarket.NewStub()

	db, err := integrity.OpenDB()
	if err != nil {
		slog.Error("duckdb", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	api := market.NewAPI(st, router, consumer, db)
	h := handler.NewMarketHandler(api)

	routerHTTP := mux.NewRouter()
	h.RegisterRoutes(routerHTTP)

	slog.Info("starting fdp server", "port", *port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), gzipMiddleware(routerHTTP)); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

type gzipResponseWriter struct {
	http.ResponseWriter
	gz *gzip.Writer
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.gz.Write(b)
}

// WriteHeader overrides the default implementation of WriteHeader
// to remove the Content-Length header.
func (w *gzipResponseWriter) WriteHeader(statusCode int) {
	w.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(statusCode)
}

func gzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		if enc := w.Header().Get("Content-Encoding"); enc != "" && enc != "identity" {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Add("Vary", "Accept-Encoding")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		next.ServeHTTP(&gzipResponseWriter{ResponseWriter: w, gz: gz}, r)
	})
}
