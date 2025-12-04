package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"sync-v3/pkg/api"
)

func main() {
	// Initialize JSON logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	port := flag.Int("port", 8082, "port to listen on")
	flag.Parse()

	// Create API server
	server, err := api.NewServer()
	if err != nil {
		slog.Error("Failed to create API server", "error", err)
		os.Exit(1)
	}
	defer server.Close()

	// Start HTTP server
	slog.Info("Starting server",
		"port", *port,
		"endpoints", []string{
			"GET  /v1/data?from={ms}&to={ms}&market={symbol}&exchange=binance&marketType=spot&frame=1m",
			"POST /v1/sql with JSON body: {\"query\": \"SELECT * FROM klines LIMIT 10\"}",
		},
	)

	// Wrap server with gzip compression middleware
	handler := gzipMiddleware(server)

	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), handler); err != nil {
		slog.Error("Server failed", "error", err)
		os.Exit(1)
	}
}

// gzipResponseWriter wraps http.ResponseWriter to provide gzip compression
type gzipResponseWriter struct {
	http.ResponseWriter
	gz *gzip.Writer
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.gz.Write(b)
}

func (w *gzipResponseWriter) WriteHeader(statusCode int) {
	// Content length is unknown once gzipped; ensure it's not set
	w.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(statusCode)
}

// gzipMiddleware compresses responses when client supports gzip.
func gzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only gzip if client advertises support
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}

		// Avoid double-compressing if already encoded
		if enc := w.Header().Get("Content-Encoding"); enc != "" && enc != "identity" {
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Add("Vary", "Accept-Encoding")

		gz := gzip.NewWriter(w)
		defer gz.Close()

		grw := &gzipResponseWriter{ResponseWriter: w, gz: gz}
		next.ServeHTTP(grw, r)
	})
}
