package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"sync-v3/pkg/api"
)

func main() {
	port := flag.Int("port", 8082, "port to listen on")
	flag.Parse()

	// Create API server
	server, err := api.NewServer()
	if err != nil {
		log.Fatalf("Failed to create API server: %v", err)
	}
	defer server.Close()

	// Start HTTP server
	log.Printf("Starting server on :%d", *port)
	log.Println("API endpoints:")
	log.Println("  GET  /v1/data?from={ms}&to={ms}&market={symbol}&exchange=binance&marketType=spot&frame=1m")
	log.Println("  POST /v1/sql with JSON body: {\"query\": \"SELECT * FROM klines LIMIT 10\"}")

	// Wrap server with gzip compression middleware
	handler := gzipMiddleware(server)

	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), handler); err != nil {
		log.Fatalf("Server failed: %v", err)
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
