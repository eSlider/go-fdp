package main

import (
	"log"
	"net/http"
	"sync-v3/pkg/api"
)

func main() {
	// Create API server
	server, err := api.NewServer()
	if err != nil {
		log.Fatalf("Failed to create API server: %v", err)
	}
	defer server.Close()

	// Start HTTP server
	log.Println("Starting server on :8080")
	log.Println("API endpoints:")
	log.Println("  GET  /v1/data?from={ms}&to={ms}&market={symbol}&exchange=binance&marketType=spot&frame=1m")
	log.Println("  POST /v1/sql with JSON body: {\"query\": \"SELECT * FROM klines LIMIT 10\"}")

	if err := http.ListenAndServe(":8080", server); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
