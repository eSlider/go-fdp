package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
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

	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), server); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
