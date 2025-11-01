package api

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestNoGzipCompression(t *testing.T) {
	// Create a test server
	server, err := NewServer()
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Close()

	// Create a test POST request without gzip support
	req := httptest.NewRequest("POST", "/v1/sql",
		bytes.NewReader([]byte(`{"query": "SELECT 1 as test"}`)))
	req.Header.Set("Content-Type", "application/json")

	// Create a response recorder
	w := httptest.NewRecorder()

	// Call ServeHTTP
	server.ServeHTTP(w, req)

	// For non-gzip requests, Vary header should not be set since we only set it when gzip is enabled
	vary := w.Header().Get("Vary")
	if vary == "Accept-Encoding" {
		t.Errorf("Expected no Vary header for non-gzip request, but got '%s'", vary)
	}

	// Read into bytes w.Body
	// var buf bytes.Buffer
	// _, err = buf.ReadFrom(w.Body)
	// if err != nil {
	// 	t.Errorf("Failed to read response body: %v", err)
	// }
	// log.Println(buf.String())

	// Verify the response body is valid JSON (not compressed)
	// var result []map[string]any
	var result []struct{ Test int }

	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Errorf("Response data is not valid JSON: %v, body: %s", err, string(w.Body.Bytes()))
	}

	if result[0].Test != 1 {
		t.Errorf("Expected 1, got %d", result[0].Test)
	}

}
