package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"sync-v3/pkg/data"
)

func HandleServerResponse[T any](w *httptest.ResponseRecorder) (r *T, err error) {
	body := w.Body
	if w.Code != http.StatusOK {
		r, err := data.JsonDecode[Error](body)
		if err != nil {
			return nil, err
		}
		return nil, r
	}
	return data.JsonDecode[T](body)
}

func QueryServer(t *testing.T, method, target string, body []byte) *httptest.ResponseRecorder {
	// Create a test server
	server, err := NewServer()
	if err != nil {
		t.Errorf("failed to create server: %v", err)
	}
	defer server.Close()

	// Create a test POST request without gzip support "/v1/sql"
	req := httptest.NewRequest(method, target, bytes.NewReader(body))

	// Create a response recorder
	w := httptest.NewRecorder()

	// Call ServeHTTP
	server.ServeHTTP(w, req)
	return w
}
