package data

import (
	"fmt"
	"net/http"

	"github.com/ggicci/httpin"
	"github.com/go-playground/validator/v10"
)

var validate = validator.New()

// EncodeGetQuery encodes the request parameters into a query string.
func EncodeGetQuery(url string, req any) (string, error) {
	httpReq, err := httpin.NewRequest(http.MethodGet, url, req)
	if err != nil {
		return "", err
	}
	return httpReq.URL.RawQuery, nil
}

func Params(req any) (string, error) {
	if req == nil {
		return "", fmt.Errorf("request cannot be nil")
	}

	// Validate the request
	if err := validate.Struct(req); err != nil {
		return "", err
	}

	return EncodeGetQuery("", req)
}
