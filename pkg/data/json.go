package data

import (
	"encoding/json"
	"io"
)

func JsonDecode[T any](w io.Reader) (*T, error) {
	result := new(T)
	if err := json.NewDecoder(w).Decode(result); err != nil {
		return nil, err
	}
	return result, nil
}
