package data

import (
	"encoding/csv"
	"fmt"
	"io"
)

// ReadHeaderlessCSV reads headerless CSV (like Binance data) and sends records to channel
// Maps CSV columns positionally to struct fields based on csv tag numbers
func ReadHeaderlessCSV[T any](reader io.Reader) (ch chan struct {
	Value *T
	Error error
}) {
	ch = make(chan struct {
		Value *T
		Error error
	})

	go func() {
		defer close(ch)

		for from := csv.NewReader(reader); true; {
			data := struct {
				Value *T
				Error error
			}{new(T), nil}

			record, err := from.Read()
			if err != nil {
				if err != io.EOF {
					data.Error = fmt.Errorf("csv read error: %w", err)
					ch <- data
				}
				return
			}
			if err = FillStruct(data.Value, record); err != nil {
				data.Error = fmt.Errorf("failed to fill struct: %w", err)
			}
			// data.Value = t
			ch <- data
		}
	}()

	return ch
}

// ReadHeaderlessCSVChan reads headerless CSV (like Binance data) and sends records to channel
// Maps CSV columns positionally to struct fields based on csv tag numbers
func ReadHeaderlessCSVChan[T any](reader io.Reader) (ch chan *T, errCh chan error) {
	ch = make(chan *T)
	errCh = make(chan error)

	go func() {
		defer close(ch)
		defer close(errCh)

		for from := csv.NewReader(reader); true; {
			record, err := from.Read()
			if err != nil {
				if err != io.EOF {
					errCh <- fmt.Errorf("csv read error: %w", err)
				}
				return
			}

			t := new(T)
			if err = FillStruct(t, record); err != nil {
				errCh <- fmt.Errorf("failed to fill struct: %w", err)
				return
			}
			ch <- t
		}
	}()

	return ch, errCh
}
