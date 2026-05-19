package data

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
)

// ReadHeaderlessCSV reads headerless CSV (like Binance data) and sends records to channel.
func ReadHeaderlessCSV[T any](reader io.Reader) (ch chan struct {
	Value *T
	Error error
}) {
	if b, ok := reader.(*Buffer); ok {
		return readHeaderlessCSVFromBytes[T](b.Bytes())
	}
	return readHeaderlessCSVFromReader[T](reader)
}

func readHeaderlessCSVFromBytes[T any](bts []byte) (ch chan struct {
	Value *T
	Error error
}) {
	return readHeaderlessCSVFromReader[T](bytes.NewReader(bts))
}

func readHeaderlessCSVFromReader[T any](reader io.Reader) (ch chan struct {
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

			record, err := from.Read()
			data := struct {
				Value *T
				Error error
			}{new(T), err}

			// Handle errors
			switch err {
			case nil:
				// pass
			case io.EOF:
				return
			default:
				switch err.(type) {
				case *csv.ParseError:
					// Sometimes data is corrupted, we need to skip it
					if errors.Is(err, csv.ErrFieldCount) {
						// fmt.Printf("field count error: %v\n", err)
						data.Error = fmt.Errorf("field count error: %v", err)
						ch <- data
						return
					}

					// data.Error = fmt.Errorf("csv read error: %w", err)
				default:
					data.Error = fmt.Errorf("csv read error: %v", err)
					ch <- data
					return
				}
			}
			if err = FillStruct(data.Value, record); err != nil {
				data.Error = fmt.Errorf("failed to fill struct: %v", err)
			}
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
