package data

import (
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/jszwec/csvutil"
)

// ReadCSVChan reads csv and sends records to channel
func ReadCSVChan[T any](reader io.Reader) (ch chan *T, errCh chan error) {
	ch = make(chan *T)
	errCh = make(chan error)

	go func() {
		defer close(ch)
		defer close(errCh)

		t := new(T)
		from := csv.NewReader(reader)
		userHeader, err := csvutil.Header(*t, "csv")
		if err != nil {
			errCh <- err
			return
		}

		dec, err := csvutil.NewDecoder(from, userHeader...)
		if err != nil {
			errCh <- err
			return
		}

		for {
			var u T
			// ReadCSV records from csv
			err := dec.Decode(&u)
			switch {
			case err == io.EOF:
				return
			case err != nil:
				errCh <- err
				return
			}
			ch <- &u
		}
	}()

	return ch, errCh
}

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
		from := csv.NewReader(reader)

		for {
			t := new(T)
			data := struct {
				Value *T
				Error error
			}{t, nil}

			record, err := from.Read()
			if err != nil {
				if err != io.EOF {
					data.Error = fmt.Errorf("csv read error: %w", err)
					ch <- data
				}
				return
			}
			// Set field value based on type
			of := reflect.ValueOf(t)
			el := of.Elem()
			typ := el.Type()

			// Map CSV columns to struct fields
			for i := 0; i < typ.NumField(); i++ {
				field := typ.Field(i)
				csvTag := field.Tag.Get("csv")

				if csvTag == "-" {
					continue
				}

				var colIdx int
				if csvTag == "" {
					colIdx = i
				} else {
					// Parse csv tag as column index
					colIdx, err = strconv.Atoi(csvTag)
					if err != nil {
						data.Error = fmt.Errorf("invalid csv tag %q on field %s: %w", csvTag, field.Name, err)
						ch <- data
						return
					}
				}

				if colIdx >= len(record) {
					continue // Skip missing columns
				}

				value := record[colIdx]
				if value == "" {
					continue // Skip empty values
				}

				fieldValue := el.Field(i)
				switch fieldValue.Kind() {
				case reflect.Int, reflect.Int64:
					intVal, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						data.Error = fmt.Errorf("failed to parse int for field %s: %w", field.Name, err)
						ch <- data
						return
					}
					fieldValue.SetInt(intVal)
				case reflect.Float64:
					floatVal, err := strconv.ParseFloat(value, 64)
					if err != nil {
						data.Error = fmt.Errorf("failed to parse float for field %s: %w", field.Name, err)
						ch <- data
						return
					}
					fieldValue.SetFloat(floatVal)
				case reflect.Bool:
					boolVal, err := strconv.ParseBool(strings.ToLower(value))
					if err != nil {
						// Binance uses "true"/"false" or other values; try to handle common cases
						boolVal = value == "true" || value == "True" || value == "1"
					}
					fieldValue.SetBool(boolVal)
				case reflect.String:
					fieldValue.SetString(value)
				default:
					data.Error = fmt.Errorf("unsupported field type %v for field %s", fieldValue.Kind(), field.Name)
					ch <- data
					return
				}
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

		from := csv.NewReader(reader)

		for {
			record, err := from.Read()
			if err != nil {
				if err != io.EOF {
					errCh <- fmt.Errorf("csv read error: %w", err)
				}
				return
			}

			// Create new instance of T
			t := new(T)
			v := reflect.ValueOf(t).Elem()
			typ := v.Type()

			// Map CSV columns to struct fields
			for i := 0; i < typ.NumField(); i++ {
				field := typ.Field(i)
				csvTag := field.Tag.Get("csv")
				if csvTag == "-" {
					continue
				}

				var colIdx int
				if csvTag == "" {
					colIdx = i
				} else {
					// Parse csv tag as column index
					colIdx, err = strconv.Atoi(csvTag)
					if err != nil {
						errCh <- fmt.Errorf("invalid csv tag %q on field %s: %w", csvTag, field.Name, err)
						return
					}
				}

				if colIdx >= len(record) {
					continue // Skip missing columns
				}

				value := record[colIdx]
				if value == "" {
					continue // Skip empty values
				}

				// Set field value based on type
				fieldValue := v.Field(i)
				switch fieldValue.Kind() {
				case reflect.Int, reflect.Int64:
					intVal, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						errCh <- fmt.Errorf("failed to parse int for field %s: %w", field.Name, err)
						return
					}
					fieldValue.SetInt(intVal)
				case reflect.Float64:
					floatVal, err := strconv.ParseFloat(value, 64)
					if err != nil {
						errCh <- fmt.Errorf("failed to parse float for field %s: %w", field.Name, err)
						return
					}
					fieldValue.SetFloat(floatVal)
				case reflect.Bool:
					boolVal, err := strconv.ParseBool(strings.ToLower(value))
					if err != nil {
						// Binance uses "true"/"false" or other values; try to handle common cases
						boolVal = value == "true" || value == "True" || value == "1"
					}
					fieldValue.SetBool(boolVal)
				case reflect.String:
					fieldValue.SetString(value)
				default:
					errCh <- fmt.Errorf("unsupported field type %v for field %s", fieldValue.Kind(), field.Name)
					return
				}
			}

			ch <- t
		}
	}()

	return ch, errCh
}
