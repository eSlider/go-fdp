package data

import (
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
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
					continue // Skip fields market as it's not used'
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

				err := SetStructField(el.Field(i), value)
				if err != nil {
					data.Error = err
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

// SetStructField sets field value based on type
func SetStructField(
	field reflect.Value,
	value string,
) (err error) {
	switch field.Kind() {
	case reflect.Int, reflect.Int64:
		intVal, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse int for field: %v", err)
		}
		field.SetInt(intVal)
	case reflect.Float64, reflect.Float32:
		floatVal, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("failed to parse float for field: %v", err)
		}
		field.SetFloat(floatVal)
	case reflect.Bool:
		boolVal, err := strconv.ParseBool(strings.ToLower(value))
		if err != nil {
			// Binance uses "true"/"false" or other values; try to handle common cases
			boolVal = value == "true" || value == "True" || value == "1"
		}
		field.SetBool(boolVal)
	case reflect.String:
		field.SetString(value)
	default:
		return fmt.Errorf("unsupported field type %v for field", field.Kind())
	}
	return nil
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

			// Create a new instance of T
			t := new(T)
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

				err := SetStructField(el.Field(i), value)
				if err != nil {
					errCh <- fmt.Errorf("failed to set field %s: %w", field.Name, err)
					return
				}
			}

			ch <- t
		}
	}()

	return ch, errCh
}
