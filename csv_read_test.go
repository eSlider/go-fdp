package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"sync-v3/pkg/binance"
	"testing"
)

func TestCSVReading(t *testing.T) {

	fmt.Println("Testing reflection-based CSV reading...")

	file, err := os.Open("data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2017-08.csv")
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	count := 0
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Printf("Error reading: %v\n", err)
			break
		}
		count++

		// Try to parse like ReadHeaderlessCSVChan does
		t := new(binance.Kline)
		v := reflect.ValueOf(t).Elem()
		typ := v.Type()

		fmt.Printf("Processing record %d with %d fields\n", count, len(record))

		// Map CSV columns to struct fields
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			csvTag := field.Tag.Get("csv")
			if csvTag == "" || csvTag == "-" {
				continue
			}

			// Parse csv tag as column index
			colIdx, err := strconv.Atoi(csvTag)
			if err != nil {
				fmt.Printf("Invalid csv tag %q on field %s: %v\n", csvTag, field.Name, err)
				continue
			}

			if colIdx >= len(record) {
				continue // Skip missing columns
			}

			value := record[colIdx]
			if value == "" {
				continue // Skip empty values
			}

			fmt.Printf("Setting field %s (type %v) to value %q\n", field.Name, field.Type.Kind(), value)

			// Set field value based on type
			fieldValue := v.Field(i)
			switch fieldValue.Kind() {
			case reflect.Int64:
				intVal, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					fmt.Printf("Failed to parse int for field %s: %v\n", field.Name, err)
					continue
				}
				fieldValue.SetInt(intVal)
			case reflect.Float64:
				floatVal, err := strconv.ParseFloat(value, 64)
				if err != nil {
					fmt.Printf("Failed to parse float for field %s: %v\n", field.Name, err)
					continue
				}
				fieldValue.SetFloat(floatVal)
			default:
				fmt.Printf("Unsupported field type %v for field %s\n", fieldValue.Kind(), field.Name)
			}
		}

		if count <= 2 {
			fmt.Printf("Parsed record: %+v\n", t)
		}

		if count >= 5 {
			break
		}
	}

	fmt.Printf("Total records processed: %d\n", count)
}
