package data

import (
	"fmt"
	"reflect"
	"strconv"
)

func ParseFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

// PopulateStructFromSlice maps []any to struct using `csv:"index"` tags
func PopulateStructFromSlice(data *[]any, v any) error {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("target must be a pointer to struct")
	}

	structVal := val.Elem()
	structType := structVal.Type()

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldVal := structVal.Field(i)

		// Get index from csv tag
		tag := field.Tag.Get("csv")
		if tag == "" {
			continue
		}

		idx, err := strconv.Atoi(tag)
		if err != nil {
			return fmt.Errorf("invalid csv tag on field %s: %v", field.Name, err)
		}

		if idx < 0 || idx >= len(*data) {
			continue // skip if index out of range
		}

		if !fieldVal.CanSet() {
			continue
		}

		srcVal := reflect.ValueOf((*data)[idx])
		if !srcVal.IsValid() {
			continue
		}

		// Handle conversion
		if err := setFieldValue(fieldVal, srcVal); err != nil {
			return fmt.Errorf("failed to set field %s: %w", field.Name, err)
		}
	}

	return nil
}

// setFieldValue converts and sets the value
func setFieldValue(fieldVal, srcVal reflect.Value) error {
	// If types match directly
	if srcVal.Type().AssignableTo(fieldVal.Type()) {
		fieldVal.Set(srcVal)
		return nil
	}

	// Common conversions (especially from string or float64)
	switch fieldVal.Kind() {
	case reflect.Int, reflect.Int64:
		switch srcVal.Kind() {
		case reflect.Float64:
			fieldVal.SetInt(int64(srcVal.Float()))
		case reflect.String:
			if i, err := strconv.ParseInt(srcVal.String(), 10, 64); err == nil {
				fieldVal.SetInt(i)
			}
		default:
			if i, err := strconv.ParseInt(fmt.Sprintf("%v", srcVal.Interface()), 10, 64); err == nil {
				fieldVal.SetInt(i)
			}
		}

	case reflect.Float64:
		switch srcVal.Kind() {
		case reflect.Int, reflect.Int64:
			fieldVal.SetFloat(float64(srcVal.Int()))
		case reflect.String:
			if f, err := strconv.ParseFloat(srcVal.String(), 64); err == nil {
				fieldVal.SetFloat(f)
			}
		default:
			if f, err := strconv.ParseFloat(fmt.Sprintf("%v", srcVal.Interface()), 64); err == nil {
				fieldVal.SetFloat(f)
			}
		}
	}

	return nil
}
