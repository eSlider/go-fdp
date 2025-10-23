package data

import (
	"encoding/csv"
	"io"

	"github.com/jszwec/csvutil"
)

func ReadCSV[T any](reader io.Reader, callback func(u T) error) (err error) {
	t := new(T)
	from := csv.NewReader(reader)
	userHeader, _ := csvutil.Header(*t, "csv")
	dec, _ := csvutil.NewDecoder(from, userHeader...)
	for {
		var u T
		// ReadCSV records from csv
		if err := dec.Decode(&u); err == io.EOF {
			break
		}

		// Run callbacks
		if err := callback(u); err != nil {
			return err
		}

	}
	return nil
}
