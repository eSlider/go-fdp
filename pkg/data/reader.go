package data

import (
	"encoding/csv"
	"io"

	"github.com/jszwec/csvutil"
)

// ReadCSV reads csv and calls callback for each record
// Alterative, without header decompression, more efficient:
// - https://github.com/gocarina/gocsv/blob/master/custom_unmarshaller_test.go
func ReadCSV[T any](reader io.Reader, callback func(u *T) error) (err error) {
	t := new(T)
	from := csv.NewReader(reader)
	userHeader, _ := csvutil.Header(*t, "csv")
	dec, _ := csvutil.NewDecoder(from, userHeader...)
	for {
		var u T
		err := dec.Decode(&u)

		// ReadCSV records from csv
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		// Run callbacks
		if err := callback(&u); err != nil {
			return err
		}

	}
	return nil
}

// ReadCSVChan reads csv and sends records to channel
func ReadCSVChan[T any](reader io.Reader) (ch chan *T, errCh chan error) {
	ch = make(chan *T)
	defer close(ch)
	errCh = make(chan error)
	defer close(errCh)

	func() {
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
