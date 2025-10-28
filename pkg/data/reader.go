package data

import (
	"encoding/csv"
	"io"

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
