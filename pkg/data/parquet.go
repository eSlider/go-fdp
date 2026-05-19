package data

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eslider/go-binance-fdp/pkg/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chonla/format"
	"github.com/go-playground/validator/v10"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

var (
	ErrFileExists = errors.New("file exists")
	ErrWriteFile  = errors.New("failed to write file")
)

// WriteParquet writes records to a parquet file
func WriteParquet[T any](
	path string,
) (
	rCh chan *T, // channel to read every record from and write to parquet until the channel is closed.
	errCh chan error,
) {
	rCh = make(chan *T)
	errCh = make(chan error, 1)
	go func() {
		// Use sync.Once to ensure errCh is closed exactly once
		var closeOnce sync.Once
		closeErrCh := func() {
			closeOnce.Do(func() {
				close(errCh)
			})
		}
		defer closeErrCh()

		// Remove existing file to overwrite
		os.Remove(path)

		// Create a directory if not exists
		parquetDir := filepath.Dir(path)
		if err := os.MkdirAll(parquetDir, 0755); err != nil {
			errCh <- fmt.Errorf("failed to create directory for parquet %s: %v", parquetDir, err)
			return
		}

		fw, err := local.NewLocalFileWriter(path)
		if err != nil {
			errCh <- errors.Join(ErrWriteFile, fmt.Errorf("can't create local file: %v", err))
			return
		}

		pw, err := writer.NewParquetWriter(fw, new(T), 2)
		if err != nil {
			errCh <- errors.Join(ErrWriteFile, fmt.Errorf("can't create parquet writer: %v", err))
			return
		}

		// len(csvData.Bytes())/6
		pw.RowGroupSize = 1440                             // 1
		pw.PageSize = 1024 * 1024                          // default 8K
		pw.CompressionType = parquet.CompressionCodec_ZSTD // Best compression and decompression speed

		// Read records from the channel until it is closed
		for entry := range rCh {
			if err = pw.Write(entry); err != nil {
				errCh <- errors.Join(ErrWriteFile, fmt.Errorf("failed to write parquet file: %v", err))
				return
			}
		}

		if err = pw.WriteStop(); err != nil {
			errCh <- errors.Join(ErrWriteFile, fmt.Errorf("failed to write parquet file: %v", err))
			return
		}

		if err = fw.Close(); err != nil {
			errCh <- errors.Join(ErrWriteFile, fmt.Errorf("failed to close parquet file: %v", err))
			return
		}
	}()
	return
}

// ReadParquet reads records from a parquet file
func ReadParquet[T any](
	path string,
) (
	rCh chan *T, // channel to send records read from parquet file
	errCh chan error,
) {
	rCh = make(chan *T)
	errCh = make(chan error)
	obj := new(T)

	go func() {
		defer close(rCh)
		defer close(errCh)

		// Check if file exists
		if !fs.FileExists(path) {
			errCh <- fmt.Errorf("parquet file does not exist: %s", path)
			return
		}

		// Open the parquet file
		fr, err := local.NewLocalFileReader(path)
		if err != nil {
			errCh <- fmt.Errorf("can't open local file: %v", err)
			return
		}
		defer fr.Close()

		// Create parquet reader
		pr, err := reader.NewParquetReader(fr, obj, 4)
		if err != nil {
			errCh <- fmt.Errorf("can't create parquet reader: %v", err)
			return
		}
		defer pr.ReadStop()

		// Read all records at once
		numRows := int(pr.GetNumRows())
		records, err := pr.ReadByNumber(numRows)
		if err != nil {
			errCh <- fmt.Errorf("failed to read parquet records: %v", err)
			return
		}

		// Send records to channel (cast to correct type)
		for _, record := range records {
			if typedRecord, ok := record.(T); ok {
				rCh <- &typedRecord
			} else {
				errCh <- fmt.Errorf("type assertion failed for record: %T", record)
				return
			}
		}
	}()

	return
}

var (
	structValidator = validator.New()
)

func QueryParquets(
	db *sql.DB,
	query string,
	dq any,
) (resCh chan map[string]any, errCh chan error) {
	resCh = make(chan map[string]any)
	errCh = make(chan error)

	go func() {
		defer close(resCh)
		defer close(errCh)
		// Validate query parameters
		if err := structValidator.Struct(dq); err != nil {
			errCh <- err
			return
		}

		// Format query
		q := format.Sprintf(query, dq)

		// If query has "%<", means not all values are filled, so we need to replace them
		if strings.Contains(q, "%<") {
			errCh <- fmt.Errorf("query is not complete: %s", q)
			return
		}

		log.Printf("Executing query: %s", q)

		rows, err := db.Query(q)
		if err != nil {
			errCh <- fmt.Errorf("failed to execute query: %w", err)
			return
		}
		defer rows.Close()

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			errCh <- fmt.Errorf("failed to get columns: %w", err)
		}

		// Convert to JSON
		for rows.Next() {
			// Create a slice of interface{} to hold the values
			valuePtrs := make([]any, len(columns))
			for i := range columns {
				valuePtrs[i] = new(any)
			}

			// Scan into the slice
			err := rows.Scan(valuePtrs...)
			if err != nil {
				errCh <- fmt.Errorf("failed to scan row: %w", err)
			}

			// Create map
			valueMap := make(map[string]any)
			for i, name := range columns {
				val := *(valuePtrs[i].(*any))
				// Handle []byte to string conversion if needed
				if b, ok := val.([]byte); ok {
					valueMap[name] = string(b)
				} else {
					valueMap[name] = val
				}
			}
			resCh <- valueMap
		}
	}()

	return resCh, errCh
}

type ResponseDate time.Time

func (r ResponseDate) GoString() string {
	return r.String()
}

// MarshalJSON implements the json.Marshaler interface
func (r ResponseDate) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(r).UnixMilli())
}

// String
func (r ResponseDate) String() string {
	// Return human-readable YY-mm-dd HH:MM:SS
	return time.Time(r).Format("2006-01-02 15:04:05")
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (r *ResponseDate) UnmarshalJSON(b []byte) error {
	// Get timestamp as int64
	v, err := strconv.Atoi(string(b))
	if err != nil {
		return err
	}
	d := AnyTimestampToTime(int64(v))
	*r = ResponseDate(*d)
	return nil
}
