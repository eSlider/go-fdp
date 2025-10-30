package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

var (
	ErrFileExists = errors.New("file exists")
	ErrWriteFile  = errors.New("failed to write file")
)

// WriteParquet writes records to parquet file
func WriteParquet[T any](
	path string,
) (
	rCh chan *T, // channel to read every record from and write to parquet until the channel is closed.
	errCh chan error,
) {
	rCh = make(chan *T)
	errCh = make(chan error)
	go func() {
		defer close(errCh)
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
		defer fw.Close()

		pw, err := writer.NewParquetWriter(fw, new(T), 2)
		if err != nil {
			errCh <- errors.Join(ErrWriteFile, fmt.Errorf("can't create parquet writer: %v", err))
			return
		}
		defer pw.WriteStop()

		// len(csvData.Bytes())/6
		// pw.RowGroupSize = 1 * 1024 * 1024                  // 1
		// pw.PageSize = 8 * 1024                             // default 8K
		pw.CompressionType = parquet.CompressionCodec_ZSTD // Best compression and decompression speed

		// Read records from the channel until it is closed
		for entry := range rCh {
			if err = pw.Write(entry); err != nil {
				errCh <- errors.Join(ErrWriteFile, fmt.Errorf("failed to write parquet file: %v", err))
				return
			}
		}

		fmt.Println("Finished writing parquet file")
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

	go func() {
		defer close(rCh)
		defer close(errCh)

		// Check if file exists
		if !FileExists(path) {
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
		pr, err := reader.NewParquetReader(fr, new(T), 4)
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
