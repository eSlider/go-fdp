package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

var (
	ErrFileExists = errors.New("file exists")
	ErrWriteFile  = errors.New("failed to write file")
)

// WriteParquet writes records to parquet file
func WriteParquet[T any](
	path string,
	rCh <-chan *T, // channel to read every record from and write to parquet until the channel is closed.
	errCh chan<- error,
) {
	if FileExists(path) {
		errCh <- errors.Join(ErrFileExists, fmt.Errorf("parquet file %s already exists, skipping", path))
		return
	}

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
	pw.RowGroupSize = 1 * 1024 * 1024                  // 1
	pw.PageSize = 8 * 1024                             // default 8K
	pw.CompressionType = parquet.CompressionCodec_ZSTD // Best compression and decompression speed

	// Read records from a channel
	for {
		select {
		// Check if channel is closed
		case entry, ok := <-rCh:
			if ok == false {
				return
			}
			if err = pw.Write(entry); err != nil {
				errCh <- errors.Join(ErrWriteFile, fmt.Errorf("failed to write parquet file: %v", err))
			}
		}
	}
}
