package data

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
)

// Decompress ZIP bytes and returns the original content
func Decompress(compressed []byte) (out *Buffer, err error) {
	// Create a reader for the compressed bytes
	reader := bytes.NewReader(compressed)
	zipReader, err := zip.NewReader(reader, int64(len(compressed)))
	if err != nil {
		return nil, fmt.Errorf("failed to create zip reader: %v", err)
	}

	// Assume a single file in ZIP for simplicity
	if len(zipReader.File) == 0 {
		return nil, fmt.Errorf("no files found in zip")
	}

	// Open the first file in the ZIP
	file, err := zipReader.File[0].Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open zip file: %v", err)
	}
	defer file.Close()

	// ReadCSV the decompressed content
	out = &Buffer{}
	_, err = io.Copy(out, file)
	if err != nil {
		return nil, fmt.Errorf("failed to read decompressed data: %v", err)
	}

	return out, nil
}
