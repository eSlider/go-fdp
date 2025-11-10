package data

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

// ExtractZip extracts a zip file to a destination folder.
func ExtractZip(zipFilePath, destinationFolder string) error {
	r, err := zip.OpenReader(zipFilePath)
	if err != nil {
		return fmt.Errorf("failed to open zip file %s: %w", zipFilePath, err)
	}
	defer r.Close()

	for _, f := range r.File {
		fpath := filepath.Join(destinationFolder, f.Name)

		if f.FileInfo().IsDir() {
			os.MkdirAll(fpath, os.ModePerm)
			continue
		}

		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return fmt.Errorf("failed to create directory for %s: %w", fpath, err)
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return fmt.Errorf("failed to open output file %s: %w", fpath, err)
		}

		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return fmt.Errorf("failed to open file in zip %s: %w", f.Name, err)
		}

		_, err = io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()

		if err != nil {
			return fmt.Errorf("failed to copy file %s: %w", f.Name, err)
		}
	}

	return nil
}
