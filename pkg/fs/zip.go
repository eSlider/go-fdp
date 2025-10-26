package fs

import (
	"fmt"
	"os"
	"sync-v3/pkg/data"
)

// ReadZip - read zip file
func ReadZip(path string) (buf *data.Buffer, err error) {
	// ReadCSV existing file
	var fc []byte

	// GetAsset reader for the file
	fc, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read existing file: %v", err)
	}

	// Write content to Buffer
	buf = new(data.Buffer)
	if _, err = buf.Write(fc); err != nil {
		return nil, fmt.Errorf("failed to write to buffer: %v", err)
	}

	return
}
