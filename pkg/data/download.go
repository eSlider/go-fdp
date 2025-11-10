package data

import (
	"fmt"
	"io"
	"net/http"
)

// DownloadFile downloads a file from the given URL to the specified destination path.
func DownloadFile(url string) (buf *Buffer, err error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to download file from %s: %w", url, err)
	}
	defer resp.Body.Close()

	// Read body into the buffer
	if _, err = io.Copy(buf, resp.Body); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status: %s", resp.Status)
	}

	return buf, err
}
