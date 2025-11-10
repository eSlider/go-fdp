package data

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Buffer is a custom type that implements io.WriterAt
type Buffer struct {
	mu   sync.Mutex
	data []byte
}

// String returns the string representation of the buffer
func (b *Buffer) String() string {
	return string(b.data)
}

// Bytes return the underlying byte slice
func (b *Buffer) Bytes() []byte {
	return b.data
}

func (b *Buffer) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	end := int(off) + len(p)
	if end > len(b.data) {
		newData := make([]byte, end)
		copy(newData, b.data)
		b.data = newData
	}

	copy(b.data[off:], p)
	return len(p), nil
}

// Write implements the io.Writer interface
func (b *Buffer) Write(p []byte) (n int, err error) {
	if b == nil {
		return 0, fmt.Errorf("nil buffer")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.data = append(b.data, p...)
	return len(p), nil
}

// Close closes the buffer
func (b *Buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = nil
	return nil
}

// ReadCSV implements the io.Reader interface
func (b *Buffer) Read(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.data) == 0 {
		return 0, io.EOF
	}

	n = copy(p, b.data)
	b.data = b.data[n:]
	if len(b.data) == 0 {
		err = io.EOF
	}
	return n, err
}

// Persist writes the buffer to a file
func (b *Buffer) Persist(path string) (err error) {
	// Check already exists
	inf, err := os.Stat(path)

	// Remove file if it's empty'
	if inf != nil && inf.Size() < 5 {
		os.Remove(path)
	}

	// Check if file exists, without throwing error by checking if it's a directory'
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to stat file %s: %v", path, err)
		}
	}

	// Create a directory if not exists
	csvDir := filepath.Dir(path)
	if err := os.MkdirAll(csvDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", csvDir, err)
	}

	// Save file
	if err := os.WriteFile(path, b.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write file %s: %v", path, err)
	}
	return
}

// Decompress buffers content if it's compressed'
func (b *Buffer) Decompress() (out *Buffer, err error) {
	return Decompress(b.Bytes())
}

// ReadIntoBuffer reads the content of a file into a Buffer
func ReadIntoBuffer(path string) (buf *Buffer, err error) {
	// ReadCSV existing file
	var fc []byte

	// GetAsset reader for the file
	fc, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read existing file: %v", err)
	}

	// Write content to Buffer
	buf = new(Buffer)
	if _, err = buf.Write(fc); err != nil {
		return nil, fmt.Errorf("failed to write to buffer: %v", err)
	}

	return
}
