package data

import (
	"errors"
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
	loc  int
}

// String returns the string representation of the buffer
func (b *Buffer) String() string {
	return string(b.data)
}

// Bytes return the underlying byte slice
func (b *Buffer) Bytes() []byte {
	return b.data
}

// Seek seeks in the underlying memory buffer.
func (b *Buffer) Seek(offset int64, whence int) (int64, error) {
	newLoc := b.loc
	switch whence {
	case io.SeekStart:
		newLoc = int(offset)
	case io.SeekCurrent:
		newLoc += int(offset)
	case io.SeekEnd:
		newLoc = len(b.data) + int(offset)
	}

	if newLoc < 0 {
		return int64(b.loc), errors.New("unable to seek to a location <0")
	}

	if newLoc > len(b.data) {
		newLoc = len(b.data)
	}

	b.loc = newLoc

	return int64(b.loc), nil
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
// func (b *Buffer) Write(p []byte) (n int, err error) {
// 	if b == nil {
// 		return 0, fmt.Errorf("nil buffer")
// 	}
// 	b.mu.Lock()
// 	defer b.mu.Unlock()
//
// 	b.data = append(b.data, p...)
// 	return len(p), nil
// }

// Write writes data from p into BufferFile.
func (b *Buffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Do we have space?
	if available := cap(b.data) - b.loc; available < len(p) {
		// How much should we expand by?
		addCap := cap(b.data)
		if addCap < len(p) {
			addCap = len(p)
		}

		newBuff := make([]byte, len(b.data), cap(b.data)+addCap)

		copy(newBuff, b.data)

		b.data = newBuff
	}

	// Write
	n = copy(b.data[b.loc:cap(b.data)], p)
	b.loc += n
	if len(b.data) < b.loc {
		b.data = b.data[:b.loc]
	}

	return n, nil
}

// Close closes the buffer
func (b *Buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = nil
	return nil
}

// Read reads data form Buffer
func (b *Buffer) Read(p []byte) (n int, err error) {
	n = copy(p, b.data[b.loc:len(b.data)])
	b.loc += n

	if b.loc == len(b.data) {
		return n, io.EOF
	}

	return n, nil
}

// ReadCSV implements the io.Reader interface
// func (b *Buffer) Read(p []byte) (n int, err error) {
// 	b.mu.Lock()
// 	defer b.mu.Unlock()
//
// 	if len(b.data) == 0 {
// 		return 0, io.EOF
// 	}
//
// 	n = copy(p, b.data)
// 	b.data = b.data[n:]
// 	if len(b.data) == 0 {
// 		err = io.EOF
// 	}
// 	return n, err
// }

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

	// GetAssets reader for the file
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
