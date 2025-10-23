package data

import (
	"fmt"
	"io"
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
