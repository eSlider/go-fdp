package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestRecord represents a simple record for testing
type TestRecord struct {
	ID     int     `parquet:"name=id, type=INT64"`
	Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Value  float64 `parquet:"name=value, type=DOUBLE"`
	Active bool    `parquet:"name=active, type=BOOLEAN"`
}

func TestWriteParquet(t *testing.T) {
	t.Run("writes records successfully", func(t *testing.T) {
		// Create a temporary file path
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "test.parquet")

		// Create test records
		records := []TestRecord{
			{ID: 1, Name: "Alice", Value: 123.45, Active: true},
			{ID: 2, Name: "Bob", Value: 678.90, Active: false},
		}

		// Call WriteParquet
		rCh, errCh := WriteParquet[TestRecord](filePath)

		// Send records to the channel
		go func() {
			defer close(rCh)
			for _, record := range records {
				rCh <- &record
			}
		}()

		// Collect any errors
		var errs []error
		for err := range errCh {
			errs = append(errs, err)
		}

		// Check for errors
		if len(errs) > 0 {
			t.Fatalf("unexpected errors: %v", errs)
		}

		// Verify file was created
		if !FileExists(filePath) {
			t.Fatal("parquet file was not created")
		}

		// Verify file has content (FileExists checks size > 4 bytes)
		info, err := os.Stat(filePath)
		if err != nil {
			t.Fatalf("failed to stat file: %v", err)
		}
		if info.Size() <= 4 {
			t.Fatal("parquet file appears to be empty or too small")
		}
	})

	t.Run("creates directory if it doesn't exist", func(t *testing.T) {
		tmpDir := t.TempDir()
		nestedDir := filepath.Join(tmpDir, "nested", "deep", "path")
		filePath := filepath.Join(nestedDir, "test.parquet")

		// Ensure directory doesn't exist
		if _, err := os.Stat(nestedDir); !os.IsNotExist(err) {
			t.Fatal("nested directory should not exist initially")
		}

		rCh, errCh := WriteParquet[TestRecord](filePath)

		// Send one record
		go func() {
			defer close(rCh)
			record := TestRecord{ID: 1, Name: "Test", Value: 1.0, Active: true}
			rCh <- &record
		}()

		// Check for errors
		var errs []error
		for err := range errCh {
			errs = append(errs, err)
		}

		if len(errs) > 0 {
			t.Fatalf("unexpected errors: %v", errs)
		}

		// Verify directory was created
		if _, err := os.Stat(nestedDir); os.IsNotExist(err) {
			t.Fatal("directory was not created")
		}

		// Verify file exists
		if !FileExists(filePath) {
			t.Fatal("parquet file was not created")
		}
	})

	t.Run("handles empty record set", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "empty.parquet")

		rCh, errCh := WriteParquet[TestRecord](filePath)

		// Close channel immediately (no records)
		close(rCh)

		// Collect errors
		var errs []error
		for err := range errCh {
			errs = append(errs, err)
		}

		if len(errs) > 0 {
			t.Fatalf("unexpected errors with empty record set: %v", errs)
		}

		// File should still be created (empty parquet file)
		if !FileExists(filePath) {
			t.Fatal("parquet file was not created even for empty record set")
		}
	})

	t.Run("works with different types", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Test with simple struct
		type SimpleRecord struct {
			Data string `parquet:"name=data, type=BYTE_ARRAY, convertedtype=UTF8"`
		}

		filePath := filepath.Join(tmpDir, "simple.parquet")
		rCh, errCh := WriteParquet[SimpleRecord](filePath)

		go func() {
			defer close(rCh)
			record := SimpleRecord{Data: "test data"}
			rCh <- &record
		}()

		var errs []error
		for err := range errCh {
			errs = append(errs, err)
		}

		if len(errs) > 0 {
			t.Fatalf("unexpected errors: %v", errs)
		}

		if !FileExists(filePath) {
			t.Fatal("parquet file was not created")
		}
	})
}

// TestReadParquet tests reading records from parquet files
func TestReadParquet(t *testing.T) {
	t.Run("reads records successfully", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "test_read.parquet")

		// First write some records
		originalRecords := []TestRecord{
			{ID: 1, Name: "Alice", Value: 123.45, Active: true},
			{ID: 2, Name: "Bob", Value: 678.90, Active: false},
			{ID: 3, Name: "Charlie", Value: 999.99, Active: true},
		}

		// Write records
		writeCh, writeErrCh := WriteParquet[TestRecord](filePath)
		go func() {
			defer close(writeCh)
			for _, record := range originalRecords {
				writeCh <- &record
			}
		}()

		// Wait for write to complete
		var writeErrs []error
		for err := range writeErrCh {
			writeErrs = append(writeErrs, err)
		}
		if len(writeErrs) > 0 {
			t.Fatalf("failed to write test data: %v", writeErrs)
		}

		// Now read the records back
		readCh, readErrCh := ReadParquet[TestRecord](filePath)

		// Collect read records
		var readRecords []TestRecord
		readDone := make(chan bool)
		go func() {
			defer close(readDone)
			for record := range readCh {
				readRecords = append(readRecords, *record)
			}
		}()

		// Wait for reading to complete
		<-readDone

		// Check for read errors
		var readErrs []error
		for err := range readErrCh {
			readErrs = append(readErrs, err)
		}
		if len(readErrs) > 0 {
			t.Fatalf("failed to read parquet file: %v", readErrs)
		}

		// Verify we read the correct number of records
		if len(readRecords) != len(originalRecords) {
			t.Fatalf("expected %d records, got %d", len(originalRecords), len(readRecords))
		}

		// Verify record data (note: order might not be preserved in parquet)
		recordMap := make(map[int]TestRecord)
		for _, record := range readRecords {
			recordMap[record.ID] = record
		}

		for _, expected := range originalRecords {
			actual, exists := recordMap[expected.ID]
			if !exists {
				t.Fatalf("record with ID %d not found", expected.ID)
			}
			if actual.Name != expected.Name || actual.Value != expected.Value || actual.Active != expected.Active {
				t.Errorf("record mismatch for ID %d: expected %+v, got %+v", expected.ID, expected, actual)
			}
		}
	})

	t.Run("handles empty parquet file", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "empty.parquet")

		// Write empty file
		writeCh, writeErrCh := WriteParquet[TestRecord](filePath)
		close(writeCh) // Close immediately without sending records

		// Wait for write to complete
		var writeErrs []error
		for err := range writeErrCh {
			writeErrs = append(writeErrs, err)
		}
		if len(writeErrs) > 0 {
			t.Fatalf("failed to write empty parquet: %v", writeErrs)
		}

		// Read from empty file
		readCh, readErrCh := ReadParquet[TestRecord](filePath)

		// Collect records (should be none)
		var readRecords []TestRecord
		readDone := make(chan bool)
		go func() {
			defer close(readDone)
			for record := range readCh {
				readRecords = append(readRecords, *record)
			}
		}()

		<-readDone

		// Check errors
		var readErrs []error
		for err := range readErrCh {
			readErrs = append(readErrs, err)
		}
		if len(readErrs) > 0 {
			t.Fatalf("unexpected errors reading empty parquet: %v", readErrs)
		}

		if len(readRecords) != 0 {
			t.Errorf("expected 0 records from empty parquet, got %d", len(readRecords))
		}
	})

	t.Run("works with different types", func(t *testing.T) {
		tmpDir := t.TempDir()

		type SimpleRecord struct {
			Data  string `parquet:"name=data, type=BYTE_ARRAY, convertedtype=UTF8"`
			Count int    `parquet:"name=count, type=INT64"`
		}

		filePath := filepath.Join(tmpDir, "simple.parquet")

		// Write records
		originalRecords := []SimpleRecord{
			{Data: "test1", Count: 10},
			{Data: "test2", Count: 20},
		}

		writeCh, writeErrCh := WriteParquet[SimpleRecord](filePath)
		go func() {
			defer close(writeCh)
			for _, record := range originalRecords {
				writeCh <- &record
			}
		}()

		for range writeErrCh {
			// Wait for write completion
		}

		// Read records back
		readCh, readErrCh := ReadParquet[SimpleRecord](filePath)

		var readRecords []SimpleRecord
		readDone := make(chan bool)
		go func() {
			defer close(readDone)
			for record := range readCh {
				readRecords = append(readRecords, *record)
			}
		}()

		<-readDone

		var readErrs []error
		for err := range readErrCh {
			readErrs = append(readErrs, err)
		}
		if len(readErrs) > 0 {
			t.Fatalf("failed to read simple records: %v", readErrs)
		}

		if len(readRecords) != len(originalRecords) {
			t.Fatalf("expected %d records, got %d", len(originalRecords), len(readRecords))
		}
	})
}

func TestReadParquetErrors(t *testing.T) {
	t.Run("handles non-existent file", func(t *testing.T) {
		nonExistentPath := "/tmp/non_existent_file.parquet"
		readCh, readErrCh := ReadParquet[TestRecord](nonExistentPath)

		// Try to read from channel (should fail)
		select {
		case <-readCh:
			t.Error("expected no records from non-existent file")
		case err := <-readErrCh:
			if err == nil {
				t.Error("expected error for non-existent file")
			}
		}
	})
}

// Benchmark for ReadParquet
func BenchmarkReadParquet(b *testing.B) {
	tmpDir := b.TempDir()
	filePath := filepath.Join(tmpDir, "bench_read.parquet")

	// Create test data
	records := make([]TestRecord, 1000)
	for i := range records {
		records[i] = TestRecord{
			ID:     i,
			Name:   fmt.Sprintf("Record%d", i),
			Value:  float64(i) * 1.5,
			Active: i%2 == 0,
		}
	}

	// Write test data once
	writeCh, writeErrCh := WriteParquet[TestRecord](filePath)
	go func() {
		defer close(writeCh)
		for _, record := range records {
			writeCh <- &record
		}
	}()

	for range writeErrCh {
		// Wait for write completion
	}

	// Benchmark reading
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		readCh, readErrCh := ReadParquet[TestRecord](filePath)

		// Drain channels
		go func() {
			for range readCh {
			}
		}()

		for range readErrCh {
		}
	}
}

// Benchmark for WriteParquet
func BenchmarkWriteParquet(b *testing.B) {
	tmpDir := b.TempDir()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filePath := filepath.Join(tmpDir, fmt.Sprintf("bench_%d.parquet", i))

		rCh, errCh := WriteParquet[TestRecord](filePath)

		go func() {
			defer close(rCh)
			record := TestRecord{
				ID:     i,
				Name:   "Benchmark",
				Value:  float64(i),
				Active: i%2 == 0,
			}
			rCh <- &record
		}()

		// Drain error channel
		for range errCh {
			// Ignore errors in benchmark
		}
	}
}
