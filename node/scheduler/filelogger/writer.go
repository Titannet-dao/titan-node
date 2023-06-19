package filelogger

import (
	"fmt"
	"os"
	"sync"
)

// DirectoryName file directory
type DirectoryName string

const (
	// DirectoryNameValidation validation result file directory
	DirectoryNameValidation DirectoryName = "Validation"
	// DirectoryNameWorkload Workload file directory
	DirectoryNameWorkload DirectoryName = "Workload"
)

var mu sync.Mutex

// Writer struct
type Writer struct {
	file *os.File
}

// NewWriter creates a new LogFileWriter.
// It takes a directory and filename, and creates a new file in this directory.
func NewWriter(directory DirectoryName, filename string) (*Writer, error) {
	dir := string(directory)
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return nil, err
	}
	filepath := fmt.Sprintf("%s/%s.txt", dir, filename)
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o755)
	if err != nil {
		return nil, err
	}

	return &Writer{
		file: file,
	}, nil
}

// WriteData writes data to the file.
// It locks the file during the write to prevent data races.
func (w *Writer) WriteData(data string) error {
	mu.Lock()
	defer mu.Unlock()

	_, err := w.file.Write([]byte(data))
	if err != nil {
		return err
	}

	return nil
}

// Close closes the file.
func (w *Writer) Close() error {
	return w.file.Close()
}
