package storage

import (
	"os"

	"github.com/ipfs/go-datastore"
)

// waitList represents a file used for storing wait list data
type waitList struct {
	path string
}

// newWaitList creates a new wait list instance
func newWaitList(path string) *waitList {
	return &waitList{path: path}
}

// put writes data to the wait list file
func (wl *waitList) put(data []byte) error {
	return os.WriteFile(wl.path, data, 0o644)
}

// get reads data from the wait list file
func (wl *waitList) get() ([]byte, error) {
	data, err := os.ReadFile(wl.path)
	if err != nil && os.IsNotExist(err) {
		return nil, datastore.ErrNotFound
	}

	return data, err
}
