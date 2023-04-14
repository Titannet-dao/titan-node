package storage

import (
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
)

// puller stores puller using the filesystem.
type puller struct {
	baseDir string
}

// newPuller initializes a new puller instance.
func newPuller(baseDir string) (*puller, error) {
	err := os.MkdirAll(baseDir, 0o755)
	if err != nil {
		return nil, err
	}

	return &puller{baseDir: baseDir}, nil
}

// store writes puller data to the filesystem.
func (p *puller) store(c cid.Cid, data []byte) error {
	filePath := filepath.Join(p.baseDir, c.Hash().String())
	return os.WriteFile(filePath, data, 0o644)
}

// retrieve reads puller data from the filesystem.
func (p *puller) get(c cid.Cid) ([]byte, error) {
	filePath := filepath.Join(p.baseDir, c.Hash().String())
	return os.ReadFile(filePath)
}

// exists checks if the asset data is stored in the filesystem.
func (p *puller) exists(c cid.Cid) (bool, error) {
	filePath := filepath.Join(p.baseDir, c.Hash().String())
	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// remove deletes asset data from the filesystem.
func (p *puller) remove(c cid.Cid) error {
	filePath := filepath.Join(p.baseDir, c.Hash().String())
	return os.Remove(filePath)
}
