package storage

import (
	"os"

	ds "github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
	"golang.org/x/xerrors"
)

// createDatastore creates a new LevelDB datastore using the specified base directory.
func createDatastore(baseDir string) (ds.Batching, error) {
	// Create the datastore directory if it doesn't exist yet.
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, xerrors.Errorf("failed to create directory %s for kv store: %w", baseDir, err)
	}

	// Create a new LevelDB datastore
	return levelds.NewDatastore(baseDir, &levelds.Options{
		Compression: ldbopts.NoCompression,
		NoSync:      false,
		Strict:      ldbopts.StrictAll,
		ReadOnly:    false,
	})
}
