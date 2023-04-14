package storage

import (
	"context"
	"encoding/binary"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
)

// blockCount block count of the asset
type blockCount struct {
	ds ds.Batching
}

// newBlockCount initializes a new blockCount with the given base directory
func newBlockCount(baseDir string) (*blockCount, error) {
	ds, err := createDatastore(baseDir)
	if err != nil {
		return nil, err
	}

	return &blockCount{ds: ds}, nil
}

// storeBlockCount stores the blockCount of the asset with the given root CID
func (c *blockCount) storeBlockCount(ctx context.Context, root cid.Cid, blockCount uint32) error {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, blockCount)
	return c.ds.Put(ctx, ds.NewKey(root.Hash().String()), bs)
}

// getBlockCount retrieves the blockCount of the asset with the given root CID
func (c *blockCount) getBlockCount(ctx context.Context, root cid.Cid) (uint32, error) {
	val, err := c.ds.Get(ctx, ds.NewKey(root.Hash().String()))
	if err != nil {
		if err == ds.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}

	return binary.LittleEndian.Uint32(val), nil
}
