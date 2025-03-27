package httpserver

import (
	"context"
	"fmt"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// readOnlyBlockStore is an implementation of the BlockService interface for a read-only block store.
type readOnlyBlockStore struct {
	hs   *HttpServer
	root cid.Cid
}

// DeleteBlock deletes a block from the block store. Since the store is read-only, it always returns an error.
func (robs *readOnlyBlockStore) DeleteBlock(context.Context, cid.Cid) error {
	return fmt.Errorf("read only block store, can not delete block")
}

// Has checks if a block with a given CID exists in the block store.
func (robs *readOnlyBlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	return robs.hs.asset.HasBlock(ctx, robs.root, c)
}

// Get retrieves a block with a given CID from the block store.
func (robs *readOnlyBlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return robs.hs.asset.GetBlock(ctx, robs.root, c)
}

// GetSize returns the size of the block with the given CID. Since the store is read-only, it always returns 0.
func (robs *readOnlyBlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	log.Errorf("read only block store, not implement GetSize %s", c.String())
	return 0, nil
}

// Put stores a block in the block store. Since the store is read-only, it always returns an error.
func (robs *readOnlyBlockStore) Put(context.Context, blocks.Block) error {
	return fmt.Errorf("read only block store, can not put")
}

// PutMany stores multiple blocks in the block store. Since the store is read-only, it always returns an error.
func (robs *readOnlyBlockStore) PutMany(context.Context, []blocks.Block) error {
	return fmt.Errorf("read only block store, can not put many")
}

// AllKeysChan returns a channel that yields all the keys in the block store. Since the store is read-only, it always returns an error.
func (robs *readOnlyBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, fmt.Errorf("read only block store, not support all keys")
}

// HashOnRead is not implemented since the block store is read-only.
func (robs *readOnlyBlockStore) HashOnRead(enabled bool) {
}
