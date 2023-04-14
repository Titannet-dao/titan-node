package asset

import (
	"context"
	"io"
	"os"

	titanindex "github.com/Filecoin-Titan/titan/node/asset/index"
	"github.com/Filecoin-Titan/titan/node/asset/storage"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

const sizeOfBuckets = 128

type Key string

// lruCache asset index for cache
type lruCache struct {
	storage storage.Storage
	cache   *lru.Cache
}

type cacheValue struct {
	bs          *blockstore.ReadOnly
	readerClose io.ReadCloser
	idx         index.Index
}

// newLRUCache creates a new LRU cache with a given storage and maximum size.
func newLRUCache(storage storage.Storage, maxSize int) (*lruCache, error) {
	b := &lruCache{storage: storage}
	cache, err := lru.NewWithEvict(maxSize, b.onEvict)
	if err != nil {
		return nil, err
	}
	b.cache = cache

	return b, nil
}

// getBlock gets the block with a given root and block ID from the cache.
func (lru *lruCache) getBlock(ctx context.Context, root, block cid.Cid) (blocks.Block, error) {
	key := Key(root.Hash().String())
	v, ok := lru.cache.Get(key)
	if !ok {
		if err := lru.add(root); err != nil {
			return nil, xerrors.Errorf("add cache %s %w", root.String(), err)
		}

		log.Debugf("add asset %s to cache", block.String())

		if v, ok = lru.cache.Get(key); !ok {
			return nil, xerrors.Errorf("asset %s not exist", root.String())
		}
	}

	if c, ok := v.(*cacheValue); ok {
		return c.bs.Get(ctx, block)
	}

	return nil, xerrors.Errorf("can not convert interface to *cacheValue")
}

// hasBlock checks whether the block with a given root and block ID is present in the cache.
func (lru *lruCache) hasBlock(ctx context.Context, root, block cid.Cid) (bool, error) {
	key := Key(root.Hash().String())
	v, ok := lru.cache.Get(key)
	if !ok {
		if err := lru.add(root); err != nil {
			return false, err
		}

		log.Debugf("check asset %s index from cache", block.String())

		if v, ok = lru.cache.Get(key); !ok {
			return false, xerrors.Errorf("asset %s not exist", root.String())
		}
	}

	if c, ok := v.(*cacheValue); ok {
		return c.bs.Has(ctx, block)
	}

	return false, xerrors.Errorf("can not convert interface to *cacheValue")
}

// assetIndex returns the index of an asset with a given root.
func (lru *lruCache) assetIndex(root cid.Cid) (index.Index, error) {
	key := Key(root.Hash().String())
	v, ok := lru.cache.Get(key)
	if !ok {
		if err := lru.add(root); err != nil {
			return nil, err
		}

		if v, ok = lru.cache.Get(key); !ok {
			return nil, xerrors.Errorf("asset %s not exist", root.String())
		}
	}

	if c, ok := v.(*cacheValue); ok {
		return c.idx, nil
	}

	return nil, xerrors.Errorf("can not convert interface to *cacheValue")
}

// add adds an asset to the cache with a given root.
func (lru *lruCache) add(root cid.Cid) error {
	reader, err := lru.storage.GetAsset(root)
	if err != nil {
		return err
	}

	f, ok := reader.(*os.File)
	if !ok {
		return xerrors.Errorf("can not convert asset %s reader to file", root.String())
	}

	idx, err := lru.getAssetIndex(f)
	if err != nil {
		return err
	}

	bs, err := blockstore.NewReadOnly(f, idx, carv2.ZeroLengthSectionAsEOF(true))
	if err != nil {
		return err
	}

	cache := &cacheValue{bs: bs, readerClose: f, idx: idx}
	lru.cache.Add(Key(root.Hash().String()), cache)

	return nil
}

func (lru *lruCache) remove(root cid.Cid) {
	lru.cache.Remove(Key(root.Hash().String()))
}

func (lru *lruCache) onEvict(key interface{}, value interface{}) {
	if c, ok := value.(*cacheValue); ok {
		c.bs.Close()
		c.readerClose.Close()
	}
}

func (lru *lruCache) getAssetIndex(r io.ReaderAt) (index.Index, error) {
	// Open the CARv2 file
	cr, err := carv2.NewReader(r)
	if err != nil {
		panic(err)
	}
	defer cr.Close()

	// Read and unmarshall index within CARv2 file.
	ir, err := cr.IndexReader()
	if err != nil {
		return nil, err
	}
	idx, err := index.ReadFrom(ir)
	if err != nil {
		return nil, err
	}

	iterableIdx, ok := idx.(index.IterableIndex)
	if !ok {
		return nil, xerrors.Errorf("idx is not IterableIndex")
	}

	records := make([]index.Record, 0)
	iterableIdx.ForEach(func(m multihash.Multihash, u uint64) error {
		record := index.Record{Cid: cid.NewCidV0(m), Offset: u}
		records = append(records, record)
		return nil
	})

	idx = titanindex.NewMultiIndexSorted(sizeOfBuckets)
	if err := idx.Load(records); err != nil {
		return nil, err
	}
	// convert to titan index
	return idx, nil
}
