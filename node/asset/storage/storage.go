package storage

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

// Storage is an interface for handling storage operations related to assets.
type Storage interface {
	StorePuller(c cid.Cid, data []byte) error
	GetPuller(c cid.Cid) ([]byte, error)
	PullerExists(c cid.Cid) (bool, error)
	DeletePuller(c cid.Cid) error

	StoreBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error

	StoreBlocksToCar(ctx context.Context, root cid.Cid) error
	StoreUserAsset(ctx context.Context, userID string, root cid.Cid, assetSize int64, r io.Reader) error
	GetAsset(root cid.Cid) (io.ReadSeekCloser, error)
	GetAssetHashesForSyncData(ctx context.Context) ([]string, error)
	AssetExists(root cid.Cid) (bool, error)
	DeleteAsset(root cid.Cid) error
	AssetCount() (int, error)

	GetBlockCount(ctx context.Context, root cid.Cid) (uint32, error)
	SetBlockCount(ctx context.Context, root cid.Cid, count uint32) error
	DeleteBlockCount(ctx context.Context, root cid.Cid) error

	// assets view
	GetTopHash(ctx context.Context) (string, error)
	GetBucketHashes(ctx context.Context) (map[uint32]string, error)
	GetAssetsInBucket(ctx context.Context, bucketID uint32) ([]cid.Cid, error)
	AddAssetToView(ctx context.Context, root cid.Cid) error
	RemoveAssetFromView(ctx context.Context, root cid.Cid) error

	StoreWaitList(data []byte) error
	GetWaitList() ([]byte, error)

	GetDiskUsageStat() (totalSpace, usage float64)
	GetFileSystemType() string
}
