package httpserver

import (
	"context"
	"io"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

// Asset represents an interface for fetching and checking asset data.
type Asset interface {
	// GetAsset fetches the asset data for a given CID and returns an io.ReadSeekCloser.
	GetAsset(root cid.Cid) (io.ReadSeekCloser, error)
	// AssetExists checks whether the asset data for a given CID exists or not.
	AssetExists(root cid.Cid) (bool, error)
	// HasBlock checks if a block with the given CID is present in the asset data for a given root CID.
	HasBlock(ctx context.Context, root, block cid.Cid) (bool, error)
	// GetBlock retrieves a block with the given CID from the asset data for a given root CID.
	GetBlock(ctx context.Context, root, block cid.Cid) (blocks.Block, error)
	// SaveUserAsset save user asset to local
	SaveUserAsset(ctx context.Context, userID string, root cid.Cid, assetSize int64, r io.Reader) error
	// SetAssetUploadProgress set progress of upload for asset
	SetAssetUploadProgress(ctx context.Context, root cid.Cid, progress *types.UploadProgress) error
	// GetUploadingAsset get asset which uploading
	GetUploadingAsset(ctx context.Context, root cid.Cid) (*types.UploadingAsset, error)
	// AllocatePathWithSize allocate path for storage file
	AllocatePathWithSize(size int64) (string, error)
}
