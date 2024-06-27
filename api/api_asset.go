package api

import (
	"context"

	"github.com/Filecoin-Titan/titan/api/types"
)

// Asset is an interface for asset manager
type Asset interface {
	// PullAsset pull the asset with given assetCID from specified sources
	PullAsset(ctx context.Context, assetCID string, sources []*types.CandidateDownloadInfo) error //perm:admin
	// PullAssetV2 pull the asset by scheduler
	PullAssetV2(ctx context.Context, req *types.AssetPullRequest) error //perm:admin
	// DeleteAsset deletes the asset with given assetCID
	DeleteAsset(ctx context.Context, assetCID string) error //perm:admin
	// GetAssetStats retrieves the statistics of assets
	GetAssetStats(ctx context.Context) (*types.AssetStats, error) //perm:admin
	// GetCachingAssetInfo retrieves the information of assets that are currently being pulled
	GetPullingAssetInfo(ctx context.Context) (*types.InProgressAsset, error) //perm:admin
	// GetAssetProgresses retrieves the progress of assets with specified assetCIDs
	GetAssetProgresses(ctx context.Context, assetCIDs []string) (*types.PullResult, error) //perm:admin
	// CreateAsset notify candidate that user upload asset, return auth token of candidate
	CreateAsset(ctx context.Context, tokenPayload *types.AuthUserUploadDownloadAsset) (string, error) //perm:admin
	// PullAssetWithURL download the file locally from the url and save it as car file
	PullAssetFromAWS(ctx context.Context, bucket, key string) error //perm:admin
	// GetAssetView get asset view
	GetAssetView(ctx context.Context) (*types.AssetView, error) //perm:admin
	// AddAssetView add asset to view
	AddAssetView(ctx context.Context, assetCIDs []string) error //perm:admin
	// GetAssetsInBucket get assets in bucket
	GetAssetsInBucket(ctx context.Context, bucketID int) ([]string, error) //perm:admin
	// SyncAssetViewAndData sync assetView and local car
	SyncAssetViewAndData(ctx context.Context) error //perm:admin
	// RequestFreeUpDisk Initiate a request to free up disk space with a certain size, size unit GiB
	RequestFreeUpDisk(ctx context.Context, size float64) error //perm:admin
	// StateFreeUpDisk shows the result of last free task is done, return nil means done
	StateFreeUpDisk(ctx context.Context) (*types.FreeUpDiskStateResp, error) //perm:admin
	// ClearFreeUpDisk clear the previous failed task
	ClearFreeUpDisk(ctx context.Context) error //perm:admin
}
