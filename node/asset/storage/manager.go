package storage

import (
	"context"
	"io"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"github.com/shirou/gopsutil/v3/disk"
)

var log = logging.Logger("asset/store")

const (
	// dir or file name
	pullerDir      = "asset-puller"
	waitListFile   = "wait-list"
	assetsDir      = "assets"
	countDir       = "count"
	assetSuffix    = ".car"
	assetsViewDir  = "assets-view"
	maxSizeOfCache = 1024
	sizeOfBucket   = 128
)

// Manager handles storage operations
type Manager struct {
	opts       *ManagerOptions
	asset      *asset
	wl         *waitList
	puller     *puller
	blockCount *blockCount
	assetsView *assetsView
}

// ManagerOptions contains configuration options for the Manager
type ManagerOptions struct {
	MetaDataPath string
	AssetsPaths  []string
}

// NewManager creates a new Manager instance
func NewManager(opts *ManagerOptions) (*Manager, error) {
	// TODO store assets in multi storage
	asset, err := newAsset(filepath.Join(opts.AssetsPaths[0], assetsDir), assetSuffix)
	if err != nil {
		return nil, err
	}

	puller, err := newPuller(filepath.Join(opts.MetaDataPath, pullerDir))
	if err != nil {
		return nil, err
	}

	blockCount, err := newBlockCount(filepath.Join(opts.MetaDataPath, countDir))
	if err != nil {
		return nil, err
	}

	assetsView, err := newAssetsView(filepath.Join(opts.MetaDataPath, assetsViewDir), sizeOfBucket)
	if err != nil {
		return nil, err
	}

	waitList := newWaitList(filepath.Join(opts.MetaDataPath, waitListFile))
	return &Manager{
		asset:      asset,
		assetsView: assetsView,
		wl:         waitList,
		puller:     puller,
		blockCount: blockCount,
		opts:       opts,
	}, nil
}

// StorePuller stores puller data in storage
func (m *Manager) StorePuller(c cid.Cid, data []byte) error {
	return m.puller.store(c, data)
}

// GetPuller retrieves puller from storage
func (m *Manager) GetPuller(c cid.Cid) ([]byte, error) {
	return m.puller.get(c)
}

// PullerExists checks if an puller exist in storage
func (m *Manager) PullerExists(c cid.Cid) (bool, error) {
	return m.puller.exists(c)
}

// DeletePuller removes an puller from storage
func (m *Manager) DeletePuller(c cid.Cid) error {
	return m.puller.remove(c)
}

// asset api
// StoreBlocks stores multiple blocks for an asset
func (m *Manager) StoreBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error {
	return m.asset.storeBlocks(ctx, root, blks)
}

// StoreAsset stores a single asset
func (m *Manager) StoreAsset(ctx context.Context, root cid.Cid) error {
	return m.asset.storeAsset(ctx, root)
}

// GetAsset retrieves an asset
func (m *Manager) GetAsset(root cid.Cid) (io.ReadSeekCloser, error) {
	return m.asset.get(root)
}

// AssetExists checks if an asset exists
func (m *Manager) AssetExists(root cid.Cid) (bool, error) {
	return m.asset.exists(root)
}

// DeleteAsset removes an asset
func (m *Manager) DeleteAsset(root cid.Cid) error {
	return m.asset.remove(root)
}

// AssetCount returns the number of assets
func (m *Manager) AssetCount() (int, error) {
	return m.asset.count()
}

// GetBlockCount retrieves the block count of an asset
func (m *Manager) GetBlockCount(ctx context.Context, root cid.Cid) (uint32, error) {
	return m.blockCount.getBlockCount(ctx, root)
}

// SetBlockCount sets the block count of an asset
func (m *Manager) SetBlockCount(ctx context.Context, root cid.Cid, count uint32) error {
	return m.blockCount.storeBlockCount(ctx, root, count)
}

// AssetsView API
// GetTopHash retrieves the top hash of assets
func (m *Manager) GetTopHash(ctx context.Context) (string, error) {
	return m.assetsView.getTopHash(ctx)
}

// GetBucketHashes retrieves the hashes for each bucket
func (m *Manager) GetBucketHashes(ctx context.Context) (map[uint32]string, error) {
	return m.assetsView.getBucketHashes(ctx)
}

// GetAssetsInBucket retrieves the assets in a specific bucket
func (m *Manager) GetAssetsInBucket(ctx context.Context, bucketID uint32) ([]cid.Cid, error) {
	hashes, err := m.assetsView.getAssetHashes(ctx, bucketID)
	if err != nil {
		return nil, err
	}

	cids := make([]cid.Cid, 0, len(hashes))
	for _, h := range hashes {
		cids = append(cids, cid.NewCidV0(h))
	}
	return cids, nil
}

// AddAssetToView adds an asset to the assets view
func (m *Manager) AddAssetToView(ctx context.Context, root cid.Cid) error {
	return m.assetsView.addAsset(ctx, root)
}

// RemoveAssetFromView removes an asset from the assets view
func (m *Manager) RemoveAssetFromView(ctx context.Context, root cid.Cid) error {
	return m.assetsView.removeAsset(ctx, root)
}

// WaitList API

// StoreWaitList stores the waitlist data
func (m *Manager) StoreWaitList(data []byte) error {
	return m.wl.put(data)
}

// GetWaitList retrieves the waitlist data
func (m *Manager) GetWaitList() ([]byte, error) {
	return m.wl.get()
}

// DiskStat API

// GetDiskUsageStat retrieves the disk usage statistics
func (m *Manager) GetDiskUsageStat() (totalSpace, usage float64) {
	usageStat, err := disk.Usage(m.opts.MetaDataPath)
	if err != nil {
		log.Errorf("get disk usage stat error: %s", err)
		return 0, 0
	}
	// TODO stat assets storage
	return float64(usageStat.Total), usageStat.UsedPercent
}

// GetFileSystemType retrieves the type of the file system
func (m *Manager) GetFileSystemType() string {
	return "not implement"
}
