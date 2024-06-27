package storage

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"github.com/shirou/gopsutil/v3/disk"
)

var log = logging.Logger("asset/store")

const (
	// dir or file name
	pullerDir     = "asset-puller"
	waitListFile  = "wait-list"
	assetsDir     = "assets"
	assetSuffix   = ".car"
	assetsViewDir = "assets-view"
	sizeOfBucket  = 128
)

// hold the assetsPath's partition
type partition struct {
	assetsPath    string
	diskPartition disk.PartitionStat
}

// Manager handles storage operations
type Manager struct {
	opts         *ManagerOptions
	asset        *asset
	wl           *waitList
	puller       *puller
	assetsView   *assetsView
	minioService IMinioService
	// mountPoints  map[string]*partition
}

// ManagerOptions contains configuration options for the Manager
type ManagerOptions struct {
	MetaDataPath string
	AssetsPaths  []string
	MinioConfig  *config.MinioConfig
	SchedulerAPI api.Scheduler
}

// NewManager creates a new Manager instance
func NewManager(opts *ManagerOptions) (*Manager, error) {
	var minio IMinioService = nil
	if iminio, err := newMinioService(opts.MinioConfig, opts.SchedulerAPI); err == nil {
		minio = iminio
	}

	// check assetsPaths if in same disk partition
	// mountPoints, err := getPartitions(opts.AssetsPaths)
	// if err != nil {
	// 	return nil, err
	// }

	// for _, mountPoint := range mountPoints {
	// 	log.Infof("mount point %s --> %s", mountPoint.assetsPath, mountPoint.diskPartition.Mountpoint)
	// }

	assetsPaths, err := newAssetsPaths(opts.AssetsPaths, assetsDir)
	if err != nil {
		return nil, err
	}

	asset, err := newAsset(assetsPaths, assetSuffix)
	if err != nil {
		return nil, err
	}

	puller, err := newPuller(filepath.Join(opts.MetaDataPath, pullerDir))
	if err != nil {
		return nil, err
	}

	assetsView, err := newAssetsView(filepath.Join(opts.MetaDataPath, assetsViewDir), sizeOfBucket)
	if err != nil {
		return nil, err
	}

	waitList := newWaitList(filepath.Join(opts.MetaDataPath, waitListFile))
	return &Manager{
		asset:        asset,
		assetsView:   assetsView,
		wl:           waitList,
		puller:       puller,
		opts:         opts,
		minioService: minio,
		// mountPoints:  mountPoints,
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

func (m *Manager) AllocatePathWithSize(size int64) (string, error) {
	return m.asset.allocatePathWithSize(size)
}

// asset api
// StoreBlocks stores multiple blocks for an asset
func (m *Manager) StoreBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error {
	return m.asset.storeBlocks(ctx, root, blks)
}

// StoreBlocksToCar stores a single asset
func (m *Manager) StoreBlocksToCar(ctx context.Context, root cid.Cid) error {
	return m.asset.storeBlocksToCar(ctx, root)
}

func (m *Manager) StoreUserAsset(ctx context.Context, userID string, root cid.Cid, assetSize int64, r io.Reader) error {
	return m.asset.saveUserAsset(ctx, userID, root, assetSize, r)
}

// GetAsset retrieves an asset
func (m *Manager) GetAsset(root cid.Cid) (io.ReadSeekCloser, error) {
	return m.asset.get(root)
}

func (m *Manager) GetAssetHashesForSyncData(ctx context.Context) ([]string, error) {
	return m.asset.getAssetHashesForSyncData()
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
		multiHash, err := hex.DecodeString(h)
		if err != nil {
			return nil, err
		}

		cids = append(cids, cid.NewCidV0(multiHash))
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

func (m *Manager) CloseAssetView() error {
	return m.assetsView.ds.Close()
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
	if m.minioService != nil {
		return m.minioService.GetMinioStat(context.Background())
	}

	if len(m.opts.AssetsPaths) == 0 {
		return 0, 0
	}

	total := float64(0)
	used := float64(0)
	for _, assetPath := range m.opts.AssetsPaths {
		usageStat, err := disk.Usage(assetPath)
		if err != nil {
			log.Errorf("get disk usage stat error: %s", err)
			return total, used / total * 100
		}

		total += float64(usageStat.Total)
		used += usageStat.UsedPercent / 100 * float64(usageStat.Total)
	}

	// log.Infof("total %0.2f, used percent %0.2f ", total, used/total*100)
	return total, used / total * 100
}

// GetFileSystemType retrieves the type of the file system
func (m *Manager) GetFileSystemType() string {
	// fsTypeMap := make(map[string]struct{})
	// for _, partition := range m.mountPoints {
	// 	fsTypeMap[partition.diskPartition.Fstype] = struct{}{}
	// }

	// fsTypes := ""
	// for fsType := range fsTypeMap {
	// 	fsTypes += "," + fsType
	// }
	// return strings.TrimPrefix(fsTypes, ",")
	return "not implement"
}

func getPartitions(assetsPaths []string) (map[string]*partition, error) {
	mountPoints := make(map[string]*partition)
	infos, _ := disk.Partitions(false)

	var rootPartition disk.PartitionStat
	for _, info := range infos {
		if info.Mountpoint == "/" {
			rootPartition = info
			break
		}
	}

	for _, assetsPath := range assetsPaths {
		isAssetsPathExistMountPoint := false
		for _, info := range infos {
			if info.Mountpoint == "/" {
				continue
			}

			if !strings.HasPrefix(assetsPath, info.Mountpoint) {
				continue
			}

			if path, ok := mountPoints[info.Mountpoint]; ok {
				return nil, fmt.Errorf("assetsPath %s and %s is in same mount piont %s", path, assetsPath, info.Mountpoint)
			}

			mountPoints[info.Mountpoint] = &partition{assetsPath: assetsPath, diskPartition: info}
			isAssetsPathExistMountPoint = true
			break
		}

		if !isAssetsPathExistMountPoint {
			if !strings.HasPrefix(assetsPath, "/") {
				return nil, fmt.Errorf("assetsPath %s not exist mount pint", assetsPath)
			}

			if pt, ok := mountPoints["/"]; ok {
				return nil, fmt.Errorf("assetsPath %s and %s is in same mount piont /", pt.assetsPath, assetsPath)
			}

			mountPoints["/"] = &partition{assetsPath: assetsPath, diskPartition: rootPartition}
		}
	}

	return mountPoints, nil
}
