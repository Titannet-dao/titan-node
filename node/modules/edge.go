package modules

import (
	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/node/asset"
	"github.com/Filecoin-Titan/titan/node/asset/storage"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/device"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	datasync "github.com/Filecoin-Titan/titan/node/sync"
	"github.com/Filecoin-Titan/titan/node/validation"
	"golang.org/x/time/rate"
)

// NewDevice creates a function that generates new instances of device.Device.
func NewDevice(bandwidthUP, bandwidthDown int64) func(nodeID dtypes.NodeID, internalIP dtypes.InternalIP, storageMgr *storage.Manager) *device.Device {
	return func(nodeID dtypes.NodeID, internalIP dtypes.InternalIP, storageMgr *storage.Manager) *device.Device {
		return device.NewDevice(string(nodeID), string(internalIP), bandwidthUP, bandwidthDown, storageMgr)
	}
}

// NewRateLimiter creates a new rate limiter based on the given device's bandwidth limits.
func NewRateLimiter(device *device.Device) *rate.Limiter {
	return rate.NewLimiter(rate.Limit(device.GetBandwidthUp()), int(device.GetBandwidthUp()))
}

// NewNodeStorageManager creates a new instance of storage.Manager with the given carfile store path.
func NewNodeStorageManager(metadataPaths dtypes.NodeMetadataPath, assetsPaths dtypes.AssetsPaths, minioConfig *config.MinioConfig, schedulerAPI api.Scheduler) (*storage.Manager, error) {
	opts := &storage.ManagerOptions{
		MetaDataPath: string(metadataPaths),
		AssetsPaths:  assetsPaths,
		MinioConfig:  minioConfig,
		SchedulerAPI: schedulerAPI,
	}
	return storage.NewManager(opts)
}

// NewAssetsManager creates a function that generates new instances of asset.Manager.
func NewAssetsManager(pullParallel int, pullTimeout int, pullRetry int, ipfsAPIURL string) func(storageMgr *storage.Manager, schedulerAPI api.Scheduler) (*asset.Manager, error) {
	return func(storageMgr *storage.Manager, schedulerAPI api.Scheduler) (*asset.Manager, error) {
		opts := &asset.ManagerOptions{
			Storage:      storageMgr,
			IPFSAPIURL:   ipfsAPIURL,
			SchedulerAPI: schedulerAPI,
			PullParallel: pullParallel,
			PullTimeout:  pullTimeout,
			PullRetry:    pullRetry,
		}
		return asset.NewManager(opts)
	}
}

// NewDataSync creates a new instance of datasync.DataSync with the given asset.Manager.
func NewDataSync(assetMgr *asset.Manager) *datasync.DataSync {
	return datasync.NewDataSync(assetMgr)
}

// NewNodeValidation creates a new instance of validation.Validation with the given asset.Manager and device.Device.
func NewNodeValidation(assetMgr *asset.Manager, device *device.Device) *validation.Validation {
	return validation.NewValidation(assetMgr, device)
}
