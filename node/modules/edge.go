package modules

import (
	"context"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
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
func NewDevice(cpu *config.CPU, memory *config.Memory, storageCfg *config.Storage, bandwidth *config.Bandwidth) func(nodeID dtypes.NodeID, internalIP dtypes.InternalIP, storageMgr *storage.Manager) *device.Device {
	return func(nodeID dtypes.NodeID, internalIP dtypes.InternalIP, storageMgr *storage.Manager) *device.Device {
		res := &device.Resources{CPU: cpu, Memory: memory, Storage: storageCfg, Bandwidth: bandwidth}
		return device.NewDevice(string(nodeID), string(internalIP), res, storageMgr)
	}
}

// NewRateLimiter creates a new rate limiter based on the given device's bandwidth limits.
func NewRateLimiter(device *device.Device) *types.RateLimiter {
	return &types.RateLimiter{
		BandwidthUpLimiter:   rate.NewLimiter(rate.Limit(device.GetBandwidthUp()), int(device.GetBandwidthUp())),
		BandwidthDownLimiter: rate.NewLimiter(rate.Limit(device.GetBandwidthDown()), int(device.GetBandwidthDown())),
	}
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
func NewAssetsManager(ctx context.Context, pullerConfig *config.Puller, ipfsAPIURL string) func(storageMgr *storage.Manager, schedulerAPI api.Scheduler, rateLimiter *types.RateLimiter) (*asset.Manager, error) {
	return func(storageMgr *storage.Manager, schedulerAPI api.Scheduler, rateLimiter *types.RateLimiter) (*asset.Manager, error) {
		opts := &asset.ManagerOptions{
			Storage:      storageMgr,
			IPFSAPIURL:   ipfsAPIURL,
			SchedulerAPI: schedulerAPI,
			PullerConfig: pullerConfig,
			RateLimiter:  rateLimiter,
		}
		return asset.NewManager(ctx, opts)
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
