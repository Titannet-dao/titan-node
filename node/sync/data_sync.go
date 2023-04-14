package sync

import (
	"context"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("datasync")

// DataSync represents a data synchronizer, which implements the Sync interface
type DataSync struct {
	Sync
}

// Sync defines the synchronization interface
type Sync interface {
	// GetTopHash returns the local top checksum
	GetTopHash(ctx context.Context) (string, error)
	// GetBucketHashes returns local checksums of all buckets
	GetBucketHashes(ctx context.Context) (map[uint32]string, error)
	// GetAssetsOfBucket retrieves assets of a bucket either from local or remote storage
	GetAssetsOfBucket(ctx context.Context, bucketNumber uint32, isRemote bool) ([]cid.Cid, error)
	DeleteAsset(root cid.Cid) error
	AddLostAsset(root cid.Cid) error
}

// NewDataSync creates a new instance of DataSync
func NewDataSync(sync Sync) *DataSync {
	return &DataSync{sync}
}

// CompareTopHash compares the local top hash with the given hash
func (ds *DataSync) CompareTopHash(ctx context.Context, topHash string) (bool, error) {
	hash, err := ds.GetTopHash(ctx)
	if err != nil {
		return false, err
	}

	return hash == topHash, nil
}

// CompareBucketHashes groups assets in a bucket and compares individual bucket checksums
func (ds *DataSync) CompareBucketHashes(ctx context.Context, hashes map[uint32]string) ([]uint32, error) {
	localHashes, err := ds.GetBucketHashes(ctx)
	if err != nil {
		return nil, err
	}

	mismatchBuckets := make([]uint32, 0)
	lostBuckets := make([]uint32, 0)

	for k, hash := range hashes {
		if h, ok := localHashes[k]; ok {
			if h != hash {
				mismatchBuckets = append(mismatchBuckets, k)
			}
			delete(localHashes, k)
		} else {
			lostBuckets = append(lostBuckets, k)
		}
	}

	extraBuckets := make([]uint32, 0)
	for k := range localHashes {
		extraBuckets = append(extraBuckets, k)
	}

	go ds.doSync(ctx, extraBuckets, lostBuckets, mismatchBuckets)

	return append(mismatchBuckets, lostBuckets...), nil
}

// doSync performs synchronization for extra, lost and mismatched buckets
func (ds *DataSync) doSync(ctx context.Context, extraBuckets, lostBuckets, mismatchBuckets []uint32) {
	if len(extraBuckets) > 0 {
		if err := ds.removeExtraAsset(ctx, extraBuckets); err != nil {
			log.Errorf("remove extra asset error:%s", err.Error())
		}
	}

	if len(lostBuckets) > 0 {
		if err := ds.addLostAsset(ctx, lostBuckets); err != nil {
			log.Errorf("add lost asset error:%s", err.Error())
		}
	}

	if len(mismatchBuckets) > 0 {
		if err := ds.repairMismatchAsset(ctx, mismatchBuckets); err != nil {
			log.Errorf("repair mismatch asset error %s", err.Error())
		}
	}
}

// removeExtraAsset removes any assets from the datastore that are not present in the specified buckets.
func (ds *DataSync) removeExtraAsset(ctx context.Context, buckets []uint32) error {
	cars := make([]cid.Cid, 0)
	for _, bucketID := range buckets {
		cs, err := ds.GetAssetsOfBucket(ctx, bucketID, false)
		if err != nil {
			return err
		}
		cars = append(cars, cs...)
	}

	for _, car := range cars {
		ds.DeleteAsset(car)
	}
	return nil
}

// addLostAsset adds any assets to the datastore that are present in the specified buckets but not currently in the datastore.
func (ds *DataSync) addLostAsset(ctx context.Context, buckets []uint32) error {
	cars := make([]cid.Cid, 0)
	for _, bucketID := range buckets {
		cs, err := ds.GetAssetsOfBucket(ctx, bucketID, false)
		if err != nil {
			return err
		}
		cars = append(cars, cs...)
	}

	for _, car := range cars {
		ds.AddLostAsset(car)
	}
	return nil
}

// repairMismatchAsset reconciles the datastore with the specified buckets, removing any assets that are not present in the buckets
// and adding any missing assets to the datastore.
func (ds *DataSync) repairMismatchAsset(ctx context.Context, buckets []uint32) error {
	extraCars := make([]cid.Cid, 0)
	lostCars := make([]cid.Cid, 0)
	for _, bucketID := range buckets {
		extras, lost, err := ds.compareBuckets(ctx, bucketID)
		if err != nil {
			return err
		}

		if len(extras) > 0 {
			extraCars = append(extraCars, extras...)
		}

		if len(lost) > 0 {
			lostCars = append(lostCars, lost...)
		}

	}

	for _, car := range extraCars {
		ds.DeleteAsset(car)
	}

	for _, car := range lostCars {
		ds.AddLostAsset(car)
	}
	return nil
}

// compareBuckets compares the assets in the specified bucket in the datastore and in the remote storage, returning the extra and lost assets.
func (ds *DataSync) compareBuckets(ctx context.Context, bucketID uint32) ([]cid.Cid, []cid.Cid, error) {
	localAssets, err := ds.GetAssetsOfBucket(ctx, bucketID, false)
	if err != nil {
		return nil, nil, err
	}
	remoteAssets, err := ds.GetAssetsOfBucket(ctx, bucketID, true)
	if err != nil {
		return nil, nil, err
	}

	return ds.compareAssets(ctx, localAssets, remoteAssets)
}

// compareAssets compares the local and remote assets and returns the extra and lost assets.
func (ds *DataSync) compareAssets(ctx context.Context, localAssets []cid.Cid, remoteAssets []cid.Cid) ([]cid.Cid, []cid.Cid, error) {
	localAssetMap := make(map[string]cid.Cid, 0)
	for _, asset := range localAssets {
		localAssetMap[asset.Hash().String()] = asset
	}

	lostAssets := make([]cid.Cid, 0)
	for _, asset := range remoteAssets {
		if _, ok := localAssetMap[asset.Hash().String()]; ok {
			delete(localAssetMap, asset.Hash().String())
		} else {
			lostAssets = append(lostAssets, asset)
		}
	}

	extraAssets := make([]cid.Cid, 0)
	for _, asset := range localAssetMap {
		extraAssets = append(extraAssets, asset)
	}
	return extraAssets, lostAssets, nil
}
