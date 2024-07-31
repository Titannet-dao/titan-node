package asset

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/asset/storage"
	"github.com/Filecoin-Titan/titan/node/ipld"
	"github.com/docker/go-units"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("asset")

type Asset struct {
	scheduler       api.Scheduler
	mgr             *Manager
	TotalBlockCount int
	apiSecret       *jwt.HMACSHA
	AWS
}

// NewAsset creates a new Asset instance
func NewAsset(storageMgr *storage.Manager, scheduler api.Scheduler, assetMgr *Manager, apiSecret *jwt.HMACSHA, rateLimiter *types.RateLimiter) *Asset {
	return &Asset{
		scheduler: scheduler,
		mgr:       assetMgr,
		apiSecret: apiSecret,
		AWS:       NewAWS(scheduler, storageMgr, rateLimiter.BandwidthDownLimiter),
	}
}

// PullAsset adds the asset to the waitList for pulling
func (a *Asset) PullAsset(ctx context.Context, rootCID string, infos []*types.CandidateDownloadInfo) error {
	if types.RunningNodeType == types.NodeEdge && len(infos) == 0 {
		return fmt.Errorf("candidate download infos can not empty")
	}

	root, err := cid.Decode(rootCID)
	if err != nil {
		return err
	}

	has, err := a.mgr.AssetExists(root)
	if err != nil {
		return err
	}

	if has {
		log.Debugf("Asset %s already exist", root.String())
		return nil
	}

	log.Infof("PullAsset %s", rootCID)

	var aws *types.AWSDownloadSources = nil
	dInfos := make([]*types.SourceDownloadInfo, len(infos))
	for _, info := range infos {
		dInfo := &types.SourceDownloadInfo{NodeID: info.NodeID, Address: info.Address, Tk: info.Tk}
		dInfos = append(dInfos, dInfo)

		if len(info.AWSBucket) > 0 && len(info.AWSKey) > 0 {
			aws = &types.AWSDownloadSources{}
			aws.Bucket = info.AWSBucket
			aws.Key = info.AWSKey
		}
	}

	dss := &types.DownloadSources{AWS: aws, Nodes: dInfos}
	aw := &assetWaiter{Root: root, Dss: dss, isSyncData: false}
	a.mgr.addToWaitList(aw)

	return nil
}

// PullAsset adds the asset to the waitList for pulling
func (a *Asset) PullAssetV2(ctx context.Context, req *types.AssetPullRequest) error {
	if types.RunningNodeType == types.NodeEdge && req.Dss == nil {
		return fmt.Errorf("candidate download infos can not empty")
	}

	root, err := cid.Decode(req.AssetCID)
	if err != nil {
		return err
	}

	has, err := a.mgr.AssetExists(root)
	if err != nil {
		return err
	}

	if has {
		log.Debugf("Asset %s already exist", root.String())
		return nil
	}

	// buf, _ := json.Marshal(req)
	// log.Infof("PullAssetV2 %s, dss %s", root.String(), string(buf))

	aw := &assetWaiter{Root: root, Dss: req.Dss, isSyncData: false, workloadID: req.WorkloadID}
	a.mgr.addToWaitList(aw)
	return nil
}

// DeleteAsset deletes the asset with the given CID
func (a *Asset) DeleteAsset(ctx context.Context, assetCID string) error {
	c, err := cid.Decode(assetCID)
	if err != nil {
		log.Errorf("Decode asset cid %s error: %s", assetCID, err.Error())
		return err
	}

	log.Debugf("DeleteAsset %s", assetCID)

	go func() {
		if err := a.mgr.DeleteAsset(c); err != nil {
			log.Errorf("delete asset failed %s", err.Error())
			return
		}

		_, diskUsage := a.mgr.GetDiskUsageStat()
		ret := types.RemoveAssetResult{BlocksCount: a.TotalBlockCount, DiskUsage: diskUsage}

		err = a.scheduler.NodeRemoveAssetResult(context.Background(), ret)
		if err != nil {
			log.Errorf("remove asset result failed %s", err.Error())
		}

		a.mgr.releaser.setFile(c.Hash().String(), err)
	}()

	return nil
}

// GetAssetStats returns statistics about the assets stored on this node
func (a *Asset) GetAssetStats(ctx context.Context) (*types.AssetStats, error) {
	assetCount, err := a.mgr.AssetCount()
	if err != nil {
		return nil, err
	}

	assetStats := &types.AssetStats{}
	assetStats.TotalBlockCount = a.TotalBlockCount
	assetStats.TotalAssetCount = assetCount
	assetStats.WaitCacheAssetCount = a.mgr.waitListLen()
	_, assetStats.DiskUsage = a.mgr.GetDiskUsageStat()

	puller := a.mgr.puller()
	if puller != nil {
		assetStats.InProgressAssetCID = puller.root.String()
	}

	log.Debugf("asset stats: %#v", *assetStats)

	return assetStats, nil
}

// GetPullingAssetInfo returns information about the asset currently being pulled
func (a *Asset) GetPullingAssetInfo(ctx context.Context) (*types.InProgressAsset, error) {
	puller := a.mgr.puller()
	if puller == nil {
		return nil, fmt.Errorf("no asset caching")
	}

	ret := &types.InProgressAsset{}
	ret.CID = puller.root.Hash().String()
	ret.TotalSize = int64(puller.totalSize)
	ret.DoneSize = int64(puller.doneSize)

	return ret, nil
}

// GetBlocksOfAsset returns a random subset of blocks for the given asset.
func (a *Asset) GetBlocksOfAsset(assetCID string, randomSeed int64, randomCount int) ([]string, error) {
	root, err := cid.Decode(assetCID)
	if err != nil {
		return nil, err
	}

	return a.mgr.GetBlocksOfAsset(root, randomSeed, randomCount)
}

// BlockCountOfAsset returns the block count for the given asset.
func (a *Asset) BlockCountOfAsset(assetCID string) (int, error) {
	c, err := cid.Decode(assetCID)
	if err != nil {
		return 0, err
	}

	count, err := a.mgr.GetBlockCount(context.Background(), c)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// CreateAsset notify candidate that user upload asset, return auth token of candidate
func (a *Asset) CreateAsset(ctx context.Context, tokenPayload *types.AuthUserUploadDownloadAsset) (string, error) {
	c, err := cid.Decode(tokenPayload.AssetCID)
	if err != nil {
		return "", err
	}

	v, ok := a.mgr.uploadingAssets.Load(c.Hash().String())
	if ok {
		asset := v.(*types.UploadingAsset)
		if asset.TokenExpiration.After(time.Now()) {
			return "", fmt.Errorf("asset %s already uploading", tokenPayload.AssetCID)
		}
	}

	asset := &types.UploadingAsset{UserID: tokenPayload.UserID, TokenExpiration: tokenPayload.Expiration, Progress: &types.UploadProgress{}}
	a.mgr.uploadingAssets.Store(c.Hash().String(), asset)

	tk, err := jwt.Sign(&tokenPayload, a.apiSecret)
	if err != nil {
		return "", err
	}

	return string(tk), nil
}

// GetAssetProgresses returns the progress of the given list of assets.
func (a *Asset) GetAssetProgresses(ctx context.Context, assetCIDs []string) (*types.PullResult, error) {
	progresses := make([]*types.AssetPullProgress, 0, len(assetCIDs))
	for _, assetCID := range assetCIDs {
		root, err := cid.Decode(assetCID)
		if err != nil {
			log.Errorf("decode cid %s", err.Error())
			return nil, err
		}

		progress, err := a.progress(root)
		if err != nil {
			log.Errorf("get asset progress %s", err.Error())
			return nil, err
		}
		progresses = append(progresses, progress)
	}

	result := &types.PullResult{
		Progresses:       progresses,
		TotalBlocksCount: a.TotalBlockCount,
	}

	if count, err := a.mgr.AssetCount(); err == nil {
		result.AssetCount = count
	}
	_, result.DiskUsage = a.mgr.GetDiskUsageStat()

	return result, nil
}

// progressForAssetPulledSucceeded returns asset pull progress for the succeeded asset.
func (a *Asset) progressForAssetPulledSucceeded(root cid.Cid) (*types.AssetPullProgress, error) {
	progress := &types.AssetPullProgress{
		CID:    root.String(),
		Status: types.ReplicaStatusSucceeded,
	}

	count, err := a.mgr.GetBlockCount(context.Background(), root)
	if err != nil {
		return nil, xerrors.Errorf("get block count %w", err)
	}

	progress.BlocksCount = count
	progress.DoneBlocksCount = count

	blk, err := a.mgr.GetBlock(context.Background(), root, root)
	if err != nil {
		return nil, xerrors.Errorf("get block %w", err)
	}

	blk = blocks.NewBlock(blk.RawData())
	linksSize := uint64(len(blk.RawData()))

	// TODO check blk data type
	node, err := ipld.DecodeNode(context.Background(), blk)
	if err == nil {
		for _, link := range node.Links() {
			linksSize += link.Size
		}
	} else {
		log.Warnf("decode node %s", err.Error())
	}

	progress.Size = int64(linksSize)
	progress.DoneSize = int64(linksSize)

	return progress, nil
}

func (a *Asset) progress(root cid.Cid) (*types.AssetPullProgress, error) {
	status, err := a.mgr.assetStatus(root)
	if err != nil {
		return nil, xerrors.Errorf("asset %s cache status %w", root.Hash(), err)
	}

	switch status {
	case types.ReplicaStatusWaiting:
		return &types.AssetPullProgress{CID: root.String(), Status: types.ReplicaStatusWaiting}, nil
	case types.ReplicaStatusPulling:
		return a.mgr.progressForPulling(root)
	case types.ReplicaStatusFailed:
		return a.mgr.progressForAssetPulledFailed(root)
	case types.ReplicaStatusSucceeded:
		return a.progressForAssetPulledSucceeded(root)
	}
	return nil, xerrors.Errorf("unknown asset %s status %d", root.String(), status)
}

func (a *Asset) GetAssetView(ctx context.Context) (*types.AssetView, error) {
	topHash, err := a.mgr.GetTopHash(ctx)
	if err != nil {
		return nil, err
	}

	bucketHashes, err := a.mgr.GetBucketHashes(ctx)
	if err != nil {
		return nil, err
	}

	return &types.AssetView{TopHash: topHash, BucketHashes: bucketHashes}, nil
}

func (a *Asset) AddAssetView(ctx context.Context, assetCIDs []string) error {
	for _, assetCID := range assetCIDs {
		root, err := cid.Decode(assetCID)
		if err != nil {
			return err
		}

		exists, err := a.mgr.AssetExists(root)
		if err != nil {
			return err
		}

		if !exists {
			return fmt.Errorf("asset %s not exist", assetCID)
		}

		return a.mgr.AddAssetToView(ctx, root)
	}
	return nil
}

func (a *Asset) GetAssetsInBucket(ctx context.Context, bucketID int) ([]string, error) {
	cids, err := a.mgr.Storage.GetAssetsInBucket(ctx, uint32(bucketID))
	if err != nil {
		return nil, err
	}

	hashes := make([]string, 0)
	for _, cid := range cids {
		hashes = append(hashes, cid.Hash().String())
	}
	return hashes, nil
}

func (a *Asset) SyncAssetViewAndData(ctx context.Context) error {
	return a.mgr.syncDataWithAssetView()
}

func (a *Asset) RequestFreeUpDisk(ctx context.Context, size float64) error {
	if a.mgr.releaser.count() > 0 {
		return errors.New("the last free-up-disk task is not done")
	}

	if size <= 0 {
		return nil
	}
	freeBytes := int64(size * units.GiB)

	ret, err := a.scheduler.FreeUpDiskSpace(ctx, "", freeBytes)

	if ret != nil && ret.NextTime > 0 {
		a.mgr.releaser.setNextTime(ret.NextTime)
	}

	if err != nil {
		return err
	}

	a.mgr.releaser.initFiles(ret.Hashes)

	return nil
}

func (a *Asset) StateFreeUpDisk(ctx context.Context) (*types.FreeUpDiskStateResp, error) {
	var res []*types.FreeUpDiskState
	for _, v := range a.mgr.releaser.load() {
		res = append(res, &types.FreeUpDiskState{
			Hash:   v.Hash,
			ErrMsg: v.ErrMsg,
		})
	}
	// if restart nextTime would be remove from memory, this retrieve the time from scheduler
	if a.mgr.releaser.nextTime == 0 {
		t, err := a.scheduler.GetNextFreeTime(ctx, "")
		if err != nil {
			return nil, fmt.Errorf("get free-up-disk state from scheduler error: %s", err.Error())
		}
		a.mgr.releaser.nextTime = t
	}
	return &types.FreeUpDiskStateResp{Hashes: res, NextTime: a.mgr.releaser.nextTime}, nil
}

func (a *Asset) ClearFreeUpDisk(ctx context.Context) error {
	if a.mgr.releaser.count() == 0 {
		return nil
	}
	a.mgr.releaser.clear()
	return nil
}
