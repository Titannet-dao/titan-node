package asset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/asset/fetcher"
	"github.com/Filecoin-Titan/titan/node/asset/index"
	"github.com/Filecoin-Titan/titan/node/asset/storage"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/ipld"
	validate "github.com/Filecoin-Titan/titan/node/validation"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-libipfs/blocks"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

const maxSizeOfCache = 128

// assetWaiter is used by Manager to store waiting assets for pulling
type assetWaiter struct {
	Root cid.Cid
	// Dss                []*types.CandidateDownloadInfo
	Dss *types.DownloadSources

	puller     *assetPuller
	isSyncData bool
	workloadID string
}

// Manager is the struct that manages asset pulling and store
type Manager struct {
	ctx context.Context
	// root cid of asset
	waitList     []*assetWaiter
	waitListLock *sync.Mutex
	pullCh       chan bool
	ipfsAPIURL   string
	lru          *lruCache
	storage.Storage
	api.Scheduler
	pullerConfig *config.Puller

	// save asset upload status
	uploadingAssets *sync.Map

	// hold the error msg, and wait for scheduler query asset progress
	pullAssetErrMsgs *sync.Map
	rateLimiter      *types.RateLimiter

	// releaser means result of scheduler.FreeUpDiskSpace
	releaser *releaser
}

// ManagerOptions is the struct that contains options for Manager
type ManagerOptions struct {
	Storage      storage.Storage
	IPFSAPIURL   string
	SchedulerAPI api.Scheduler
	PullerConfig *config.Puller
	RateLimiter  *types.RateLimiter
}

// NewManager creates a new instance of Manager
func NewManager(ctx context.Context, opts *ManagerOptions) (*Manager, error) {
	lru, err := newLRUCache(opts.Storage, maxSizeOfCache)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		ctx:          ctx,
		waitList:     make([]*assetWaiter, 0),
		waitListLock: &sync.Mutex{},
		pullCh:       make(chan bool),
		Storage:      opts.Storage,
		ipfsAPIURL:   opts.IPFSAPIURL,
		lru:          lru,
		Scheduler:    opts.SchedulerAPI,
		pullerConfig: opts.PullerConfig,
		rateLimiter:  opts.RateLimiter,

		uploadingAssets:  &sync.Map{},
		pullAssetErrMsgs: &sync.Map{},

		releaser: NewReleaser(),
	}

	m.restoreWaitListFromStore()

	go m.start()

	go m.regularSyncDataWithAssetView()

	return m, nil
}

// startTick is a helper function that is used to check waitList and save puller
func (m *Manager) startTick() {
	defer log.Debugf("manager tick finish")

	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ticker.C:
			if len(m.waitList) > 0 {
				puller := m.puller()
				if puller != nil {
					if err := m.savePuller(puller); err != nil {
						log.Error("save puller error:%s", err.Error())
					}

					log.Debugf("total block %d, done block %d, total size %d, done size %d",
						len(puller.blocksPulledSuccessList)+len(puller.blocksWaitList),
						len(puller.blocksPulledSuccessList),
						puller.totalSize,
						puller.doneSize)
				}
			}
		case <-m.ctx.Done():
			return
		}
	}
}

// triggerPuller is a helper function that is used to trigger asset downloads
func (m *Manager) triggerPuller() {
	select {
	case m.pullCh <- true:
	default:
	}
}

// start is a helper function that starts the Manager and begins downloading assets
func (m *Manager) start() {
	defer log.Debugf("Manager finish")

	go m.startTick()

	// delay 15 second to pull asset if exist waitList
	time.AfterFunc(15*time.Second, m.triggerPuller)

	for {
		select {
		case <-m.pullCh:
			m.pullAssets()
		case <-m.ctx.Done():
			return
		}
	}
}

// The assetView needs to be closed before app exiting
func (m *Manager) Stop() error {
	log.Infof("Asset manager stop")
	return m.CloseAssetView()
}

// pullAssets pulls all assets that are waiting to be pulled
func (m *Manager) pullAssets() {
	for len(m.waitList) > 0 {
		m.doPullAsset()
	}
}

// doPullAsset pulls a single asset from the waitList
func (m *Manager) doPullAsset() {
	aw := m.headFromWaitList()
	if aw == nil {
		return
	}
	defer m.removeAssetFromWaitList(aw.Root)

	opts := &pullerOptions{
		root:        aw.Root,
		dss:         aw.Dss,
		storage:     m.Storage,
		ipfsAPIURL:  m.ipfsAPIURL,
		config:      m.pullerConfig,
		rateLimiter: m.rateLimiter.BandwidthDownLimiter,
		httpClient:  client.NewHTTP3Client(),
		workloadID:  aw.workloadID,
	}

	assetPuller, err := m.restoreAssetPullerOrNew(opts)
	if err != nil {
		log.Errorf("restore asset puller error:%s", err)
		return
	}

	aw.puller = assetPuller
	if err = assetPuller.pullAsset(); err != nil {
		log.Errorf("pull asset error: %s", err)
	}

	m.onPullAssetFinish(assetPuller, aw.isSyncData)
}

// headFromWaitList returns the first assetWaiter in waitList, which is the oldest asset waiting to be downloaded

func (m *Manager) headFromWaitList() *assetWaiter {
	m.waitListLock.Lock()
	defer m.waitListLock.Unlock()

	if len(m.waitList) == 0 {
		return nil
	}
	return m.waitList[0]
}

// removeAssetFromWaitList removes an assetWaiter from waitList by the root CID
// and returns the removed assetWaiter if it exists, otherwise returns nil
func (m *Manager) removeAssetFromWaitList(root cid.Cid) *assetWaiter {
	m.waitListLock.Lock()
	defer m.waitListLock.Unlock()

	if len(m.waitList) == 0 {
		return nil
	}

	for i, cw := range m.waitList {
		if cw.Root.Hash().String() == root.Hash().String() {
			m.waitList = append(m.waitList[:i], m.waitList[i+1:]...)

			if err := m.saveWaitList(); err != nil {
				log.Errorf("save wait list error: %s", err.Error())
			}
			return cw
		}
	}

	return nil
}

// addToWaitList adds an assetWaiter to waitList if the asset with the root CID is not already waiting to be downloaded
func (m *Manager) addToWaitList(aw *assetWaiter) {
	m.waitListLock.Lock()
	defer m.waitListLock.Unlock()

	for _, waiter := range m.waitList {
		if waiter.Root.Hash().String() == aw.Root.Hash().String() {
			return
		}
	}

	m.waitList = append(m.waitList, aw)

	if err := m.saveWaitList(); err != nil {
		log.Errorf("save wait list error: %s", err.Error())
	}

	m.triggerPuller()
}

// savePuller saves the assetPuller to the data store if it exists and has not completed downloading
func (m *Manager) savePuller(puller *assetPuller) error {
	if puller == nil || puller.isPulledComplete() {
		return nil
	}

	buf, err := puller.encode()
	if err != nil {
		return err
	}
	return m.StorePuller(puller.root, buf)
}

// onPullAssetFinish is called when an assetPuller finishes downloading an asset
func (m *Manager) onPullAssetFinish(puller *assetPuller, isSyncData bool) {
	log.Debugf("onPullAssetFinish, asset %s totalSize %d doneSize %d", puller.root.String(), puller.totalSize, puller.doneSize)

	if puller.isPulledComplete() {
		if len(puller.blocksPulledSuccessList) > 0 {
			if err := m.StoreBlocksToCar(context.Background(), puller.root); err != nil {
				log.Errorf("store asset error: %s", err.Error())
			}
		}

		if isSyncData {
			if err := m.AddAssetToView(context.Background(), puller.root); err != nil {
				log.Errorf("sync data add asset to view %s", err.Error())
			}
		}

	} else {
		log.Infof("pull asset failed, remove %s", puller.root.String())
		if err := m.DeleteAsset(puller.root); err != nil {
			log.Errorf("DeleteAsset failed %s", err.Error())
		}
	}

	if err := m.DeletePuller(puller.root); err != nil && !os.IsNotExist(err) {
		log.Errorf("remove asset puller error:%s", err.Error())
	}

	if err := m.submitPullerWorkloads(puller); err != nil {
		log.Errorf("submitPullerWorkloadReport error %s", err.Error())
	}

	// speed := float64(puller.totalSize) / float64(time.Since(puller.startTime)) * float64(time.Second)
	// if speed > 0 {
	// 	log.Debugf("UpdateBandwidths, bandwidthDown %d", int64(speed))
	// 	m.Scheduler.UpdateBandwidths(context.Background(), int64(speed), 0)
	// }

	if len(puller.errMsgs) > 0 {
		// TODO: if pull asset msg is out max, delete
		m.pullAssetErrMsgs.Store(puller.root.Hash().String(), puller.errMsgs)
	}
}

// saveWaitList encodes the waitList and stores it in the datastore.
func (m *Manager) saveWaitList() error {
	data, err := encode(&m.waitList)
	if err != nil {
		return err
	}

	return m.StoreWaitList(data)
}

// restoreWaitListFromStore retrieves the waitList from the datastore and decodes it.
func (m *Manager) restoreWaitListFromStore() {
	data, err := m.GetWaitList()
	if err != nil {
		if err != datastore.ErrNotFound {
			log.Errorf("restoreWaitListFromStore error:%s", err)
		}
		return
	}

	if len(data) == 0 {
		return
	}

	err = decode(data, &m.waitList)
	if err != nil {
		log.Errorf("restoreWaitListFromStore error:%s", err)
		return
	}

	log.Debugf("restoreWaitListFromStore:%#v", m.waitList)
}

// waitListLen returns the number of items in the waitList.
func (m *Manager) waitListLen() int {
	return len(m.waitList)
}

// Puller returns the asset puller associated with the first waiting item in the waitList.
func (m *Manager) puller() *assetPuller {
	for _, cw := range m.waitList {
		if cw.puller != nil {
			return cw.puller
		}
	}
	return nil
}

// DeleteAsset removes an asset from the datastore and the waitList.
func (m *Manager) DeleteAsset(root cid.Cid) error {
	// remove lru puller
	m.lru.remove(root)

	if _, err := m.deleteAssetFromWaitList(root); err != nil {
		return err
	}

	if ok, err := m.PullerExists(root); err == nil && ok {
		if err := m.DeletePuller(root); err != nil {
			log.Errorf("DeletePuller error %s", err.Error())
		}
	}

	if _, ok := m.uploadingAssets.Load(root.Hash().String()); ok {
		m.uploadingAssets.Delete(root.Hash().String())
		// TODO remove user asset
	}

	if err := m.Storage.DeleteAsset(root); err != nil {
		if e, ok := err.(*os.PathError); !ok {
			return err
		} else if e.Err != syscall.ENOENT {
			return err
		}
	}

	return m.RemoveAssetFromView(context.Background(), root)
}

// restoreAssetPullerOrNew retrieves the asset puller associated with the given root CID, or creates a new one.
func (m *Manager) restoreAssetPullerOrNew(opts *pullerOptions) (*assetPuller, error) {
	cc, err := newAssetPuller(opts)
	if err != nil {
		return nil, err
	}

	data, err := m.GetPuller(opts.root)
	if err != nil && !os.IsNotExist(err) {
		log.Errorf("get asset puller error %s", err.Error())
		return nil, err
	}

	if len(data) > 0 {
		err = cc.decode(data)
		if err != nil {
			return nil, err
		}

		// cover new download sources
		if opts.dss != nil {
			cc.dss = opts.dss
		}

		cc.workloadID = opts.workloadID
	}
	return cc, nil
}

// deleteAssetFromWaitList removes an asset from the waitList.
// return true if exist in waitList
func (m *Manager) deleteAssetFromWaitList(root cid.Cid) (bool, error) {
	if c := m.removeAssetFromWaitList(root); c != nil {
		if c.puller != nil {
			err := c.puller.cancelPulling()
			if err != nil {
				return false, err
			}
		}

		return true, nil
	}
	return false, nil
}

func (m *Manager) assetStatus(root cid.Cid) (types.ReplicaStatus, error) {
	// cachedStatus returns the asset status of a given root CID
	if ok, err := m.AssetExists(root); err == nil && ok {
		return types.ReplicaStatusSucceeded, nil
	} else if err != nil {
		log.Warnf("check asset exists error %s", err.Error())
	}

	for _, aw := range m.waitList {
		if aw.Root.Hash().String() == root.Hash().String() {
			if aw.puller != nil {
				return types.ReplicaStatusPulling, nil
			}
			return types.ReplicaStatusWaiting, nil
		}
	}

	if v, ok := m.uploadingAssets.Load(root.Hash().String()); ok {
		asset := v.(*types.UploadingAsset)
		if asset.TokenExpiration.Before(time.Now()) {
			return types.ReplicaStatusFailed, nil
		}

		if asset.Progress.DoneSize == 0 {
			return types.ReplicaStatusWaiting, nil
		}
		return types.ReplicaStatusPulling, nil
	}

	return types.ReplicaStatusFailed, nil
}

func (m *Manager) progressForPulling(root cid.Cid) (*types.AssetPullProgress, error) {
	if m.puller().root.Hash().String() == root.Hash().String() {
		return m.puller().getAssetProgress(), nil
	}

	if v, ok := m.uploadingAssets.Load(root.Hash().String()); ok {
		asset := v.(*types.UploadingAsset)
		return &types.AssetPullProgress{
			CID:      root.String(),
			Status:   types.ReplicaStatusPulling,
			Size:     asset.Progress.TotalSize,
			DoneSize: asset.Progress.DoneSize,
			ClientID: asset.UserID,
		}, nil
	}

	return nil, fmt.Errorf("asset %s not at pulling status", root.String())
}

// progressForAssetPulledFailed returns the progress of a failed asset pull operation
func (m *Manager) progressForAssetPulledFailed(root cid.Cid) (*types.AssetPullProgress, error) {
	progress := &types.AssetPullProgress{
		CID:    root.String(),
		Status: types.ReplicaStatusFailed,
	}

	if v, ok := m.pullAssetErrMsgs.LoadAndDelete(root.Hash().String()); ok {
		msg := v.([]*fetcher.ErrMsg)
		buf, err := json.Marshal(msg)
		if err != nil {
			log.Errorf("marshal fetcher.ErrMsg %s", err.Error())
		} else {
			progress.Msg = string(buf)
		}
	}

	data, err := m.GetPuller(root)
	if os.IsNotExist(err) {
		return progress, nil
	}

	if err != nil {
		return nil, err
	}

	cc := &assetPuller{}
	err = cc.decode(data)
	if err != nil {
		return nil, err
	}

	progress.BlocksCount = len(cc.blocksPulledSuccessList) + len(cc.blocksWaitList)
	progress.DoneBlocksCount = len(cc.blocksPulledSuccessList)
	progress.Size = int64(cc.totalSize)
	progress.DoneSize = int64(cc.doneSize)

	return progress, nil
}

// GetBlock returns the block with the given CID from the LRU cache
func (m *Manager) GetBlock(ctx context.Context, root, block cid.Cid) (blocks.Block, error) {
	return m.lru.getBlock(ctx, root, block)
}

func (m *Manager) GetBlockCount(ctx context.Context, root cid.Cid) (int, error) {
	idx, err := m.lru.assetIndex(root)
	if err != nil {
		return 0, err
	}

	multiIndex, ok := idx.(*index.MultiIndexSorted)
	if !ok {
		return 0, xerrors.Errorf("idx is not titan MultiIndexSorted")
	}
	return int(multiIndex.TotalRecordCount()), nil
}

// HasBlock checks if a block with the given CID exists in the LRU cache
func (m *Manager) HasBlock(ctx context.Context, root, block cid.Cid) (bool, error) {
	return m.lru.hasBlock(ctx, root, block)
}

// GetBlocksOfAsset returns a random selection of blocks for the given root CID
// return map, key is random number, value is cid string
func (m *Manager) GetBlocksOfAsset(root cid.Cid, randomSeed int64, randomCount int) ([]string, error) {
	log.Debugf("GetBlocksOfAsset root %s, randomSeed %d, randomCount %d", root.String(), randomSeed, randomCount)

	random, err := randomBlockFromAsset(root, randomSeed, m.lru)
	if err != nil {
		return nil, err
	}

	rets := make([]string, 0, randomCount)
	count := 0
	for {
		// TODO get block for check empty block data
		blk, err := random.GetBlock(context.Background())
		if err != nil {
			return nil, err
		}

		if len(blk.RawData()) == 0 {
			continue
		}

		rets = append(rets, blk.Cid().String())

		count++
		if count >= randomCount {
			break
		}
	}

	return rets, nil
}

// AddLostAsset adds a lost asset to the Manager's waitList if it is not already present in the storage
func (m *Manager) AddLostAsset(root cid.Cid) error {
	if has, err := m.AssetExists(root); err != nil {
		return err
	} else if has {
		log.Debugf("add lost asset, %s already exist", root.String())
		return m.AddAssetToView(context.TODO(), root)
	}

	downloadInfos, err := m.GetAssetSourceDownloadInfo(context.Background(), root.String())
	if err != nil {
		return xerrors.Errorf("get candidate download infos: %w", err.Error())
	}
	aws := types.AWSDownloadSources{Bucket: downloadInfos.AWSBucket, Key: downloadInfos.AWSKey}
	dss := types.DownloadSources{Nodes: downloadInfos.SourceList, AWS: &aws}
	aw := assetWaiter{Root: root, Dss: &dss, isSyncData: true, workloadID: downloadInfos.WorkloadID}
	m.addToWaitList(&aw)

	return nil
}

// GetAssetsOfBucket retrieves the list of assets in a given bucket ID from the storage
func (m *Manager) GetAssetsOfBucket(ctx context.Context, bucketID uint32, isRemote bool) ([]cid.Cid, error) {
	if !isRemote {
		return m.Storage.GetAssetsInBucket(ctx, bucketID)
	}

	multiHashes, err := m.GetAssetListForBucket(ctx, bucketID)
	if err != nil {
		return nil, err
	}

	cidList := make([]cid.Cid, 0, len(multiHashes))
	for _, multiHash := range multiHashes {
		hash, err := multihash.FromHexString(multiHash)
		if err != nil {
			log.Errorf("new multi hash from string error %s", err.Error())
			continue
		}
		cidList = append(cidList, cid.NewCidV0(hash))
	}
	return cidList, nil
}

// GetAssetForValidation returns a new instance of asset based on a given random seed
func (m *Manager) GetAssetForValidation(ctx context.Context, randomSeed int64) (validate.Asset, error) {
	return newRandomCheck(randomSeed, m.Storage, m.lru)
}

func (m *Manager) ScanBlocks(ctx context.Context, root cid.Cid) error {
	reader, err := m.GetAsset(root)
	if err != nil {
		return err
	}

	f, ok := reader.(*os.File)
	if !ok {
		return xerrors.Errorf("can not convert asset %s reader to file", root.String())
	}

	bs, err := blockstore.NewReadOnly(f, nil, carv2.ZeroLengthSectionAsEOF(true))
	if err != nil {
		return err
	}

	block, err := bs.Get(ctx, root)
	if err != nil {
		return xerrors.Errorf("get block %s error %w", root.String(), err)
	}

	node, err := ipld.DecodeNode(context.Background(), block)
	if err != nil {
		log.Errorf("decode block error:%s", err.Error())
		return err
	}

	if len(node.Links()) > 0 {
		return m.getNodes(ctx, bs, node.Links())
	}

	return nil
}

func (m *Manager) getNodes(ctx context.Context, bs *blockstore.ReadOnly, links []*format.Link) error {
	for _, link := range links {
		block, err := bs.Get(context.Background(), link.Cid)
		if err != nil {
			return xerrors.Errorf("get block %s error %w", link.Cid.String(), err)
		}

		node, err := ipld.DecodeNode(context.Background(), block)
		if err != nil {
			log.Errorf("decode block error:%s", err.Error())
			return err
		}

		if len(node.Links()) > 0 {
			err = m.getNodes(ctx, bs, node.Links())
			if err != nil {
				return err
			}
		}

	}

	return nil
}

func (m *Manager) submitPullerWorkloads(puller *assetPuller) error {
	if len(puller.workloads) == 0 {
		return nil
	}

	workloads := make([]types.Workload, 0, len(puller.workloads))
	for _, w := range puller.workloads {
		workloads = append(workloads, *w)
	}

	req := types.WorkloadRecordReq{AssetCID: puller.root.String(), Workloads: workloads, WorkloadID: puller.workloadID}
	log.Debugf("WorkloadRecordReq ", req)

	return m.SubmitWorkloadReport(context.Background(), &req)
}

func (m *Manager) SaveUserAsset(ctx context.Context, userID string, root cid.Cid, assetSize int64, r io.Reader) error {
	if err := m.Storage.StoreUserAsset(ctx, userID, root, assetSize, r); err != nil {
		m.uploadingAssets.Delete(root.Hash().String())
		return err
	}
	return nil
}

func (m *Manager) SetAssetUploadProgress(ctx context.Context, root cid.Cid, progress *types.UploadProgress) error {
	log.Debugf("SetAssetUploadProgress %s %d/%d", root.String(), progress.DoneSize, progress.TotalSize)

	if progress.DoneSize == progress.TotalSize {
		m.uploadingAssets.Delete(root.Hash().String())
		return nil
	}

	v, ok := m.uploadingAssets.Load(root.Hash().String())
	if !ok {
		return fmt.Errorf("asset %s not in uploading status", root.String())
	}

	asset := v.(*types.UploadingAsset)
	asset.Progress = progress

	m.uploadingAssets.Store(root.Hash().String(), asset)

	return nil
}

func (m *Manager) GetUploadingAsset(ctx context.Context, root cid.Cid) (*types.UploadingAsset, error) {
	v, ok := m.uploadingAssets.Load(root.Hash().String())
	if !ok {
		return nil, fmt.Errorf("asset %s not in update status", root.String())
	}

	return v.(*types.UploadingAsset), nil
}

func (m *Manager) regularSyncDataWithAssetView() {
	defer log.Debugf("regularSyncDataWithAssetView finish")
	if err := m.syncDataWithAssetView(); err != nil {
		log.Errorln(err)
	}

	ticker := time.NewTicker(2 * time.Hour)
	for {
		select {
		case <-ticker.C:
			if err := m.syncDataWithAssetView(); err != nil {
				log.Errorln(err)
			}
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Manager) syncDataWithAssetView() error {
	now := time.Now()

	bucketHashMap, err := m.GetBucketHashes(context.Background())
	if err != nil {
		return fmt.Errorf("syncDataWithAssetView, GetBucketHashes error %s", err.Error())
	}

	assetCIDsInView := make([]cid.Cid, 0)
	for bucket := range bucketHashMap {
		cids, err := m.Storage.GetAssetsInBucket(context.Background(), bucket)
		if err != nil {
			return fmt.Errorf("syncDataWithAssetView, GetAssetsInBucket erorr %s", err.Error())
		}
		assetCIDsInView = append(assetCIDsInView, cids...)
	}

	// Only check resources that have been successfully downloaded for more than 30 minutes
	allAssetHashes, err := m.GetAssetHashesForSyncData(context.Background())
	if err != nil {
		return fmt.Errorf("syncDataWithAssetView, GetAllAssetHashes erorr %s", err.Error())
	}

	assetsInView := make(map[string]cid.Cid)
	assetsInStorage := make(map[string]cid.Cid)

	for _, cid := range assetCIDsInView {
		assetsInView[cid.Hash().String()] = cid
	}

	for _, hash := range allAssetHashes {
		multihash, err := multihash.FromHexString(hash)
		if err != nil {
			return fmt.Errorf("syncDataWithAssetView, GetAllAssetHashes erorr %s", err.Error())
		}
		cid := cid.NewCidV1(cid.Raw, multihash)
		assetsInStorage[hash] = cid
	}

	for _, cid := range assetCIDsInView {
		if _, ok := assetsInStorage[cid.Hash().String()]; ok {
			delete(assetsInView, cid.Hash().String())
			delete(assetsInStorage, cid.Hash().String())
		}
	}

	// add lost assets
	for _, cid := range assetsInView {
		m.AddLostAsset(cid)
		log.Debugf("add lost asset %s", cid.Hash())
	}

	// remove extra assets
	for _, cid := range assetsInStorage {
		m.DeleteAsset(cid)
		log.Debugf("delete extra asset %s", cid.Hash())
	}

	log.Debugf("syncDataWithAssetView cost %ds", time.Since(now)/time.Second)

	return nil
}
