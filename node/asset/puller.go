package asset

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/asset/fetcher"
	"github.com/Filecoin-Titan/titan/node/asset/storage"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/ipld"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"golang.org/x/time/rate"
)

type pulledResult struct {
	nextLayerCIDs []string
	linksSize     uint64
	doneSize      uint64
}

// assetPuller represents a struct that is responsible for downloading and managing the progress of an asset pull operation
type assetPuller struct {
	root            cid.Cid
	storage         storage.Storage
	bFetcher        fetcher.BlockFetcher
	downloadSources []*types.CandidateDownloadInfo

	blocksWaitList          []string
	blocksPulledSuccessList []string
	// nextLayerCIDs just for restore pull task
	nextLayerCIDs []string
	totalSize     uint64
	doneSize      uint64

	cancel context.CancelFunc
	// pull block async
	config *config.Puller

	workloads map[string]*types.Workload
	startTime time.Time

	errMsgs     []*fetcher.ErrMsg
	rateLimiter *rate.Limiter
}

type pullerOptions struct {
	root        cid.Cid
	dss         []*types.CandidateDownloadInfo
	storage     storage.Storage
	ipfsAPIURL  string
	config      *config.Puller
	httpClient  *http.Client
	rateLimiter *rate.Limiter
}

// newAssetPuller creates a new asset puller with the given options
func newAssetPuller(opts *pullerOptions) (*assetPuller, error) {
	if types.RunningNodeType == types.NodeEdge && len(opts.dss) == 0 {
		return nil, fmt.Errorf("newAssetPuller error, puller options dss cannot empty")
	}

	var blockFetcher fetcher.BlockFetcher
	if len(opts.dss) != 0 {
		blockFetcher = fetcher.NewCandidateFetcher(opts.httpClient)
	} else {
		blockFetcher = fetcher.NewIPFSClient(opts.ipfsAPIURL)
	}
	return &assetPuller{
		root:            opts.root,
		storage:         opts.storage,
		downloadSources: opts.dss,
		bFetcher:        blockFetcher,
		config:          opts.config,
		rateLimiter:     opts.rateLimiter,
		startTime:       time.Now(),
		errMsgs:         make([]*fetcher.ErrMsg, 0),
		workloads:       make(map[string]*types.Workload),
	}, nil
}

// getBlocksFromWaitList get n block from front of wait list
func (ap *assetPuller) getBlocksFromWaitList(n int) []string {
	if len(ap.blocksWaitList) < n {
		n = len(ap.blocksWaitList)
	}

	return ap.blocksWaitList[:n]
}

// removeBlocksFromWaitList remove n block from front of wait list
func (ap *assetPuller) removeBlocksFromWaitList(n int) {
	if len(ap.blocksWaitList) < n {
		n = len(ap.blocksWaitList)
	}
	ap.blocksWaitList = ap.blocksWaitList[n:]
}

// pullAsset pulls the asset by downloading its blocks
func (ap *assetPuller) pullAsset() error {
	if isContainAWSDownloadSource(ap.downloadSources) {
		err := ap.pullAssetFromAWS()
		if err == nil {
			return nil
		}
		log.Errorf("pull asset from aws %s", err.Error())
	}

	nextLayerCIDs := ap.blocksWaitList
	if len(nextLayerCIDs) == 0 {
		nextLayerCIDs = append(nextLayerCIDs, ap.root.String())
	}

	for len(nextLayerCIDs) > 0 {
		ret, err := ap.pullBlocksWithBreadthFirst(nextLayerCIDs)
		if err != nil {
			return err
		}

		if ap.totalSize == 0 {
			ap.totalSize = ret.linksSize + ret.doneSize
			// check usabe disk space
			if ap.totalSize >= uint64(ap.usableDiskSpace()) {
				return fmt.Errorf("not enough disk space, need %d, usable %d, pull asset %s", ap.totalSize, ap.usableDiskSpace(), ap.root.String())
			}
		}

		nextLayerCIDs = ret.nextLayerCIDs
	}
	return nil
}

// pullBlocksWithBreadthFirst pulls blocks with breadth first algorithm.
func (ap *assetPuller) pullBlocksWithBreadthFirst(layerCIDs []string) (result *pulledResult, err error) {
	ap.blocksWaitList = layerCIDs
	result = &pulledResult{nextLayerCIDs: ap.nextLayerCIDs}
	for len(ap.blocksWaitList) > 0 {
		doLen := len(ap.blocksWaitList)
		if doLen > ap.config.PullBlockParallel {
			doLen = ap.config.PullBlockParallel
		}

		blocks := ap.getBlocksFromWaitList(doLen)
		ret, err := ap.pullBlocks(blocks)
		if err != nil {
			return nil, err
		}

		result.linksSize += ret.linksSize
		result.doneSize += ret.doneSize
		result.nextLayerCIDs = append(result.nextLayerCIDs, ret.nextLayerCIDs...)

		ap.doneSize += ret.doneSize
		ap.blocksPulledSuccessList = append(ap.blocksPulledSuccessList, blocks...)
		ap.nextLayerCIDs = append(ap.nextLayerCIDs, ret.nextLayerCIDs...)
		ap.removeBlocksFromWaitList(doLen)

	}
	ap.nextLayerCIDs = make([]string, 0)

	return result, nil
}

// pullBlocks fetches blocks for given cids, stores them in the storage
func (ap *assetPuller) pullBlocks(cids []string) (*pulledResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ap.config.PullBlockTimeout)*time.Second)
	defer cancel()

	ap.cancel = cancel

	errMsgs, workloads, blks, err := ap.bFetcher.FetchBlocks(ctx, cids, ap.downloadSources)
	if err != nil {
		log.Errorf("fetch blocks err: %s", err.Error())
		return nil, err
	}

	if len(errMsgs) > 0 {
		ap.errMsgs = append(ap.errMsgs, errMsgs...)
	}

	ap.mergeWorkloads(workloads)
	// retry
	retryCount := 0
	cidMap := ap.toMap(cids)
	for len(blks) < len(cids) && retryCount < ap.config.PullBlockRetry {
		unPullBlocks := ap.filterUnPulledBlocks(blks, cidMap)
		bs, err := ap.retryFetchBlocks(unPullBlocks)
		if err != nil {
			return nil, err
		}
		retryCount++
		blks = append(blks, bs...)
	}

	if len(blks) != len(cids) {
		return nil, fmt.Errorf("pull blocks failed, already pulled blocks len:%d, need blocks len:%d", len(blks), len(cids))
	}

	linksSize := uint64(0)
	doneSize := uint64(0)
	linksMap := make(map[string][]string)
	for _, b := range blks {
		// get block links
		node, err := ipld.DecodeNode(context.Background(), b)
		if err != nil {
			log.Errorf("decode block error:%s", err.Error())
			return nil, err
		}

		links := node.Links()
		subCIDs := make([]string, 0, len(links))
		for _, link := range links {
			subCIDs = append(subCIDs, link.Cid.String())
			linksSize += link.Size
		}

		doneSize += uint64(len(b.RawData()))
		linksMap[b.Cid().String()] = subCIDs
	}

	nextLayerCIDs := make([]string, 0)
	for _, cid := range cids {
		links := linksMap[cid]
		nextLayerCIDs = append(nextLayerCIDs, links...)
	}

	err = ap.storage.StoreBlocks(context.Background(), ap.root, blks)
	if err != nil {
		return nil, err
	}

	ret := &pulledResult{nextLayerCIDs: nextLayerCIDs, linksSize: linksSize, doneSize: doneSize}

	return ret, nil
}

func (ap *assetPuller) toMap(cids []string) map[string]struct{} {
	ret := make(map[string]struct{})
	for _, cid := range cids {
		ret[cid] = struct{}{}
	}
	return ret
}

func (ap *assetPuller) filterUnPulledBlocks(blks []blocks.Block, cidMap map[string]struct{}) []string {
	for _, blk := range blks {
		delete(cidMap, blk.Cid().String())
	}

	cids := make([]string, 0, len(cidMap))
	for cid := range cidMap {
		cids = append(cids, cid)
	}
	return cids
}

func (ap *assetPuller) retryFetchBlocks(cids []string) ([]blocks.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ap.config.PullBlockTimeout)*time.Second)
	defer cancel()

	ap.cancel = cancel

	errMsgs, workloads, blks, err := ap.bFetcher.FetchBlocks(ctx, cids, ap.downloadSources)
	if err != nil {
		log.Errorf("retry fetch blocks err %s", err.Error())
		return nil, err
	}

	if len(errMsgs) > 0 {
		ap.errMsgs = append(ap.errMsgs, errMsgs...)
	}

	ap.mergeWorkloads(workloads)
	return blks, nil
}

// isPulledComplete checks if asset pulling is completed or not
func (ap *assetPuller) isPulledComplete() bool {
	if ap.totalSize == 0 {
		return false
	}

	// TODO done size maybe max than total size
	if ap.doneSize >= ap.totalSize {
		return true
	}

	return false
}

// cancelPulling cancels the asset pulling
func (ap *assetPuller) cancelPulling() error {
	if ap.cancel != nil {
		ap.cancel()
	}
	return nil
}

// encode encodes the asset puller to bytes
func (ap *assetPuller) encode() ([]byte, error) {
	eac := &AssetPullerEncoder{
		Root:                    ap.root.String(),
		BlocksWaitList:          ap.blocksWaitList,
		BlocksPulledSuccessList: ap.blocksPulledSuccessList,
		NextLayerCIDs:           ap.nextLayerCIDs,
		DownloadSources:         ap.downloadSources,
		TotalSize:               ap.totalSize,
		DoneSize:                ap.doneSize,
	}

	return encode(eac)
}

// decode decodes the bytes into an asset puller
func (ap *assetPuller) decode(data []byte) error {
	eac := &AssetPullerEncoder{}
	err := decode(data, eac)
	if err != nil {
		return err
	}

	c, err := cid.Decode(eac.Root)
	if err != nil {
		return err
	}

	ap.root = c
	ap.blocksWaitList = eac.BlocksWaitList
	ap.blocksPulledSuccessList = eac.BlocksPulledSuccessList
	ap.nextLayerCIDs = eac.NextLayerCIDs
	ap.downloadSources = eac.DownloadSources
	ap.totalSize = eac.TotalSize
	ap.doneSize = eac.DoneSize

	return nil
}

// getAssetProgress returns the current progress of the asset
func (ap *assetPuller) getAssetProgress() *types.AssetPullProgress {
	return &types.AssetPullProgress{
		CID:             ap.root.String(),
		Status:          types.ReplicaStatusPulling,
		BlocksCount:     len(ap.blocksPulledSuccessList) + len(ap.blocksWaitList),
		DoneBlocksCount: len(ap.blocksPulledSuccessList),
		Size:            int64(ap.totalSize),
		DoneSize:        int64(ap.doneSize),
	}
}

func (ap *assetPuller) mergeWorkloads(workloads []*types.Workload) {
	for _, w := range workloads {
		worload, ok := ap.workloads[w.SourceID]
		if !ok {
			worload = &types.Workload{SourceID: w.SourceID}
		}

		worload.DownloadSize += w.DownloadSize
		worload.CostTime += w.CostTime
		ap.workloads[w.SourceID] = worload

	}
}

func (ap *assetPuller) usableDiskSpace() int64 {
	totalSpace, usage := ap.storage.GetDiskUsageStat()
	usable := totalSpace - (totalSpace * (usage / float64(100)))
	return int64(usable)
}

func (ap *assetPuller) pullAssetFromAWS() error {
	bucket, key := getAWSBucketAndKey(ap.downloadSources)
	if len(bucket) == 0 && len(key) == 0 {
		return fmt.Errorf("bucket and key is empty, can not pull asset from aws")
	}

	log.Debugf("pull asset %s from aws bucket=%s, ket=%s", ap.root.String(), bucket, key)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ap.cancel = cancel
	ap.totalSize = 0
	ap.doneSize = 0

	startTime := time.Now()

	awsClient := &awsClient{storage: ap.storage, rateLimiter: ap.rateLimiter}
	cid, size, err := awsClient.pullAssetFromAWS(ctx, bucket, key)
	if err != nil {
		ap.errMsgs = append(ap.errMsgs, &fetcher.ErrMsg{Cid: ap.root.String(), Source: bucket, Msg: err.Error()})
		return err
	}

	if cid.Hash().String() != ap.root.Hash().String() {
		if err = ap.storage.DeleteAsset(cid); err != nil {
			log.Errorln("download an unwanted asset from aws, delete it error ", err.Error())
		}

		err = fmt.Errorf("download asset from aws bucket=%s key=%s not match cid %s", bucket, key, cid.String())
		ap.errMsgs = append(ap.errMsgs, &fetcher.ErrMsg{Cid: ap.root.String(), Source: bucket, Msg: err.Error()})

		return err
	}

	ap.totalSize = uint64(size)
	ap.doneSize = uint64(size)

	costTime := time.Since(startTime) / time.Millisecond
	ap.workloads["aws"] = &types.Workload{SourceID: "aws", DownloadSize: int64(size), CostTime: int64(costTime)}

	return nil
}

func getAWSBucketAndKey(downloadInfos []*types.CandidateDownloadInfo) (string, string) {
	for _, downloadSource := range downloadInfos {
		if len(downloadSource.AWSBucket) > 0 {
			return downloadSource.AWSBucket, downloadSource.AWSKey
		}
	}
	return "", ""
}
