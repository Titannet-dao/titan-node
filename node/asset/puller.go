package asset

import (
	"context"
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/asset/fetcher"
	"github.com/Filecoin-Titan/titan/node/asset/storage"
	"github.com/ipfs/go-cid"
	legacy "github.com/ipfs/go-ipld-legacy"
	"github.com/ipfs/go-libipfs/blocks"
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

	isFinish bool
	cancel   context.CancelFunc
	// pull block async
	parallel int
	timeout  int
	retry    int
}

type pullerOptions struct {
	root     cid.Cid
	dss      []*types.CandidateDownloadInfo
	storage  storage.Storage
	bFetcher fetcher.BlockFetcher
	parallel int
	// pull block time out
	timeout int
	// retry times of pull block on failed
	retry int
}

// newAssetPuller creates a new asset puller with the given options
func newAssetPuller(opts *pullerOptions) *assetPuller {
	return &assetPuller{
		root:            opts.root,
		storage:         opts.storage,
		downloadSources: opts.dss,
		bFetcher:        opts.bFetcher,
		parallel:        opts.parallel,
		timeout:         opts.timeout,
		retry:           opts.retry,
	}
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
	defer func() {
		ap.isFinish = true
	}()

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
		if doLen > ap.parallel {
			doLen = ap.parallel
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ap.timeout)*time.Second)
	defer cancel()

	ap.cancel = cancel

	blks, err := ap.bFetcher.FetchBlocks(ctx, cids, ap.downloadSources)
	if err != nil {
		log.Errorf("fetch blocks err: %s", err.Error())
		return nil, err
	}

	// retry
	retryCount := 0
	cidMap := ap.toMap(cids)
	for len(blks) < len(cids) && retryCount < ap.retry {
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
		node, err := legacy.DecodeNode(context.Background(), b)
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
		delete(cidMap, blk.String())
	}

	cids := make([]string, 0, len(cidMap))
	for cid := range cidMap {
		cids = append(cids, cid)
	}
	return cids
}

func (ap *assetPuller) retryFetchBlocks(cids []string) ([]blocks.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ap.timeout)*time.Second)
	defer cancel()

	ap.cancel = cancel

	blks, err := ap.bFetcher.FetchBlocks(ctx, cids, ap.downloadSources)
	if err != nil {
		log.Errorf("retry fetch blocks err %s", err.Error())
		return nil, err
	}
	return blks, nil
}

// isPulledComplete checks if asset pulling is completed or not
func (ap *assetPuller) isPulledComplete() bool {
	if ap.totalSize == 0 {
		return false
	}

	if ap.doneSize != ap.totalSize {
		return false
	}

	return true
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
