package assets

import (
	"context"
	"crypto"
	"database/sql"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/filecoin-project/go-statemachine"
	"github.com/ipfs/go-datastore"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"

	"github.com/Filecoin-Titan/titan/node/cidutil"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("asset")

const (
	nodePullAssetTimeout         = 60 * time.Second      // Timeout for pulling assets (Unit:Second)
	assetExpirationCheckInterval = 60 * 30 * time.Second // Interval for checking expired assets (Unit:Second)
	seedReplicaCount             = 1                     // The number of pull replica in the first stage
	pullProgressInterval         = 20 * time.Second      // Interval to get asset pull progress from node (Unit:Second)

	maxConcurrentPulls = 10  // Maximum number of concurrent asset pulls
	maxAssetReplicas   = 100 // Maximum number of replicas per asset

	maxRetryCount    = 3    // TODO Select
	maxNodeDiskUsage = 95.0 // If the node disk size is greater than this value, pulling will not continue

	numAssetBuckets = 128 // Number of asset buckets in assets view
)

// Manager manages asset replicas
type Manager struct {
	nodeMgr            *node.Manager // node manager
	earliestExpiration time.Time     // Earliest expiration date for an asset
	stateMachineWait   sync.WaitGroup
	assetStateMachines *statemachine.StateGroup
	lock               sync.Mutex
	apTickers          map[string]*assetTicker       // timeout timer for asset pulling
	config             dtypes.GetSchedulerConfigFunc // scheduler config
	*db.SQLDB
}

type assetTicker struct {
	ticker *time.Ticker
	close  chan struct{}
}

func (t *assetTicker) run(job func() error) {
	for {
		select {
		case <-t.ticker.C:
			err := job()
			if err != nil {
				log.Error(err.Error())
				break
			}
			return
		case <-t.close:
			return
		}
	}
}

// NewManager returns a new AssetManager instance
func NewManager(nodeManager *node.Manager, ds datastore.Batching, configFunc dtypes.GetSchedulerConfigFunc, sdb *db.SQLDB) *Manager {
	m := &Manager{
		nodeMgr:            nodeManager,
		earliestExpiration: time.Now(),
		apTickers:          make(map[string]*assetTicker),
		config:             configFunc,
		SQLDB:              sdb,
	}

	m.stateMachineWait.Add(1)
	m.assetStateMachines = statemachine.New(ds, m, AssetPullingInfo{})

	return m
}

// Start initializes and starts the asset state machine and associated tickers
func (m *Manager) Start(ctx context.Context) {
	if err := m.restartStateMachines(ctx); err != nil {
		log.Errorf("restartStateMachines err: %s", err.Error())
	}
	go m.assetExpirationCheck(ctx)
	go m.assetPullProgressCheck(ctx)
}

// Terminate stops the asset state machine
func (m *Manager) Terminate(ctx context.Context) error {
	return m.assetStateMachines.Stop(ctx)
}

// assetExpirationCheck Periodically checks asset expiration
func (m *Manager) assetExpirationCheck(ctx context.Context) {
	ticker := time.NewTicker(assetExpirationCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.processExpiredAssets()
		case <-ctx.Done():
			return
		}
	}
}

// assetPullProgressCheck Periodically gets asset pull progress
func (m *Manager) assetPullProgressCheck(ctx context.Context) {
	ticker := time.NewTicker(pullProgressInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.retrieveNodePullProgresses()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) retrieveNodePullProgresses() {
	nodePulls := make(map[string][]string)

	// Process pulling assets
	for hash := range m.apTickers {
		cid, err := cidutil.HashToCID(hash)
		if err != nil {
			log.Errorf("retrieveNodePullProgresses %s HashString2CIDString err:%s", hash, err.Error())
			continue
		}

		nodes, err := m.LoadUnfinishedPullAssetNodes(hash)
		if err != nil {
			log.Errorf("retrieveNodePullProgresses %s LoadUnfinishedPullAssetNodes err:%s", hash, err.Error())
			continue
		}

		for _, nodeID := range nodes {
			list := nodePulls[nodeID]
			nodePulls[nodeID] = append(list, cid)
		}
	}

	getCP := func(nodeID string, cids []string) {
		// request node
		result, err := m.requestNodePullProgresses(nodeID, cids)
		if err != nil {
			log.Errorf("retrieveNodePullProgresses %s requestNodePullProgresses err:%s", nodeID, err.Error())
			return
		}

		// update asset info
		m.updateAssetPullResults(nodeID, result)
	}

	for nodeID, cids := range nodePulls {
		go getCP(nodeID, cids)
	}
}

func (m *Manager) requestNodePullProgresses(nodeID string, cids []string) (result *types.PullResult, err error) {
	node := m.nodeMgr.GetNode(nodeID)
	if node != nil {
		result, err = node.GetAssetProgresses(context.Background(), cids)
	} else {
		err = xerrors.Errorf("node %s not found", nodeID)
	}

	return
}

// CreateAssetPullTask creates a new asset pull task
func (m *Manager) CreateAssetPullTask(info *types.PullAssetReq) error {
	m.stateMachineWait.Wait()

	if len(m.apTickers) >= maxConcurrentPulls {
		return xerrors.Errorf("The asset in the pulling exceeds the limit %d, please wait", maxConcurrentPulls)
	}

	if info.Replicas > maxAssetReplicas {
		return xerrors.Errorf("The number of replicas %d exceeds the limit %d", info.Replicas, maxAssetReplicas)
	}

	log.Infof("asset event: %s, add asset replica: %d,expiration: %s", info.CID, info.Replicas, info.Expiration.String())

	assetRecord, err := m.LoadAssetRecord(info.Hash)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	if assetRecord == nil {
		// create asset task
		return m.assetStateMachines.Send(AssetHash(info.Hash), AssetStartPulls{
			ID:                info.CID,
			Hash:              AssetHash(info.Hash),
			Replicas:          info.Replicas,
			ServerID:          info.ServerID,
			CreatedAt:         time.Now().Unix(),
			Expiration:        info.Expiration.Unix(),
			CandidateReplicas: m.GetCandidateReplicaCount(),
		})
	}

	return m.replenishAssetReplicas(assetRecord, info)
}

// replenishAssetReplicas updates the existing asset replicas if needed
func (m *Manager) replenishAssetReplicas(assetRecord *types.AssetRecord, info *types.PullAssetReq) error {
	// Check if the asset is in servicing state
	if assetRecord.State != string(Servicing) {
		return xerrors.Errorf("asset state is %s", assetRecord.State)
	}

	// get the existing asset replicas
	replicaInfos, err := m.LoadAssetReplicas(assetRecord.Hash)
	if err != nil {
		return xerrors.Errorf("asset %s load replicas err: %s", assetRecord.CID, err.Error())
	}

	rInfo := ReplenishReplicas{
		ID:                info.CID,
		Hash:              AssetHash(info.Hash),
		Replicas:          info.Replicas,
		ServerID:          info.ServerID,
		CreatedAt:         assetRecord.CreateTime.Unix(),
		Expiration:        info.Expiration.Unix(),
		CandidateReplicas: m.GetCandidateReplicaCount(),
		Size:              assetRecord.TotalSize,
		Blocks:            assetRecord.TotalBlocks,
		State:             SeedSelect,
	}

	for _, r := range replicaInfos {
		if r.Status != types.ReplicaStatusSucceeded {
			continue
		}

		if r.IsCandidate {
			rInfo.CandidateReplicaSucceeds = append(rInfo.CandidateReplicaSucceeds, r.NodeID)
		} else {
			rInfo.EdgeReplicaSucceeds = append(rInfo.EdgeReplicaSucceeds, r.NodeID)
		}
	}

	// Check if there is a need to update replicas and send the update request if needed
	if len(rInfo.EdgeReplicaSucceeds) < int(info.Replicas) || len(rInfo.CandidateReplicaSucceeds) < m.GetCandidateReplicaCount()+seedReplicaCount {
		return m.assetStateMachines.Send(AssetHash(info.Hash), rInfo)
	}

	return xerrors.New("Asset do not need to be replenish")
}

// RestartPullAssets restarts asset pulls
func (m *Manager) RestartPullAssets(hashes []types.AssetHash) error {
	for _, hash := range hashes {
		err := m.assetStateMachines.Send(hash, PullAssetRestart{})
		if err != nil {
			log.Errorf("RestartPullAssets send err:%s", err.Error())
		}
	}

	return nil
}

// RemoveReplica remove a replica for node
func (m *Manager) RemoveReplica(cid, hash, nodeID string) error {
	err := m.DeleteAssetReplica(hash, nodeID)
	if err != nil {
		return err
	}

	go m.requestAssetDeletion(nodeID, cid)

	return nil
}

// RemoveAsset removes an asset
func (m *Manager) RemoveAsset(cid, hash string) error {
	cInfos, err := m.LoadAssetReplicas(hash)
	if err != nil {
		return xerrors.Errorf("RemoveAsset %s LoadAssetReplicas err:%s", cid, err.Error())
	}

	err = m.DeleteAssetRecord(hash)
	if err != nil {
		return xerrors.Errorf("RemoveAsset %s DeleteAssetRecord err: %s", hash, err.Error())
	}

	if exist, _ := m.assetStateMachines.Has(AssetHash(hash)); exist {
		// remove asset
		err = m.assetStateMachines.Send(AssetHash(hash), AssetRemove{})
		if err != nil {
			return xerrors.Errorf("send to state machine err: %s ", err.Error())
		}
	}

	log.Infof("asset event %s , remove asset", cid)

	for _, cInfo := range cInfos {
		// asset view
		err = m.removeAssetFromView(cInfo.NodeID, cid)
		if err != nil {
			log.Errorf("RemoveAsset %s removeAssetFromView err:%s", cInfo.NodeID, err.Error())
		}

		go m.requestAssetDeletion(cInfo.NodeID, cid)
	}

	return nil
}

// updateAssetPullResults updates asset pull results
func (m *Manager) updateAssetPullResults(nodeID string, result *types.PullResult) {
	isCandidate := false
	pullingCount := 0

	nodeInfo := m.nodeMgr.GetNode(nodeID)
	if nodeInfo != nil {
		isCandidate = nodeInfo.Type == types.NodeCandidate
		// update node info
		nodeInfo.DiskUsage = result.DiskUsage
		defer nodeInfo.SetCurPullingCount(pullingCount)
	}

	for _, progress := range result.Progresses {
		log.Debugf("updateAssetPullResults node_id: %s, status: %d, size: %d/%d, cid: %s ", nodeID, progress.Status, progress.DoneSize, progress.Size, progress.CID)

		hash, err := cidutil.CIDToHash(progress.CID)
		if err != nil {
			log.Errorf("%s cid to hash err:%s", progress.CID, err.Error())
			continue
		}

		exist, _ := m.assetStateMachines.Has(AssetHash(hash))
		if !exist {
			continue
		}

		{
			m.lock.Lock()
			tickerC, ok := m.apTickers[hash]
			if ok {
				tickerC.ticker.Reset(nodePullAssetTimeout)
			}
			m.lock.Unlock()
		}

		if progress.Status == types.ReplicaStatusWaiting {
			pullingCount++
			continue
		}

		// save replica info to db
		cInfo := &types.ReplicaInfo{
			Status:   progress.Status,
			DoneSize: progress.DoneSize,
			Hash:     hash,
			NodeID:   nodeID,
		}

		err = m.UpdateUnfinishedReplica(cInfo)
		if err != nil {
			log.Errorf("updateAssetPullResults %s UpdateUnfinishedReplica err:%s", nodeID, err.Error())
			continue
		}

		if progress.Status == types.ReplicaStatusPulling {
			pullingCount++

			err = m.assetStateMachines.Send(AssetHash(hash), InfoUpdate{
				Blocks: int64(progress.BlocksCount),
				Size:   progress.Size,
			})
			if err != nil {
				log.Errorf("updateAssetPullResults %s statemachine send err:%s", nodeID, err.Error())
			}

			continue
		}

		// asset view
		err = m.addAssetToView(nodeID, progress.CID)
		if err != nil {
			log.Errorf("updateAssetPullResults %s addAssetToView err:%s", nodeID, err.Error())
			continue
		}

		err = m.assetStateMachines.Send(AssetHash(hash), PulledResult{
			ResultInfo: &NodePulledResult{
				NodeID:      nodeID,
				Status:      int64(progress.Status),
				BlocksCount: int64(progress.BlocksCount),
				Size:        progress.Size,
				IsCandidate: isCandidate,
			},
		})
		if err != nil {
			log.Errorf("updateAssetPullResults %s statemachine send err:%s", nodeID, err.Error())
			continue
		}
	}
}

// addOrResetAssetTicker adds or resets the asset ticker with a given hash
func (m *Manager) addOrResetAssetTicker(hash string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	fn := func() error {
		// update replicas status
		err := m.UpdateUnfinishedReplicasStatus(hash, types.ReplicaStatusFailed)
		if err != nil {
			return xerrors.Errorf("addOrResetAssetTicker %s UpdateStatusOfUnfinishedReplicas err:%s", hash, err.Error())
		}

		err = m.assetStateMachines.Send(AssetHash(hash), PullFailed{error: xerrors.New("node pull asset response timeout")})
		if err != nil {
			return xerrors.Errorf("addOrResetAssetTicker %s send time out err:%s", hash, err.Error())
		}

		return nil
	}

	t, ok := m.apTickers[hash]
	if ok {
		t.ticker.Reset(nodePullAssetTimeout)
		return
	}

	m.apTickers[hash] = &assetTicker{
		ticker: time.NewTicker(nodePullAssetTimeout),
		close:  make(chan struct{}),
	}

	go m.apTickers[hash].run(fn)
}

// removeTickerForAsset removes the asset ticker for a given key
func (m *Manager) removeTickerForAsset(key string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	t, ok := m.apTickers[key]
	if !ok {
		return
	}

	t.ticker.Stop()
	close(t.close)
	delete(m.apTickers, key)
}

// UpdateAssetExpiration updates the asset expiration for a given CID
func (m *Manager) UpdateAssetExpiration(cid string, t time.Time) error {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return err
	}

	log.Infof("asset event %s, reset asset expiration:%s", cid, t.String())

	err = m.UpdateAssetRecordExpiry(hash, t)
	if err != nil {
		return err
	}

	m.updateEarliestExpiration(t)

	return nil
}

// processExpiredAssets checks for expired assets and removes them
func (m *Manager) processExpiredAssets() {
	if m.earliestExpiration.After(time.Now()) {
		return
	}

	records, err := m.LoadExpiredAssetRecords(m.nodeMgr.ServerID)
	if err != nil {
		log.Errorf("LoadExpiredAssetRecords err:%s", err.Error())
		return
	}

	for _, record := range records {
		// do remove
		err = m.RemoveAsset(record.CID, record.Hash)
		log.Infof("the asset cid(%s) has expired, being removed, err: %v", record.CID, err)
	}

	// reset expiration
	expiration, err := m.LoadMinExpiryOfAssetRecords(m.nodeMgr.ServerID)
	if err != nil {
		return
	}

	m.updateEarliestExpiration(expiration)
}

// updateEarliestExpiration updates the earliest expiration time
func (m *Manager) updateEarliestExpiration(t time.Time) {
	if m.earliestExpiration.After(t) {
		m.earliestExpiration = t
	}
}

// requestAssetDeletion notifies a node to delete an asset by its CID
func (m *Manager) requestAssetDeletion(nodeID, cid string) error {
	node := m.nodeMgr.GetNode(nodeID)
	if node != nil {
		return node.DeleteAsset(context.Background(), cid)
	}

	return xerrors.Errorf("node %s not found", nodeID)
}

// GetCandidateReplicaCount get the candidate replica count from the configuration
func (m *Manager) GetCandidateReplicaCount() int {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("get schedulerConfig err:%s", err.Error())
		return 0
	}

	return cfg.CandidateReplicas
}

// GetAssetRecordInfo get the asset record info for cid
func (m *Manager) GetAssetRecordInfo(cid string) (*types.AssetRecord, error) {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, err
	}

	dInfo, err := m.LoadAssetRecord(hash)
	if err != nil {
		return nil, err
	}

	dInfo.ReplicaInfos, err = m.LoadAssetReplicas(hash)
	if err != nil {
		log.Errorf("GetAssetRecordInfo hash:%s, LoadAssetReplicas err:%s", hash, err.Error())
	}

	return dInfo, err
}

// saveReplicaInformation stores replica information for nodes
func (m *Manager) saveReplicaInformation(nodes map[string]*node.Node, hash string, isCandidate bool) error {
	// save replica info
	replicaInfos := make([]*types.ReplicaInfo, 0)

	for _, node := range nodes {
		replicaInfos = append(replicaInfos, &types.ReplicaInfo{
			NodeID:      node.NodeID,
			Status:      types.ReplicaStatusWaiting,
			Hash:        hash,
			IsCandidate: isCandidate,
		})
	}

	return m.BatchSaveReplicas(replicaInfos)
}

// getDownloadSources gets download sources for a given CID
func (m *Manager) getDownloadSources(cid string, nodes []string) []*types.CandidateDownloadInfo {
	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sources := make([]*types.CandidateDownloadInfo, 0)
	for _, nodeID := range nodes {
		cNode := m.nodeMgr.GetCandidateNode(nodeID)
		if cNode == nil {
			continue
		}

		credentials, err := cNode.Credentials(cid, titanRsa, m.nodeMgr.PrivateKey)
		if err != nil {
			continue
		}
		source := &types.CandidateDownloadInfo{
			URL:         cNode.DownloadAddr(),
			Credentials: credentials,
		}

		sources = append(sources, source)
	}

	return sources
}

// chooseCandidateNodesForAssetReplica selects candidate nodes to pull asset replicas
func (m *Manager) chooseCandidateNodesForAssetReplica(count int, filterNodes []string) map[string]*node.Node {
	selectMap := make(map[string]*node.Node)
	if count <= 0 {
		return selectMap
	}

	if len(filterNodes) >= m.nodeMgr.Candidates {
		return selectMap
	}

	filterMap := make(map[string]struct{})
	for _, nodeID := range filterNodes {
		filterMap[nodeID] = struct{}{}
	}

	num := count * maxRetryCount

	for i := 0; i < num; i++ {
		node := m.nodeMgr.GetRandomCandidate()
		if node == nil {
			continue
		}
		nodeID := node.NodeID

		if _, exist := filterMap[nodeID]; exist {
			continue
		}

		if node.DiskUsage > maxNodeDiskUsage {
			continue
		}

		selectMap[nodeID] = node
		if len(selectMap) >= count {
			break
		}
	}

	return selectMap
}

// chooseEdgeNodesForAssetReplica selects edge nodes to pull asset replicas
func (m *Manager) chooseEdgeNodesForAssetReplica(count int, filterNodes []string) map[string]*node.Node {
	selectMap := make(map[string]*node.Node)
	if count <= 0 {
		return selectMap
	}

	if len(filterNodes) >= m.nodeMgr.Edges {
		return selectMap
	}

	filterMap := make(map[string]struct{})
	for _, nodeID := range filterNodes {
		filterMap[nodeID] = struct{}{}
	}

	for i := 0; i < count*maxRetryCount; i++ {
		node := m.nodeMgr.GetRandomEdge()
		if node == nil {
			continue
		}
		nodeID := node.NodeID

		if _, exist := filterMap[nodeID]; exist {
			continue
		}

		if node.DiskUsage > maxNodeDiskUsage {
			continue
		}

		selectMap[nodeID] = node
		if len(selectMap) >= count {
			break
		}
	}

	return selectMap
}
