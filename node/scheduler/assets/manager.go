package assets

import (
	"context"
	"crypto"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/terrors"
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
	// The number of pull replica in the first stage
	seedReplicaCount = 1
	// Interval to get asset pull progress from node (Unit:Second)
	pullProgressInterval = 5 * time.Second
	// Interval to check candidate backup of asset (Unit:Minute)
	checkCandidateBackupInterval = 10 * time.Minute
	// Maximum number of replicas per asset
	assetEdgeReplicasLimit = 500
	// The number of retries to select the pull asset node
	selectNodeRetryLimit = 3
	// If the node disk size is greater than this value, pulling will not continue
	maxNodeDiskUsage = 95.0
	// Number of asset buckets in assets view
	numAssetBuckets = 128
	// When the node is offline for more than this value, the scheduler will assign other nodes to pull the assets to increase the reliability of the assets
	maxNodeOfflineTime = 24 * time.Hour
	// Total bandwidth limit provided by the asset (The larger the bandwidth provided, the more backups are required)
	assetBandwidthLimit = 10000 // unit:MiB/s
	// If the node does not reply more than once, the asset pull timeout is determined.
	assetTimeoutLimit = 3

	checkAssetReplicaLimit = 100
)

// Manager manages asset replicas
type Manager struct {
	nodeMgr            *node.Manager // node manager
	stateMachineWait   sync.WaitGroup
	assetStateMachines *statemachine.StateGroup
	pullingAssets      sync.Map                      // map[string]int                // Assignments where assets are being pulled
	config             dtypes.GetSchedulerConfigFunc // scheduler config
	*db.SQLDB
	assetRemoveWaitGroup map[string]*sync.WaitGroup
	removeMapLock        sync.Mutex

	fillSwitch bool
}

// NewManager returns a new AssetManager instance
func NewManager(nodeManager *node.Manager, ds datastore.Batching, configFunc dtypes.GetSchedulerConfigFunc, sdb *db.SQLDB) *Manager {
	m := &Manager{
		nodeMgr: nodeManager,
		// pullingAssets:        make(map[string]int),
		config:               configFunc,
		SQLDB:                sdb,
		assetRemoveWaitGroup: make(map[string]*sync.WaitGroup),
		fillSwitch:           true,
	}

	// state machine initialization
	m.stateMachineWait.Add(1)
	m.assetStateMachines = statemachine.New(ds, m, AssetPullingInfo{})

	return m
}

// Start initializes and starts the asset state machine and associated tickers
func (m *Manager) Start(ctx context.Context) {
	if err := m.initStateMachines(ctx); err != nil {
		log.Errorf("restartStateMachines err: %s", err.Error())
	}

	go m.startCheckAssetsTimer()
	go m.startCheckPullProgressesTimer(ctx)
	go m.startCheckCandidateBackupTimer()
	go m.initFillDiskTimer()
}

// Terminate stops the asset state machine
func (m *Manager) Terminate(ctx context.Context) error {
	return m.assetStateMachines.Stop(ctx)
}

func (m *Manager) startCheckCandidateBackupTimer() {
	ticker := time.NewTicker(checkCandidateBackupInterval)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.retrieveCandidateBackupOfAssets()
	}
}

// startCheckAssetsTimer Periodically Check for expired assets, check for missing replicas of assets
func (m *Manager) startCheckAssetsTimer() {
	now := time.Now()

	nextTime := time.Date(now.Year(), now.Month(), now.Day(), 2, 0, 0, 0, now.Location())
	if now.After(nextTime) {
		nextTime = nextTime.Add(24 * time.Hour)
	}

	duration := nextTime.Sub(now)

	timer := time.NewTimer(duration)
	defer timer.Stop()

	offset := 0

	for {
		<-timer.C

		log.Debugln("start assets check ")

		m.processExpiredAssets()
		offset = m.processMissingAssetReplicas(offset)

		timer.Reset(24 * time.Hour)
	}
}

// startCheckPullProgressesTimer Periodically gets asset pull progress
func (m *Manager) startCheckPullProgressesTimer(ctx context.Context) {
	ticker := time.NewTicker(pullProgressInterval)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.retrieveNodePullProgresses()
	}
}

func (m *Manager) setAssetTimeout(hash string) {
	// update replicas status
	err := m.UpdateReplicasStatusToFailed(hash)
	if err != nil {
		log.Errorf("setAssetTimeout %s UpdateReplicasStatusToFailed err:%s", hash, err.Error())
		return
	}

	err = m.assetStateMachines.Send(AssetHash(hash), PullFailed{error: xerrors.New("node pull asset response timeout")})
	if err != nil {
		log.Errorf("setAssetTimeout %s send time out err:%s", hash, err.Error())
	}
}

func (m *Manager) retrieveCandidateBackupOfAssets() {
	hashes, err := m.LoadReplenishBackups()
	if err != nil && err != sql.ErrNoRows {
		log.Errorf("LoadReplenishBackups err:%s", err.Error())
		return
	}

	for _, hash := range hashes {
		exist, err := m.AssetExists(hash, m.nodeMgr.ServerID)
		if err != nil || !exist {
			continue
		}

		rInfo := AssetForceState{
			State:   CandidatesSelect,
			Details: "candidate deactivate",
		}

		err = m.assetStateMachines.Send(AssetHash(hash), rInfo)
		if err != nil {
			log.Errorf("retrieveCandidateBackupOfAssets %s send err:%s", hash, err.Error())
		}
	}
}

func (m *Manager) getPullingAssetLen() int {
	i := 0
	m.pullingAssets.Range(func(key, value interface{}) bool {
		i++
		return true
	})

	return i
}

func (m *Manager) retrieveNodePullProgresses() {
	nodePulls := make(map[string][]string)

	m.pullingAssets.Range(func(key, value interface{}) bool {
		hash := key.(string)
		count := value.(int)

		if count >= assetTimeoutLimit {
			m.setAssetTimeout(hash)
			return true
		}
		m.startAssetTimeoutCounting(hash, count+1)

		cid, err := cidutil.HashToCID(hash)
		if err != nil {
			log.Errorf("retrieveNodePullProgresses %s HashString2CIDString err:%s", hash, err.Error())
			return true
		}

		replicas, err := m.LoadReplicasByStatus(hash, []types.ReplicaStatus{types.ReplicaStatusPulling, types.ReplicaStatusWaiting})
		if err != nil {
			log.Errorf("retrieveNodePullProgresses %s LoadReplicas err:%s", hash, err.Error())
			return true
		}

		for _, replica := range replicas {
			nodeID := replica.NodeID
			list := nodePulls[nodeID]
			nodePulls[nodeID] = append(list, cid)
		}

		return true
	})

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
	if node == nil {
		err = xerrors.Errorf("node %s not found", nodeID)
		return
	}

	result, err = node.GetAssetProgresses(context.Background(), cids)
	return
}

// CreateAssetUploadTask create a new asset upload task
func (m *Manager) CreateAssetUploadTask(hash string, req *types.CreateAssetReq) (*types.CreateAssetRsp, error) {
	// Waiting for state machine initialization
	m.stateMachineWait.Wait()
	log.Infof("asset event: %s, add asset ", req.AssetCID)
	exist, err := m.AssetExistsOfUser(hash, req.UserID)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.DatabaseErr.Int(), Message: err.Error()}
	}

	if exist {
		return nil, &api.ErrWeb{Code: terrors.NoDuplicateUploads.Int(), Message: fmt.Sprintf("Asset is exist, no duplicate uploads")}
	}

	if m.getPullingAssetLen() >= m.getAssetPullTaskLimit() {
		return nil, &api.ErrWeb{Code: terrors.BusyServer.Int(), Message: fmt.Sprintf("The task has reached its limit. Please try again later.")}
	}

	replicaCount := int64(10)
	bandwidth := int64(0)
	expiration := time.Now().Add(30 * 24 * time.Hour)

	cfg, err := m.config()
	if err == nil {
		replicaCount = int64(cfg.UploadAssetReplicaCount)
		expiration = time.Now().Add(time.Duration(cfg.UploadAssetExpiration) * 24 * time.Hour)
	}

	assetRecord, err := m.LoadAssetRecord(hash)
	if err != nil && err != sql.ErrNoRows {
		return nil, &api.ErrWeb{Code: terrors.DatabaseErr.Int(), Message: err.Error()}
	}

	err = m.SaveAssetUser(hash, req.UserID, req.AssetName, req.AssetType, req.AssetSize, expiration, req.Password, req.GroupID)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.DatabaseErr.Int(), Message: err.Error()}
	}

	if assetRecord != nil && assetRecord.State != "" && assetRecord.State != Remove.String() && assetRecord.State != UploadFailed.String() {
		info := &types.CreateAssetRsp{AlreadyExists: true}

		m.UpdateAssetRecordExpiration(hash, expiration)

		return info, nil
	}

	record := &types.AssetRecord{
		Hash:                  hash,
		CID:                   req.AssetCID,
		ServerID:              m.nodeMgr.ServerID,
		NeedEdgeReplica:       replicaCount,
		NeedCandidateReplicas: int64(m.GetCandidateReplicaCount()),
		Expiration:            expiration,
		NeedBandwidth:         bandwidth,
		State:                 UploadInit.String(),
		TotalSize:             req.AssetSize,
		CreatedTime:           time.Now(),
	}

	err = m.SaveAssetRecord(record)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.DatabaseErr.Int(), Message: err.Error()}
	}

	cNode := m.nodeMgr.GetCandidateNode(req.NodeID)
	if cNode == nil {
		cNodes, str := m.chooseCandidateNodes(1, nil)
		if len(cNodes) == 0 {
			return nil, &api.ErrWeb{Code: terrors.NotFoundNode.Int(), Message: fmt.Sprintf("not found node :%s", str)}
		}

		for _, n := range cNodes {
			cNode = n
			break
		}
	}

	payload := &types.AuthUserUploadDownloadAsset{
		UserID:     req.UserID,
		AssetCID:   req.AssetCID,
		AssetSize:  req.AssetSize,
		Expiration: time.Now().Add(time.Hour),
	}

	token, err := cNode.API.CreateAsset(context.Background(), payload)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.RequestNodeErr.Int(), Message: err.Error()}
	}

	rInfo := AssetForceState{
		State:      UploadInit,
		Requester:  req.UserID,
		SeedNodeID: cNode.NodeID,
	}

	// create asset task
	err = m.assetStateMachines.Send(AssetHash(hash), rInfo)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.NotFound.Int(), Message: err.Error()}
	}

	uploadURL := fmt.Sprintf("http://%s/upload", cNode.RemoteAddr)
	if len(cNode.ExternalURL) > 0 {
		uploadURL = fmt.Sprintf("%s/upload", cNode.ExternalURL)
	}

	return &types.CreateAssetRsp{UploadURL: uploadURL, Token: token}, nil
}

func (m *Manager) CreateBaseAsset(cid, nodeID string, size, replicas int64) error {
	log.Infof("CreateBaseAsset cid:%s , nodeID:%s", cid, nodeID)

	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	expiration := time.Now().Add(360 * 24 * time.Hour)

	return m.CreateAssetPullTask(&types.PullAssetReq{CID: cid, Replicas: replicas, Expiration: expiration, Hash: hash, UserID: "admin", SeedNodeID: nodeID})
}

// CreateAssetPullTask create a new asset pull task
func (m *Manager) CreateAssetPullTask(info *types.PullAssetReq) error {
	// Waiting for state machine initialization
	m.stateMachineWait.Wait()

	if m.getPullingAssetLen() >= m.getAssetPullTaskLimit() {
		return xerrors.Errorf("The asset in the pulling exceeds the limit %d, please wait", m.getAssetPullTaskLimit())
	}

	if info.Replicas > assetEdgeReplicasLimit {
		return xerrors.Errorf("The number of replicas %d exceeds the limit %d", info.Replicas, assetEdgeReplicasLimit)
	}

	if info.Bandwidth > assetBandwidthLimit {
		return xerrors.Errorf("The number of bandwidthDown %d exceeds the limit %d", info.Bandwidth, assetBandwidthLimit)
	}

	log.Infof("asset event: %s, add asset replica: %d,expiration: %s", info.CID, info.Replicas, info.Expiration.String())

	assetRecord, err := m.LoadAssetRecord(info.Hash)
	if err != nil && err != sql.ErrNoRows {
		return xerrors.Errorf("LoadAssetRecord err:%s", err.Error())
	}

	if assetRecord == nil {
		record := &types.AssetRecord{
			Hash:                  info.Hash,
			CID:                   info.CID,
			ServerID:              m.nodeMgr.ServerID,
			NeedEdgeReplica:       info.Replicas,
			NeedCandidateReplicas: int64(m.GetCandidateReplicaCount()),
			Expiration:            info.Expiration,
			NeedBandwidth:         info.Bandwidth,
			State:                 SeedSelect.String(),
			CreatedTime:           time.Now(),
		}

		err = m.SaveAssetRecord(record)
		if err != nil {
			return xerrors.Errorf("SaveAssetRecord err:%s", err.Error())
		}

		rInfo := AssetForceState{
			State:      SeedSelect,
			Requester:  info.UserID,
			SeedNodeID: info.SeedNodeID,
		}

		// create asset task
		return m.assetStateMachines.Send(AssetHash(info.Hash), rInfo)
	}

	if exist, _ := m.assetStateMachines.Has(AssetHash(assetRecord.Hash)); !exist {
		return xerrors.Errorf("No operation rights, the asset belongs to another scheduler %s", assetRecord.Hash)
	}

	// Check if the asset is in servicing state
	if assetRecord.State != Servicing.String() && assetRecord.State != Remove.String() {
		return xerrors.Errorf("asset state is %s , no tasks can be created in this state", assetRecord.State)
	}

	if assetRecord.State == Remove.String() {
		assetRecord.NeedEdgeReplica = 0
		assetRecord.NeedBandwidth = 0
	}

	if info.Replicas <= assetRecord.NeedEdgeReplica && info.Bandwidth <= assetRecord.NeedBandwidth {
		return xerrors.New("No increase in the number of replicas or bandwidth")
	}

	assetRecord.NeedEdgeReplica = info.Replicas
	assetRecord.Expiration = info.Expiration
	assetRecord.NeedBandwidth = info.Bandwidth

	return m.replenishAssetReplicas(assetRecord, 0, info.UserID, "", SeedSelect, info.SeedNodeID)
}

// replenishAssetReplicas updates the existing asset replicas if needed
func (m *Manager) replenishAssetReplicas(assetRecord *types.AssetRecord, replenishReplicas int64, userID, details string, state AssetState, seedNodeID string) error {
	log.Debugf("replenishAssetReplicas : %d", replenishReplicas)

	record := &types.AssetRecord{
		Hash:                  assetRecord.Hash,
		CID:                   assetRecord.CID,
		ServerID:              m.nodeMgr.ServerID,
		NeedEdgeReplica:       assetRecord.NeedEdgeReplica,
		NeedCandidateReplicas: int64(m.GetCandidateReplicaCount()),
		Expiration:            assetRecord.Expiration,
		ReplenishReplicas:     replenishReplicas,
		NeedBandwidth:         assetRecord.NeedBandwidth,
		State:                 state.String(),
		TotalSize:             assetRecord.TotalSize,
		CreatedTime:           assetRecord.CreatedTime,
	}

	err := m.SaveAssetRecord(record)
	if err != nil {
		return xerrors.Errorf("SaveAssetRecord err:%s", err.Error())
	}

	rInfo := AssetForceState{
		State:      state,
		Requester:  userID,
		Details:    details,
		SeedNodeID: seedNodeID,
	}

	return m.assetStateMachines.Send(AssetHash(assetRecord.Hash), rInfo)
}

// CandidateDeactivate Candidate node are deactivated. Backup their assets.
func (m *Manager) CandidateDeactivate(nodeID string) error {
	hashes, err := m.LoadAllHashesOfNode(nodeID)
	if err != nil {
		return err
	}

	return m.SaveReplenishBackup(hashes)
}

// RestartPullAssets restarts asset pulls
func (m *Manager) RestartPullAssets(hashes []types.AssetHash) error {
	if m.getPullingAssetLen()+len(hashes) >= m.getAssetPullTaskLimit() {
		return xerrors.Errorf("The asset in the pulling exceeds the limit %d, please wait", m.getAssetPullTaskLimit())
	}

	for _, hash := range hashes {
		if exist, _ := m.assetStateMachines.Has(AssetHash(hash)); !exist {
			continue
		}

		err := m.assetStateMachines.Send(AssetHash(hash), PullAssetRestart{})
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
		return xerrors.Errorf("RemoveReplica %s DeleteAssetReplica err: %s", hash, err.Error())
	}

	// asset view
	err = m.removeAssetFromView(nodeID, cid)
	if err != nil {
		return xerrors.Errorf("RemoveReplica %s removeAssetFromView err: %s", hash, err.Error())
	}

	go m.requestAssetDelete(nodeID, cid)

	return nil
}

// WaitAssetRemove Waiting for the state machine to delete an asset
func (m *Manager) WaitAssetRemove(key string) *sync.WaitGroup {
	m.removeMapLock.Lock()
	defer m.removeMapLock.Unlock()

	if wg, ok := m.assetRemoveWaitGroup[key]; ok {
		return wg
	}

	var wg sync.WaitGroup
	wg.Add(1)

	m.assetRemoveWaitGroup[key] = &wg

	return &wg
}

// AssetRemoveDone Deletion of assets completed
func (m *Manager) AssetRemoveDone(key string) {
	m.removeMapLock.Lock()
	defer m.removeMapLock.Unlock()

	if wg, ok := m.assetRemoveWaitGroup[key]; ok {
		wg.Done()
		wg = nil
		delete(m.assetRemoveWaitGroup, key)
	}
}

// RemoveAsset removes an asset
func (m *Manager) RemoveAsset(hash string, isWait bool) error {
	if exist, _ := m.assetStateMachines.Has(AssetHash(hash)); !exist {
		return xerrors.Errorf("not found asset %s", hash)
	}

	wg := m.WaitAssetRemove(hash)
	defer m.AssetRemoveDone(hash)

	err := m.assetStateMachines.Send(AssetHash(hash), AssetForceState{State: Remove})
	if err != nil {
		return err
	}

	if isWait {
		wg.Wait()
	}

	return nil
}

// updateAssetPullResults updates asset pull results
func (m *Manager) updateAssetPullResults(nodeID string, result *types.PullResult) {
	haveChange := false
	for _, progress := range result.Progresses {
		log.Debugf("updateAssetPullResults node_id: %s, status: %d, block:%d/%d, size: %d/%d, cid: %s ", nodeID, progress.Status, progress.DoneBlocksCount, progress.BlocksCount, progress.DoneSize, progress.Size, progress.CID)

		hash, err := cidutil.CIDToHash(progress.CID)
		if err != nil {
			log.Errorf("%s cid to hash err:%s", progress.CID, err.Error())
			continue
		}

		exist, _ := m.assetStateMachines.Has(AssetHash(hash))
		if !exist {
			continue
		}

		m.startAssetTimeoutCounting(hash, 0)

		if progress.Status == types.ReplicaStatusWaiting {
			continue
		}

		// save replica info to db
		cInfo := &types.ReplicaInfo{
			Status:   progress.Status,
			DoneSize: progress.DoneSize,
			Hash:     hash,
			NodeID:   nodeID,
		}

		err = m.UpdateReplicaInfo(cInfo)
		if err != nil {
			log.Errorf("updateAssetPullResults %s UpdateUnfinishedReplica err:%s", nodeID, err.Error())
			continue
		}

		if progress.Status == types.ReplicaStatusPulling {
			err = m.assetStateMachines.Send(AssetHash(hash), InfoUpdate{
				Blocks: int64(progress.BlocksCount),
				Size:   progress.Size,
			})
			if err != nil {
				log.Errorf("updateAssetPullResults %s statemachine send err:%s", nodeID, err.Error())
			}

			continue
		}

		if progress.Status == types.ReplicaStatusSucceeded {
			haveChange = true

			record, err := m.LoadAssetRecord(hash)
			if err != nil {
				log.Errorf("updateAssetPullResults %s LoadAssetRecord err:%s", nodeID, err.Error())
				continue
			}

			err = m.SaveReplicaEvent(cInfo.Hash, record.CID, cInfo.NodeID, cInfo.DoneSize, record.Expiration, types.ReplicaEventAdd)
			if err != nil {
				log.Errorf("updateAssetPullResults %s SaveReplicaEvent err:%s", nodeID, err.Error())
				continue
			}

			// asset view
			err = m.addAssetToView(nodeID, progress.CID)
			if err != nil {
				log.Errorf("updateAssetPullResults %s addAssetToView err:%s", nodeID, err.Error())
				continue
			}
		}

		err = m.assetStateMachines.Send(AssetHash(hash), PulledResult{
			BlocksCount: int64(progress.BlocksCount),
			Size:        progress.Size,
		})
		if err != nil {
			log.Errorf("updateAssetPullResults %s statemachine send err:%s", nodeID, err.Error())
			continue
		}
	}

	if haveChange {
		// update node Disk Use
		m.nodeMgr.UpdateNodeDiskUsage(nodeID)
	}
}

// Reset the count of no response asset tasks
func (m *Manager) startAssetTimeoutCounting(hash string, count int) {
	m.pullingAssets.Store(hash, count)
}

func (m *Manager) stopAssetTimeoutCounting(hash string) {
	m.pullingAssets.Delete(hash)
}

// UpdateAssetExpiration updates the asset expiration for a given CID
func (m *Manager) UpdateAssetExpiration(cid string, t time.Time) error {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return err
	}

	log.Infof("asset event %s, reset asset expiration:%s", cid, t.String())

	return m.UpdateAssetRecordExpiration(hash, t)
}

// processMissingAssetReplicas checks for missing replicas of assets and adds missing replicas
func (m *Manager) processMissingAssetReplicas(offset int) int {
	aRows, err := m.LoadAllAssetRecords(m.nodeMgr.ServerID, checkAssetReplicaLimit, offset, []string{Servicing.String(), EdgesFailed.String()})
	if err != nil {
		log.Errorf("LoadAllAssetRecords err:%s", err.Error())
		return offset
	}
	defer aRows.Close()

	size := 0
	// loading asset records
	for aRows.Next() {
		size++

		cInfo := &types.AssetRecord{}
		err = aRows.StructScan(cInfo)
		if err != nil {
			log.Errorf("asset StructScan err: %s", err.Error())
			continue
		}

		effectiveEdges, details, err := m.checkAssetReliability(cInfo.Hash)
		if err != nil {
			log.Errorf("checkAssetReliability err: %s", err.Error())
			continue
		}

		missingEdges := cInfo.NeedEdgeReplica - int64(effectiveEdges)

		if missingEdges <= 0 && cInfo.State == Servicing.String() {
			// Asset are healthy and do not need to be replenish replicas
			continue
		}

		// do replenish replicas
		err = m.replenishAssetReplicas(cInfo, missingEdges, string(m.nodeMgr.ServerID), details, CandidatesSelect, "")
		if err != nil {
			log.Errorf("replenishAssetReplicas err: %s", err.Error())
			continue
		}
	}

	if size == checkAssetReplicaLimit {
		offset++
	} else {
		offset = 0
	}

	return offset
}

// Check the reliability of assets
func (m *Manager) checkAssetReliability(hash string) (effectiveEdges int, details string, outErr error) {
	// loading asset replicas
	replicas, outErr := m.LoadReplicasByStatus(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if outErr != nil {
		log.Errorf("checkAssetReliability LoadReplicasByHash err:%s", outErr.Error())
		return
	}

	details = "offline node:"

	for _, rInfo := range replicas {
		// Are the nodes unreliable
		nodeID := rInfo.NodeID
		lastSeen, err := m.LoadNodeLastSeenTime(nodeID)
		if err != nil {
			log.Errorf("checkAssetReliability LoadLastSeenOfNode err: %s", err.Error())
			continue
		}

		if rInfo.IsCandidate {
			continue
		}

		if lastSeen.Add(maxNodeOfflineTime).After(time.Now()) {
			effectiveEdges++
		} else {
			details = fmt.Sprintf("%s%s,", details, nodeID)
		}
	}

	return
}

// processExpiredAssets checks for expired assets and removes them
func (m *Manager) processExpiredAssets() {
	records, err := m.LoadExpiredAssetRecords(m.nodeMgr.ServerID, ActiveStates)
	if err != nil {
		log.Errorf("LoadExpiredAssetRecords err:%s", err.Error())
		return
	}

	for _, record := range records {
		// do remove
		err = m.RemoveAsset(record.Hash, false)
		log.Infof("the asset cid(%s) has expired, being removed, err: %v", record.CID, err)
	}
}

// requestAssetDelete notifies a node to delete an asset by its CID
func (m *Manager) requestAssetDelete(nodeID, cid string) error {
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
		return seedReplicaCount
	}

	log.Infof("GetCandidateReplicaCount %d", cfg.CandidateReplicas)

	return seedReplicaCount + cfg.CandidateReplicas
}

func (m *Manager) getAssetPullTaskLimit() int {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("get schedulerConfig err:%s", err.Error())
		return 0
	}

	return cfg.AssetPullTaskLimit
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

	return m.SaveReplicasStatus(replicaInfos)
}

// getDownloadSources gets download sources for a given CID
func (m *Manager) getDownloadSources(hash string) []*types.CandidateDownloadInfo {
	replicaInfos, err := m.LoadReplicasByStatus(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		return nil
	}

	sources := make([]*types.CandidateDownloadInfo, 0)
	for _, replica := range replicaInfos {
		nodeID := replica.NodeID
		cNode := m.nodeMgr.GetNode(nodeID)
		if cNode == nil {
			continue
		}

		if cNode.Type != types.NodeCandidate {
			if cNode.NATType != types.NatTypeNo || cNode.Info.ExternalIP == "" {
				continue
			}
		}

		source := &types.CandidateDownloadInfo{
			NodeID:  nodeID,
			Address: cNode.DownloadAddr(),
		}

		sources = append(sources, source)
	}
	return sources
}

// chooseCandidateNodes selects candidate nodes to pull asset replicas
func (m *Manager) chooseCandidateNodes(count int, filterNodes []string) (map[string]*node.Node, string) {
	str := fmt.Sprintf("need node:%d , filter node:%d , cur node:%d , randNum : ", count, len(filterNodes), m.nodeMgr.Candidates)

	selectMap := make(map[string]*node.Node)
	if count <= 0 {
		return selectMap, str
	}

	if len(filterNodes) >= m.nodeMgr.Candidates {
		return selectMap, str
	}

	filterMap := make(map[string]struct{})
	for _, nodeID := range filterNodes {
		filterMap[nodeID] = struct{}{}
	}

	num := count * selectNodeRetryLimit

	for i := 0; i < num; i++ {
		node, rNum := m.nodeMgr.GetRandomCandidate()
		str = fmt.Sprintf("%s%d,", str, rNum)

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

	return selectMap, str
}

// chooseEdgeNodes selects edge nodes to store asset replicas
// count: number of nodes to select
// bandwidthDown: required cumulative bandwidth among selected nodes
// filterNodes: exclude nodes that have already been considered
// size: the minimum free storage space required for each selected node
func (m *Manager) chooseEdgeNodes(count int, bandwidthDown int64, filterNodes []string, size float64) (map[string]*node.Node, string) {
	str := fmt.Sprintf("need node:%d , filter node:%d , cur node:%d , randNum : ", count, len(filterNodes), m.nodeMgr.Edges)
	selectMap := make(map[string]*node.Node)
	if count <= 0 {
		count = 1
	}

	// If the number of filterNodes is equal or greater than the total edge nodes,
	// there are no nodes to consider, so return an empty map and the result string
	if len(filterNodes) >= m.nodeMgr.Edges {
		return selectMap, str
	}

	filterMap := make(map[string]struct{})
	for _, nodeID := range filterNodes {
		filterMap[nodeID] = struct{}{}
	}

	// shouldSelectNode determines whether a given node should be selected based on specific criteria.
	// It calculates the node's residual capacity and compares it with thresholds and limits to make a decision.
	//
	// Parameters:
	// - node: The node to evaluate.
	//
	// Returns:
	// - bool: true if the node satisfies the selection criteria, false otherwise.
	selectNodes := func(node *node.Node) bool {
		if node == nil {
			return false
		}
		nodeID := node.NodeID

		if _, exist := filterMap[nodeID]; exist {
			return false
		}

		// Calculate node residual capacity
		residual := (100 - node.DiskUsage) * node.Info.DiskSpace
		if residual <= size {
			log.Debugf("node %s disk residual %.2f", nodeID, residual)
			return false
		}

		if _, exist := selectMap[nodeID]; exist {
			return false
		}

		bandwidthDown -= int64(node.BandwidthDown)
		selectMap[nodeID] = node
		if len(selectMap) >= count && bandwidthDown <= 0 {
			return true
		}

		if len(selectMap) >= assetEdgeReplicasLimit-len(filterNodes) {
			return true
		}

		return false
	}

	// If count is greater or equal to the difference between total edge nodes and the filterNodes length,
	// choose all unfiltered nodes
	if count >= m.nodeMgr.Edges-len(filterNodes) {
		// choose all
		nodes := m.nodeMgr.GetAllEdgeNode()

		for _, node := range nodes {
			if selectNodes(node) {
				break
			}
		}
	} else {
		// choose random
		for i := 0; i < count*selectNodeRetryLimit; i++ {
			node, rNum := m.nodeMgr.GetRandomEdge()
			str = fmt.Sprintf("%s%d,", str, rNum)
			if selectNodes(node) {
				break
			}
		}
	}

	return selectMap, str
}

func (m *Manager) GetAssetCount() (int, error) {
	return m.LoadAssetCount(m.nodeMgr.ServerID, Remove.String())
}

func (m *Manager) generateTokenForDownloadSources(sources []*types.CandidateDownloadInfo, titanRsa *titanrsa.Rsa, assetCID string, clientID string) ([]*types.CandidateDownloadInfo, []*types.TokenPayload, error) {
	payloads := make([]*types.TokenPayload, 0)
	downloadSources := make([]*types.CandidateDownloadInfo, 0, len(sources))

	for _, source := range sources {
		node := m.nodeMgr.GetNode(source.NodeID)
		if node == nil {
			continue
		}

		tk, payload, err := node.Token(assetCID, clientID, titanRsa, m.nodeMgr.PrivateKey)
		if err != nil {
			continue
		}

		downloadSource := *source
		downloadSource.Tk = tk
		downloadSources = append(downloadSources, &downloadSource)

		payloads = append(payloads, payload)
	}

	return downloadSources, payloads, nil
}

func (m *Manager) GenerateToken(assetCID string, sources []*types.CandidateDownloadInfo, nodes map[string]*node.Node) (map[string][]*types.CandidateDownloadInfo, []*types.TokenPayload, error) {
	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	downloadSources := make(map[string][]*types.CandidateDownloadInfo)
	tkPayloads := make([]*types.TokenPayload, 0)

	for _, node := range nodes {
		newSources, payloads, err := m.generateTokenForDownloadSources(sources, titanRsa, assetCID, node.NodeID)
		if err != nil {
			return nil, nil, err
		}

		tkPayloads = append(tkPayloads, payloads...)
		downloadSources[node.NodeID] = newSources
	}
	return downloadSources, tkPayloads, nil
}

func (m *Manager) SaveTokenPayload(payloads []*types.TokenPayload) error {
	records := make([]*types.WorkloadRecord, 0, len(payloads))
	for _, payload := range payloads {
		record := &types.WorkloadRecord{TokenPayload: *payload, Status: types.WorkloadStatusCreate, ClientEndTime: payload.Expiration.Unix()}
		records = append(records, record)
	}

	return m.SaveWorkloadRecord(records)
}
