package assets

import (
	"bytes"
	"context"
	"crypto"
	"database/sql"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/terrors"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/filecoin-project/go-statemachine"
	"github.com/google/uuid"
	"github.com/ipfs/go-datastore"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"

	"github.com/Filecoin-Titan/titan/node/cidutil"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/Filecoin-Titan/titan/node/scheduler/validation"
	"github.com/Filecoin-Titan/titan/node/scheduler/workload"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("asset")

const (
	// The number of pull replica in the first stage
	seedReplicaCount = 1
	// Interval to get asset pull progress from node (Unit:Second)
	pullProgressInterval = 30 * time.Second
	// Interval to get asset pull progress from node (Unit:Second)
	uploadProgressInterval = time.Second
	// Interval to check candidate backup of asset (Unit:Minute)
	checkCandidateBackupInterval = 5 * time.Minute
	// The number of retries to select the pull asset node
	selectNodeRetryLimit = 2
	// If the node disk size is greater than this value, pulling will not continue
	maxNodeDiskUsage = 95.0
	// Number of asset buckets in assets view
	numAssetBuckets = 128
	// When the node is offline for more than this value, the scheduler will assign other nodes to pull the assets to increase the reliability of the assets
	maxNodeOfflineTime = 24 * time.Hour
	// If the node does not reply more than once, the asset pull timeout is determined.
	assetTimeoutLimit = 3

	checkAssetReplicaLimit = 50

	nodeProfitsLimitOfDay = 50000.0
	// Maximum number of candidates for asset upload
	maxCandidateForSelect = 5

	expirationOfStorageAsset = 150 // day

	defaultReplicaCount = 200

	// AssetEdgeReplicasLimit Maximum number of replicas per asset
	AssetEdgeReplicasLimit = 1000
	// AssetBandwidthLimit Total bandwidth limit provided by the asset (The larger the bandwidth provided, the more backups are required)
	AssetBandwidthLimit = 10000 // unit:MiB/s
)

// Manager manages asset replicas
type Manager struct {
	nodeMgr            *node.Manager       // node manager
	validationMgr      *validation.Manager // node manager
	workloadMgr        *workload.Manager
	stateMachineWait   sync.WaitGroup
	assetStateMachines *statemachine.StateGroup
	pullingAssets      sync.Map                      // map[string]int                // Assignments where assets are being pulled
	config             dtypes.GetSchedulerConfigFunc // scheduler config
	*db.SQLDB
	assetRemoveWaitGroup map[string]*sync.WaitGroup
	removeMapLock        sync.Mutex

	fillSwitch bool

	fillAssets sync.Map // Data being downloaded from aws
	// fillAssetNodes sync.Map // The node that is downloading data from aws

	isPullSpecifyAsset bool
	sortEdges          bool

	assetPullTaskLimit    int
	candidateReplicaCount int
}

type pullingAssetsInfo struct {
	count      int
	expiration time.Time
}

// NewManager returns a new AssetManager instance
func NewManager(vManager *validation.Manager, nManager *node.Manager, ds datastore.Batching, configFunc dtypes.GetSchedulerConfigFunc, sdb *db.SQLDB, wMgr *workload.Manager) *Manager {
	m := &Manager{
		nodeMgr:       nManager,
		validationMgr: vManager,
		// pullingAssets:        make(map[string]int),
		config:               configFunc,
		SQLDB:                sdb,
		assetRemoveWaitGroup: make(map[string]*sync.WaitGroup),
		fillSwitch:           true,
		workloadMgr:          wMgr,
	}

	m.initCfg()

	// state machine initialization
	m.stateMachineWait.Add(1)
	m.assetStateMachines = statemachine.New(ds, m, AssetPullingInfo{})

	return m
}

// Start initializes and starts the asset state machine and associated tickers
func (m *Manager) Start(ctx context.Context) {
	if err := m.initStateMachines(); err != nil {
		log.Errorf("restartStateMachines err: %s", err.Error())
	}

	go m.startCheckAssetsTimer()
	go m.startCheckPullProgressesTimer()
	go m.startCheckUploadProgressesTimer()
	go m.startCheckCandidateBackupTimer()
	go m.initFillDiskTimer()
}

// Terminate stops the asset state machine
func (m *Manager) Terminate(ctx context.Context) error {
	log.Infoln("Terminate stop")
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

	nextTime := time.Date(now.Year(), now.Month(), now.Day(), 4, 0, 0, 0, time.UTC)
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
		m.cleanUploadFailedAssetReplicas()

		timer.Reset(24 * time.Hour)
	}
}

// startCheckPullProgressesTimer Periodically gets asset pull progress
func (m *Manager) startCheckPullProgressesTimer() {
	ticker := time.NewTicker(pullProgressInterval)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.retrieveNodePullProgresses(false)
	}
}

// startCheckUploadProgressesTimer Periodically gets asset upload progress
func (m *Manager) startCheckUploadProgressesTimer() {
	for {
		time.Sleep(uploadProgressInterval)

		m.retrieveNodePullProgresses(true)
	}
}

func (m *Manager) setAssetTimeout(hash, msg string) {
	nodes, err := m.LoadNodesOfPullingReplica(hash)
	if err != nil {
		log.Errorf("setAssetTimeout %s LoadNodesOfPullingReplica err:%s", hash, err.Error())
		return
	}

	// update replicas status
	err = m.UpdateReplicasStatusToFailed(hash)
	if err != nil {
		log.Errorf("setAssetTimeout %s UpdateReplicasStatusToFailed err:%s", hash, err.Error())
		return
	}

	cid, _ := cidutil.HashToCID(hash)

	for _, nodeID := range nodes {
		go m.requestAssetDelete(nodeID, cid)
	}

	err = m.assetStateMachines.Send(AssetHash(hash), PullFailed{error: xerrors.Errorf("pull timeout ; %s", msg)})
	if err != nil {
		log.Errorf("setAssetTimeout %s send time out err:%s", hash, err.Error())
	}
}

func (m *Manager) retrieveCandidateBackupOfAssets() {
	hashes, err := m.LoadReplenishBackups(10)
	if err != nil && err != sql.ErrNoRows {
		log.Errorf("LoadReplenishBackups err:%s", err.Error())
		return
	}

	// delete hashes

	for _, hash := range hashes {
		stateInfo, err := m.LoadAssetStateInfo(hash, m.nodeMgr.ServerID)
		if err != nil {
			err = m.DeleteReplenishBackup(hash)
			if err != nil {
				log.Errorf("retrieveCandidateBackupOfAssets DeleteReplenishBackup %s err:%s", hash, err.Error())
			}
			continue
		}

		if stateInfo.State == Remove.String() || stateInfo.State == Stop.String() {
			err = m.DeleteReplenishBackup(hash)
			if err != nil {
				log.Errorf("retrieveCandidateBackupOfAssets DeleteReplenishBackup %s err:%s", hash, err.Error())
			}
			continue
		}

		if m.isAssetTaskExist(hash) {
			err = m.DeleteReplenishBackup(hash)
			if err != nil {
				log.Errorf("retrieveCandidateBackupOfAssets DeleteReplenishBackup %s err:%s", hash, err.Error())
			}
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

func (m *Manager) getPullingAssetList() []string {
	list := make([]string, 0)
	m.pullingAssets.Range(func(key, value interface{}) bool {
		hash := key.(string)

		list = append(list, hash)
		return true
	})

	return list
}

func (m *Manager) retrieveNodePullProgresses(isUpload bool) {
	nodePulls := make(map[string][]string)

	toMap := make(map[string]*pullingAssetsInfo)

	m.pullingAssets.Range(func(key, value interface{}) bool {
		hash := key.(string)
		info := value.(*pullingAssetsInfo)

		toMap[hash] = info
		return true
	})

	for hash, info := range toMap {
		stateInfo, err := m.LoadAssetStateInfo(hash, m.nodeMgr.ServerID)
		if err != nil {
			continue
		}

		if isUpload {
			if stateInfo.State != SeedUploading.String() {
				continue
			}
		} else {
			if stateInfo.State != EdgesPulling.String() && stateInfo.State != CandidatesPulling.String() && stateInfo.State != SeedPulling.String() && stateInfo.State != SeedSyncing.String() {
				continue
			}
		}

		if info.count >= assetTimeoutLimit {
			m.setAssetTimeout(hash, fmt.Sprintf("count:%d", info.count))
			continue
		}

		if info.expiration.Before(time.Now()) {
			m.setAssetTimeout(hash, fmt.Sprintf("expiration:%s", info.expiration.String()))
			continue
		}

		m.startAssetTimeoutCounting(hash, info.count+1, 0)

		cid, err := cidutil.HashToCID(hash)
		if err != nil {
			log.Errorf("retrieveNodePullProgresses %s HashString2CIDString err:%s", hash, err.Error())
			continue
		}

		log.Infof("retrieveNodePullProgresses check %s \n", cid)

		pList, err := m.LoadNodesOfPullingReplica(hash)
		if err != nil {
			log.Errorf("retrieveNodePullProgresses %s LoadReplicas err:%s", hash, err.Error())
			continue
		}

		for _, nodeID := range pList {
			list := nodePulls[nodeID]
			nodePulls[nodeID] = append(list, cid)
		}
	}

	getCP := func(nodeID string, cids []string, delay int) {
		if delay > 0 {
			time.Sleep(time.Duration(delay) * time.Second)
		}

		// request node
		result, err := m.requestNodePullProgresses(nodeID, cids)
		if err != nil {
			log.Errorf("retrieveNodePullProgresses %s requestNodePullProgresses err:%s", nodeID, err.Error())
			return
		}

		// update asset info
		m.updateAssetPullResults(nodeID, result)
	}

	duration := 1
	if isUpload {
		duration = 0
	}

	delay := 0
	for nodeID, cids := range nodePulls {
		delay += duration
		if delay > 50 {
			delay = 0
		}

		go getCP(nodeID, cids, delay)
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

func (m *Manager) getSourceDownloadInfo(cNode *node.Node, cid string, titanRsa *titanrsa.Rsa) *types.SourceDownloadInfo {
	tk, err := cNode.EncryptToken(cid, uuid.NewString(), titanRsa, m.nodeMgr.PrivateKey)
	if err != nil {
		return nil
	}

	return &types.SourceDownloadInfo{
		NodeID:  cNode.NodeID,
		Address: cNode.DownloadAddr(),
		Tk:      tk,
	}
}

// GenerateTokenForDownloadSources Generate Token
func (m *Manager) GenerateTokenForDownloadSources(cid string) ([]*types.SourceDownloadInfo, error) {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.CidToHashFiled.Int(), Message: err.Error()}
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	var out []*types.SourceDownloadInfo

	limit := 5
	replicas, err := m.LoadReplicasByStatus(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.AssetNotFound.Int(), Message: err.Error()}
	}

	for _, rInfo := range replicas {
		cNode := m.nodeMgr.GetCandidateNode(rInfo.NodeID)
		if cNode == nil {
			continue
		}

		sInfo := m.getSourceDownloadInfo(cNode, cid, titanRsa)
		if sInfo == nil {
			continue
		}

		out = append(out, sInfo)

		if len(out) >= limit {
			break
		}
	}

	if len(out) == 0 {
		return nil, &api.ErrWeb{Code: terrors.NotFoundNode.Int()}
	}

	return out, nil
}

// GenerateTokenForDownloadSource Generate Token
func (m *Manager) GenerateTokenForDownloadSource(nodeID, cid string) (*types.SourceDownloadInfo, error) {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.CidToHashFiled.Int(), Message: err.Error()}
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	var out *types.SourceDownloadInfo

	if nodeID == "" {
		replicas, err := m.LoadReplicasByStatus(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
		if err != nil {
			return nil, &api.ErrWeb{Code: terrors.AssetNotFound.Int(), Message: err.Error()}
		}

		for _, rInfo := range replicas {
			cNode := m.nodeMgr.GetCandidateNode(rInfo.NodeID)
			if cNode == nil {
				continue
			}

			sInfo := m.getSourceDownloadInfo(cNode, cid, titanRsa)
			if sInfo == nil {
				continue
			}

			out = sInfo
			break
		}
	} else {
		cNode := m.nodeMgr.GetCandidateNode(nodeID)
		if cNode != nil {
			sInfo := m.getSourceDownloadInfo(cNode, cid, titanRsa)
			if sInfo != nil {
				out = sInfo
			}
		}
	}

	if out == nil {
		return nil, &api.ErrWeb{Code: terrors.NotFoundNode.Int()}
	}

	return out, nil
}

// replenishAssetReplicas updates the existing asset replicas if needed
func (m *Manager) replenishAssetReplicas(assetRecord *types.AssetRecord, replenishReplicas int64, note, details string, state AssetState, seedNodeID string) error {
	log.Debugf("replenishAssetReplicas : %d", replenishReplicas)

	record := &types.AssetRecord{
		Hash:                  assetRecord.Hash,
		CID:                   assetRecord.CID,
		ServerID:              m.nodeMgr.ServerID,
		NeedEdgeReplica:       assetRecord.NeedEdgeReplica,
		NeedCandidateReplicas: assetRecord.NeedCandidateReplicas,
		Expiration:            assetRecord.Expiration,
		ReplenishReplicas:     replenishReplicas,
		NeedBandwidth:         assetRecord.NeedBandwidth,
		State:                 state.String(),
		TotalSize:             assetRecord.TotalSize,
		CreatedTime:           assetRecord.CreatedTime,
		Note:                  note,
	}

	err := m.SaveAssetRecord(record)
	if err != nil {
		return xerrors.Errorf("SaveAssetRecord err:%s", err.Error())
	}

	rInfo := AssetForceState{
		State: state,
		// Requester:  note,
		Details:     details,
		SeedNodeIDs: []string{seedNodeID},
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
	if len(m.getPullingAssetList())+len(hashes) > m.assetPullTaskLimit {
		return xerrors.Errorf("The asset in the pulling exceeds the limit %d, please wait", m.assetPullTaskLimit)
	}

	for _, hash := range hashes {
		if exist, _ := m.assetStateMachines.Has(AssetHash(hash)); !exist {
			continue
		}

		// From CandidatesSelect
		rInfo := AssetForceState{
			State:   CandidatesSelect,
			Details: "candidate deactivate",
		}

		err := m.assetStateMachines.Send(AssetHash(hash), rInfo)
		if err != nil {
			log.Errorf("retrieveCandidateBackupOfAssets %s send err:%s", hash, err.Error())
		}

		// err := m.assetStateMachines.Send(AssetHash(hash), PullAssetRestart{})
		// if err != nil {
		// 	log.Errorf("RestartPullAssets send err:%s", err.Error())
		// }
	}

	return nil
}

// RemoveReplica remove a replica for node
func (m *Manager) RemoveReplica(cid, hash, nodeID string) error {
	err := m.DeleteAssetReplica(hash, nodeID, cid)
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
func (m *Manager) waitAssetRemove(key string) *sync.WaitGroup {
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
func (m *Manager) assetRemoveDone(key string) {
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
		// return xerrors.Errorf("not found asset %s", hash)
		return &api.ErrWeb{Code: terrors.HashNotFound.Int(), Message: fmt.Sprintf("not found asset %s", hash)}
	}

	wg := m.waitAssetRemove(hash)
	defer m.assetRemoveDone(hash)

	err := m.assetStateMachines.Send(AssetHash(hash), AssetForceState{State: Remove})
	if err != nil {
		return err
	}

	if isWait {
		wg.Wait()
	}

	return nil
}

// StopAsset stop an asset
func (m *Manager) StopAsset(hashs []string) error {
	for _, hash := range hashs {
		if exist, _ := m.assetStateMachines.Has(AssetHash(hash)); !exist {
			continue
		}

		err := m.assetStateMachines.Send(AssetHash(hash), AssetForceState{State: Stop})
		if err != nil {
			log.Errorf("StopAsset assetStateMachines err:%s", err.Error())
		}

		// m.setAssetTimeout(hash, "stop")
	}

	return nil
}

// updateAssetPullResults updates asset pull results
func (m *Manager) updateAssetPullResults(nodeID string, result *types.PullResult) {
	succeededCIDs := make([]string, 0)
	haveChange := false
	doneCount := 0
	downloadTraffic := int64(0)

	speeds := []int64{}

	for _, progress := range result.Progresses {
		log.Infof("updateAssetPullResults node_id: %s, status: %d, block:%d/%d, size: %d/%d, cid: %s , msg:%s", nodeID, progress.Status, progress.DoneBlocksCount, progress.BlocksCount, progress.DoneSize, progress.Size, progress.CID, progress.Msg)

		hash, err := cidutil.CIDToHash(progress.CID)
		if err != nil {
			log.Errorf("%s cid to hash err:%s", progress.CID, err.Error())
			continue
		}

		exist, _ := m.assetStateMachines.Has(AssetHash(hash))
		if !exist {
			continue
		}

		exist = m.isAssetTaskExist(hash)
		if !exist {
			continue
		}

		m.startAssetTimeoutCounting(hash, 0, 0)

		if progress.Status == types.ReplicaStatusWaiting {
			continue
		}

		if progress.Speed > 0 {
			speeds = append(speeds, progress.Speed)
		}

		// save replica info to db
		cInfo := &types.ReplicaInfo{
			Status:   progress.Status,
			DoneSize: progress.DoneSize,
			Hash:     hash,
			NodeID:   nodeID,
			ClientID: progress.ClientID,
			Speed:    progress.Speed,
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

		record, err := m.LoadAssetRecord(hash)
		if err != nil {
			log.Errorf("updateAssetPullResults %s LoadAssetRecord err:%s", nodeID, err.Error())
			continue
		}

		if len(progress.Msg) > 1024 {
			progress.Msg = string([]rune(progress.Msg)[:1024])
		}

		event := &types.AssetReplicaEventInfo{
			Hash:      cInfo.Hash,
			Cid:       record.CID,
			NodeID:    nodeID,
			TotalSize: cInfo.DoneSize,
			Source:    types.AssetSource(record.Source),
			ClientID:  progress.ClientID,
			DoneSize:  progress.DoneSize,
			TraceID:   progress.TraceID,
			Msg:       progress.Msg,
		}

		if progress.Status == types.ReplicaStatusSucceeded {
			haveChange = true
			doneCount++

			succeededCIDs = append(succeededCIDs, record.CID)

			// asset view
			err = m.addAssetToView(nodeID, progress.CID)
			if err != nil {
				log.Errorf("updateAssetPullResults %s addAssetToView err:%s", nodeID, err.Error())
				continue
			}

			event.Event = types.ReplicaEventAdd

			err = m.SaveReplicaEvent(event, 1, 0)
			if err != nil {
				log.Errorf("updateAssetPullResults %s SaveReplicaEvent err:%s", nodeID, err.Error())
				continue
			}

			downloadTraffic += cInfo.DoneSize
		} else if progress.Status == types.ReplicaStatusFailed {
			event.Event = types.ReplicaEventFailed

			err = m.SaveReplicaEvent(event, 0, 1)
			if err != nil {
				log.Errorf("updateAssetPullResults %s SaveReplicaEvent err:%s", nodeID, err.Error())
			}

			doneCount++
		}

		err = m.assetStateMachines.Send(AssetHash(hash), PulledResult{
			BlocksCount: int64(progress.BlocksCount),
			Size:        progress.Size,
		})
		if err != nil {
			log.Errorf("updateAssetPullResults %s %s statemachine send err:%s", nodeID, progress.CID, err.Error())
			continue
		}
	}

	node := m.nodeMgr.GetNode(nodeID)
	if node != nil {
		node.DiskUsage = result.DiskUsage
		node.DownloadTraffic += downloadTraffic

		// update node BandwidthDown
		if len(speeds) > 0 {
			tSp := int64(0)
			for _, speed := range speeds {
				tSp += speed
			}

			s := tSp / int64(len(speeds))
			m.nodeMgr.UpdateNodeBandwidths(nodeID, s, 0)
		}
	}

	if haveChange {
		// update node Disk Use
		m.nodeMgr.UpdateNodeDiskUsage(nodeID, result.DiskUsage)

		node := m.nodeMgr.GetNode(nodeID)
		if node != nil {
			for _, cid := range succeededCIDs {
				err := node.AddAssetView(context.Background(), []string{cid})
				if err != nil {
					log.Errorf("updateAssetPullResults %s %v AddAssetView send err:%s", nodeID, succeededCIDs, err.Error())
				}
			}
		}
	}
}

// Reset the count of no response asset tasks
func (m *Manager) startAssetTimeoutCounting(hash string, count int, size int64) {
	// log.Infof("startAssetTimeoutCounting asset:%s, count:%d", hash, count)

	info := &pullingAssetsInfo{count: 0}

	infoI, _ := m.pullingAssets.Load(hash)
	if infoI != nil {
		info = infoI.(*pullingAssetsInfo)
	} else {
		needTime := int64(60 * 60 * 2)
		if size > 0 {
			needTime = size / (50 * 1024) // 50 kbps
		}

		if needTime < 5*60 {
			needTime = 5 * 60
		}
		info.expiration = time.Now().Add(time.Second * time.Duration(needTime))
	}
	info.count = count

	m.pullingAssets.Store(hash, info)
}

func (m *Manager) stopAssetTimeoutCounting(hash string) {
	m.pullingAssets.Delete(hash)
}

func (m *Manager) isAssetTaskExist(hash string) bool {
	_, exist := m.pullingAssets.Load(hash)

	return exist
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

// ResetAssetReplicaCount updates the asset replica count for a given CID
func (m *Manager) ResetAssetReplicaCount(cid string, count int) error {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return err
	}

	// TODO

	log.Infof("asset event %s, reset asset replica count:%s", cid, count)

	return m.UpdateAssetRecordReplicaCount(hash, count)
}

// cleanUploadFailedAssetReplicas clean upload failed assets
func (m *Manager) cleanUploadFailedAssetReplicas() {
	aRows, err := m.LoadAllAssetRecords(m.nodeMgr.ServerID, checkAssetReplicaLimit, 0, []string{UploadFailed.String(), SyncFailed.String(), SeedFailed.String()})
	if err != nil {
		log.Errorf("LoadAllAssetRecords err:%s", err.Error())
		return
	}
	defer aRows.Close()

	// loading asset records
	for aRows.Next() {
		cInfo := &types.AssetRecord{}
		err = aRows.StructScan(cInfo)
		if err != nil {
			log.Errorf("asset StructScan err: %s", err.Error())
			continue
		}

		err = m.RemoveAsset(cInfo.Hash, false)
		if err != nil {
			log.Errorf("RemoveAsset %s err:%s", cInfo.Hash, err.Error())
		}
	}
}

// processMissingAssetReplicas checks for missing replicas of assets and adds missing replicas
func (m *Manager) processMissingAssetReplicas(offset int) int {
	aRows, err := m.LoadAllAssetRecords(m.nodeMgr.ServerID, checkAssetReplicaLimit, offset, []string{Servicing.String()})
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

		if cInfo.Source != int64(AssetSourceStorage) {
			continue
		}

		effectiveEdges, candidateReplica, deleteNodes, err := m.checkAssetReliability(cInfo.Hash)
		if err != nil {
			log.Errorf("checkAssetReliability err: %s", err.Error())
			continue
		}

		for _, nodeID := range deleteNodes {
			err = m.RemoveReplica(cInfo.CID, cInfo.Hash, nodeID)
			if err != nil {
				log.Errorf("RemoveReplica %s err: %s", nodeID, err.Error())
			}
		}

		missingEdges := cInfo.NeedEdgeReplica - int64(effectiveEdges)
		missingCandidates := cInfo.NeedCandidateReplicas - int64(candidateReplica)

		if missingCandidates <= 0 && missingEdges <= 0 && cInfo.State == Servicing.String() && len(deleteNodes) == 0 {
			// Asset are healthy and do not need to be replenish replicas
			continue
		}

		// do replenish replicas
		err = m.replenishAssetReplicas(cInfo, missingEdges, string(m.nodeMgr.ServerID), "", CandidatesSelect, "")
		if err != nil {
			log.Errorf("replenishAssetReplicas err: %s", err.Error())
			continue
		}
	}

	if size == checkAssetReplicaLimit {
		offset += size
	} else {
		offset = 0
	}

	return offset
}

// Check the reliability of assets
func (m *Manager) checkAssetReliability(hash string) (effectiveEdges, candidateReplica int, deleteNodes []string, outErr error) {
	// loading asset replicas
	replicas, outErr := m.LoadReplicasByStatus(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if outErr != nil {
		log.Errorf("checkAssetReliability LoadReplicasByHash err:%s", outErr.Error())
		return
	}

	deleteNodes = make([]string, 0)

	for _, rInfo := range replicas {
		// Are the nodes unreliable
		nodeID := rInfo.NodeID
		lastSeen, err := m.LoadNodeLastSeenTime(nodeID)
		if err != nil {
			log.Errorf("checkAssetReliability %s LoadLastSeenOfNode err: %s", nodeID, err.Error())
			deleteNodes = append(deleteNodes, nodeID)
			continue
		}

		if lastSeen.Add(maxNodeOfflineTime * 10).Before(time.Now()) {
			log.Warnf("checkAssetReliability %s lastSeen %s", nodeID, lastSeen.String())
			deleteNodes = append(deleteNodes, nodeID)
			continue
		}

		if rInfo.IsCandidate {
			candidateReplica++
		} else {
			if lastSeen.Add(maxNodeOfflineTime).After(time.Now()) {
				effectiveEdges++
			}
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

func (m *Manager) initCfg() {
	m.assetPullTaskLimit = 2
	m.candidateReplicaCount = seedReplicaCount

	cfg, err := m.config()
	if err != nil {
		log.Errorf("get schedulerConfig err:%s", err.Error())
		return
	}

	m.assetPullTaskLimit = cfg.AssetPullTaskLimit
	m.candidateReplicaCount = seedReplicaCount + cfg.CandidateReplicas
}

// getDownloadSources gets download sources for a given CID
func (m *Manager) getDownloadSources(hash string) []*types.SourceDownloadInfo {
	replicaInfos, err := m.LoadReplicasByStatus(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		return nil
	}

	sources := make([]*types.SourceDownloadInfo, 0)
	limit := 50

	for _, replica := range replicaInfos {
		nodeID := replica.NodeID
		cNode := m.nodeMgr.GetNode(nodeID)
		if cNode == nil {
			continue
		}

		if cNode.NetFlowUpExcess(float64(replica.DoneSize)) {
			continue
		}

		if cNode.Type == types.NodeEdge && (cNode.NATType != types.NatTypeNo.String() && cNode.NATType != types.NatTypeFullCone.String() && cNode.NATType != types.NatTypeUnknown.String()) {
			continue
		}

		// log.Infof("getDownloadSources %s, source:%s %s; %s \n", hash, cNode.NodeID, cNode.Type.String(), cNode.NATType)

		source := &types.SourceDownloadInfo{
			NodeID:  nodeID,
			Address: cNode.DownloadAddr(),
		}

		sources = append(sources, source)

		if len(sources) > limit {
			break
		}
	}

	return sources
}

// chooseCandidateNodes selects candidate nodes to pull asset replicas
func (m *Manager) chooseCandidateNodes(count int, filterNodes []string, size float64) (map[string]*node.Node, string) {
	// _, nodes := m.nodeMgr.GetAllCandidateNodes()
	// curNode := len(nodes)

	str := fmt.Sprintf("need node:%d , filter node:%d , randNum : ", count, len(filterNodes))

	selectMap := make(map[string]*node.Node)
	if count <= 0 {
		return selectMap, str
	}

	filterMap := make(map[string]struct{})
	for _, nodeID := range filterNodes {
		filterMap[nodeID] = struct{}{}
	}

	num := count * selectNodeRetryLimit
	nodes := m.nodeMgr.GetRandomCandidates(num)
	for nodeID := range nodes {
		node := m.nodeMgr.GetCandidateNode(nodeID)
		if node == nil {
			continue
		}

		nodeID := node.NodeID

		if !node.DiskEnough(size) {
			log.Infof("chooseCandidateNodes %s DiskEnough...", nodeID)
			continue
		}

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
		if !node.DiskEnough(size) {
			log.Debugf("chooseEdgeNodes node %s disk residual n.DiskUsage:%.2f, n.DiskSpace:%.2f, AvailableDiskSpace:%.2f, TitanDiskUsage:%.2f", node.NodeID, node.DiskUsage, node.DiskSpace, node.AvailableDiskSpace, node.TitanDiskUsage)
			return false
		}

		if node.NetFlowDownExcess(size) {
			log.Debugf("chooseEdgeNodes node %s net flow excess n.NetFlowDown:%d, n.DownloadTraffic:%d", node.NodeID, node.NetFlowDown, node.DownloadTraffic)
			return false
		}

		if _, exist := selectMap[nodeID]; exist {
			return false
		}

		// pCount, err := m.nodeMgr.GetNodePullingCount(m.nodeMgr.ServerID, node.NodeID, PullingStates)
		// if err != nil || pCount > 0 {
		// 	log.Debugf("chooseEdgeNodes node %s pull count:%d , err:%v", node.NodeID, pCount, err)
		// 	return false
		// }

		bandwidthDown -= int64(node.BandwidthDown)
		selectMap[nodeID] = node
		if len(selectMap) >= count && bandwidthDown <= 0 {
			return true
		}

		if len(selectMap) >= AssetEdgeReplicasLimit {
			return true
		}

		return false
	}

	m.sortEdges = !m.sortEdges
	// If count is greater or equal to the difference between total edge nodes and the filterNodes length,
	// choose all unfiltered nodes
	// if count >= m.nodeMgr.Edges-len(filterNodes) {
	// choose all
	if m.sortEdges {
		nodes := m.nodeMgr.GetResourceEdgeNodes()
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].TitanDiskUsage < nodes[j].TitanDiskUsage
		})

		for _, node := range nodes {
			// node := nodes[i]
			if selectNodes(node) {
				break
			}
		}
	} else {
		// choose random
		nodes := m.nodeMgr.GetRandomEdges(count * selectNodeRetryLimit)
		str = fmt.Sprintf("%s%v,", str, nodes)
		for nodeID := range nodes {
			node := m.nodeMgr.GetEdgeNode(nodeID)
			if selectNodes(node) {
				break
			}
		}
	}

	return selectMap, str
}

// GetAssetCount get asset count
func (m *Manager) GetAssetCount() (int, error) {
	return m.LoadAssetCount(m.nodeMgr.ServerID, Remove.String())
}

func (m *Manager) generateTokenForDownloadSources(sources []*types.SourceDownloadInfo, titanRsa *titanrsa.Rsa, assetCID string, clientID string, size int64) ([]*types.SourceDownloadInfo, *types.WorkloadRecord, error) {
	workloadList := make([]*types.Workload, 0)
	downloadSources := make([]*types.SourceDownloadInfo, 0, len(sources))

	for _, source := range sources {
		workloadList = append(workloadList, &types.Workload{SourceID: source.NodeID})

		node := m.nodeMgr.GetNode(source.NodeID)
		downloadSource := *source

		if node != nil {
			tk, err := node.EncryptToken(assetCID, clientID, titanRsa, m.nodeMgr.PrivateKey)
			if err != nil {
				continue
			}
			downloadSource.Tk = tk
		}

		downloadSources = append(downloadSources, &downloadSource)
	}

	record := &types.WorkloadRecord{
		WorkloadID: uuid.NewString(),
		AssetCID:   assetCID,
		ClientID:   clientID,
		AssetSize:  size,
		Event:      types.WorkloadEventPull,
		Status:     types.WorkloadStatusCreate,
	}

	if len(workloadList) > 0 {
		buffer := &bytes.Buffer{}
		enc := gob.NewEncoder(buffer)
		err := enc.Encode(workloadList)
		if err != nil {
			log.Errorf("encode error:%s", err.Error())
			return downloadSources, record, nil
		}

		record.Workloads = buffer.Bytes()
	}

	return downloadSources, record, nil
}

func (m *Manager) generateToken(assetCID string, sources []*types.SourceDownloadInfo, node *node.Node, size int64, titanRsa *titanrsa.Rsa) ([]*types.SourceDownloadInfo, *types.WorkloadRecord, error) {
	ts := make([]*types.SourceDownloadInfo, 0)
	if len(sources) > 3 {
		index1 := rand.Intn(len(sources))
		ts = append(ts, sources[index1])

		index2 := rand.Intn(len(sources))
		ts = append(ts, sources[index2])

		index3 := rand.Intn(len(sources))
		ts = append(ts, sources[index3])
	} else {
		ts = sources
	}

	return m.generateTokenForDownloadSources(ts, titanRsa, assetCID, node.NodeID, size)
}
