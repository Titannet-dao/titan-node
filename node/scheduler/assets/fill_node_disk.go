package assets

import (
	"context"
	"math/rand"
	"sort"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/docker/go-units"
)

const (
	// Interval to dispatch tasks to populate node disks
	fillDiskInterval = 2 * time.Minute
)

// initFillDiskTimer dispatch tasks to populate node disks
func (m *Manager) initFillDiskTimer() {
	ticker := time.NewTicker(fillDiskInterval)
	defer ticker.Stop()

	edgeCount := m.getFillAssetEdgeCount()
	candidateCount := int64(m.candidateReplicaCount)

	for {
		<-ticker.C
		if m.fillSwitch {
			m.fillDiskTasks(edgeCount, candidateCount)
		}
	}
}

func (m *Manager) getFillAssetEdgeCount() int64 {
	cfg, err := m.config()
	if err != nil {
		return 4000
	}

	return cfg.FillAssetEdgeCount
}

func (m *Manager) StartFillDiskTimer() {
	m.fillSwitch = true
}

func (m *Manager) StopFillDiskTimer() {
	m.fillSwitch = false
}

func (m *Manager) autoRefillAssetReplicas() bool {
	fillCount := int64(200)

	info, err := m.LoadNeedRefillAssetRecords(m.nodeMgr.ServerID, fillCount, Servicing.String())
	if err != nil {
		log.Errorf("autoRefillAssetReplicas LoadNeedRefillAssetRecords err:%s", err.Error())
		return false
	}

	if info == nil || info.Source == int64(types.AssetSourceAWS) {
		return false
	}

	info.NeedEdgeReplica = info.NeedEdgeReplica * 2
	if info.NeedEdgeReplica > fillCount {
		info.NeedEdgeReplica = fillCount
	}

	// do replenish replicas
	err = m.replenishAssetReplicas(info, 0, info.Note, "", CandidatesSelect, "")
	if err != nil {
		log.Errorf("autoRefillAssetReplicas replenishAssetReplicas err: %s", err.Error())
		return false
	}

	return true
}

func (m *Manager) autoRestartAssetReplicas(isStorage bool) bool {
	list, err := m.LoadRecords([]string{SeedFailed.String(), CandidatesFailed.String(), EdgesFailed.String()}, m.nodeMgr.ServerID)
	if err != nil {
		log.Errorf("autoRestartAssetReplicas LoadAssetRecords err:%s", err.Error())
		return false
	}

	if len(list) < 1 {
		return false
	}

	tList := make([]*types.AssetRecord, 0)
	if isStorage {
		for _, info := range list {
			if info.Source == int64(AssetSourceStorage) {
				tList = append(tList, info)
			}
		}

		list = tList

	} else {
		m.isPullSpecifyAsset = !m.isPullSpecifyAsset

		tList := make([]*types.AssetRecord, 0)
		for _, info := range list {
			if info.Source != int64(types.AssetSourceAWS) {
				continue
			}

			if m.isPullSpecifyAsset && info.TotalSize > units.GiB {
				continue
			}

			tList = append(tList, info)
		}

		if len(tList) > 0 {
			list = tList
		}
	}

	if len(list) == 0 {
		return false
	}

	index := rand.Intn(len(list))
	randomElement := list[index]

	err = m.RestartPullAssets([]types.AssetHash{types.AssetHash(randomElement.Hash)})
	if err != nil {
		log.Errorf("autoRestartAssetReplicas RestartPullAssets err:%s", err.Error())
		return false
	}

	return true
}

func (m *Manager) pullAssetFromAWSs(edgeCount, candidateCount int64) bool {
	task := m.getPullAssetTask()
	if task != nil {
		log.Infof("awsTask cur task : %s , count: %d/%d ,cid:%s", task.Bucket, task.ResponseCount, task.TotalCount, task.Cid)
		if float64(task.ResponseCount) >= float64(task.TotalCount)*0.9 || task.Expiration.Before(time.Now()) {
			defer m.fillAssets.Delete(task.Bucket)

			// is ok
			// start asset task
			hash, err := cidutil.CIDToHash(task.Cid)
			if err != nil {
				log.Errorf("awsTask CIDToHash %s err:%s", task.Cid, err.Error())
				return false
			}

			err = m.CreateAssetPullTask(&types.PullAssetReq{
				CID:        task.Cid,
				Replicas:   int64(task.Replicas),
				Expiration: time.Now().Add(3 * 360 * 24 * time.Hour),
				Hash:       hash,
				Bucket:     task.Bucket,
			})
			if err != nil {
				log.Errorf("awsTask CreateAssetPullTask %s err:%s", task.Cid, err.Error())
				return false
			}
		}

		return true
	}

	if m.nodeMgr.Edges < int(edgeCount) {
		return false
	}

	// download data from aws
	list, err := m.ListAWSData(1, 0, false)
	if err != nil {
		return false
	}

	dataLen := len(list)
	if dataLen == 0 {
		return false
	}

	info := list[0]

	info.IsDistribute = true
	err = m.UpdateAWSData(info)
	if err != nil {
		log.Errorf("pullAssetFromAWS UpdateAWSData err:%s", err.Error())
		return false
	}

	// edgeCount = int64(info.Replicas)

	m.updateFillAssetInfo(info.Bucket, candidateCount, info.Replicas)

	go m.requestNodePullAsset(info.Bucket, info.Cid, candidateCount, info.Size)

	return true
}

func (m *Manager) fillDiskTasks(edgeCount, candidateCount int64) {
	pullList := m.getPullingAssetList()
	limitCount := m.assetPullTaskLimit
	if len(pullList) >= limitCount {
		log.Infof("awsTask , The asset in the pulling exceeds the limit %d, please wait", limitCount)
		for _, p := range pullList {
			log.Debugf("awsTask , pull hash %s", p)
		}
		return
	}

	log.Infof("awsTask, edge count : %d , limit count : %d pullCount : %d limitCount : %d", m.nodeMgr.Edges, edgeCount, len(pullList), limitCount)

	m.autoRestartAssetReplicas(true)

	if m.autoRefillAssetReplicas() {
		return
	}

	m.pullAssetFromAWSs(edgeCount, candidateCount)

	if m.autoRestartAssetReplicas(false) {
		return
	}

	return
}

func (m *Manager) requestNodePullAsset(bucket, cid string, candidateCount int64, size float64) {
	_, nodes := m.nodeMgr.GetAllCandidateNodes()

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Type == nodes[j].Type {
			return nodes[i].TitanDiskUsage < nodes[j].TitanDiskUsage
		}

		return nodes[i].Type == types.NodeCandidate
	})

	cCount := 0
	for _, node := range nodes {
		if cCount >= int(candidateCount) {
			break
		}

		// Merge L1 nodes
		// if node.Type == types.NodeValidator {
		// 	continue
		// }

		if !node.DiskEnough(size) {
			continue
		}

		if exist, err := m.checkAssetIfExist(node, cid); err != nil {
			// log.Warnf("requestNodePullAsset checkAssetIfExist error %s", err)
			continue
		} else if exist {
			continue
		}

		if err := m.pullAssetFromAWS(node, bucket); err == nil {
			log.Infof("awsTask pullAssetFromAWS node %s %s", node.NodeID, bucket)
			cCount++
		}
	}

	m.updateFillAssetInfo(bucket, int64(cCount), 0)
}

func (m *Manager) pullAssetFromAWS(node *node.Node, bucket string) error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	return node.PullAssetFromAWS(ctx, bucket, "")
}

func (m *Manager) checkAssetIfExist(node *node.Node, cid string) (bool, error) {
	if cid == "" {
		return false, nil
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	pullResult, err := node.GetAssetProgresses(ctx, []string{cid})
	if err != nil {
		return false, err
	}

	for _, asset := range pullResult.Progresses {
		if asset.Status == types.ReplicaStatusSucceeded {
			return true, nil
		}
	}

	return false, nil
}

func (m *Manager) storeNodeToFillAsset(cid string, nNode *node.Node) {
	info := &fillAssetNodeInfo{
		edgeList:      make([]*node.Node, 0),
		candidateList: make([]*node.Node, 0),
	}

	infoI, exist := m.fillAssetNodes.LoadOrStore(cid, info)
	if exist && infoI != nil {
		info = infoI.(*fillAssetNodeInfo)
	}

	if nNode.Type == types.NodeEdge {
		info.edgeList = append(info.edgeList, nNode)
	} else {
		info.candidateList = append(info.candidateList, nNode)
	}

	m.fillAssetNodes.Store(cid, info)
}

func (m *Manager) getNodesFromAWSAsset(cid string) *fillAssetNodeInfo {
	infoI, exist := m.fillAssetNodes.Load(cid)
	if exist && infoI != nil {
		info := infoI.(*fillAssetNodeInfo)
		return info
	}

	return nil
}

func (m *Manager) removeNodesFromFillAsset(cid string, cleanCandidate bool) {
	if !cleanCandidate {
		m.fillAssetNodes.Delete(cid)
		return
	}

	infoI, exist := m.fillAssetNodes.Load(cid)
	if exist && infoI != nil {
		info := infoI.(*fillAssetNodeInfo)
		if info != nil {
			info.candidateList = nil
			m.fillAssetNodes.Store(cid, info)
		}
	}
}

func (m *Manager) updateFillAssetInfo(bucket string, count int64, replica int) {
	info := &fillAssetInfo{
		Expiration: time.Now().Add(30 * time.Minute),
		Bucket:     bucket,
		Replicas:   replica,
		CreateTime: time.Now(),
	}

	infoI, exist := m.fillAssets.Load(bucket)
	if exist && infoI != nil {
		info = infoI.(*fillAssetInfo)
	}

	info.TotalCount = count

	m.fillAssets.Store(bucket, info)
}

func (m *Manager) UpdateFillAssetResponseCount(bucket, cid, nodeID string, size int64) {
	infoI, exist := m.fillAssets.Load(bucket)
	if !exist || infoI == nil {
		return
	}

	info := infoI.(*fillAssetInfo)
	if info == nil || info.Bucket == "" {
		return
	}

	info.ResponseCount++

	if cid != "" {
		info.Cid = cid

		node := m.nodeMgr.GetNode(nodeID)
		if node != nil {
			m.storeNodeToFillAsset(cid, node)
		}

		// // workload
		// wID := m.createSeedWorkload(AssetPullingInfo{CID: cid, Size: size, Source: AssetSourceAWS}, nodeID)
		// costTime := int64(time.Since(info.CreateTime) / time.Millisecond)

		// m.workloadMgr.PushResult(&types.WorkloadRecordReq{AssetCID: cid, WorkloadID: wID, Workloads: []types.Workload{{SourceID: types.DownloadSourceAWS.String(), DownloadSize: size, CostTime: costTime}}}, nodeID)
	}

	m.fillAssets.Store(bucket, info)
}

func (m *Manager) getPullAssetTask() *fillAssetInfo {
	var fInfo *fillAssetInfo

	m.fillAssets.Range(func(key, value interface{}) bool {
		info := value.(*fillAssetInfo)
		if info == nil || info.Bucket == "" {
			return true
		}

		fInfo = info
		return false
	})

	return fInfo
}

type fillAssetInfo struct {
	TotalCount    int64
	ResponseCount int
	Expiration    time.Time
	Cid           string
	Bucket        string
	Replicas      int
	CreateTime    time.Time
}

type fillAssetNodeInfo struct {
	candidateList []*node.Node
	edgeList      []*node.Node
}
