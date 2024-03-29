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
	fillDiskInterval = 5 * time.Minute
)

// initFillDiskTimer dispatch tasks to populate node disks
func (m *Manager) initFillDiskTimer() {
	ticker := time.NewTicker(fillDiskInterval)
	defer ticker.Stop()

	edgeCount := m.getFillAssetEdgeCount()
	candidateCount := int64(m.GetCandidateReplicaCount())

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

func (m *Manager) autoRefillAssetReplicas(edgeCount int64) bool {
	info, err := m.LoadNeedRefillAssetRecords(m.nodeMgr.ServerID, edgeCount, Servicing.String())
	if err != nil {
		log.Errorf("autoRefillAssetReplicas LoadNeedRefillAssetRecords err:%s", err.Error())
		return false
	}

	if info == nil {
		return false
	}

	info.NeedEdgeReplica = edgeCount

	// do replenish replicas
	err = m.replenishAssetReplicas(info, 0, string(m.nodeMgr.ServerID), info.Note, CandidatesSelect, "")
	if err != nil {
		log.Errorf("autoRefillAssetReplicas replenishAssetReplicas err: %s", err.Error())
		return false
	}

	return true
}

func (m *Manager) autoRestartAssetReplicas() bool {
	m.isPullSpecifyAsset = !m.isPullSpecifyAsset

	list, err := m.LoadAWSRecords([]string{EdgesFailed.String()}, m.nodeMgr.ServerID)
	if err != nil {
		log.Errorf("autoRestartAssetReplicas LoadAssetRecords err:%s", err.Error())
		return false
	}

	if len(list) < 1 {
		return false
	}

	if m.isPullSpecifyAsset {
		tList := make([]*types.AssetRecord, 0)
		for _, info := range list {
			if info.TotalSize < units.GiB {
				tList = append(tList, info)
			}
		}

		if len(tList) > 0 {
			list = tList
		}
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
				Expiration: time.Now().Add(360 * 24 * time.Hour),
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

	go m.requestNodePullAsset(info.Bucket, info.Cid, info.Size, edgeCount, candidateCount)

	return true
}

func (m *Manager) fillDiskTasks(edgeCount, candidateCount int64) {
	pullCount := m.getPullingAssetLen()
	limitCount := m.getAssetPullTaskLimit()
	if pullCount >= limitCount {
		log.Infof("The asset in the pulling exceeds the limit %d, please wait", limitCount)
		return
	}

	log.Infof("awsTask, edge count : %d , limit count : %d pullCount : %d limitCount : %d", m.nodeMgr.Edges, edgeCount, pullCount, limitCount)

	if m.pullAssetFromAWSs(edgeCount, candidateCount) {
		return
	}

	if m.autoRefillAssetReplicas(edgeCount) {
		return
	}

	if m.autoRestartAssetReplicas() {
		return
	}

	return
}

func (m *Manager) requestNodePullAsset(bucket, cid string, size float64, edgeCount, candidateCount int64) {
	cNodes := m.nodeMgr.GetCandidateNodes(m.nodeMgr.Candidates, true)
	sort.Slice(cNodes, func(i, j int) bool {
		return cNodes[i].TitanDiskUsage < cNodes[j].TitanDiskUsage
	})

	cCount := 0
	for i := 0; i < len(cNodes); i++ {
		if cCount >= int(candidateCount) {
			break
		}
		node := cNodes[i]

		if exist, err := m.checkAssetIfExist(node, cid); err != nil {
			// log.Warnf("requestNodePullAsset checkAssetIfExist error %s", err)
			continue
		} else if exist {
			continue
		}

		if err := m.pullAssetFromAWS(node, bucket); err == nil {
			// log.Warnf("requestNodePullAsset pullAssetFromAWS error %s", err.Error())
			cCount++
		}
	}

	// eNodes := m.nodeMgr.GetAllEdgeNode()
	// sort.Slice(eNodes, func(i, j int) bool {
	// 	return eNodes[i].TitanDiskUsage < eNodes[j].TitanDiskUsage
	// })

	eCount := 0
	// for i := 0; i < len(eNodes); i++ {
	// 	if eCount >= int(edgeCount) {
	// 		break
	// 	}
	// 	node := eNodes[i]

	// 	if !node.DiskEnough(size) {
	// 		continue
	// 	}

	// 	if node.PullAssetCount > 0 {
	// 		continue
	// 	}
	// 	// pCount, err := m.nodeMgr.GetNodePullingCount(node.NodeID)
	// 	// if err != nil || pCount > 0 {
	// 	// 	continue
	// 	// }

	// 	if exist, err := m.checkAssetIfExist(node, cid); err != nil {
	// 		// log.Warnf("requestNodePullAsset checkAssetIfExist error %s", err)
	// 		continue
	// 	} else if exist {
	// 		continue
	// 	}

	// 	if err := m.pullAssetFromAWS(node, bucket); err == nil {
	// 		// log.Warnf("requestNodePullAsset pullAssetFromAWS error %s", err.Error())
	// 		eCount++
	// 	}
	// }

	m.updateFillAssetInfo(bucket, int64(cCount+eCount), 0)
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

func (m *Manager) getNodesFromFillAsset(cid string) *fillAssetNodeInfo {
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
		Expiration: time.Now().Add(2 * time.Hour),
		Bucket:     bucket,
		Replicas:   replica,
	}

	infoI, exist := m.fillAssets.Load(bucket)
	if exist && infoI != nil {
		info = infoI.(*fillAssetInfo)
	}

	info.TotalCount = count

	m.fillAssets.Store(bucket, info)
}

func (m *Manager) UpdateFillAssetResponseCount(bucket, cid, nodeID string) {
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
}

type fillAssetNodeInfo struct {
	candidateList []*node.Node
	edgeList      []*node.Node
}
