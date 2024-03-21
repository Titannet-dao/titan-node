package assets

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
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
	rows, err := m.LoadAssetRecords([]string{EdgesFailed.String()}, 10, 0, m.nodeMgr.ServerID)
	if err != nil {
		log.Errorf("autoRestartAssetReplicas LoadAssetRecords err:%s", err.Error())
		return false
	}
	defer rows.Close()

	isPulling := false

	// loading assets to local
	for rows.Next() {
		cInfo := &types.AssetRecord{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("asset StructScan err: %s", err.Error())
			continue
		}

		if cInfo.Note == "" {
			continue
		}

		if !strings.Contains(cInfo.State, "Failed") {
			continue
		}

		err = m.RestartPullAssets([]types.AssetHash{types.AssetHash(cInfo.Hash)})
		if err != nil {
			log.Errorf("autoRestartAssetReplicas RestartPullAssets err:%s", err.Error())
			continue
		}

		isPulling = true
	}

	return isPulling
}

func (m *Manager) pullAssetFromAWSs(edgeCount, candidateCount int64) {
	task := m.getPullAssetTask()
	if task != nil {
		log.Infof("awsTask cur task : %s , count: %d/%d ,cid:%s", task.Bucket, task.ResponseCount, task.TotalCount, task.Cid)
		if float64(task.ResponseCount) >= float64(task.TotalCount)*0.6 || task.Expiration.Before(time.Now()) {
			defer m.fillAssets.Delete(task.Bucket)

			// is ok
			// start asset task
			hash, err := cidutil.CIDToHash(task.Cid)
			if err != nil {
				log.Errorf("awsTask CIDToHash %s err:%s", task.Cid, err.Error())
				return
			}

			err = m.CreateAssetPullTask(&types.PullAssetReq{
				CID:        task.Cid,
				Replicas:   edgeCount,
				Expiration: time.Now().Add(360 * 24 * time.Hour),
				Hash:       hash,
				UserID:     task.Bucket,
			})
			if err != nil {
				log.Errorf("awsTask CreateAssetPullTask %s err:%s", task.Cid, err.Error())
				return
			}

			return
		}

		return
	}

	if m.nodeMgr.Edges < int(edgeCount) {
		return
	}

	// download data from aws
	list, err := m.ListAWSData(1, 0, false)
	if err != nil {
		log.Warnf("pullAssetFromAWS ListAWSData err:%s", err.Error())
		return
	}

	dataLen := len(list)
	if dataLen == 0 {
		return
	}

	info := list[0]

	info.IsDistribute = true
	err = m.UpdateAWSData(info)
	if err != nil {
		log.Errorf("pullAssetFromAWS UpdateAWSData err:%s", err.Error())
		return
	}

	m.updateFillAssetInfo(info.Bucket, candidateCount+edgeCount)

	go m.requestNodePullAsset(info.Bucket, info.Cid, info.Size, edgeCount, candidateCount)
}

func (m *Manager) fillDiskTasks(edgeCount, candidateCount int64) {
	pullCount := m.getPullingAssetLen()
	limitCount := m.getAssetPullTaskLimit()
	if pullCount >= limitCount {
		log.Infof("The asset in the pulling exceeds the limit %d, please wait", limitCount)
		return
	}

	log.Infof("awsTask cur task is nil, edge count : %d , limit count : %d pullCount : %d limitCount : %d", m.nodeMgr.Edges, edgeCount, pullCount, limitCount)

	m.pullAssetFromAWSs(edgeCount, candidateCount)

	if m.autoRefillAssetReplicas(edgeCount) {
		return
	}

	// if m.autoRestartAssetReplicas() {
	// 	return
	// }

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

	eNodes := m.nodeMgr.GetAllEdgeNode()
	sort.Slice(eNodes, func(i, j int) bool {
		return eNodes[i].TitanDiskUsage < eNodes[j].TitanDiskUsage
	})

	eCount := 0
	for i := 0; i < len(eNodes); i++ {
		if eCount >= int(edgeCount) {
			break
		}
		node := eNodes[i]

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
			// log.Warnf("requestNodePullAsset pullAssetFromAWS error %s", err.Error())
			eCount++
		}
	}
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

func (m *Manager) updateFillAssetInfo(bucket string, count int64) {
	info := &fillAssetInfo{
		Expiration: time.Now().Add(2 * time.Hour),
		Bucket:     bucket,
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
}

type fillAssetNodeInfo struct {
	candidateList []*node.Node
	edgeList      []*node.Node
}
