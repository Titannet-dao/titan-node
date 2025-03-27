package assets

import (
	"context"
	"math/rand"
	"sort"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
)

const (
	// Interval to dispatch tasks to populate node disks
	fillDiskInterval = 2 * time.Minute
)

// initFillDiskTimer dispatch tasks to populate node disks
func (m *Manager) initFillDiskTimer() {
	ticker := time.NewTicker(fillDiskInterval)
	defer ticker.Stop()

	for {
		<-ticker.C
		if m.fillSwitch {
			m.fillDiskTasks()
		}
	}
}

// StartFillDiskTimer open
func (m *Manager) StartFillDiskTimer() {
	m.fillSwitch = true
}

// StopFillDiskTimer close
func (m *Manager) StopFillDiskTimer() {
	m.fillSwitch = false
}

func (m *Manager) autoRestartAssetReplicas(isStorage bool) bool {
	list, err := m.LoadAssetRecords([]string{SeedFailed.String(), CandidatesFailed.String()}, m.nodeMgr.ServerID)
	if err != nil {
		log.Errorf("autoRestartAssetReplicas LoadAssetRecords err:%s", err.Error())
		return false
	}

	if len(list) < 1 {
		list, err = m.LoadAssetRecords([]string{EdgesFailed.String()}, m.nodeMgr.ServerID)
		if err != nil {
			log.Errorf("autoRestartAssetReplicas LoadAssetRecords err:%s", err.Error())
			return false
		}

		if len(list) < 1 {
			return false
		}
	}

	count := 5

	if isStorage {
		tList := make([]*types.AssetRecord, 0)
		for _, info := range list {
			if info.Source == int64(AssetSourceStorage) {
				tList = append(tList, info)
			}
		}

		if len(tList) > 0 {
			list = tList
		}
	}

	if len(list) < count {
		count = len(list)
	}

	rand.Shuffle(len(list), func(i, j int) {
		list[i], list[j] = list[j], list[i]
	})

	randomElements := list[:count]
	if len(randomElements) == 0 {
		return false
	}

	for _, randomElement := range randomElements {
		err = m.RestartPullAssets([]types.AssetHash{types.AssetHash(randomElement.Hash)})
		if err != nil {
			log.Errorf("autoRestartAssetReplicas RestartPullAssets err:%s", err.Error())
		}
	}

	return true
}

func (m *Manager) pullAssetFromIPFS() bool {
	if m.nodeMgr.Candidates < 1 {
		return false
	}

	// download data from aws
	list, err := m.ListAssetData(10, 0)
	if err != nil {
		return false
	}

	if len(list) == 0 {
		return false
	}

	for _, info := range list {
		info.Status = 1

		err = m.CreateAssetPullTask(&types.PullAssetInfo{
			CID:        info.Cid,
			Replicas:   info.Replicas,
			Expiration: time.Now().Add(3 * 360 * 24 * time.Hour),
			Hash:       info.Hash,
			Owner:      info.Owner,
		})
		if err != nil {
			log.Errorf("awsTask pullAssetFromIPFS CreateAssetPullTask %s err:%s", info.Cid, err.Error())
			info.Status = 2
		}

		err = m.UpdateAssetData(info)
		if err != nil {
			log.Errorf("pullAssetFromIPFS UpdateAssetData err:%s", err.Error())
		}
	}

	return true
}

func (m *Manager) pullAssetFromAWSs() bool {
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

			err = m.CreateAssetPullTask(&types.PullAssetInfo{
				CID:        task.Cid,
				Replicas:   int64(task.Replicas),
				Expiration: time.Now().Add(3 * 360 * 24 * time.Hour),
				Hash:       hash,
				Bucket:     task.Bucket,
				SeedNodeID: task.NodeID,
			})
			if err != nil {
				log.Errorf("awsTask CreateAssetPullTask %s err:%s", task.Cid, err.Error())
				return false
			}
		}

		return true
	}

	// if m.nodeMgr.Candidates < 2 {
	// 	return false
	// }

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
	candidateCount := int64(m.candidateReplicaCount)

	m.updateFillAssetInfo(info.Bucket, candidateCount, info.Replicas)

	go m.requestNodePullAsset(info.Bucket, info.Cid, candidateCount, info.Size)

	return true
}

func (m *Manager) fillDiskTasks() {
	pullList := m.getPullingAssetList()
	limitCount := m.assetPullTaskLimit
	if len(pullList) >= limitCount {
		log.Infof("awsTask , The asset in the pulling exceeds the limit %d, please wait", limitCount)
		for _, p := range pullList {
			log.Debugf("awsTask , pull hash %s", p)
		}
		return
	}

	log.Infof("awsTask, edge count : %d ,pullCount : %d limitCount : %d", m.nodeMgr.Edges, len(pullList), limitCount)

	if m.pullAssetFromIPFS() {
		return
	}

	if m.autoRestartAssetReplicas(true) {
		return
	}

	if m.pullAssetFromAWSs() {
		return
	}

	m.autoRestartAssetReplicas(false)

	return
}

func (m *Manager) requestNodePullAsset(bucket, cid string, candidateCount int64, size float64) {
	_, nodes := m.nodeMgr.GetResourceCandidateNodes()
	if len(nodes) == 0 {
		nodes = m.nodeMgr.GetResourceEdgeNodes()
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].TitanDiskUsage < nodes[j].TitanDiskUsage
	})

	cCount := 0
	for _, node := range nodes {
		if cCount >= int(candidateCount) {
			break
		}

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

// UpdateFillAssetResponseCount update pull result from aws
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
		info.NodeID = nodeID
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
	NodeID        string
}
