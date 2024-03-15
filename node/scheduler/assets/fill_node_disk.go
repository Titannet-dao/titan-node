package assets

import (
	"context"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
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

	for {
		<-ticker.C
		if m.fillSwitch {
			err := m.fillDiskTasks()
			if err != nil {
				log.Errorf("fillDiskTasks err:%s", err.Error())
			}
		}
	}
}

func (m *Manager) StartFillDiskTimer() {
	m.fillSwitch = true
}

func (m *Manager) StopFillDiskTimer() {
	m.fillSwitch = false
}

func (m *Manager) autoRefillAssetReplicas() bool {
	num := int64(m.nodeMgr.Edges - 50)
	if num < 1 {
		return false
	}

	batchSize := int64(50)

	info, err := m.LoadNeedRefillAssetRecords(m.nodeMgr.ServerID, num, Servicing.String())
	if err != nil {
		log.Errorf("autoRefillAssetReplicas LoadNeedRefillAssetRecords err:%s", err.Error())
		return false
	}

	if info == nil {
		return false
	}

	inc := info.NeedEdgeReplica
	if inc > batchSize {
		inc = batchSize
	}

	info.NeedEdgeReplica += inc
	if info.NeedEdgeReplica > num {
		info.NeedEdgeReplica = num
	}

	// do replenish replicas
	err = m.replenishAssetReplicas(info, 0, string(m.nodeMgr.ServerID), "", CandidatesSelect, "")
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

		err = m.RestartPullAssets([]types.AssetHash{types.AssetHash(cInfo.Hash)})
		if err != nil {
			log.Errorf("autoRestartAssetReplicas RestartPullAssets err:%s", err.Error())
			continue
		}

		isPulling = true
	}

	return isPulling
}

func (m *Manager) pullAssetFromAWSs() {
	// download data from aws
	list, err := m.ListAWSData(1)
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

	m.requestNodePullAsset(info.Bucket, info.Cid)
	// nodes := m.nodeMgr.GetCandidateNodes(1, true)
	// for i, node := range nodes {
	// 	if dataLen > i {
	// 		info := list[i]
	// 		log.Infof("pullAssetFromAWS %s : %s", node.NodeID, info.Bucket)

	// 		err = node.PullAssetFromAWS(context.Background(), info.Bucket, "")
	// 		if err != nil {
	// 			log.Errorf("pullAssetFromAWS %s PullAssetFromAWS err:%s", node.NodeID, err.Error())
	// 			continue
	// 		}

	// 		info.IsDistribute = true
	// 		err = m.UpdateAWSData(info)
	// 		if err != nil {
	// 			log.Errorf("pullAssetFromAWS UpdateAWSData %s err:%s", node.NodeID, err.Error())
	// 			continue
	// 		}

	// 	}
	// }
}

func (m *Manager) fillDiskTasks() error {
	log.Infoln("fillDiskTasks -----------------")

	if m.getPullingAssetLen() >= m.getAssetPullTaskLimit() {
		log.Infof("The asset in the pulling exceeds the limit %d, please wait", m.getAssetPullTaskLimit())
		return nil
	}

	m.pullAssetFromAWSs()

	//
	// if m.autoRefillAssetReplicas() {
	// 	return nil
	// }

	// if m.autoRestartAssetReplicas() {
	// 	return nil
	// }

	return nil
}

func (m *Manager) requestNodePullAsset(bucket, cid string) {
	cNodes := m.nodeMgr.GetCandidateNodes(m.nodeMgr.Candidates, true)
	count := 0

	for _, node := range cNodes {
		if exist, err := m.checkAssetIfExist(node, cid); err != nil {
			log.Warnf("requestNodePullAsset checkAssetIfExist error %s", err)
			continue
		} else if exist {
			continue
		}

		if err := m.pullAssetFromAWS(node, bucket); err != nil {
			log.Warnf("requestNodePullAsset pullAssetFromAWS error %s", err.Error())
		} else {
			count++
		}
	}

	eNodes := m.nodeMgr.GetAllEdgeNode()

	for _, node := range eNodes {
		if exist, err := m.checkAssetIfExist(node, cid); err != nil {
			log.Warnf("requestNodePullAsset checkAssetIfExist error %s", err)
			continue
		} else if exist {
			continue
		}

		if err := m.pullAssetFromAWS(node, bucket); err != nil {
			log.Warnf("requestNodePullAsset pullAssetFromAWS error %s", err.Error())
		} else {
			count++
		}
	}

	log.Infof("bucket %s , count %d", bucket, count)
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
