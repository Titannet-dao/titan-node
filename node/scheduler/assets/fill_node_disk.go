package assets

import (
	"context"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
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
	rows, err := m.LoadAssetRecords([]string{EdgesFailed.String()}, 1, 0, m.nodeMgr.ServerID)
	if err != nil {
		log.Errorf("autoRestartAssetReplicas LoadAssetRecords err:%s", err.Error())
		return false
	}
	defer rows.Close()

	// loading assets to local
	for rows.Next() {
		cInfo := &types.AssetRecord{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("asset StructScan err: %s", err.Error())
			return false
		}

		err = m.RestartPullAssets([]types.AssetHash{types.AssetHash(cInfo.Hash)})
		if err != nil {
			log.Errorf("autoRestartAssetReplicas RestartPullAssets err:%s", err.Error())
			return false
		}

		return true
	}

	return false
}

func (m *Manager) fillDiskTasks() error {
	if m.getPullingAssetLen() >= m.getAssetPullTaskLimit() {
		log.Infof("The asset in the pulling exceeds the limit %d, please wait", m.getAssetPullTaskLimit())
		return nil
	}

	//
	if m.autoRefillAssetReplicas() {
		return nil
	}

	if m.autoRestartAssetReplicas() {
		return nil
	}

	// download data from aws
	list, err := m.ListAWSData(1)
	if err != nil {
		return err
	}

	dataLen := len(list)
	if dataLen == 0 {
		return nil
	}

	nodes := m.nodeMgr.GetCandidateNodes(1, true)
	for i, node := range nodes {
		if dataLen > i {
			info := list[i]
			log.Infof("PullAssetFromAWS %s : %s", node.NodeID, info.Bucket)

			err = node.PullAssetFromAWS(context.Background(), info.Bucket, "")
			if err != nil {
				log.Errorf("PullAssetFromAWS %s err:%s", node.NodeID, err.Error())
				continue
			}

			info.IsDistribute = true
			err = m.UpdateAWSData(info)
			if err != nil {
				log.Errorf("UpdateAWSData %s err:%s", node.NodeID, err.Error())
				continue
			}

		}
	}

	return nil
}
