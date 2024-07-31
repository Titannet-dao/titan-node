package node

import (
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
)

func (m *Manager) startNodePenaltyTimer() {
	time.Sleep(penaltyFreeTime)

	ticker := time.NewTicker(penaltyInterval)
	defer ticker.Stop()

	for {
		<-ticker.C

		m.penaltyNode()
	}
}

func (m *Manager) penaltyNode() {
	list, err := m.LoadNodeInfosOfType(int(types.NodeCandidate))
	if err != nil {
		log.Errorf("LoadNodeInfosOfType err:%s", err.Error())
		return
	}

	offlineNodes := make(map[string]float64)
	detailsList := make([]*types.ProfitDetails, 0)

	for _, info := range list {
		if m.GetNode(info.NodeID) != nil {
			continue
		}

		if info.DeactivateTime > 0 {
			continue
		}

		if info.Profit <= 0.0001 {
			continue
		}

		dInfo := m.CalculatePenalty(info.NodeID, info.Profit, (info.OfflineDuration + 1))
		if dInfo != nil {
			detailsList = append(detailsList, dInfo)
		}

		offlineNodes[info.NodeID] = 0
	}

	if len(offlineNodes) > 0 {
		err := m.UpdateNodePenalty(offlineNodes)
		if err != nil {
			log.Errorf("UpdateNodePenalty err:%s", err.Error())
		}
	}

	if len(detailsList) > 0 {
		for _, data := range detailsList {
			err := m.AddNodeProfit(data)
			if err != nil {
				log.Errorf("AddNodeProfit %s,%d, %.4f err:%s", data.NodeID, data.PType, data.Profit, err.Error())
			}
		}
	}
}
