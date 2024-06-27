package node

import (
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
)

func (m *Manager) startNodePenaltyTimer() {
	offset := time.Minute * 10
	time.Sleep(offset)

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

	for _, info := range list {
		if m.GetNode(info.NodeID) != nil {
			continue
		}

		if info.DeactivateTime > 0 {
			continue
		}

		if info.Profit <= 0 {
			continue
		}

		pn := m.CalculatePenalty(info.NodeID, info.Profit, (info.OfflineDuration + 1))
		if pn > info.Profit {
			pn = info.Profit
		}

		offlineNodes[info.NodeID] = pn
	}

	if len(offlineNodes) > 0 {
		err := m.UpdateNodePenalty(offlineNodes)
		if err != nil {
			log.Errorf("UpdateNodePenalty err:%s", err.Error())
		}
	}
}
