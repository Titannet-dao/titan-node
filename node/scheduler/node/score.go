package node

import (
	"time"
)

const (
	onlineScoreRatio = 100.0

	scoreErr = "Invalid score"
)

func (m *Manager) startCalcScoreTimer() {
	ticker := time.NewTicker(calcScoreTime)
	defer ticker.Stop()

	for {
		<-ticker.C

		// m.calcScores()
	}
}

func (m *Manager) getScoreLevel(score int) int {
	for i := 0; i < len(nodeScoreLevel); i++ {
		value := nodeScoreLevel[i]
		if value >= score {
			return i
		}
	}

	return 0
}

func (m *Manager) getNodeScoreLevel(node *Node) int {
	return m.getScoreLevel(int(onlineScoreRatio * node.OnlineRate))
}
