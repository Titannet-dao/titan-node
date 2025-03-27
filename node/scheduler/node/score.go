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

// func (m *Manager) calcScores() {
// 	_, nodes := m.GetResourceCandidateNodes()

// 	end := time.Now()

// 	startMinute := end.Add(-5 * time.Minute)
// 	startWeek := end.Add(-7 * 24 * time.Hour)
// 	startDay := end.Add(-24 * time.Hour)
// 	startHour := end.Add(-time.Hour)

// 	alpha := 0.6

// 	for _, node := range nodes {
// 		// TODO get bandwidth from node
// 		bandwidthUpNode := node.BandwidthUp
// 		bandwidthDownNode := node.BandwidthDown

// 		// get bandwidth from server
// 		bandwidthUps, err := m.LoadBandwidthUpFromRetrieve(node.NodeID, startMinute, end)
// 		if err != nil {
// 			log.Errorf("calcScores LoadBandwidthUpFromRetrieve err:%s", err.Error())
// 			continue
// 		}

// 		bandwidthDowns, err := m.LoadBandwidthDownFromReplica(node.NodeID, startMinute, end)
// 		if err != nil {
// 			log.Errorf("calcScores LoadBandwidthDownFromReplica err:%s", err.Error())
// 			continue
// 		}

// 		bs, err := m.LoadBandwidthDownFromValidation(node.NodeID, startMinute, end)
// 		if err != nil {
// 			log.Errorf("calcScores LoadBandwidthDownFromValidation err:%s", err.Error())
// 			continue
// 		}

// 		for _, b := range bs {
// 			bandwidthDowns = append(bandwidthDowns, int64(b))
// 		}

// 		bandwidthUpServer := calculateMean(bandwidthUps)
// 		bandwidthDownServer := calculateMean(bandwidthDowns)

// 		bandwidthUp := mergeBandwidth(float64(bandwidthUpNode), bandwidthUpServer, alpha)
// 		bandwidthDown := mergeBandwidth(float64(bandwidthDownNode), bandwidthDownServer, alpha)

// 		historyBandwidthUps, err := m.LoadBandwidthScores(node.NodeID, startWeek, end, "bandwidth_up")
// 		if err != nil {
// 			log.Errorf("calcScores LoadBandwidthEvents err:%s", err.Error())
// 			continue
// 		}

// 		historyBandwidthDowns, err := m.LoadBandwidthScores(node.NodeID, startWeek, end, "bandwidth_down")
// 		if err != nil {
// 			log.Errorf("calcScores LoadBandwidthEvents err:%s", err.Error())
// 			continue
// 		}

// 		historyBandwidthUps = append(historyBandwidthUps, bandwidthUp)
// 		historyBandwidthDowns = append(historyBandwidthDowns, bandwidthDown)

// 		// TODO count
// 		upSucceedCount := int64(1)
// 		upTotalCount := int64(1)
// 		curBandwidthUpScore := calcCurScore(bandwidthUp, upSucceedCount, upTotalCount, historyBandwidthUps)
// 		downSucceedCount := int64(1)
// 		downTotalCount := int64(1)
// 		curBandwidthDownScore := calcCurScore(bandwidthDown, downSucceedCount, downTotalCount, historyBandwidthDowns)

// 		historyHourBandwidthUpScore, err := m.LoadBandwidthScores(node.NodeID, startHour, end, "bandwidth_up_score")
// 		if err != nil {
// 			log.Errorf("calcScores LoadBandwidthEvents err:%s", err.Error())
// 			continue
// 		}
// 		historyDayBandwidthUpScore, err := m.LoadBandwidthScores(node.NodeID, startDay, end, "bandwidth_up_score")
// 		if err != nil {
// 			log.Errorf("calcScores LoadBandwidthEvents err:%s", err.Error())
// 			continue
// 		}
// 		historyWeekBandwidthUpScore, err := m.LoadBandwidthScores(node.NodeID, startWeek, end, "bandwidth_up_score")
// 		if err != nil {
// 			log.Errorf("calcScores LoadBandwidthEvents err:%s", err.Error())
// 			continue
// 		}

// 		historyHourBandwidthDownScore, err := m.LoadBandwidthScores(node.NodeID, startHour, end, "bandwidth_down_score")
// 		if err != nil {
// 			log.Errorf("calcScores LoadBandwidthEvents err:%s", err.Error())
// 			continue
// 		}
// 		historyDayBandwidthDownScore, err := m.LoadBandwidthScores(node.NodeID, startDay, end, "bandwidth_down_score")
// 		if err != nil {
// 			log.Errorf("calcScores LoadBandwidthEvents err:%s", err.Error())
// 			continue
// 		}
// 		historyWeekBandwidthDownScore, err := m.LoadBandwidthScores(node.NodeID, startWeek, end, "bandwidth_down_score")
// 		if err != nil {
// 			log.Errorf("calcScores LoadBandwidthEvents err:%s", err.Error())
// 			continue
// 		}

// 		bandwidthUpScore := calcScore(curBandwidthUpScore, historyHourBandwidthUpScore, historyDayBandwidthUpScore, historyWeekBandwidthUpScore)
// 		bandwidthDownScore := calcScore(curBandwidthDownScore, historyHourBandwidthDownScore, historyDayBandwidthDownScore, historyWeekBandwidthDownScore)

// 		node.BandwidthUpScore = bandwidthUpScore
// 		node.BandwidthDownScore = bandwidthDownScore

// 		err = m.SaveBandwidthScore(&types.BandwidthScore{
// 			NodeID: node.NodeID, BandwidthUp: bandwidthUp, BandwidthDown: bandwidthDown,
// 			BandwidthUpNode: bandwidthUpNode, BandwidthDownNode: bandwidthDownNode,
// 			BandwidthUpServer: int64(bandwidthUpServer), BandwidthDownServer: int64(bandwidthDownServer),
// 			BandwidthUpScore: curBandwidthUpScore, BandwidthDownScore: curBandwidthDownScore,
// 			BandwidthUpSucceed: upSucceedCount, BandwidthUpTotal: upTotalCount,
// 			BandwidthDownSucceed: downSucceedCount, BandwidthDownTotal: downTotalCount,
// 			BandwidthUpFinalScore: bandwidthUpScore, BandwidthDownFinalScore: bandwidthDownScore,
// 		})
// 		if err != nil {
// 			log.Errorf("calcScores SaveBandwidthEvent err:%s", err.Error())
// 			continue
// 		}

// 		// log.Infof("calcScores bandwidthDown %s : %d,[%d]", node.NodeID, bandwidthDownScore, curBandwidthDownScore)
// 	}
// }

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
