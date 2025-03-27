package node

import (
	"math"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
)

func (m *Manager) startBandwidthEventTimer() {
	nextTick := time.Now().Truncate(bandwidthEventTime)
	if nextTick.Before(time.Now()) {
		nextTick = nextTick.Add(bandwidthEventTime)
	}

	time.Sleep(time.Until(nextTick))

	ticker := time.NewTicker(bandwidthEventTime)
	defer ticker.Stop()

	m.saveBandwidthEvent()

	for {
		t := <-ticker.C

		m.saveBandwidthEvent()

		hour := t.Hour()
		if hour == 0 {
			// statistics
			m.statisticsEvent()
		}
	}
}

func (m *Manager) statisticsEvent() {
	bandwidthMap := make(map[string]*types.BandwidthEvent)
	countMap := make(map[string]int64)

	now := time.Now()

	// Calculate yesterday's date
	yesterday := now.AddDate(0, 0, -1)
	// Set yesterday's start time (00:00:00)
	yesterdayStart := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, yesterday.Location())
	// Set yesterday's end time (23:59:59)
	yesterdayEnd := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, yesterday.Location())

	list, err := m.LoadBandwidthEvents(yesterdayStart, yesterdayEnd)
	if err != nil {
		log.Errorf("statisticsEvent LoadBandwidthEvents err:%s", err.Error())
		return
	}

	for _, info := range list {
		nodeID := info.NodeID
		event, ok := bandwidthMap[nodeID]
		if !ok {
			bandwidthMap[nodeID] = info
			countMap[nodeID] = 1
			continue
		}

		if info.BandwidthUpPeak > event.BandwidthUpPeak {
			event.BandwidthUpPeak = info.BandwidthUpPeak
		}

		if info.BandwidthDownPeak > event.BandwidthDownPeak {
			event.BandwidthDownPeak = info.BandwidthDownPeak
		}

		event.TaskSuccess += info.TaskSuccess
		event.TaskTotal += info.TaskTotal
		event.Size += info.Size

		// need average
		event.BandwidthUpFree += info.BandwidthUpFree
		event.BandwidthDownFree += info.BandwidthDownFree
		event.Score += info.Score
		countMap[nodeID]++
	}

	bandwidthList := make([]*types.BandwidthEvent, 0)
	for _, info := range bandwidthMap {
		info.BandwidthUpFree = info.BandwidthUpFree / countMap[info.NodeID]
		info.BandwidthDownFree = info.BandwidthDownFree / countMap[info.NodeID]
		info.Score = info.Score / countMap[info.NodeID]

		info.BandwidthUpLoad = info.BandwidthUpPeak - info.BandwidthUpFree
		info.BandwidthDownLoad = info.BandwidthDownPeak - info.BandwidthDownFree

		info.CreateTime = yesterdayStart

		bandwidthList = append(bandwidthList, info)
	}

	err = m.CleanBandwidthEvent(yesterdayStart, yesterdayEnd)
	if err != nil {
		log.Errorf("CleanBandwidthEvent err:%s", err.Error())
		return
	}

	err = m.SaveBandwidthEvent(bandwidthList)
	if err != nil {
		log.Errorf("SaveBandwidthEvent err:%s", err.Error())
	}
}

func (m *Manager) saveBandwidthEvent() {
	bandwidthList := make([]*types.BandwidthEvent, 0)

	end := time.Now()
	start := end.Add(-time.Hour)

	cList := m.GetAllCandidateNodes()
	for _, node := range cList {
		list, err := m.LoadServiceEventByNode(node.NodeID, start, end)
		if err != nil {
			log.Errorf("LoadServiceEventByNode err:%s", err.Error())
		}
		size := int64(0)
		tCount := int64(0)
		sCount := int64(0)

		for _, info := range list {
			tCount++
			if info.Status == types.ServiceTypeSucceed {
				sCount++
				size += info.Size
			}
		}

		fs := node.CalcQualityScore(sCount, tCount)

		bandwidthList = append(bandwidthList,
			&types.BandwidthEvent{
				NodeID:          node.NodeID,
				BandwidthUpPeak: node.BandwidthUp, BandwidthDownPeak: node.BandwidthDown,
				BandwidthUpFree: node.BandwidthFreeUp, BandwidthDownFree: node.BandwidthFreeDown,
				BandwidthUpLoad:   int64(math.Max(0, float64(node.BandwidthUp-node.BandwidthFreeUp))),
				BandwidthDownLoad: int64(math.Max(0, float64(node.BandwidthDown-node.BandwidthFreeDown))),
				TaskSuccess:       sCount, TaskTotal: tCount,
				Score: fs, Size: size, CreateTime: start,
			})

	}

	err := m.SaveBandwidthEvent(bandwidthList)
	if err != nil {
		log.Errorf("SaveBandwidthEvent err:%s", err.Error())
	}
}

func (m *Manager) getPairsNode() []string {
	nodes, _ := m.GetResourceCandidateNodes()

	nodeCount := len(nodes)
	if nodeCount%2 != 0 {
		nodes = append(nodes, "") // Add a dummy node if odd
	}

	return nodes
}

func (m *Manager) qualityCheckTimer() {
	ticker := time.NewTicker(qualityCheckTime)
	defer ticker.Stop()

	time.Sleep(5 * time.Minute)
	nodes := m.getPairsNode()

	round := 0
	for {
		<-ticker.C

		rounds := len(nodes) - 1
		pairs := m.generatePairs(nodes, round)

		log.Infof("qualityCheck -- Round %d pairs:\n", round+1)
		for _, pair := range pairs {
			if pair.Node1 != "" && pair.Node2 != "" {
				log.Infof("qualityCheck -- %s <-> %s\n", pair.Node1, pair.Node2)
				// go testBandwidth(pair)
			}
		}

		round++
		if round >= rounds {
			log.Infoln("qualityCheck -- All pairings completed. Restarting from the beginning.")
			round = 0

			nodes = m.getPairsNode()
		}
	}
}

func (m *Manager) generatePairs(nodes []string, round int) []pair {
	n := len(nodes)
	if n < 2 {
		return []pair{}
	}

	pairs := make([]pair, n/2)

	for i := 0; i < n/2; i++ {
		j := (round + i) % (n - 1)
		if i == 0 {
			pairs[i] = pair{nodes[j], nodes[n-1]}
		} else {
			pairs[i] = pair{nodes[j], nodes[(round-i+n-1)%(n-1)]}
		}
	}

	return pairs
}

type pair struct {
	Node1, Node2 string
}
