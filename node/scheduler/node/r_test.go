package node

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestXxx(t *testing.T) {
	// 51,[51]
	// hour score c_9b8df163-fc73-4364-949e-2c88da4ee249 : [51 51 51 51 51 51 51 51 51 51 51]
	// day score c_9b8df163-fc73-4364-949e-2c88da4ee249 : [60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 58 50 49 49 49 49 48 48 48 48 48 48 48 47 47 47 47 47 47 47 47 47 47 47 77 51 51 51 51 51 51 51 51 51 51 51 51 51 51 51 51 51]
	// week score c_9b8df163-fc73-4364-949e-2c88da4ee249 : [60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 58 50 49 49 49 49 48 48 48 48 48 48 48 47 47 47 47 47 47 47 47 47 47 47 77 51 51 51 51 51 51 51 51 51 51 51 51 51 51 51 51 51]

	// curBandwidthUpScore := float64(51)
	// historyHourBandwidthUpScore := []float64{51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51}
	// historyDayBandwidthUpScore := []float64{60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 58, 50, 49, 49, 49, 49, 48, 48, 48, 48, 48, 48, 48, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 77, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51}
	// // historyWeekBandwidthUpScore := []int64{}

	// bandwidthUpScore := calcScore(curBandwidthUpScore, historyHourBandwidthUpScore, historyDayBandwidthUpScore, historyDayBandwidthUpScore)

	max := math.Max(10, 20)
	min := math.Min(10, 20)
	t.Logf("max: %.2f", max)
	t.Logf("min: %.2f", min)

	max = math.Max(10, 5)
	min = math.Min(10, 5)
	t.Logf("max: %.2f", max)
	t.Logf("min: %.2f", min)
}

const (
	interval = 10 * time.Second
)

type NodeInfo struct {
	ID int
}

type NodePair struct {
	Node1, Node2 *NodeInfo
}

func getNodes(count int) []*NodeInfo {
	nodes := make([]*NodeInfo, count)
	for i := range nodes {
		nodes[i] = &NodeInfo{ID: i}
	}

	if count%2 != 0 {
		nodes = append(nodes, &NodeInfo{ID: -1}) // Add a dummy node if odd
	}

	return nodes
}

func TestXxx2(t *testing.T) {
	count := 3
	nodes := getNodes(count)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	round := 0
	for {
		<-ticker.C

		rounds := len(nodes) - 1
		pairs := generatePairs(nodes, round)

		fmt.Printf("Round %d pairs:\n", round+1)
		for _, pair := range pairs {
			if pair.Node1.ID != -1 && pair.Node2.ID != -1 {
				fmt.Printf("Node %d <-> Node %d\n", pair.Node1.ID, pair.Node2.ID)
				// go testBandwidth(pair)
			}
		}

		round++
		if round >= rounds {
			fmt.Println("All pairings completed. Restarting from the beginning.")
			round = 0

			count++
			nodes = getNodes(count)
		}
	}
}

func generatePairs(nodes []*NodeInfo, round int) []NodePair {
	n := len(nodes)
	pairs := make([]NodePair, n/2)

	for i := 0; i < n/2; i++ {
		j := (round + i) % (n - 1)
		if i == 0 {
			pairs[i] = NodePair{nodes[j], nodes[n-1]}
		} else {
			pairs[i] = NodePair{nodes[j], nodes[(round-i+n-1)%(n-1)]}
		}
	}

	return pairs
}

type ServiceType int

const (
	ServiceTypeUpload ServiceType = iota
	ServiceTypeDownload
)

type ServiceStatus int

const (
	ServiceTypeSucceed ServiceStatus = iota
	ServiceTypeFailed
)

type ServiceEvent struct {
	TraceID   string        `db:"trace_id"`
	NodeID    string        `db:"node_id"`
	Type      ServiceType   `db:"type"`
	Size      int64         `db:"size"`
	Status    ServiceStatus `db:"status"`
	Peak      int64         `db:"peak"`
	EndTime   time.Time     `db:"end_time"`
	StartTime time.Time     `db:"start_time"`
	Speed     int64         `db:"speed"`

	Score int64 `db:"score"`
}

type ServiceStats struct {
	NodeID               string
	UploadSuccessCount   int64
	UploadTotalCount     int64
	UploadFailCount      int64
	UploadAvgSpeed       int64
	DownloadSuccessCount int64
	DownloadTotalCount   int64
	DownloadFailCount    int64
	DownloadAvgSpeed     int64
	AvgScore             int64

	DownloadSpeeds []int64
	UploadSpeeds   []int64
	Scores         []int64
}

func TestXxx3(t *testing.T) {
	stats := getStats(getEvents())

	for _, info := range stats {
		fmt.Printf("Node[%s] upload:[%d]/[%d]/[%d] [%d] download:[%d]/[%d]/[%d] [%d] score:[%d] \n",
			info.NodeID,
			info.UploadSuccessCount, info.UploadFailCount, info.UploadTotalCount, info.UploadAvgSpeed,
			info.DownloadSuccessCount, info.DownloadFailCount, info.DownloadTotalCount, info.DownloadAvgSpeed,
			info.AvgScore)
	}
}

func getEvents() []*ServiceEvent {
	return []*ServiceEvent{
		{NodeID: "Node_1", Type: ServiceTypeUpload, Status: ServiceTypeSucceed, Speed: 100000, Score: 20},
		{NodeID: "Node_1", Type: ServiceTypeUpload, Status: ServiceTypeFailed, Speed: 100000, Score: 20},
		{NodeID: "Node_1", Type: ServiceTypeUpload, Status: ServiceTypeSucceed, Speed: 20000, Score: 5},
		{NodeID: "Node_1", Type: ServiceTypeUpload, Status: ServiceTypeFailed, Speed: 100000, Score: 20},
		{NodeID: "Node_1", Type: ServiceTypeUpload, Status: ServiceTypeSucceed, Speed: 500000, Score: 50},
		{NodeID: "Node_1", Type: ServiceTypeUpload, Status: ServiceTypeFailed, Speed: 100000, Score: 20},
		{NodeID: "Node_1", Type: ServiceTypeDownload, Status: ServiceTypeSucceed, Speed: 100000, Score: 20},
		{NodeID: "Node_1", Type: ServiceTypeDownload, Status: ServiceTypeFailed, Speed: 100000, Score: 20},
		{NodeID: "Node_1", Type: ServiceTypeDownload, Status: ServiceTypeSucceed, Speed: 20000, Score: 5},
		{NodeID: "Node_1", Type: ServiceTypeDownload, Status: ServiceTypeFailed, Speed: 100000, Score: 20},
		{NodeID: "Node_1", Type: ServiceTypeDownload, Status: ServiceTypeSucceed, Speed: 300000, Score: 20},

		{NodeID: "Node_2", Type: ServiceTypeUpload, Status: ServiceTypeSucceed, Speed: 500, Score: 3},
		{NodeID: "Node_2", Type: ServiceTypeUpload, Status: ServiceTypeFailed, Speed: 100000, Score: 20},
		{NodeID: "Node_2", Type: ServiceTypeUpload, Status: ServiceTypeSucceed, Speed: 20000, Score: 5},
		{NodeID: "Node_2", Type: ServiceTypeUpload, Status: ServiceTypeFailed, Speed: 100000, Score: 20},
		{NodeID: "Node_2", Type: ServiceTypeDownload, Status: ServiceTypeFailed, Speed: 100000, Score: 20},
		{NodeID: "Node_2", Type: ServiceTypeDownload, Status: ServiceTypeSucceed, Speed: 300000, Score: 20},

		{NodeID: "Node_3", Type: ServiceTypeUpload, Status: ServiceTypeSucceed, Speed: 20000, Score: 5},
		{NodeID: "Node_3", Type: ServiceTypeUpload, Status: ServiceTypeFailed, Speed: 100000, Score: 20},
		{NodeID: "Node_3", Type: ServiceTypeDownload, Status: ServiceTypeFailed, Speed: 100000, Score: 20},
	}
}

func getStats(events []*ServiceEvent) map[string]*ServiceStats {
	nodeStats := make(map[string]*ServiceStats)

	for _, event := range events {
		nodeID := event.NodeID

		statsInfo, ok := nodeStats[nodeID]
		if !ok {
			statsInfo = &ServiceStats{NodeID: nodeID, DownloadSpeeds: []int64{}, UploadSpeeds: []int64{}, Scores: []int64{}}
			nodeStats[nodeID] = statsInfo
		}

		if event.Type == ServiceTypeUpload { // Upload
			statsInfo.UploadTotalCount++

			if event.Status == ServiceTypeSucceed {
				statsInfo.UploadSuccessCount++
				statsInfo.UploadSpeeds = append(statsInfo.UploadSpeeds, event.Speed)
				statsInfo.Scores = append(statsInfo.Scores, event.Score)
			} else if event.Status == ServiceTypeFailed {
				statsInfo.UploadFailCount++
			}
		} else if event.Type == ServiceTypeDownload { // Download
			statsInfo.DownloadTotalCount++

			if event.Status == ServiceTypeSucceed {
				statsInfo.DownloadSuccessCount++
				statsInfo.DownloadSpeeds = append(statsInfo.DownloadSpeeds, event.Speed)
				statsInfo.Scores = append(statsInfo.Scores, event.Score)
			} else if event.Status == ServiceTypeFailed {
				statsInfo.DownloadFailCount++
			}
		}
	}

	for _, stats := range nodeStats {
		if len(stats.DownloadSpeeds) > 0 {
			total := int64(0)
			for _, value := range stats.DownloadSpeeds {
				total += value
			}

			stats.DownloadAvgSpeed = total / int64(len(stats.DownloadSpeeds))
		}

		if len(stats.UploadSpeeds) > 0 {
			total := int64(0)
			for _, value := range stats.UploadSpeeds {
				total += value
			}

			stats.UploadAvgSpeed = total / int64(len(stats.UploadSpeeds))
		}

		if len(stats.Scores) > 0 {
			total := int64(0)
			for _, value := range stats.Scores {
				total += value
			}

			stats.AvgScore = total / int64(len(stats.Scores))
		}

		stats.DownloadSpeeds = nil
		stats.UploadSpeeds = nil
		stats.Scores = nil
	}

	return nodeStats
}
