package node

import (
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
)

// startNodeKeepaliveTimer periodically sends keepalive requests to all nodes and checks if any nodes have been offline for too long
func (m *Manager) startNodeKeepaliveTimer() {
	time.Sleep(penaltyFreeTime)

	ticker := time.NewTicker(keepaliveTime)
	defer ticker.Stop()

	minute := 10 // penalty free time
	count := 2

	for {
		<-ticker.C

		m.nodesKeepalive(minute, count == 2)
		minute = 1
		if count == 2 {
			count = 0
		}
		count++
	}
}

// nodesKeepalive checks all nodes in the manager's lists for keepalive
func (m *Manager) nodesKeepalive(minute int, isSave bool) {
	now := time.Now()
	t := now.Add(-keepaliveTime)
	timeWindow := (minute * 60) / 5

	m.serverTodayOnlineTimeWindow += timeWindow

	nodes := make([]*types.NodeDynamicInfo, 0)
	detailsList := make([]*types.ProfitDetails, 0)
	nodeOnlineCount := make(map[string]int, 0)

	saveDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

	eList := m.GetValidEdgeNode()
	for _, node := range eList {
		if m.checkNodeStatus(node, t) {
			node.OnlineDuration += minute
			node.TodayOnlineTimeWindow += timeWindow
		}

		if isSave {
			profitMinute := node.TodayOnlineTimeWindow * 5 / 60

			incr, dInfo := m.GetEdgeBaseProfitDetails(node, profitMinute)
			if dInfo != nil {
				detailsList = append(detailsList, dInfo)
			}
			node.IncomeIncr = incr
			nodes = append(nodes, &node.NodeDynamicInfo)

			nodeOnlineCount[node.NodeID] = node.TodayOnlineTimeWindow
			node.TodayOnlineTimeWindow = 0
		}
	}

	cList := m.GetAllCandidateNodes()
	for _, node := range cList {
		if m.checkNodeStatus(node, t) {
			if !node.IsAbnormal() {
				node.OnlineDuration += minute
				node.TodayOnlineTimeWindow += timeWindow
			}
		}

		if isSave && !node.IsAbnormal() {
			if qualifiedNAT(node.NATType) {
				profitMinute := node.TodayOnlineTimeWindow * 5 / 60

				dInfo := m.GetCandidateBaseProfitDetails(node, profitMinute)
				if dInfo != nil {
					detailsList = append(detailsList, dInfo)
				}
			}
			nodes = append(nodes, &node.NodeDynamicInfo)

			nodeOnlineCount[node.NodeID] = node.TodayOnlineTimeWindow
			node.TodayOnlineTimeWindow = 0
		}
	}

	l3List := m.GetValidL3Node()
	for _, node := range l3List {
		if m.checkNodeStatus(node, t) {
			node.OnlineDuration += minute
			node.TodayOnlineTimeWindow += timeWindow
		}

		if isSave {
			profitMinute := node.TodayOnlineTimeWindow * 5 / 60

			incr, dInfo := m.GetEdgeBaseProfitDetails(node, profitMinute)
			if dInfo != nil {
				detailsList = append(detailsList, dInfo)
			}
			node.IncomeIncr = incr
			nodes = append(nodes, &node.NodeDynamicInfo)

			nodeOnlineCount[node.NodeID] = node.TodayOnlineTimeWindow
			node.TodayOnlineTimeWindow = 0
		}
	}

	l5List := m.GetValidL5Node()
	for _, node := range l5List {
		if m.checkNodeStatus(node, t) {
			node.OnlineDuration += minute
			node.TodayOnlineTimeWindow += timeWindow
		}

		if isSave {
			nodeOnlineCount[node.NodeID] = node.TodayOnlineTimeWindow
			node.TodayOnlineTimeWindow = 0
		}
	}

	if !isSave {
		return
	}
	log.Infoln("nodesKeepalive save info start")

	nodeOnlineCount[string(m.ServerID)] = m.serverTodayOnlineTimeWindow
	m.serverTodayOnlineTimeWindow = 0

	err := m.UpdateNodeDynamicInfo(nodes)
	if err != nil {
		log.Errorf("updateNodeData UpdateNodeDynamicInfo err:%s", err.Error())
	}

	err = m.AddNodeProfitDetails(detailsList)
	if err != nil {
		log.Errorf("updateNodeData AddNodeProfits err:%s", err.Error())
	}

	err = m.UpdateNodeOnlineCount(nodeOnlineCount, saveDate)
	if err != nil {
		log.Errorf("updateNodeData UpdateNodeOnlineCount err:%s", err.Error())
	}

	log.Infoln("nodesKeepalive save info done")
}

// SetNodeOffline removes the node's IP and geo information from the manager.
func (m *Manager) SetNodeOffline(node *Node) {
	m.IPMgr.RemoveNodeIP(node.NodeID, node.ExternalIP)
	m.GeoMgr.RemoveNodeGeo(node.NodeID, node.Type, node.AreaID)

	if node.Type == types.NodeCandidate {
		m.deleteCandidateNode(node)
	} else if node.Type == types.NodeEdge {
		m.deleteEdgeNode(node)
	} else if node.Type == types.NodeL5 {
		m.deleteL5Node(node)
	} else if node.Type == types.NodeL3 {
		m.deleteL3Node(node)
	}

	log.Infof("node offline %s, %s", node.NodeID, node.ExternalIP)
}

// checkNodeStatus checks if a node has sent a keepalive recently and updates node status accordingly
func (m *Manager) checkNodeStatus(node *Node, t time.Time) bool {
	lastTime := node.LastRequestTime()

	if !lastTime.After(t) {
		m.SetNodeOffline(node)

		return false
	}

	return true
}
