package node

import (
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
)

// startNodeKeepaliveTimer periodically sends keepalive requests to all nodes and checks if any nodes have been offline for too long
func (m *Manager) startNodeKeepaliveTimer() {
	start := time.Now()

	offset := (time.Minute - time.Duration(start.Second())*time.Second - time.Duration(start.Nanosecond())) + (time.Minute * 10)
	time.Sleep(offset)

	ticker := time.NewTicker(keepaliveTime)
	defer ticker.Stop()

	for {
		<-ticker.C

		m.nodesKeepalive()
	}
}

// nodesKeepalive checks all nodes in the manager's lists for keepalive
func (m *Manager) nodesKeepalive() {
	now := time.Now()

	// date := now.Format("2006-01-02")
	date := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

	t := now.Add(-keepaliveTime)

	nodes := []string{
		string(m.ServerID),
	}

	m.edgeNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		if node == nil {
			return true
		}

		node.KeepaliveCount++
		if m.checkNodeStatus(node, t) {
			nodes = append(nodes, node.NodeID)
		}

		return true
	})

	m.candidateNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		if node == nil {
			return true
		}

		if m.checkNodeStatus(node, t) {
			nodes = append(nodes, node.NodeID)
		}

		return true
	})

	if len(nodes) > 0 {
		err := m.UpdateOnlineCount(nodes, 2, date)
		if err != nil {
			log.Errorf("UpdateNodeInfos err:%s", err.Error())
		}
	}
}

// checkNodeStatus checks if a node has sent a keepalive recently and updates node status accordingly
func (m *Manager) checkNodeStatus(node *Node, t time.Time) bool {
	lastTime := node.LastRequestTime()

	if !lastTime.After(t) {
		m.RemoveNodeIP(node.NodeID, node.ExternalIP)
		m.RemoveNodeGeo(node.NodeID, node.GeoInfo)

		if node.Type == types.NodeCandidate || node.Type == types.NodeValidator {
			m.deleteCandidateNode(node)
		} else if node.Type == types.NodeEdge {
			m.deleteEdgeNode(node)
		}

		log.Infof("node offline %s, %s", node.NodeID, node.ExternalIP)

		return false
	}

	return true
}
