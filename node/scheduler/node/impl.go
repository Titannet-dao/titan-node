package node

import (
	"github.com/Filecoin-Titan/titan/api/types"
)

// GetAllNodes  Get all valid candidate and edge nodes
func (m *Manager) GetAllNodes() []*Node {
	var nodes []*Node

	m.candidateNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)

		if node.IsAbnormal() {
			return true
		}

		nodes = append(nodes, node)
		return true
	})

	m.edgeNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)

		if node.IsAbnormal() {
			return true
		}

		nodes = append(nodes, node)
		return true
	})

	return nodes
}

// GetAllCandidateNodes  Get all valid candidate nodes
func (m *Manager) GetAllCandidateNodes() ([]string, []*Node) {
	var ids []string
	var nodes []*Node
	m.candidateNodes.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		node := value.(*Node)

		if node.IsAbnormal() {
			return true
		}

		ids = append(ids, nodeID)
		nodes = append(nodes, node)
		return true
	})

	return ids, nodes
}

// GetCandidateNodes return n candidate node
func (m *Manager) GetCandidateNodes(num int) []*Node {
	var out []*Node
	m.candidateNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)

		out = append(out, node)
		return len(out) < num
	})

	return out
}

// GetNode retrieves a node with the given node ID
func (m *Manager) GetNode(nodeID string) *Node {
	edge := m.GetEdgeNode(nodeID)
	if edge != nil {
		return edge
	}

	candidate := m.GetCandidateNode(nodeID)
	if candidate != nil {
		return candidate
	}

	return nil
}

// GetEdgeNode retrieves an edge node with the given node ID
func (m *Manager) GetEdgeNode(nodeID string) *Node {
	nodeI, exist := m.edgeNodes.Load(nodeID)
	if exist && nodeI != nil {
		node := nodeI.(*Node)

		return node
	}

	return nil
}

// GetCandidateNode retrieves a candidate node with the given node ID
func (m *Manager) GetCandidateNode(nodeID string) *Node {
	nodeI, exist := m.candidateNodes.Load(nodeID)
	if exist && nodeI != nil {
		node := nodeI.(*Node)

		return node
	}

	return nil
}

// GetOnlineNodeCount returns online node count of the given type
func (m *Manager) GetOnlineNodeCount(nodeType types.NodeType) int {
	i := 0
	if nodeType == types.NodeUnknown || nodeType == types.NodeCandidate {
		i += m.Candidates
	}

	if nodeType == types.NodeUnknown || nodeType == types.NodeEdge {
		i += m.Edges
	}

	return i
}

// NodeOnline registers a node as online
func (m *Manager) NodeOnline(node *Node, info *types.NodeInfo) error {
	err := m.saveInfo(info)
	if err != nil {
		return err
	}

	switch node.Type {
	case types.NodeEdge:
		m.storeEdgeNode(node)
	case types.NodeCandidate, types.NodeValidator:
		m.storeCandidateNode(node)
	}

	// m.UpdateNodeDiskUsage(info.NodeID, info.DiskUsage)

	return nil
}

// GetRandomCandidates returns a random candidate node
func (m *Manager) GetRandomCandidates(count int) map[string]int {
	return m.weightMgr.getCandidateWeightRandom(count)
}

// GetRandomEdges returns a random edge node
func (m *Manager) GetRandomEdges(count int) map[string]int {
	return m.weightMgr.getEdgeWeightRandom(count)
}
