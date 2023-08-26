package node

import (
	"github.com/Filecoin-Titan/titan/api/types"
)

// GetAllCandidateNodes  returns a list of all candidate nodes
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
func (m *Manager) GetCandidateNodes(n int) []*Node {
	var out []*Node
	m.candidateNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		out = append(out, node)
		return len(out) < n
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
	node.OnlineDuration = info.OnlineDuration
	node.DiskSpace = info.DiskSpace
	node.ExternalIP = info.ExternalIP
	node.DiskUsage = info.DiskUsage
	node.NodeID = info.NodeID
	node.Type = info.Type
	node.BandwidthDown = info.BandwidthDown
	node.BandwidthUp = info.BandwidthUp
	node.PortMapping = info.PortMapping
	node.DeactivateTime = info.DeactivateTime

	err := m.saveInfo(info)
	if err != nil {
		return err
	}

	switch node.Type {
	case types.NodeEdge:
		m.storeEdgeNode(node)
	case types.NodeCandidate:
		m.storeCandidateNode(node)
	}

	return nil
}

// GetRandomCandidate returns a random candidate node
func (m *Manager) GetRandomCandidate() (*Node, int) {
	nodeID, weight := m.weightMgr.getCandidateWeightRandom()
	return m.GetCandidateNode(nodeID), weight
}

// GetRandomEdge returns a random edge node
func (m *Manager) GetRandomEdge() (*Node, int) {
	nodeID, weight := m.weightMgr.getEdgeWeightRandom()
	return m.GetEdgeNode(nodeID), weight
}
