package node

import (
	"fmt"
	"math/rand"

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

	l5 := m.GetL5Node(nodeID)
	if l5 != nil {
		return l5
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

// GetCandidateNode retrieves a candidate node with the given node ID
func (m *Manager) GetL5Node(nodeID string) *Node {
	nodeI, exist := m.l5Nodes.Load(nodeID)
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
	case types.NodeCandidate:
		m.storeCandidateNode(node)
	case types.NodeL5:
		m.storeL5Node(node)
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

// SetTunserverURL set node Tunserver URL
func (m *Manager) SetTunserverURL(edgeID, candidateID string) error {
	node := m.GetEdgeNode(edgeID)
	if node != nil {
		node.WSServerID = candidateID
	}

	return m.SaveWSServerID(edgeID, candidateID)
}

// UpdateTunserverURL update node Tunserver URL
func (m *Manager) UpdateTunserverURL(edgeID string) (*Node, error) {
	var vNode *Node
	// select candidate
	_, list := m.GetAllCandidateNodes()
	if len(list) > 0 {
		index := rand.Intn(len(list))
		vNode = list[index]
	}

	if vNode == nil {
		return vNode, fmt.Errorf("node not found")
	}

	vID := vNode.NodeID

	node := m.GetEdgeNode(edgeID)
	if node != nil {
		node.WSServerID = vID
	}

	return vNode, m.SaveWSServerID(edgeID, vID)
}
