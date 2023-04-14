package node

import (
	"github.com/Filecoin-Titan/titan/api/types"
)

// NodesQuit nodes quit and removes their replicas
func (m *Manager) NodesQuit(nodeIDs []string) {
	err := m.UpdateNodesQuitted(nodeIDs)
	if err != nil {
		log.Errorf("NodesQuit: UpdateNodesQuitted err:%s", err.Error())
		return
	}

	log.Infof("node event , nodes quit:%v", nodeIDs)

	hashes, err := m.LoadAssetHashesOfNodes(nodeIDs)
	if err != nil {
		log.Errorf("NodesQuit: LoadAssetHashesOfNodes err:%s", err.Error())
		return
	}

	err = m.DeleteReplicasForNodes(nodeIDs)
	if err != nil {
		log.Errorf("NodesQuit: DeleteReplicasForNodes err:%s", err.Error())
		return
	}

	for _, hash := range hashes {
		// TODO change asset state
		log.Infof("NodesQuit: Need to add replica for asset:%s", hash)
	}
}

// GetAllCandidateNodes  returns a list of all candidate nodes
func (m *Manager) GetAllCandidateNodes() []string {
	var out []string
	m.candidateNodes.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		out = append(out, nodeID)
		return true
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
func (m *Manager) NodeOnline(node *Node) error {
	nodeID := node.NodeID

	err := m.saveInfo(node.NodeInfo)
	if err != nil {
		return err
	}

	if node.Type == types.NodeEdge {
		m.storeEdgeNode(node)
		return nil
	}

	if node.Type == types.NodeCandidate {
		m.storeCandidateNode(node)
		// update validator owner
		return m.UpdateValidatorInfo(m.ServerID, nodeID)
	}

	return nil
}
