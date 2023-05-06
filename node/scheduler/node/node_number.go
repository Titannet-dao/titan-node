package node

import "math/rand"

// distributeCandidateNodeNum assigns an undistributed candidate node number a node ID and returns the assigned number
func (m *Manager) distributeCandidateNodeNum(nodeID string) int {
	m.nodeNumLock.Lock()
	defer m.nodeNumLock.Unlock()

	var num int
	if len(m.cUndistributedNodeNum) > 0 {
		for c := range m.cUndistributedNodeNum {
			num = c
			break
		}

		if _, exist := m.cDistributedNodeNum[num]; !exist {
			m.cDistributedNodeNum[num] = nodeID
			delete(m.cUndistributedNodeNum, num)
			return num
		}
	}

	m.cNodeNumMax++

	num = m.cNodeNumMax
	m.cDistributedNodeNum[num] = nodeID
	return num
}

// distributeEdgeNodeNum assigns an undistributed edge node number to a node ID and returns the assigned number
func (m *Manager) distributeEdgeNodeNum(nodeID string) int {
	m.nodeNumLock.Lock()
	defer m.nodeNumLock.Unlock()

	var num int
	if len(m.eUndistributedNodeNum) > 0 {
		for c := range m.eUndistributedNodeNum {
			num = c
			break
		}

		if _, exist := m.eDistributedNodeNum[num]; !exist {
			m.eDistributedNodeNum[num] = nodeID
			delete(m.eUndistributedNodeNum, num)
			return num
		}
	}

	m.eNodeNumMax++

	num = m.eNodeNumMax
	m.eDistributedNodeNum[num] = nodeID
	return num
}

// repayCandidateNodeNum returns an undistributed candidate node number to the pool
func (m *Manager) repayCandidateNodeNum(num int) {
	m.nodeNumLock.Lock()
	defer m.nodeNumLock.Unlock()

	delete(m.cDistributedNodeNum, num)
	m.cUndistributedNodeNum[num] = ""
}

// repayEdgeNodeNum returns an undistributed edge node number to the pool
func (m *Manager) repayEdgeNodeNum(num int) {
	m.nodeNumLock.Lock()
	defer m.nodeNumLock.Unlock()

	delete(m.eDistributedNodeNum, num)
	m.eUndistributedNodeNum[num] = ""
}

// getNodeNumRandom returns a random integer up to max (inclusive) using the provided Rand generator
func (m *Manager) getNodeNumRandom(max int, r *rand.Rand) int {
	return r.Intn(max) + 1
}

// GetRandomCandidate returns a random candidate node
func (m *Manager) GetRandomCandidate() (*Node, int) {
	num := m.getNodeNumRandom(m.cNodeNumMax, m.cNodeNumRand)
	nodeID, exist := m.cDistributedNodeNum[num]
	if !exist {
		return nil, num
	}

	return m.GetCandidateNode(nodeID), num
}

// GetRandomEdge returns a random edge node
func (m *Manager) GetRandomEdge() (*Node, int) {
	num := m.getNodeNumRandom(m.eNodeNumMax, m.eNodeNumRand)
	nodeID, exist := m.eDistributedNodeNum[num]
	if !exist {
		return nil, num
	}

	return m.GetEdgeNode(nodeID), num
}
