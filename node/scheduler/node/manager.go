package node

import (
	"crypto/rsa"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/filecoin-project/pubsub"

	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("node")

const (
	// keepaliveTime is the interval between keepalive requests
	keepaliveTime = 30 * time.Second // seconds

	// saveInfoInterval is the interval at which node information is saved during keepalive requests
	saveInfoInterval = 2 // keepalive saves information every 2 times

	oneDay = 24 * time.Hour
)

// Manager is the node manager responsible for managing the online nodes
type Manager struct {
	edgeNodes      sync.Map
	candidateNodes sync.Map
	Edges          int // online edge node count
	Candidates     int // online candidate node count
	weightMgr      *weightManager
	config         dtypes.GetSchedulerConfigFunc
	notify         *pubsub.PubSub
	*db.SQLDB
	*rsa.PrivateKey // scheduler privateKey
	dtypes.ServerID // scheduler server id
}

// NewManager creates a new instance of the node manager
func NewManager(sdb *db.SQLDB, serverID dtypes.ServerID, pk *rsa.PrivateKey, pb *pubsub.PubSub, config dtypes.GetSchedulerConfigFunc) *Manager {
	nodeManager := &Manager{
		SQLDB:      sdb,
		ServerID:   serverID,
		PrivateKey: pk,
		notify:     pb,
		config:     config,
		weightMgr:  newWeightManager(config),
	}

	go nodeManager.startNodeKeepaliveTimer()
	go nodeManager.startNodeTimer()

	return nodeManager
}

// startNodeKeepaliveTimer periodically sends keepalive requests to all nodes and checks if any nodes have been offline for too long
func (m *Manager) startNodeKeepaliveTimer() {
	ticker := time.NewTicker(keepaliveTime)
	defer ticker.Stop()

	count := 0

	for {
		<-ticker.C
		count++

		saveInfo := count%saveInfoInterval == 0
		m.nodesKeepalive(saveInfo)
	}
}

func (m *Manager) startNodeTimer() {
	now := time.Now()

	nextTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	if now.After(nextTime) {
		nextTime = nextTime.Add(oneDay)
	}

	duration := nextTime.Sub(now)

	timer := time.NewTimer(duration)
	defer timer.Stop()

	for {
		<-timer.C

		log.Debugln("start node timer...")

		m.redistributeNodeSelectWeights()

		m.checkNodeDeactivate()

		timer.Reset(oneDay)
	}
}

// storeEdgeNode adds an edge node to the manager's list of edge nodes
func (m *Manager) storeEdgeNode(node *Node) {
	if node == nil {
		return
	}
	nodeID := node.NodeID
	_, loaded := m.edgeNodes.LoadOrStore(nodeID, node)
	if loaded {
		return
	}
	m.Edges++

	m.DistributeNodeWeight(node)

	m.notify.Pub(node, types.EventNodeOnline.String())
}

// adds a candidate node to the manager's list of candidate nodes
func (m *Manager) storeCandidateNode(node *Node) {
	if node == nil {
		return
	}

	nodeID := node.NodeID
	_, loaded := m.candidateNodes.LoadOrStore(nodeID, node)
	if loaded {
		return
	}
	m.Candidates++

	m.DistributeNodeWeight(node)

	m.notify.Pub(node, types.EventNodeOnline.String())
}

// deleteEdgeNode removes an edge node from the manager's list of edge nodes
func (m *Manager) deleteEdgeNode(node *Node) {
	m.RepayNodeWeight(node)
	m.notify.Pub(node, types.EventNodeOffline.String())

	nodeID := node.NodeID
	_, loaded := m.edgeNodes.LoadAndDelete(nodeID)
	if !loaded {
		return
	}
	m.Edges--
}

// deleteCandidateNode removes a candidate node from the manager's list of candidate nodes
func (m *Manager) deleteCandidateNode(node *Node) {
	m.RepayNodeWeight(node)
	m.notify.Pub(node, types.EventNodeOffline.String())

	nodeID := node.NodeID
	_, loaded := m.candidateNodes.LoadAndDelete(nodeID)
	if !loaded {
		return
	}
	m.Candidates--
}

// DistributeNodeWeight Distribute Node Weight
func (m *Manager) DistributeNodeWeight(node *Node) {
	if node.IsAbnormal() {
		return
	}

	if node.IsPrivateMinioOnly {
		return
	}

	score := m.getNodeScoreLevel(node.NodeID)
	wNum := m.weightMgr.getWeightNum(score)
	if node.Type == types.NodeCandidate {
		node.selectWeights = m.weightMgr.distributeCandidateWeight(node.NodeID, wNum)
	} else if node.Type == types.NodeEdge {
		node.selectWeights = m.weightMgr.distributeEdgeWeight(node.NodeID, wNum)
	}
}

// RepayNodeWeight Repay Node Weight
func (m *Manager) RepayNodeWeight(node *Node) {
	if node.Type == types.NodeCandidate {
		m.weightMgr.repayCandidateWeight(node.selectWeights)
		node.selectWeights = nil
	} else if node.Type == types.NodeEdge {
		m.weightMgr.repayEdgeWeight(node.selectWeights)
		node.selectWeights = nil
	}
}

// nodeKeepalive checks if a node has sent a keepalive recently and updates node status accordingly
func (m *Manager) nodeKeepalive(node *Node, t time.Time, isSave bool) bool {
	lastTime := node.LastRequestTime()

	if !lastTime.After(t) {
		node.ClientCloser()
		if node.Type == types.NodeCandidate {
			m.deleteCandidateNode(node)
		} else if node.Type == types.NodeEdge {
			m.deleteEdgeNode(node)
		}
		node = nil
		return false
	}

	if isSave {
		// Minute
		node.OnlineDuration += int((saveInfoInterval * keepaliveTime) / time.Minute)
	}

	return true
}

// nodesKeepalive checks all nodes in the manager's lists for keepalive
func (m *Manager) nodesKeepalive(isSave bool) {
	t := time.Now().Add(-keepaliveTime)

	nodes := make([]*types.NodeDynamicInfo, 0)

	m.edgeNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		if node == nil {
			return true
		}

		if m.nodeKeepalive(node, t, isSave) {
			nodes = append(nodes, &types.NodeDynamicInfo{
				NodeID:         node.NodeID,
				OnlineDuration: node.OnlineDuration,
				DiskUsage:      node.DiskUsage,
				LastSeen:       time.Now(),
			})
		}

		return true
	})

	m.candidateNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		if node == nil {
			return true
		}

		if m.nodeKeepalive(node, t, isSave) {
			nodes = append(nodes, &types.NodeDynamicInfo{
				NodeID:         node.NodeID,
				OnlineDuration: node.OnlineDuration,
				DiskUsage:      node.DiskUsage,
				LastSeen:       time.Now(),
			})
		}

		return true
	})

	if isSave {
		err := m.UpdateOnlineDuration(nodes)
		if err != nil {
			log.Errorf("UpdateNodeInfos err:%s", err.Error())
		}
	}
}

// saveInfo Save node information when it comes online
func (m *Manager) saveInfo(n *types.NodeInfo) error {
	n.LastSeen = time.Now()

	err := m.SaveNodeInfo(n)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) redistributeNodeSelectWeights() {
	// repay all weights
	m.weightMgr.cleanWeights()

	// redistribute weights
	m.candidateNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)

		if node.IsAbnormal() {
			return true
		}

		if node.IsPrivateMinioOnly {
			return true
		}

		score := m.getNodeScoreLevel(node.NodeID)
		wNum := m.weightMgr.getWeightNum(score)
		node.selectWeights = m.weightMgr.distributeCandidateWeight(node.NodeID, wNum)

		return true
	})

	m.edgeNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)

		if node.IsAbnormal() {
			return true
		}

		score := m.getNodeScoreLevel(node.NodeID)
		wNum := m.weightMgr.getWeightNum(score)
		node.selectWeights = m.weightMgr.distributeEdgeWeight(node.NodeID, wNum)

		return true
	})
}

// GetAllEdgeNode load all edge node
func (m *Manager) GetAllEdgeNode() []*Node {
	nodes := make([]*Node, 0)

	m.edgeNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		nodes = append(nodes, node)

		return true
	})

	return nodes
}

// UpdateNodeBandwidths update node bandwidthDown and bandwidthUp
func (m *Manager) UpdateNodeBandwidths(nodeID string, bandwidthDown, bandwidthUp int64) {
	node := m.GetNode(nodeID)
	if node == nil {
		return
	}

	if bandwidthDown > 0 {
		node.BandwidthDown = bandwidthDown
	}
	if bandwidthUp > 0 {
		node.BandwidthUp = bandwidthUp
	}

	err := m.UpdateBandwidths(nodeID, node.BandwidthDown, node.BandwidthUp)
	if err != nil {
		log.Errorf("UpdateBandwidths err:%s", err.Error())
	}
}

func (m *Manager) checkNodeDeactivate() {
	nodes, err := m.LoadDeactivateNodes()
	if err != nil {
		log.Errorf("LoadDeactivateNodes err:%s", err.Error())
		return
	}

	for _, nodeID := range nodes {
		err = m.DeleteAssetRecordsOfNode(nodeID)
		if err != nil {
			log.Errorf("DeleteAssetOfNode err:%s", err.Error())
		}
	}
}
