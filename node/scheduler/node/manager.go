package node

import (
	"crypto/rsa"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/filecoin-project/pubsub"

	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("node")

const (
	syncEdgeCountTime = 5 * time.Minute //
	// keepaliveTime is the interval between keepalive requests
	keepaliveTime       = 30 * time.Second // seconds
	calculatePointsTime = 30 * time.Minute

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
	etcdcli        *etcdcli.Client
	*db.SQLDB
	*rsa.PrivateKey // scheduler privateKey
	dtypes.ServerID // scheduler server id

	ipLimit           int
	TotalNetworkEdges int // Number of edge nodes in the entire network (including those on other schedulers)

	nodeIPs sync.Map
}

// NewManager creates a new instance of the node manager
func NewManager(sdb *db.SQLDB, serverID dtypes.ServerID, pk *rsa.PrivateKey, pb *pubsub.PubSub, config dtypes.GetSchedulerConfigFunc, ec *etcdcli.Client) *Manager {
	nodeManager := &Manager{
		SQLDB:      sdb,
		ServerID:   serverID,
		PrivateKey: pk,
		notify:     pb,
		config:     config,
		etcdcli:    ec,
		weightMgr:  newWeightManager(config),
	}

	nodeManager.ipLimit = nodeManager.getIPLimit()
	log.Infof("nodeManager.ipLimit %d", nodeManager.ipLimit)

	go nodeManager.startNodeKeepaliveTimer()
	go nodeManager.startCheckNodeTimer()
	go nodeManager.startSyncEdgeCountTimer()
	// go nodeManager.startCalculatePointsTimer()

	return nodeManager
}

func (m *Manager) getIPLimit() int {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("get config err:%s", err.Error())
		return 0
	}

	return cfg.IPLimit
}

func (m *Manager) CheckNodeIP(nodeID, ip string) bool {
	listI, exist := m.nodeIPs.Load(ip)
	if exist && listI != nil {
		nodes := listI.([]string)

		for _, nID := range nodes {
			if nID == nodeID {
				return true
			}
		}

		if len(nodes) < m.ipLimit {
			nodes = append(nodes, nodeID)
			m.nodeIPs.Store(ip, nodes)
			return true
		}

		return false
	} else {
		nodes := []string{nodeID}
		m.nodeIPs.Store(ip, nodes)

		return true
	}
}

func (m *Manager) RemoveNodeIP(nodeID, ip string) {
	listI, exist := m.nodeIPs.Load(ip)
	if exist && listI != nil {
		nodes := listI.([]string)

		list := []string{}

		for _, nID := range nodes {
			if nID != nodeID {
				list = append(list, nID)
			}
		}

		m.nodeIPs.Store(ip, list)
	}
}

func (m *Manager) GetNodeOfIP(ip string) []string {
	listI, exist := m.nodeIPs.Load(ip)
	if exist && listI != nil {
		nodes := listI.([]string)

		return nodes
	}

	return nil
}

func (m *Manager) CheckIPExist(ip string) bool {
	_, exist := m.nodeIPs.Load(ip)

	return exist
}

func (m *Manager) syncEdgeCountFromNetwork() {
	err := m.etcdcli.PutEdgeCount(string(m.ServerID), m.Edges)
	if err != nil {
		log.Errorf("SyncEdgeCountFromNetwork PutEdgeCount err:%s", err.Error())
	}

	count, err := m.etcdcli.GetEdgeCounts(string(m.ServerID))
	if err != nil {
		log.Errorf("SyncEdgeCountFromNetwork GetEdgeCounts err:%s", err.Error())
	}

	m.TotalNetworkEdges = count + m.Edges
}

// startSyncEdgeCountTimer
func (m *Manager) startSyncEdgeCountTimer() {
	ticker := time.NewTicker(syncEdgeCountTime)
	defer ticker.Stop()

	for {
		<-ticker.C

		m.syncEdgeCountFromNetwork()
	}
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

func (m *Manager) startCheckNodeTimer() {
	now := time.Now()

	nextTime := time.Date(now.Year(), now.Month(), now.Day(), 12, 0, 0, 0, now.Location())
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

	isValidator, err := m.IsValidator(node.NodeID)
	if err != nil || isValidator {
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
	isValidator, err := m.IsValidator(node.NodeID)
	if err != nil || isValidator {
		return
	}

	if node.Type == types.NodeCandidate {
		m.weightMgr.repayCandidateWeight(node.selectWeights)
		node.selectWeights = nil
	} else if node.Type == types.NodeEdge {
		m.weightMgr.repayEdgeWeight(node.selectWeights)
		node.selectWeights = nil
	}
}

// nodeKeepalive checks if a node has sent a keepalive recently and updates node status accordingly
func (m *Manager) nodeKeepalive(node *Node, t time.Time) bool {
	lastTime := node.LastRequestTime()

	if !lastTime.After(t) {
		m.RemoveNodeIP(node.NodeID, node.ExternalIP)

		node.ClientCloser()
		if node.Type == types.NodeCandidate || node.Type == types.NodeValidator {
			m.deleteCandidateNode(node)
		} else if node.Type == types.NodeEdge {
			m.deleteEdgeNode(node)
		}

		log.Infof("node offline %s", node.NodeID)

		return false
	}

	return true
}

// nodesKeepalive checks all nodes in the manager's lists for keepalive
func (m *Manager) nodesKeepalive(isSave bool) {
	t := time.Now().Add(-keepaliveTime)

	nodes := make([]*types.NodeSnapshot, 0)

	m.edgeNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		if node == nil {
			return true
		}

		if m.nodeKeepalive(node, t) {
			if isSave {
				// Minute
				node.OnlineDuration += int((saveInfoInterval * keepaliveTime) / time.Minute)

				// add node mc
				mc := node.CalculateMCx(m.TotalNetworkEdges)
				// update client incomeIncr (Increase value every thirty minutes)
				node.IncomeIncr = (mc * 360)

				profit := mc * float64((saveInfoInterval*keepaliveTime)/(5*time.Second))
				// log.Infof("nodeKeepalive %s : Mc:[%v] , i:[%v]", node.NodeID, mc, float64(keepaliveTime/(5*time.Second)))

				nodes = append(nodes, &types.NodeSnapshot{
					NodeID:             node.NodeID,
					OnlineDuration:     node.OnlineDuration,
					DiskUsage:          node.DiskUsage,
					LastSeen:           time.Now(),
					BandwidthDown:      node.BandwidthDown,
					BandwidthUp:        node.BandwidthUp,
					Profit:             profit,
					TitanDiskUsage:     node.TitanDiskUsage,
					AvailableDiskSpace: node.AvailableDiskSpace,
				})
			}
		}

		return true
	})

	m.candidateNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		if node == nil {
			return true
		}

		if m.nodeKeepalive(node, t) {
			if isSave {
				// Minute
				node.OnlineDuration += int((saveInfoInterval * keepaliveTime) / time.Minute)

				nodes = append(nodes, &types.NodeSnapshot{
					NodeID:             node.NodeID,
					OnlineDuration:     node.OnlineDuration,
					DiskUsage:          node.DiskUsage,
					LastSeen:           time.Now(),
					BandwidthDown:      node.BandwidthDown,
					BandwidthUp:        node.BandwidthUp,
					TitanDiskUsage:     node.TitanDiskUsage,
					AvailableDiskSpace: node.AvailableDiskSpace,
				})
			}
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

	return m.SaveNodeInfo(n)
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

		isValidator, err := m.IsValidator(node.NodeID)
		if err != nil || isValidator {
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

		if node.IsAbnormal() {
			return true
		}

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

func (m *Manager) UpdateNodeDiskUsage(nodeID string, diskUsage float64) {
	node := m.GetNode(nodeID)
	if node == nil {
		return
	}

	node.DiskUsage = diskUsage

	size, err := m.LoadReplicaSizeByNodeID(nodeID)
	if err != nil {
		log.Errorf("LoadReplicaSizeByNodeID %s err:%s", nodeID, err.Error())
		return
	}

	node.TitanDiskUsage = float64(size)
	// if node.DiskSpace <= 0 {
	// 	node.DiskUsage = 100
	// } else {
	// 	node.DiskUsage = (float64(size) / node.DiskSpace) * 100
	// }
	log.Infof("LoadReplicaSizeByNodeID %s update:%v", nodeID, node.TitanDiskUsage)
}
