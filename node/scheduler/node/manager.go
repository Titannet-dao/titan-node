package node

import (
	"crypto/rsa"
	"math"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/docker/go-units"
	"github.com/filecoin-project/pubsub"

	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	logging "github.com/ipfs/go-log/v2"
)

var (
	log = logging.Logger("node")

	levelSelectWeight = []int{1, 2, 3, 4, 5}
	nodeScoreLevel    = []int{20, 50, 70, 90, 100}
)

const (
	// keepaliveTime is the interval between keepalive requests
	keepaliveTime = 10 * time.Second // seconds

	// saveInfoInterval is the interval at which node information is saved during keepalive requests
	saveInfoInterval = 5 * time.Minute // keepalive saves information
	penaltyInterval  = 60 * time.Second

	oneDay = 24 * time.Hour

	penaltyFreeTime = 10 * time.Minute
)

// Manager is the node manager responsible for managing the online nodes
type Manager struct {
	edgeNodes      sync.Map
	candidateNodes sync.Map
	l5Nodes        sync.Map
	Edges          int // online edge node count
	Candidates     int // online candidate node count
	weightMgr      *weightManager
	config         dtypes.GetSchedulerConfigFunc
	notify         *pubsub.PubSub
	etcdcli        *etcdcli.Client
	*db.SQLDB
	*rsa.PrivateKey // scheduler privateKey
	dtypes.ServerID // scheduler server id

	// TotalNetworkEdges int // Number of edge nodes in the entire network (including those on other schedulers)

	GeoMgr *GeoMgr
	IPMgr  *IPMgr

	serverOnlineCounts map[time.Time]int
}

// NewManager creates a new instance of the node manager
func NewManager(sdb *db.SQLDB, serverID dtypes.ServerID, pk *rsa.PrivateKey, pb *pubsub.PubSub, config dtypes.GetSchedulerConfigFunc, ec *etcdcli.Client) *Manager {
	ipLimit := 5
	cfg, err := config()
	if err == nil {
		ipLimit = cfg.IPLimit
	}

	nodeManager := &Manager{
		SQLDB:      sdb,
		ServerID:   serverID,
		PrivateKey: pk,
		notify:     pb,
		config:     config,
		etcdcli:    ec,
		weightMgr:  newWeightManager(config),
		GeoMgr:     newGeoMgr(),
		IPMgr:      newIPMgr(ipLimit),
	}

	nodeManager.updateServerOnlineCounts()

	go nodeManager.startNodeKeepaliveTimer()
	go nodeManager.startSaveNodeDataTimer()
	go nodeManager.startNodePenaltyTimer()
	go nodeManager.startCheckNodeTimer()

	return nodeManager
}

func (m *Manager) startSaveNodeDataTimer() {
	time.Sleep(penaltyFreeTime)

	ticker := time.NewTicker(saveInfoInterval)
	defer ticker.Stop()

	// Give nodes a data to make up for 10 minutes
	m.updateNodeData(true)

	for {
		<-ticker.C

		m.updateNodeData(false)
	}
}

func (m *Manager) startCheckNodeTimer() {
	now := time.Now()

	nextTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 1, 0, 0, time.UTC)
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

		err := m.CleanData()
		if err != nil {
			log.Errorf("CleanEvents err:%s", err.Error())
		}

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

func (m *Manager) storeL5Node(node *Node) {
	if node == nil {
		return
	}

	nodeID := node.NodeID
	_, loaded := m.l5Nodes.LoadOrStore(nodeID, node)
	if loaded {
		return
	}
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

// deleteL5Node removes a l5 node from the manager's list of l5 nodes
func (m *Manager) deleteL5Node(node *Node) {
	nodeID := node.NodeID
	_, loaded := m.candidateNodes.LoadAndDelete(nodeID)
	if !loaded {
		return
	}
}

// DistributeNodeWeight Distribute Node Weight
func (m *Manager) DistributeNodeWeight(node *Node) {
	if node.IsAbnormal() {
		return
	}

	node.Level = m.getNodeScoreLevel(node)
	wNum := m.weightMgr.getWeightNum(node.Level)
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

// nodesKeepalive checks all nodes in the manager's lists for keepalive
func (m *Manager) updateNodeData(isCompensate bool) {
	nodes := make([]*types.NodeDynamicInfo, 0)
	detailsList := make([]*types.ProfitDetails, 0)

	minute := 5
	if isCompensate {
		minute = 10
	}
	eList := m.GetAllEdgeNode()
	for _, node := range eList {
		incr, dInfo := m.GetEdgeBaseProfitDetails(node, minute)
		if dInfo != nil {
			detailsList = append(detailsList, dInfo)
		}

		node.OnlineDuration += minute
		// add node mc
		node.IncomeIncr = incr

		nodes = append(nodes, &node.NodeDynamicInfo)

	}

	_, cList := m.GetAllCandidateNodes()
	for _, node := range cList {
		if qualifiedNAT(node.NATType) {
			dInfo := m.GetCandidateBaseProfitDetails(node, minute)
			if dInfo != nil {
				detailsList = append(detailsList, dInfo)
			}
		}

		node.OnlineDuration += minute

		nodes = append(nodes, &node.NodeDynamicInfo)
	}

	if len(nodes) > 0 {
		eList, err := m.UpdateNodeDynamicInfo(nodes)
		if err != nil {
			log.Errorf("UpdateNodeDynamicInfo err:%s", err.Error())
		}

		if len(eList) > 0 {
			for _, str := range eList {
				log.Errorln(str)
			}
		}
	}

	if len(detailsList) > 0 {
		for _, data := range detailsList {
			err := m.AddNodeProfit(data)
			if err != nil {
				log.Errorf("AddNodeProfit %s,%d, %.4f err:%s", data.NodeID, data.PType, data.Profit, err.Error())
			}
		}
	}
}

func roundDivision(a, b int) int {
	result := float64(a) / float64(b)
	return int(math.Round(result))
}

func qualifiedNAT(natType string) bool {
	if natType == types.NatTypeNo.String() || natType == types.NatTypeFullCone.String() || natType == types.NatTypeUnknown.String() {
		return true
	}

	return false
}

// saveInfo Save node information when it comes online
func (m *Manager) saveInfo(n *types.NodeInfo) error {
	n.LastSeen = time.Now()

	return m.SaveNodeInfo(n)
}

func (m *Manager) updateServerOnlineCounts() {
	now := time.Now()

	m.serverOnlineCounts = make(map[time.Time]int)

	for i := 1; i < 8; i++ {
		date := now.AddDate(0, 0, -i)
		date = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())

		count, err := m.GetOnlineCount(string(m.ServerID), date)
		if err != nil {
			log.Errorf("GetOnlineCount %s err: %s", string(m.ServerID), err.Error())
		} else {
			m.serverOnlineCounts[date] = count
		}
	}
}

func (m *Manager) redistributeNodeSelectWeights() {
	// repay all weights
	m.weightMgr.cleanWeights()

	m.updateServerOnlineCounts()

	// redistribute weights
	_, cList := m.GetAllCandidateNodes()
	for _, node := range cList {
		info, err := m.LoadNodeInfo(node.NodeID)
		if err != nil {
			continue
		}

		node.OnlineRate = m.ComputeNodeOnlineRate(node.NodeID, info.FirstTime)

		node.Level = m.getNodeScoreLevel(node)
		wNum := m.weightMgr.getWeightNum(node.Level)
		node.selectWeights = m.weightMgr.distributeCandidateWeight(node.NodeID, wNum)
	}

	eList := m.GetAllEdgeNode()
	for _, node := range eList {
		info, err := m.LoadNodeInfo(node.NodeID)
		if err != nil {
			continue
		}

		node.OnlineRate = m.ComputeNodeOnlineRate(node.NodeID, info.FirstTime)

		node.Level = m.getNodeScoreLevel(node)
		wNum := m.weightMgr.getWeightNum(node.Level)
		node.selectWeights = m.weightMgr.distributeEdgeWeight(node.NodeID, wNum)
	}
}

// ComputeNodeOnlineRate Compute node online rate
func (m *Manager) ComputeNodeOnlineRate(nodeID string, firstTime time.Time) float64 {
	nodeC := 0
	serverC := 0

	for date, serverCount := range m.serverOnlineCounts {
		log.Infof("%s %s/%s %v", nodeID, date.String(), firstTime.String(), firstTime.Before(date))
		if firstTime.Before(date) {
			nodeCount, err := m.GetOnlineCount(nodeID, date)
			if err != nil {
				log.Errorf("GetOnlineCount %s err: %v", nodeID, err)
			}

			nodeC += nodeCount
			serverC += serverCount
		}
	}

	log.Infof("%s serverOnlineCounts [%v] %d/%d", nodeID, m.serverOnlineCounts, nodeC, serverC)

	if serverC == 0 {
		return 1.0
	}

	if nodeC >= serverC {
		return 1.0
	}

	return float64(nodeC) / float64(serverC)
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
	nodes, err := m.LoadDeactivateNodes(time.Now().Unix())
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

// UpdateNodeDiskUsage update node disk usage
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

	if node.ClientType == types.NodeAndroid || node.ClientType == types.NodeIOS {
		if size > 5*units.GiB {
			size = 5 * units.GiB
		}
	}

	node.TitanDiskUsage = float64(size)

	log.Infof("LoadReplicaSizeByNodeID %s update:%v", nodeID, node.TitanDiskUsage)
}
