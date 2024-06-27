package node

import (
	"crypto/rsa"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/docker/go-units"
	"github.com/filecoin-project/pubsub"

	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("node")

const (
	// keepaliveTime is the interval between keepalive requests
	keepaliveTime = 10 * time.Second // seconds

	// saveInfoInterval is the interval at which node information is saved during keepalive requests
	saveInfoInterval = 5 * time.Minute // keepalive saves information
	penaltyInterval  = 60 * time.Second

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

	ipLimit int
	// TotalNetworkEdges int // Number of edge nodes in the entire network (including those on other schedulers)

	nodeIPs   map[string][]string
	ipMapLock *sync.RWMutex

	nodeScoreLevel map[string][]int
	geoMgr         *GeoMgr

	serverOnlineCounts map[time.Time]int

	candidateOnlineCounts map[string]int
	cOnlineCountsLock     sync.RWMutex
	edgeOnlineCounts      map[string]int
	eOnlineCountsLock     sync.RWMutex
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
		geoMgr:     newMgr(),
		ipMapLock:  new(sync.RWMutex),
		nodeIPs:    make(map[string][]string),
	}

	nodeManager.ipLimit = nodeManager.getIPLimit()
	nodeManager.initLevelScale()
	nodeManager.updateServerOnlineCounts()

	go nodeManager.startNodeKeepaliveTimer()
	go nodeManager.startCheckNodeTimer()
	go nodeManager.startSaveNodeDataTimer()
	go nodeManager.startNodePenaltyTimer()
	// go nodeManager.startSyncEdgeCountTimer()
	// go nodeManager.startCalculatePointsTimer()

	// go nodeManager.startMxTimer()

	return nodeManager
}

func (m *Manager) AddNodeGeo(nodeInfo *types.NodeInfo, geo *region.GeoInfo) {
	m.geoMgr.AddNode(geo.Continent, geo.Country, geo.Province, geo.City, nodeInfo)
}

func (m *Manager) RemoveNodeGeo(nodeID string, geo *region.GeoInfo) {
	m.geoMgr.RemoveNode(geo.Continent, geo.Country, geo.Province, geo.City, nodeID)
}

func (m *Manager) FindNodesFromGeo(continent, country, province, city string) []*types.NodeInfo {
	return m.geoMgr.FindNodes(continent, country, province, city)
}

func (m *Manager) GetGeoKey(continent, country, province string) map[string]int {
	return m.geoMgr.GetGeoKey(continent, country, province)
}

func (m *Manager) getIPLimit() int {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("get config err:%s", err.Error())
		return 0
	}

	return cfg.IPLimit
}

func (m *Manager) StoreNodeIP(nodeID, ip string) bool {
	m.ipMapLock.Lock()
	defer m.ipMapLock.Unlock()

	list, exist := m.nodeIPs[ip]
	if exist {
		for _, nID := range list {
			if nID == nodeID {
				return true
			}
		}

		if len(list) < m.ipLimit {
			list = append(list, nodeID)
			m.nodeIPs[ip] = list
			return true
		}

		return false
	} else {
		m.nodeIPs[ip] = []string{nodeID}

		return true
	}
}

func (m *Manager) RemoveNodeIP(nodeID, ip string) {
	m.ipMapLock.Lock()
	defer m.ipMapLock.Unlock()

	list, exist := m.nodeIPs[ip]
	if exist {
		nList := []string{}

		for _, nID := range list {
			if nID != nodeID {
				nList = append(nList, nID)
			}
		}

		m.nodeIPs[ip] = nList
	}
}

func (m *Manager) GetNodeOfIP(ip string) []string {
	return m.nodeIPs[ip]
}

func (m *Manager) CheckIPExist(ip string) bool {
	_, exist := m.nodeIPs[ip]

	return exist
}

// func (m *Manager) syncEdgeCountFromNetwork() {
// 	err := m.etcdcli.PutEdgeCount(string(m.ServerID), m.Edges)
// 	if err != nil {
// 		log.Errorf("SyncEdgeCountFromNetwork PutEdgeCount err:%s", err.Error())
// 	}

// 	count, err := m.etcdcli.GetEdgeCounts(string(m.ServerID))
// 	if err != nil {
// 		log.Errorf("SyncEdgeCountFromNetwork GetEdgeCounts err:%s", err.Error())
// 	}

// 	m.TotalNetworkEdges = count + m.Edges
// }

// // startSyncEdgeCountTimer
// func (m *Manager) startSyncEdgeCountTimer() {
// 	ticker := time.NewTicker(syncEdgeCountTime)
// 	defer ticker.Stop()

// 	for {
// 		<-ticker.C

// 		m.syncEdgeCountFromNetwork()
// 	}
// }

func (m *Manager) startSaveNodeDataTimer() {
	ticker := time.NewTicker(saveInfoInterval)
	defer ticker.Stop()

	for {
		<-ticker.C

		m.updateNodeData()
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

	// Merge L1 nodes
	// if node.Type == types.NodeValidator {
	// 	return
	// }

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

// nodesKeepalive checks all nodes in the manager's lists for keepalive
func (m *Manager) updateNodeData() {
	nodes := make([]*types.NodeDynamicInfo, 0)

	onlineDuration := 5

	detailsList := make([]*types.ProfitDetails, 0)

	m.edgeNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		if node == nil {
			return true
		}

		if node.IsAbnormal() {
			return true
		}

		incr, dInfo := m.GetEdgeBaseProfitDetails(node)
		detailsList = append(detailsList, dInfo)
		node.Profit += dInfo.Profit

		node.OnlineDuration += onlineDuration
		node.KeepaliveCount = 0
		// add node mc
		node.IncomeIncr = incr

		nodes = append(nodes, &node.NodeDynamicInfo)

		return true
	})

	m.candidateNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		if node == nil {
		}

		if node.IsAbnormal() {
			return true
		}

		dInfo := m.GetCandidateBaseProfitDetails(node)
		detailsList = append(detailsList, dInfo)
		node.Profit += dInfo.Profit

		node.OnlineDuration += onlineDuration
		nodes = append(nodes, &node.NodeDynamicInfo)

		return true
	})

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
	m.candidateNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)

		if node.IsAbnormal() {
			return true
		}

		node.OnlineRate = m.ComputeNodeOnlineRate(node.NodeID, node.FirstTime)

		// Merge L1 nodes
		// if node.Type == types.NodeValidator {
		// 	return true
		// }

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

		node.OnlineRate = m.ComputeNodeOnlineRate(node.NodeID, node.FirstTime)

		score := m.getNodeScoreLevel(node.NodeID)
		wNum := m.weightMgr.getWeightNum(score)
		node.selectWeights = m.weightMgr.distributeEdgeWeight(node.NodeID, wNum)

		return true
	})
}

func (m *Manager) ComputeNodeOnlineRate(nodeID string, firstTime time.Time) float64 {
	nodeC := 0
	serverC := 0

	for date, serverCount := range m.serverOnlineCounts {
		if firstTime.Before(date) {
			continue
		}

		nodeCount, err := m.GetOnlineCount(nodeID, date)
		if err != nil {
			log.Errorf("GetOnlineCount %s err: %v", nodeID, err)
		}

		nodeC += nodeCount
		serverC += serverCount
	}

	if serverC == 0 {
		return 1.0
	}

	if nodeC > serverC {
		return 1
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
	// if node.DiskSpace <= 0 {
	// 	node.DiskUsage = 100
	// } else {
	// 	node.DiskUsage = (float64(size) / node.DiskSpace) * 100
	// }
	log.Infof("LoadReplicaSizeByNodeID %s update:%v", nodeID, node.TitanDiskUsage)
}
