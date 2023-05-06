package node

import (
	"crypto/rsa"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/filecoin-project/pubsub"
	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("node")

const (
	// keepaliveTime is the interval between keepalive requests
	keepaliveTime = 30 * time.Second // seconds

	// saveInfoInterval is the interval at which node information is saved during keepalive requests
	saveInfoInterval = 10 // keepalive saves information every 10 times

	// Processing validation result data from 5 days ago
	vResultDay = 5 * 24 * time.Hour
	// Process 1000 pieces of validation result data at a time
	vResultLimit = 1000
)

// Manager is the node manager responsible for managing the online nodes
type Manager struct {
	edgeNodes      sync.Map
	candidateNodes sync.Map
	Edges          int // online edge node count
	Candidates     int // online candidate node count

	etcdcli *etcdcli.Client
	notify  *pubsub.PubSub
	*db.SQLDB
	*rsa.PrivateKey // scheduler privateKey
	dtypes.ServerID // scheduler server id

	// Each node assigned a node number, when pulling resources, randomly select n node number, and select the node holding these node number.
	nodeNumLock           sync.RWMutex
	cNodeNumRand          *rand.Rand
	eNodeNumRand          *rand.Rand
	cNodeNumMax           int            // Candidate node number , Distribute from 1
	eNodeNumMax           int            // Edge node number , Distribute from 1
	cDistributedNodeNum   map[int]string // Already allocated candidate node numbers
	cUndistributedNodeNum map[int]string // Undistributed candidate node numbers
	eDistributedNodeNum   map[int]string // Already allocated edge node numbers
	eUndistributedNodeNum map[int]string // Undistributed edge node numbers
}

// NewManager creates a new instance of the node manager
func NewManager(sdb *db.SQLDB, serverID dtypes.ServerID, pk *rsa.PrivateKey, pb *pubsub.PubSub, ec *etcdcli.Client) *Manager {
	pullSelectSeed := time.Now().UnixNano()

	nodeManager := &Manager{
		SQLDB:      sdb,
		ServerID:   serverID,
		PrivateKey: pk,
		notify:     pb,
		etcdcli:    ec,

		cNodeNumRand:          rand.New(rand.NewSource(pullSelectSeed)),
		eNodeNumRand:          rand.New(rand.NewSource(pullSelectSeed)),
		cDistributedNodeNum:   make(map[int]string),
		cUndistributedNodeNum: make(map[int]string),
		eDistributedNodeNum:   make(map[int]string),
		eUndistributedNodeNum: make(map[int]string),
	}

	go nodeManager.startNodeKeepaliveTimer()
	// go nodeManager.startHandleValidationResultTimer()

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

func (m *Manager) startHandleValidationResultTimer() {
	now := time.Now()

	nextTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	if now.After(nextTime) {
		nextTime = nextTime.Add(24 * time.Hour)
	}

	duration := nextTime.Sub(now)

	timer := time.NewTimer(duration)
	defer timer.Stop()

	for {
		<-timer.C

		log.Debugln("start validation result check ")

		m.handleValidationResults()

		timer.Reset(24 * time.Hour)
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

	num := m.distributeEdgeNodeNum(nodeID)
	node.nodeNum = num

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

	num := m.distributeCandidateNodeNum(nodeID)
	node.nodeNum = num

	m.notify.Pub(node, types.EventNodeOnline.String())
}

// deleteEdgeNode removes an edge node from the manager's list of edge nodes
func (m *Manager) deleteEdgeNode(node *Node) {
	m.repayEdgeNodeNum(node.nodeNum)
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
	m.repayCandidateNodeNum(node.nodeNum)
	m.notify.Pub(node, types.EventNodeOffline.String())

	nodeID := node.NodeID
	_, loaded := m.candidateNodes.LoadAndDelete(nodeID)
	if !loaded {
		return
	}
	m.Candidates--
}

// nodeKeepalive checks if a node has sent a keepalive recently and updates node status accordingly
func (m *Manager) nodeKeepalive(node *Node, t time.Time, isSave bool) {
	lastTime := node.LastRequestTime()

	nodeID := node.NodeID

	if !lastTime.After(t) {
		node.ClientCloser()
		if node.Type == types.NodeCandidate {
			m.deleteCandidateNode(node)
		} else if node.Type == types.NodeEdge {
			m.deleteEdgeNode(node)
		}
		node = nil
		return
	}

	if isSave {
		// Minute
		node.OnlineDuration += int((saveInfoInterval * keepaliveTime) / time.Minute)

		err := m.UpdateNodeOnlineTime(nodeID, node.OnlineDuration)
		if err != nil {
			log.Errorf("UpdateNodeOnlineTime err:%s,nodeID:%s", err.Error(), nodeID)
		}
	}
}

// nodesKeepalive checks all nodes in the manager's lists for keepalive
func (m *Manager) nodesKeepalive(isSave bool) {
	t := time.Now().Add(-keepaliveTime)

	m.edgeNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		if node == nil {
			return true
		}

		go m.nodeKeepalive(node, t, isSave)

		return true
	})

	m.candidateNodes.Range(func(key, value interface{}) bool {
		node := value.(*Node)
		if node == nil {
			return true
		}

		go m.nodeKeepalive(node, t, isSave)

		return true
	})
}

func (m *Manager) nodeSession(nodeID, remoteAddr string) error {
	lastTime := time.Now()

	node := m.GetNode(nodeID)
	if node != nil {
		node.SetLastRequestTime(lastTime)

		if remoteAddr != node.remoteAddr {
			return xerrors.New("remoteAddr inconsistent")
		}
	}

	return nil
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

// NewNodeID create a node id
func (m *Manager) NewNodeID(nType types.NodeType) (string, error) {
	nodeID := ""
	switch nType {
	case types.NodeEdge:
		nodeID = "e_"
	case types.NodeCandidate:
		nodeID = "c_"
	default:
		return nodeID, xerrors.Errorf("node type %s is error", nType.String())
	}

	uid := uuid.NewString()
	uid = strings.Replace(uid, "-", "", -1)

	return fmt.Sprintf("%s%s", nodeID, uid), nil
}

func (m *Manager) handleValidationResults() {
	// TODO Need to save the leaseID to find the session next time
	leaseID, err := m.etcdcli.AcquireMasterLock(types.RunningNodeType.String())
	if err != nil {
		log.Errorf("handleValidationResults SetMasterScheduler err:%s", err.Error())
		return
	}

	defer func() {
		log.Infoln("handleValidationResults done")

		err = m.etcdcli.ReleaseMasterLock(leaseID, types.RunningNodeType.String())
		if err != nil {
			log.Errorf("RemoveMasterScheduler err:%s", err.Error())
		}
	}()

	log.Infof("handleValidationResults %s", m.ServerID)

	mTime := time.Now().Add(-vResultDay)

	// do handle validation result
	for {
		rows, err := m.LoadValidationResults(mTime, vResultLimit)
		if err != nil {
			log.Errorf("LoadValidationResults err:%s", err.Error())
			return
		}

		ids := make([]int, 0)
		nodeProfits := make(map[string]float64)

		for rows.Next() {
			info := &types.ValidationResultInfo{}
			err = rows.StructScan(info)
			if err != nil {
				log.Errorf("ValidationResultInfo StructScan err: %s", err.Error())
				continue
			}

			ids = append(ids, info.ID)

			if info.Profit == 0 {
				continue
			}

			nodeProfits[info.NodeID] += info.Profit
		}
		rows.Close()

		if len(ids) == 0 {
			return
		}

		err = m.UpdateNodeProfitsByValidationResult(ids, nodeProfits)
		if err != nil {
			log.Errorf("UpdateNodeProfitsByValidationResult err:%s", err.Error())
		}
	}
}
