package node

import (
	"crypto/rsa"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/filecoin-project/pubsub"
	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("node")

const (
	// offlineTimeMax is the maximum amount of time a node can be offline before being considered as quit
	offlineTimeMax = 24 // hours

	// keepaliveTime is the interval between keepalive requests
	keepaliveTime = 30 * time.Second // seconds

	// saveInfoInterval is the interval at which node information is saved during keepalive requests
	saveInfoInterval = 10 // keepalive saves information every 10 times
)

// Manager is the node manager responsible for managing the online nodes
type Manager struct {
	edgeNodes      sync.Map
	candidateNodes sync.Map
	Edges          int // online edge node count
	Candidates     int // online candidate node count

	notify *pubsub.PubSub
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
func NewManager(sdb *db.SQLDB, serverID dtypes.ServerID, k *rsa.PrivateKey, p *pubsub.PubSub) *Manager {
	pullSelectSeed := time.Now().UnixNano()

	nodeManager := &Manager{
		SQLDB:      sdb,
		ServerID:   serverID,
		PrivateKey: k,
		notify:     p,

		cNodeNumRand:          rand.New(rand.NewSource(pullSelectSeed)),
		eNodeNumRand:          rand.New(rand.NewSource(pullSelectSeed)),
		cDistributedNodeNum:   make(map[int]string),
		cUndistributedNodeNum: make(map[int]string),
		eDistributedNodeNum:   make(map[int]string),
		eUndistributedNodeNum: make(map[int]string),
	}

	go nodeManager.run()

	return nodeManager
}

// run periodically sends keepalive requests to all nodes and checks if any nodes have been offline for too long
func (m *Manager) run() {
	ticker := time.NewTicker(keepaliveTime)
	defer ticker.Stop()

	count := 0

	for {
		<-ticker.C
		count++
		saveInfo := count%saveInfoInterval == 0
		m.nodesKeepalive(saveInfo)
		// Check how long a node has been offline
		m.checkNodesTTL()
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

// KeepaliveCallBackFunc Callback function to handle node keepalive
func KeepaliveCallBackFunc(nodeMgr *Manager) (dtypes.SessionCallbackFunc, error) {
	return func(nodeID, remoteAddr string) error {
		lastTime := time.Now()

		node := nodeMgr.GetNode(nodeID)
		if node != nil {
			node.SetLastRequestTime(lastTime)

			if remoteAddr != node.remoteAddr {
				return xerrors.New("remoteAddr inconsistent")
			}
		}

		return nil
	}, nil
}

// checkNodesTTL Check for nodes that have timed out and quit them
func (m *Manager) checkNodesTTL() {
	nodes, err := m.LoadTimeoutNodes(offlineTimeMax, m.ServerID)
	if err != nil {
		log.Errorf("checkWhetherNodeQuits LoadTimeoutNodes err:%s", err.Error())
		return
	}

	if len(nodes) > 0 {
		m.NodesQuit(nodes)
	}
}

// saveInfo Save node information when it comes online
func (m *Manager) saveInfo(n *types.NodeInfo) error {
	n.IsQuitted = false
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
