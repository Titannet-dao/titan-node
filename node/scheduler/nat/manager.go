package nat

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("scheduler/nat")

const (
	miniCandidateCount = 2
	detectInterval     = 30
	maxRetry           = 10
)

type Manager struct {
	nodeManager  *node.Manager
	schedulerCfg *config.SchedulerCfg
	http3Client  *http.Client

	retryEdgeLock *sync.Mutex
	edgeMap       *sync.Map
	retryEdgeList []*retryNode

	retryCandidateLock *sync.Mutex
	candidateMap       *sync.Map
	retryCandidateList []*retryNode
}

type retryNode struct {
	id    string
	retry int
}

func NewManager(nodeMgr *node.Manager, config *config.SchedulerCfg) *Manager {
	http3Client := client.NewHTTP3Client()
	http3Client.Timeout = 5 * time.Second

	m := &Manager{
		nodeManager:        nodeMgr,
		retryEdgeLock:      &sync.Mutex{},
		retryEdgeList:      make([]*retryNode, 0),
		edgeMap:            &sync.Map{},
		retryCandidateLock: &sync.Mutex{},
		retryCandidateList: make([]*retryNode, 0),
		candidateMap:       &sync.Map{},
		schedulerCfg:       config,
		http3Client:        http3Client,
	}
	go m.startTicker()

	return m
}

func (m *Manager) startTicker() {
	for {
		time.Sleep(detectInterval * time.Second)

		eList := m.retryEdgeList
		var eRetryNodes []*retryNode
		for len(eList) > 0 {
			eRetryNodes, eList = m.edgesFromHead(m.schedulerCfg.NatDetectConcurrency, eList)
			m.retryDetectEdgesNatType(eRetryNodes)
		}

		cList := m.retryCandidateList
		var cRetryNodes []*retryNode
		for len(cList) > 0 {
			cRetryNodes, cList = m.candidatesFromHead(m.schedulerCfg.NatDetectConcurrency, cList)
			m.retryDetectCandidatesNatType(cRetryNodes)
		}
	}
}

func (m *Manager) edgesFromHead(n int, eList []*retryNode) ([]*retryNode, []*retryNode) {
	m.retryEdgeLock.Lock()
	defer m.retryEdgeLock.Unlock()

	if len(eList) == 0 {
		return nil, eList
	}

	if len(eList) < n {
		n = len(eList)
	}

	retryNodes := eList[0:n]
	eList = eList[n:]
	return retryNodes, eList
}

func (m *Manager) candidatesFromHead(n int, eList []*retryNode) ([]*retryNode, []*retryNode) {
	m.retryCandidateLock.Lock()
	defer m.retryCandidateLock.Unlock()

	if len(eList) == 0 {
		return nil, eList
	}

	if len(eList) < n {
		n = len(eList)
	}

	retryNodes := eList[0:n]
	eList = eList[n:]
	return retryNodes, eList
}

func (m *Manager) addEdgeNode(n *retryNode) {
	m.retryEdgeLock.Lock()
	defer m.retryEdgeLock.Unlock()
	for _, node := range m.retryEdgeList {
		if node.id == n.id {
			return
		}
	}

	m.retryEdgeList = append(m.retryEdgeList, n)
}

func (m *Manager) deleteEdgeNode(n *retryNode) {
	m.retryEdgeLock.Lock()
	defer m.retryEdgeLock.Unlock()
	index := -1
	for i, node := range m.retryEdgeList {
		if node.id == n.id {
			index = i
			break
		}
	}

	if index < 0 {
		return
	}

	endIndex := len(m.retryEdgeList) - 1

	m.retryEdgeList[index] = m.retryEdgeList[endIndex]
	m.retryEdgeList = m.retryEdgeList[:endIndex]
}

func (m *Manager) addCandidateNode(n *retryNode) {
	m.retryCandidateLock.Lock()
	defer m.retryCandidateLock.Unlock()
	for _, node := range m.retryCandidateList {
		if node.id == n.id {
			return
		}
	}

	m.retryCandidateList = append(m.retryCandidateList, n)
}

func (m *Manager) deleteCandidateNode(n *retryNode) {
	m.retryCandidateLock.Lock()
	defer m.retryCandidateLock.Unlock()
	index := -1
	for i, node := range m.retryCandidateList {
		if node.id == n.id {
			index = i
			break
		}
	}

	if index < 0 {
		return
	}

	endIndex := len(m.retryCandidateList) - 1

	m.retryCandidateList[index] = m.retryCandidateList[endIndex]
	m.retryCandidateList = m.retryCandidateList[:endIndex]
}

func (m *Manager) isInRetryEdgeList(nodeID string) bool {
	m.retryEdgeLock.Lock()
	defer m.retryEdgeLock.Unlock()
	for _, node := range m.retryEdgeList {
		if node.id == nodeID {
			return true
		}
	}

	return false
}

func (m *Manager) isInRetryCandidateList(nodeID string) bool {
	m.retryCandidateLock.Lock()
	defer m.retryCandidateLock.Unlock()
	for _, node := range m.retryCandidateList {
		if node.id == nodeID {
			return true
		}
	}

	return false
}

func (m *Manager) delayEdgeDetectNatType(n *retryNode) {
	m.addEdgeNode(n)
}

func (m *Manager) delayCandidateDetectNatType(n *retryNode) {
	m.addCandidateNode(n)
}

func (m *Manager) retryDetectEdgesNatType(nodes []*retryNode) {
	wg := &sync.WaitGroup{}

	for _, node := range nodes {
		wg.Add(1)
		go func(n *retryNode) {
			defer wg.Done()
			m.retryEdgeDetectNatType(n)
		}(node)
	}

	wg.Wait()
}

func (m *Manager) retryDetectCandidatesNatType(nodes []*retryNode) {
	wg := &sync.WaitGroup{}

	for _, node := range nodes {
		wg.Add(1)
		go func(n *retryNode) {
			defer wg.Done()
			m.retryCandidateDetectNatType(n)
		}(node)
	}

	wg.Wait()
}

func (m *Manager) retryCandidateDetectNatType(rInfo *retryNode) {
	rInfo.retry++
	nodeID := rInfo.id

	_, ok := m.candidateMap.LoadOrStore(nodeID, struct{}{})
	if ok {
		log.Warnf("node %s determining nat type")
		return
	}
	defer m.candidateMap.Delete(nodeID)

	cNode := m.nodeManager.GetNode(nodeID)
	if cNode == nil {
		m.deleteCandidateNode(rInfo)
		return
	}

	_, caNodes := m.nodeManager.GetAllCandidateNodes()
	cNodes := make([]*node.Node, 0)
	for _, node := range caNodes {

		if node.NodeID == nodeID {
			continue
		}
		cNodes = append(cNodes, node)

		if len(cNodes) >= miniCandidateCount {
			break
		}
	}

	cNode.NATType = determineNodeNATType(context.Background(), cNode, cNodes, m.http3Client)

	if cNode.NATType != types.NatTypeUnknown.String() || rInfo.retry >= (maxRetry*2) {
		m.deleteCandidateNode(rInfo)
	}
	log.Debugf("retry detect node %s nat type %s , %d", rInfo.id, cNode.NATType, rInfo.retry)
}

func (m *Manager) retryEdgeDetectNatType(rInfo *retryNode) {
	rInfo.retry++
	nodeID := rInfo.id

	_, ok := m.edgeMap.LoadOrStore(nodeID, struct{}{})
	if ok {
		log.Warnf("node %s determining nat type")
		return
	}
	defer m.edgeMap.Delete(nodeID)

	eNode := m.nodeManager.GetNode(nodeID)
	if eNode == nil {
		m.deleteEdgeNode(rInfo)
		return
	}

	cNodes := m.nodeManager.GetCandidateNodes(miniCandidateCount)

	eNode.NATType = determineNodeNATType(context.Background(), eNode, cNodes, m.http3Client)

	if eNode.NATType != types.NatTypeUnknown.String() || rInfo.retry >= maxRetry {
		m.deleteEdgeNode(rInfo)
	}
	log.Debugf("retry detect node %s nat type %s , %d", rInfo.id, eNode.NATType, rInfo.retry)
}

func (m *Manager) DetermineCandidateNATType(ctx context.Context, nodeID string) {
	if m.isInRetryCandidateList(nodeID) {
		log.Debugf("node %s waiting to retry", nodeID)
		return
	}

	_, ok := m.candidateMap.LoadOrStore(nodeID, struct{}{})
	if ok {
		log.Warnf("node %s determining nat type")
		return
	}
	defer m.candidateMap.Delete(nodeID)

	eNode := m.nodeManager.GetNode(nodeID)
	if eNode == nil {
		log.Errorf("node %s offline or not exists", nodeID)
		return
	}

	_, caNodes := m.nodeManager.GetAllCandidateNodes()
	cNodes := make([]*node.Node, 0)
	for _, node := range caNodes {

		if node.NodeID == nodeID {
			continue
		}
		cNodes = append(cNodes, node)

		if len(cNodes) >= miniCandidateCount {
			break
		}
	}

	eNode.NATType = determineNodeNATType(ctx, eNode, cNodes, m.http3Client)

	if eNode.NATType == types.NatTypeUnknown.String() {
		m.delayCandidateDetectNatType(&retryNode{id: nodeID, retry: 0})
	}
	log.Debugf("%s nat type %s", nodeID, eNode.NATType)
}

func (m *Manager) DetermineEdgeNATType(ctx context.Context, nodeID string) {
	if m.isInRetryEdgeList(nodeID) {
		log.Debugf("node %s waiting to retry", nodeID)
		return
	}

	_, ok := m.edgeMap.LoadOrStore(nodeID, struct{}{})
	if ok {
		log.Warnf("node %s determining nat type")
		return
	}
	defer m.edgeMap.Delete(nodeID)

	eNode := m.nodeManager.GetNode(nodeID)
	if eNode == nil {
		log.Errorf("node %s offline or not exists", nodeID)
		return
	}

	cNodes := m.nodeManager.GetCandidateNodes(miniCandidateCount)

	eNode.NATType = determineNodeNATType(ctx, eNode, cNodes, m.http3Client)

	if eNode.NATType == types.NatTypeUnknown.String() {
		m.delayEdgeDetectNatType(&retryNode{id: nodeID, retry: 0})
	}
	log.Debugf("%s nat type %s", nodeID, eNode.NATType)
}

// GetCandidateURLsForDetectNat Get the rpc url of the specified number of candidate nodes
func (m *Manager) GetCandidateURLsForDetectNat(ctx context.Context) ([]string, error) {
	// minimum of 3 candidates is required for user detect nat
	needCandidateCount := miniCandidateCount + 1
	candidates := m.nodeManager.GetCandidateNodes(needCandidateCount)
	if len(candidates) < needCandidateCount {
		return nil, fmt.Errorf("minimum of %d candidates is required", needCandidateCount)
	}

	urls := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		urls = append(urls, candidate.RPCURL())
	}
	return urls, nil
}
