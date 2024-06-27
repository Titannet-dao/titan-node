package node

import (
	"math/rand"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
)

type weightManager struct {
	config dtypes.GetSchedulerConfigFunc
	// Each node assigned a select weight, when pulling resources, randomly select n select weight, and select the node holding these select weight.

	// Weight distribution management for candidate nodes
	candidateLock           *sync.RWMutex
	candidateRand           *rand.Rand
	candidateMax            int            // Candidate select weight , Distribute from 1
	distributedCandidates   map[int]string // Already allocated candidate select weights
	undistributedCandidates map[int]string // Undistributed candidate select weights

	// Weight distribution management for edge nodes
	edgeLock           *sync.RWMutex
	edgeRand           *rand.Rand
	edgeMax            int            // Edge select weight , Distribute from 1
	distributedEdges   map[int]string // Already allocated edge select weights
	undistributedEdges map[int]string // Undistributed edge select weights

	levelSelectWeight map[string]int
}

func newWeightManager(config dtypes.GetSchedulerConfigFunc) *weightManager {
	pullSelectSeed := time.Now().UnixNano()

	manager := &weightManager{
		edgeLock:                new(sync.RWMutex),
		candidateLock:           new(sync.RWMutex),
		candidateRand:           rand.New(rand.NewSource(pullSelectSeed)),
		edgeRand:                rand.New(rand.NewSource(pullSelectSeed)),
		distributedCandidates:   make(map[int]string),
		undistributedCandidates: make(map[int]string),
		distributedEdges:        make(map[int]string),
		undistributedEdges:      make(map[int]string),
		config:                  config,
	}

	manager.initWeightScale()

	return manager
}

// Assigns undistributed weight to candidate node and returns the assigned weights
func (wm *weightManager) distributeCandidateWeight(nodeID string, n int) []int {
	return wm.distributeWeight(nodeID, n, wm.candidateLock, &wm.candidateMax, wm.distributedCandidates, wm.undistributedCandidates)
}

// Assigns undistributed weight to edge node and returns the assigned weights
func (wm *weightManager) distributeEdgeWeight(nodeID string, n int) []int {
	return wm.distributeWeight(nodeID, n, wm.edgeLock, &wm.edgeMax, wm.distributedEdges, wm.undistributedEdges)
}

func (wm *weightManager) distributeWeight(nodeID string, n int, lock *sync.RWMutex, max *int, distributed map[int]string, undistributed map[int]string) []int {
	lock.Lock()
	defer lock.Unlock()

	assigned := make([]int, 0)

	for i := 0; i < n; i++ {
		weight := wm.getWeight(undistributed, max)
		delete(undistributed, weight)
		assigned = append(assigned, weight)
	}

	for _, w := range assigned {
		distributed[w] = nodeID
	}

	return assigned
}

func (wm *weightManager) getWeight(undistributed map[int]string, max *int) int {
	if len(undistributed) > 0 {
		for w := range undistributed {
			return w
		}
	}

	(*max)++
	return *max
}

// Repays the selection weight to candidate undistributed weights
func (wm *weightManager) repayCandidateWeight(weights []int) {
	if weights == nil {
		return
	}

	wm.repayWeight(weights, wm.candidateLock, wm.distributedCandidates, wm.undistributedCandidates)
}

// Repays the selection weight to edge undistributed weights
func (wm *weightManager) repayEdgeWeight(weights []int) {
	if weights == nil {
		return
	}

	wm.repayWeight(weights, wm.edgeLock, wm.distributedEdges, wm.undistributedEdges)
}

func (wm *weightManager) repayWeight(weights []int, lock *sync.RWMutex, distributed map[int]string, undistributed map[int]string) {
	lock.Lock()
	defer lock.Unlock()

	for _, w := range weights {
		delete(distributed, w)
		undistributed[w] = ""
	}
}

func (wm *weightManager) getCandidateWeightRandom(count int) map[string]int {
	return wm.getWeightRandoms(wm.candidateLock, wm.candidateRand, wm.candidateMax, wm.distributedCandidates, count)
}

func (wm *weightManager) getEdgeWeightRandom(count int) map[string]int {
	return wm.getWeightRandoms(wm.edgeLock, wm.edgeRand, wm.edgeMax, wm.distributedEdges, count)
}

func (wm *weightManager) getWeightRandoms(lock *sync.RWMutex, r *rand.Rand, max int, distributed map[int]string, count int) map[string]int {
	lock.Lock()
	defer lock.Unlock()

	outS := make(map[string]int, 0)

	if max <= 0 {
		return outS
	}

	for i := 0; i < count; i++ {
		w := r.Intn(max) + 1

		outS[distributed[w]] = w
	}

	return outS
}

func (wm *weightManager) cleanWeights() {
	wm.candidateLock.Lock()
	defer wm.candidateLock.Unlock()

	wm.edgeLock.Lock()
	defer wm.edgeLock.Unlock()

	wm.distributedCandidates = make(map[int]string)
	wm.undistributedCandidates = make(map[int]string)
	wm.distributedEdges = make(map[int]string)
	wm.undistributedEdges = make(map[int]string)

	wm.candidateMax = 0
	wm.edgeMax = 0
}

func (wm *weightManager) initWeightScale() {
	wm.levelSelectWeight = map[string]int{}
	cfg, err := wm.config()
	if err != nil {
		log.Errorf("get config err:%s", err.Error())
		return
	}

	wm.levelSelectWeight = cfg.LevelSelectWeight
}

func (wm *weightManager) getWeightNum(scoreLevel string) int {
	num, exist := wm.levelSelectWeight[scoreLevel]
	if exist {
		return num
	}

	return 1
}
