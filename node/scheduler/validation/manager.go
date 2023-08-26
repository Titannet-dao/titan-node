package validation

import (
	"context"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/scheduler/assets"
	"github.com/Filecoin-Titan/titan/node/scheduler/leadership"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/filecoin-project/pubsub"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("validation")

const (
	validationWorkers = 50
	oneDay            = 24 * time.Hour
)

// VWindow represents a validation window that contains a validator id and validatable node list.
type VWindow struct {
	NodeID           string // Node ID of the validation window.
	ValidatableNodes map[string]int64
}

func newVWindow(nID string) *VWindow {
	return &VWindow{
		NodeID:           nID,
		ValidatableNodes: make(map[string]int64),
	}
}

// ValidatableGroup Each ValidatableGroup will be paired with a VWindow
type ValidatableGroup struct {
	sumBwUp int64
	nodes   map[string]int64
	lock    sync.RWMutex
}

func newValidatableGroup() *ValidatableGroup {
	return &ValidatableGroup{
		nodes: make(map[string]int64),
	}
}

// Manager validation manager
type Manager struct {
	nodeMgr  *node.Manager
	assetMgr *assets.Manager
	notify   *pubsub.PubSub

	// Each validator provides n window(VWindow) for titan according to the bandwidth down, and each window corresponds to a group(ValidatableGroup).
	// All nodes will randomly fall into a group(ValidatableGroup).
	// When the validation starts, the window is paired with the group.
	validationPairLock sync.RWMutex
	vWindows           []*VWindow          // The validator allocates n window according to the size of the bandwidth down
	validatableGroups  []*ValidatableGroup // Each VWindow has a ValidatableGroup
	unpairedGroup      *ValidatableGroup   // Save unpaired Validatable nodes

	seed       int64
	curRoundID string
	close      chan struct{}
	config     dtypes.GetSchedulerConfigFunc

	updateCh chan struct{}

	nextElectionTime time.Time

	profit float64

	// validation result worker
	resultQueue chan *api.ValidationResult

	leadershipMgr *leadership.Manager
}

// NewManager return new node manager instance
func NewManager(nodeMgr *node.Manager, assetMgr *assets.Manager, configFunc dtypes.GetSchedulerConfigFunc, p *pubsub.PubSub, lmgr *leadership.Manager) *Manager {
	manager := &Manager{
		nodeMgr:       nodeMgr,
		assetMgr:      assetMgr,
		config:        configFunc,
		close:         make(chan struct{}),
		unpairedGroup: newValidatableGroup(),
		updateCh:      make(chan struct{}, 1),
		notify:        p,
		resultQueue:   make(chan *api.ValidationResult),
		leadershipMgr: lmgr,
	}

	return manager
}

// Start start validate and elect task
func (m *Manager) Start(ctx context.Context) {
	go m.startValidationTicker(ctx)
	go m.startElectionTicker()
	go m.startHandleResultsTimer()

	m.subscribeNodeEvents()
	m.pullResults()
}

// Stop stop
func (m *Manager) Stop(ctx context.Context) error {
	return m.stopValidation(ctx)
}

func (m *Manager) subscribeNodeEvents() {
	subOnline := m.notify.Sub(types.EventNodeOnline.String())
	subOffline := m.notify.Sub(types.EventNodeOffline.String())

	go func() {
		defer m.notify.Unsub(subOnline)
		defer m.notify.Unsub(subOffline)

		for {
			select {
			case u := <-subOnline:
				node := u.(*node.Node)
				m.onNodeStateChange(node, true)
			case u := <-subOffline:
				node := u.(*node.Node)
				m.onNodeStateChange(node, false)
			}
		}
	}()
}

// onNodeStateChange  changes in the state of a node (i.e. whether it comes online or goes offline)
func (m *Manager) onNodeStateChange(node *node.Node, isOnline bool) {
	if node == nil {
		return
	}

	nodeID := node.NodeID

	isV, err := m.nodeMgr.IsValidator(nodeID)
	if err != nil {
		log.Errorf("IsValidator err:%s", err.Error())
		return
	}

	if isOnline {
		if node.IsAbnormal() {
			return
		}

		if isV {
			m.addValidator(nodeID, node.BandwidthDown)

			// update validator owner
			err = m.nodeMgr.UpdateValidatorInfo(m.nodeMgr.ServerID, nodeID)
			if err != nil {
				log.Errorf("UpdateValidatorInfo err:%s", err.Error())
			}
		} else {
			m.addValidatableNode(nodeID, node.BandwidthDown)
		}

		return
	}

	if isV {
		m.removeValidator(nodeID)
	} else {
		m.removeValidatableNode(nodeID)
	}
}

// GetNextElectionTime Get the time of the next election
func (m *Manager) GetNextElectionTime() time.Time {
	return m.nextElectionTime
}
