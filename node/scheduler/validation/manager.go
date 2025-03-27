package validation

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/lotuscli"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/scheduler/leadership"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/filecoin-project/pubsub"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("validation")

const (
	filecoinEpochDuration  = 30
	gameChainEpochLookback = 10

	validationWorkers = 50
	oneDay            = 24 * time.Hour
)

// Manager validation manager
type Manager struct {
	nodeMgr *node.Manager
	notify  *pubsub.PubSub

	seed       int64
	curRoundID string
	close      chan struct{}
	config     dtypes.GetSchedulerConfigFunc

	updateCh chan struct{}

	// validation result worker
	resultQueue chan *api.ValidationResult

	leadershipMgr *leadership.Manager

	lck             sync.Mutex
	isCacheValid    bool // use cache to reduce 'ChainHead' calls
	cachedEpoch     uint64
	cachedTimestamp time.Time

	lotusRPCAddress string

	enableValidation bool

	validators []string
	vLock      sync.Mutex
}

func (m *Manager) addValidator(nodeID string) {
	m.lck.Lock()
	defer m.lck.Unlock()

	m.validators = append(m.validators, nodeID)
}

// IsValidator checks if the given nodeID is a validator.
func (m *Manager) IsValidator(nodeID string) bool {
	m.lck.Lock()
	defer m.lck.Unlock()

	if len(m.validators) == 0 {
		return false
	}

	for _, nID := range m.validators {
		if nID == nodeID {
			return true
		}
	}

	return false
}

// GetValidators returns a list of validators.
func (m *Manager) GetValidators() []string {
	return m.validators
}

func (m *Manager) cleanValidator() {
	m.lck.Lock()
	defer m.lck.Unlock()

	m.validators = []string{}
}

// NewManager return new node manager instance
func NewManager(nodeMgr *node.Manager, configFunc dtypes.GetSchedulerConfigFunc, p *pubsub.PubSub, lmgr *leadership.Manager) *Manager {
	manager := &Manager{
		nodeMgr:       nodeMgr,
		config:        configFunc,
		close:         make(chan struct{}),
		updateCh:      make(chan struct{}, 1),
		notify:        p,
		resultQueue:   make(chan *api.ValidationResult),
		leadershipMgr: lmgr,
	}

	manager.initCfg()

	return manager
}

func (m *Manager) initCfg() {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("get schedulerConfig err:%s", err.Error())
		return
	}

	m.lotusRPCAddress = cfg.LotusRPCAddress
	m.enableValidation = cfg.EnableValidation
}

// Start start validate and elect task
func (m *Manager) Start(ctx context.Context) {
	go m.startValidationTicker()

	m.handleResults()
}

// Stop stop
func (m *Manager) Stop(ctx context.Context) error {
	return m.stopValidation()
}

func (m *Manager) getGameEpoch() (uint64, error) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if !m.isCacheValid {
		m.cachedTimestamp = time.Now()
		h, err := lotuscli.ChainHead(m.lotusRPCAddress)
		if err != nil {
			return 0, err
		}

		m.cachedEpoch = h
		m.isCacheValid = true
	}

	duration := time.Since(m.cachedTimestamp)
	if duration < 0 {
		return 0, xerrors.Errorf("current time is not correct with negative duration: %s", duration)
	}

	elapseEpoch := int64(duration.Seconds()) / filecoinEpochDuration

	return m.cachedEpoch + uint64(elapseEpoch), nil
}

func (m *Manager) getSeedFromFilecoin() (int64, error) {
	seed := time.Now().UnixNano()

	height, err := m.getGameEpoch()
	if err != nil {
		return seed, xerrors.Errorf("getGameEpoch failed: %w", err)
	}

	if height <= gameChainEpochLookback {
		return seed, xerrors.Errorf("getGameEpoch return invalid height: %d", height)
	}

	lookback := height - gameChainEpochLookback
	tps, err := m.getTipsetByHeight(lookback)
	if err != nil {
		return seed, xerrors.Errorf("getTipsetByHeight failed: %w", err)
	}

	rs := tps.MinTicket().VRFProof
	if len(rs) >= 3 {
		s := binary.BigEndian.Uint32(rs)
		log.Debugf("lotus Randomness:%d \n", s)
		return int64(s), nil
	}

	return seed, xerrors.Errorf("VRFProof size %d < 3", len(rs))
}

func (m *Manager) getTipsetByHeight(height uint64) (*lotuscli.TipSet, error) {
	iheight := int64(height)
	for i := 0; i < gameChainEpochLookback && iheight > 0; i++ {
		tps, err := lotuscli.ChainGetTipSetByHeight(m.lotusRPCAddress, iheight)
		if err != nil {
			return nil, err
		}

		if len(tps.Blocks()) > 0 {
			return tps, nil
		}

		iheight--
	}

	return nil, xerrors.Errorf("getTipsetByHeight can't found a non-empty tipset from height: %d", height)
}
