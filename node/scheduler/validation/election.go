package validation

import (
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
)

var (
	firstElectionInterval = 10 * time.Minute   // Time of the first election
	electionCycle         = 1 * 24 * time.Hour // Election cycle
)

func getTimeAfter(t time.Duration) time.Time {
	return time.Now().Add(t)
}

// triggers the election process at a regular interval.
func (m *Manager) startElectionTicker() {
	// validators, err := m.nodeMgr.LoadValidators(m.nodeMgr.ServerID)
	// if err != nil {
	// 	log.Errorf("electionTicker LoadValidators err: %v", err)
	// 	return
	// }

	expiration := m.electionCycle

	// expiration := m.getElectionCycle()
	// if len(validators) <= 0 {
	// 	expiration = firstElectionInterval
	// }

	m.nextElectionTime = getTimeAfter(firstElectionInterval)

	ticker := time.NewTicker(firstElectionInterval)
	defer ticker.Stop()

	doElect := func() {
		m.nextElectionTime = getTimeAfter(expiration)

		ticker.Reset(expiration)
		err := m.elect()
		if err != nil {
			log.Errorf("elect err:%s", err.Error())
		}
	}

	for {
		select {
		case <-ticker.C:
			doElect()
		case <-m.updateCh:
			doElect()
		}
	}
}

// elect triggers an election and updates the list of validators.
func (m *Manager) elect() error {
	log.Debugln("start elect ")

	m.electValidatorsFromEdge()
	// validators, validatables := m.electValidators()

	// m.ResetValidatorGroup(validators, validatables)

	// return m.nodeMgr.UpdateValidators(validators, m.nodeMgr.ServerID)

	return nil
}

func (m *Manager) CompulsoryElection(validators []string) error {
	for _, nid2 := range validators {
		node := m.nodeMgr.GetNode(nid2)
		if node != nil {
			node.Type = types.NodeValidator
		}
	}

	return m.nodeMgr.UpdateValidators(validators, m.nodeMgr.ServerID)
}

// StartElection triggers an election manually.
func (m *Manager) StartElection() {
	// TODO need to add restrictions to disallow frequent calls?
	m.updateCh <- struct{}{}
}

// // performs the election process and returns the list of elected validators.
// func (m *Manager) electValidators() ([]string, []string) {
// 	ratio := m.getValidatorRatio()

// 	list, _ := m.nodeMgr.GetAllCandidateNodes()

// 	needValidatorCount := int(math.Ceil(float64(len(list)) * ratio))
// 	if needValidatorCount <= 0 {
// 		return nil, list
// 	}

// 	rand.Shuffle(len(list), func(i, j int) {
// 		list[i], list[j] = list[j], list[i]
// 	})

// 	if needValidatorCount > len(list) {
// 		needValidatorCount = len(list)
// 	}

// 	validators := list[:needValidatorCount]
// 	validatables := list[needValidatorCount:]

// 	return validators, validatables
// }

func (m *Manager) electValidatorsFromEdge() {
	if m.l2ValidatorCount <= 0 {
		return
	}

	list := m.nodeMgr.GetAllEdgeNode()

	curCount := 0

	for _, node := range list {
		switch node.NATType {
		case types.NatTypeFullCone, types.NatTypeNo:
			if node.IsNewVersion && curCount < m.l2ValidatorCount {
				node.Type = types.NodeValidator
				curCount++
			} else {
				node.Type = types.NodeEdge
			}
		default:
			node.Type = types.NodeEdge
		}
	}
}
