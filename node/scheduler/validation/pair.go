package validation

import (
	"math"
	"math/rand"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/docker/go-units"
)

const (
	bandwidthRatio = 1                    // The ratio of the total upstream bandwidth on edge nodes to the downstream bandwidth on validation nodes.
	toleranceBwUp  = int64(5 * units.MiB) // The tolerance for uplink bandwidth deviation per group, set to 5M.
)

// reduces the ValidatableGroup's bandwidth to an value between the min and max averages
func (b *ValidatableGroup) divideNodesToAverage(maxAverage, minAverage int64) (out map[string]int64) {
	out = make(map[string]int64)

	b.lock.Lock()
	defer func() {
		for nodeID, bUp := range out {
			delete(b.nodes, nodeID)
			b.sumBwUp -= bUp
		}

		b.lock.Unlock()
	}()

	if b.sumBwUp < maxAverage || len(b.nodes) <= 1 {
		return
	}

	maxReduce := b.sumBwUp - minAverage
	minReduce := b.sumBwUp - maxAverage

	tempBwUp := int64(0)
	for nodeID, bwUp := range b.nodes {
		if bwUp > maxReduce {
			continue
		}

		if bwUp >= minReduce {
			out = make(map[string]int64)
			out[nodeID] = bwUp
			return
		}

		out[nodeID] = bwUp
		tempBwUp += bwUp
		if tempBwUp >= minReduce && tempBwUp <= maxReduce {
			return
		}

		if tempBwUp > maxReduce {
			return
		}
	}

	out = make(map[string]int64)
	return
}

// adds a validatable node to the validatableGroup
func (b *ValidatableGroup) addNode(nodeID string, bwUp int64) {
	b.lock.Lock()
	defer b.lock.Unlock()

	_, exist := b.nodes[nodeID]
	if exist {
		return
	}

	b.nodes[nodeID] = bwUp
	b.sumBwUp += bwUp
}

// adds multiple validatable nodes to the validatableGroup
func (b *ValidatableGroup) addNodes(groups map[string]int64) {
	b.lock.Lock()
	defer b.lock.Unlock()

	for nodeID, bwUp := range groups {
		_, exist := b.nodes[nodeID]
		if exist {
			continue
		}

		b.nodes[nodeID] = bwUp
		b.sumBwUp += bwUp
	}
}

// removes a validatable node from the validatableGroup
func (b *ValidatableGroup) removeNode(nodeID string) {
	b.lock.Lock()
	defer b.lock.Unlock()

	bwUp, exist := b.nodes[nodeID]
	if !exist {
		return
	}

	delete(b.nodes, nodeID)
	b.sumBwUp -= bwUp
}

func (m *Manager) resetGroup() {
	m.validationPairLock.Lock()
	defer m.validationPairLock.Unlock()

	m.unpairedGroup = newValidatableGroup()
	edges := m.nodeMgr.GetAllEdgeNode()
	for _, node := range edges {
		m.unpairedGroup.addNode(node.NodeID, node.BandwidthUp)
	}

	// init
	m.validatableGroups = make([]*ValidatableGroup, 0)
	m.vWindows = make([]*VWindow, 0)

	_, candidates := m.nodeMgr.GetAllValidCandidateNodes()
	for _, node := range candidates {
		if node.Type != types.NodeValidator {
			continue
		}

		bwDn := float64(node.BandwidthDown)
		count := int(math.Floor((bwDn * bandwidthRatio) / m.getValidatorBaseBwDn()))
		if count < 1 {
			count = 1
		}

		for i := 0; i < count; i++ {
			vr := newVWindow(node.NodeID)
			m.vWindows = append(m.vWindows, vr)

			bg := newValidatableGroup()
			m.validatableGroups = append(m.validatableGroups, bg)
		}
	}
}

// ResetValidatorGroup clears and initializes the validator and validatable groups
func (m *Manager) ResetValidatorGroup(validators, validatables []string) {
	m.validationPairLock.Lock()
	defer m.validationPairLock.Unlock()

	// remove old
	for _, group := range m.validatableGroups {
		m.unpairedGroup.addNodes(group.nodes)
	}

	for _, nodeID := range validatables {
		node := m.nodeMgr.GetCandidateNode(nodeID)
		if node == nil {
			continue
		}
		m.unpairedGroup.addNode(nodeID, node.BandwidthUp)
	}

	// init
	m.validatableGroups = make([]*ValidatableGroup, 0)
	m.vWindows = make([]*VWindow, 0)

	for _, nodeID := range validators {
		// if validator exist
		m.unpairedGroup.removeNode(nodeID)

		node := m.nodeMgr.GetCandidateNode(nodeID)
		if node == nil {
			continue
		}

		bwDn := float64(node.BandwidthDown)

		count := int(math.Floor((bwDn * bandwidthRatio) / m.getValidatorBaseBwDn()))
		if count < 1 {
			count = 1
		}

		for i := 0; i < count; i++ {
			vr := newVWindow(nodeID)
			m.vWindows = append(m.vWindows, vr)

			bg := newValidatableGroup()
			m.validatableGroups = append(m.validatableGroups, bg)
		}
	}
}

// // adds a validator window to the manager with the specified node ID and bandwidth down
// func (m *Manager) addValidator(nodeID string, bwDn int64) {
// 	m.validationPairLock.Lock()
// 	defer m.validationPairLock.Unlock()

// 	count := int(math.Floor((float64(bwDn) * bandwidthRatio) / m.getValidatorBaseBwDn()))
// 	if count < 1 {
// 		count = 1
// 	}

// 	// Do not process if node present
// 	for _, v := range m.vWindows {
// 		if v.NodeID == nodeID {
// 			return
// 		}
// 	}

// 	for i := 0; i < count; i++ {
// 		vr := newVWindow(nodeID)
// 		m.vWindows = append(m.vWindows, vr)

// 		bg := newValidatableGroup()
// 		m.validatableGroups = append(m.validatableGroups, bg)
// 	}
// }

// // removes the validator window with the specified node ID from the manager
// func (m *Manager) removeValidator(nodeID string) {
// 	m.validationPairLock.Lock()
// 	defer m.validationPairLock.Unlock()

// 	var indexes []int
// 	for i, v := range m.vWindows {
// 		if v.NodeID == nodeID {
// 			indexes = append(indexes, i)
// 		}
// 	}

// 	if len(indexes) == 0 {
// 		return
// 	}

// 	// update validator windows
// 	start := indexes[0]
// 	end := indexes[len(indexes)-1] + 1 // does not contain end , need to ++

// 	s1 := m.vWindows[:start]
// 	s2 := m.vWindows[end:]

// 	m.vWindows = append(s1, s2...)

// 	// update validatableGroups
// 	rIndex := len(m.validatableGroups) - len(indexes)
// 	removeGroups := m.validatableGroups[rIndex:]

// 	m.validatableGroups = m.validatableGroups[:rIndex]

// 	// add validatable nodes to unpairedGroup
// 	for _, group := range removeGroups {
// 		m.unpairedGroup.addNodes(group.nodes)
// 	}
// }

// // adds a validatable node to unpairedGroup with the specified node ID and bandwidth up
// func (m *Manager) addValidatableNode(nodeID string, bandwidthUp int64) {
// 	m.unpairedGroup.addNode(nodeID, bandwidthUp)
// }

// // removes the validatable node with the specified node ID from the manager
// func (m *Manager) removeValidatableNode(nodeID string) {
// 	m.validationPairLock.Lock()
// 	defer m.validationPairLock.Unlock()

// 	if _, exist := m.unpairedGroup.nodes[nodeID]; exist {
// 		m.unpairedGroup.removeNode(nodeID)
// 	}

// 	for _, bg := range m.validatableGroups {
// 		bwUp, exist := bg.nodes[nodeID]
// 		if exist {
// 			bg.sumBwUp -= bwUp
// 			delete(bg.nodes, nodeID)
// 			return
// 		}
// 	}
// }

// divides the validatable nodes in the manager into groups with similar average bandwidth
func (m *Manager) divideIntoGroups() {
	m.validationPairLock.Lock()
	defer m.validationPairLock.Unlock()

	sumBwUp := m.unpairedGroup.sumBwUp
	groupCount := len(m.validatableGroups)
	for _, group := range m.validatableGroups {
		sumBwUp += group.sumBwUp
	}

	averageUp := int64(math.Ceil(float64(sumBwUp) / float64(groupCount)))
	maxAverage := averageUp + toleranceBwUp
	minAverage := averageUp - toleranceBwUp

	for _, group := range m.validatableGroups {
		rm := group.divideNodesToAverage(maxAverage, minAverage)
		if len(rm) > 0 {
			m.unpairedGroup.addNodes(rm)
		}
	}

	// O n+m (n is the group count, m is the validatable node count)
	for _, groups := range m.validatableGroups {
		if groups.sumBwUp >= maxAverage {
			continue
		}

		for nodeID, bwUp := range m.unpairedGroup.nodes {
			if bwUp > maxAverage || groups.sumBwUp+bwUp <= maxAverage {
				groups.addNode(nodeID, bwUp)
				m.unpairedGroup.removeNode(nodeID)
			}

			if groups.sumBwUp >= maxAverage {
				break
			}
		}

		if len(m.unpairedGroup.nodes) == 0 {
			break
		}

	}
}

// PairValidatorsAndValidatableNodes randomly pair validators and validatable nodes based on their bandwidth capabilities.
func (m *Manager) PairValidatorsAndValidatableNodes() []*VWindow {
	m.divideIntoGroups()

	vs := len(m.vWindows)
	bs := len(m.validatableGroups)
	if vs != bs {
		log.Errorf("group len are not the same vs:%d,bs:%d", vs, bs)
		return nil
	}

	rand.Shuffle(len(m.validatableGroups), func(i, j int) {
		m.validatableGroups[i], m.validatableGroups[j] = m.validatableGroups[j], m.validatableGroups[i]
	})

	for i, v := range m.vWindows {
		groups := m.validatableGroups[i]

		v.ValidatableNodes = groups.nodes
	}

	return m.vWindows
}

func (m *Manager) getValidatorBaseBwDn() float64 {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("get schedulerConfig err:%s", err.Error())
		return 0
	}

	return float64(cfg.ValidatorBaseBwDn * units.MiB)
}

func (m *Manager) getLotusURL() string {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("get schedulerConfig err:%s", err.Error())
		return ""
	}

	return cfg.LotusRPCAddress
}
