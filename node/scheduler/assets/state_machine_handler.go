package assets

import (
	"math"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/alecthomas/units"
	"github.com/filecoin-project/go-statemachine"
	"golang.org/x/xerrors"
)

var (
	// MinRetryTime defines the minimum time duration between retries
	MinRetryTime = 1 * time.Minute

	// MaxRetryCount defines the maximum number of retries allowed
	MaxRetryCount = 1
)

// failedCoolDown is called when a retry needs to be attempted and waits for the specified time duration
func failedCoolDown(ctx statemachine.Context, info AssetPullingInfo) error {
	retryStart := time.Now().Add(MinRetryTime)
	if time.Now().Before(retryStart) {
		log.Debugf("%s(%s), waiting %s before retrying", info.State, info.Hash, time.Until(retryStart))
		select {
		case <-time.After(time.Until(retryStart)):
		case <-ctx.Context().Done():
			return ctx.Context().Err()
		}
	}

	return nil
}

// handleSeedSelect handles the selection of seed nodes for asset pull
func (m *Manager) handleSeedSelect(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle select seed: %s", info.Hash)

	if len(info.CandidateReplicaSucceeds) >= seedReplicaCount {
		// The number of candidate node replicas has reached the requirement
		return ctx.Send(SkipStep{})
	}

	nodes := make(map[string]*node.Node)
	if info.SeedNodeID != "" {
		cNode := m.nodeMgr.GetCandidateNode(info.SeedNodeID)
		if cNode == nil {
			return ctx.Send(SelectFailed{error: xerrors.Errorf("node not found; %s", info.SeedNodeID)})
		}
		nodes[cNode.NodeID] = cNode
	} else {
		nodeInfo := m.getNodesFromFillAsset(info.CID)
		if nodeInfo != nil && nodeInfo.candidateList != nil && len(nodeInfo.candidateList) > 0 {
			seed := nodeInfo.candidateList[0]
			nodes[seed.NodeID] = seed
		} else {
			// find nodes
			str := ""
			nodes, str = m.chooseCandidateNodes(seedReplicaCount, info.CandidateReplicaSucceeds)
			if len(nodes) < 1 {
				return ctx.Send(SelectFailed{error: xerrors.Errorf("node not found; %s", str)})
			}
		}
	}

	// save to db
	err := m.saveReplicaInformation(nodes, info.Hash.String(), true)
	if err != nil {
		return ctx.Send(SelectFailed{error: err})
	}

	m.startAssetTimeoutCounting(info.Hash.String(), 0, info.Size)

	// send a cache request to the node
	go func() {
		for _, node := range nodes {
			err := node.PullAsset(ctx.Context(), info.CID, nil)
			if err != nil {
				log.Errorf("%s pull asset err:%s", node.NodeID, err.Error())
				continue
			}
		}
	}()

	return ctx.Send(PullRequestSent{})
}

// handleSeedPulling handles the asset pulling process of seed nodes
func (m *Manager) handleSeedPulling(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle seed pulling, %s", info.Hash)

	if len(info.CandidateReplicaSucceeds) >= seedReplicaCount {
		return ctx.Send(PullSucceed{})
	}

	if info.CandidateWaitings == 0 {
		return ctx.Send(PullFailed{error: xerrors.New("node pull failed")})
	}

	return nil
}

// handleUploadInit handles the upload init
func (m *Manager) handleUploadInit(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle upload init: %s, nodeID: %s", info.Hash, info.SeedNodeID)

	if len(info.CandidateReplicaSucceeds) >= seedReplicaCount {
		// The number of candidate node replicas has reached the requirement
		return ctx.Send(SkipStep{})
	}

	cNode := m.nodeMgr.GetCandidateNode(info.SeedNodeID)
	if cNode == nil {
		return ctx.Send(SelectFailed{})
	}

	nodes := map[string]*node.Node{
		cNode.NodeID: cNode,
	}

	// save to db
	err := m.saveReplicaInformation(nodes, info.Hash.String(), true)
	if err != nil {
		return ctx.Send(SelectFailed{error: err})
	}

	m.startAssetTimeoutCounting(info.Hash.String(), 0, info.Size)

	return ctx.Send(PullRequestSent{})
}

// handleSeedUploading handles the asset upload process of seed nodes
func (m *Manager) handleSeedUploading(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle seed upload, %s", info.Hash)

	if len(info.CandidateReplicaSucceeds) >= seedReplicaCount {
		return ctx.Send(PullSucceed{})
	}

	if info.CandidateWaitings == 0 {
		return ctx.Send(PullFailed{error: xerrors.New("user upload failed")})
	}

	return nil
}

// handleCandidatesSelect handles the selection of candidate nodes for asset pull
func (m *Manager) handleCandidatesSelect(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle candidates select, %s", info.Hash)

	sources := m.getDownloadSources(info.Hash.String(), info.Note)
	if len(sources) < 1 && info.Note == "" {
		return ctx.Send(SelectFailed{error: xerrors.New("source node not found")})
	}

	needCount := info.CandidateReplicas - int64(len(info.CandidateReplicaSucceeds))
	if needCount < 1 {
		err := m.DeleteReplenishBackup(info.Hash.String())
		if err != nil {
			log.Errorf("%s handle candidates DeleteReplenishBackup err, %s", info.Hash.String(), err.Error())
		}

		// The number of candidate node replicas has reached the requirement
		return ctx.Send(SkipStep{})
	}

	nodes := make(map[string]*node.Node)

	nodeInfo := m.getNodesFromFillAsset(info.CID)
	if nodeInfo != nil && nodeInfo.candidateList != nil && len(nodeInfo.candidateList) > 1 {
		ns := nodeInfo.candidateList[1:]
		for _, n := range ns {
			nodes[n.NodeID] = n
		}

		m.removeNodesFromFillAsset(info.CID, true)
	} else {
		// find nodes
		str := ""
		nodes, str = m.chooseCandidateNodes(int(needCount), info.CandidateReplicaSucceeds)
		if len(nodes) < 1 {
			return ctx.Send(SelectFailed{error: xerrors.Errorf("node not found; %s", str)})
		}
	}

	downloadSources, payloads, err := m.GenerateToken(info.CID, sources, nodes)
	if err != nil {
		return ctx.Send(SelectFailed{error: err})
	}

	// err = m.SaveTokenPayload(payloads)
	// if err != nil {
	// 	return ctx.Send(SelectFailed{error: err})
	// }

	// save to db
	err = m.saveReplicaInformation(nodes, info.Hash.String(), true)
	if err != nil {
		return ctx.Send(SelectFailed{error: err})
	}

	m.startAssetTimeoutCounting(info.Hash.String(), 0, info.Size)

	// send a pull request to the node
	go func() {
		err = m.SaveTokenPayload(payloads)
		if err != nil {
			log.Errorf("%s len:%d SaveTokenPayload err:%s", info.Hash, len(payloads), err.Error())
		}

		for _, node := range nodes {
			err := node.PullAsset(ctx.Context(), info.CID, downloadSources[node.NodeID])
			if err != nil {
				log.Errorf("%s pull asset err:%s", node.NodeID, err.Error())
				continue
			}
		}
	}()

	return ctx.Send(PullRequestSent{})
}

// handleCandidatesPulling handles the asset pulling process of candidate nodes
func (m *Manager) handleCandidatesPulling(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle candidates pulling,cid: %s, hash:%s , waiting:%d", info.CID, info.Hash.String(), info.CandidateWaitings)

	if int64(len(info.CandidateReplicaSucceeds)) >= info.CandidateReplicas {
		err := m.DeleteReplenishBackup(info.Hash.String())
		if err != nil {
			log.Errorf("%s handle candidates DeleteReplenishBackup err, %s", info.Hash.String(), err.Error())
		}

		return ctx.Send(PullSucceed{})
	}

	if info.CandidateWaitings == 0 {
		return ctx.Send(PullFailed{error: xerrors.New("node pull failed")})
	}

	return nil
}

func (m *Manager) getCurBandwidthUp(nodes []string) int64 {
	bandwidthUp := int64(0)
	for _, nodeID := range nodes {
		node := m.nodeMgr.GetEdgeNode(nodeID)
		if node != nil {
			bandwidthUp += node.BandwidthUp
		}
	}

	// B to MiB
	v := int64(math.Ceil(float64(bandwidthUp) / float64(units.MiB)))
	return v
}

// handleEdgesSelect handles the selection of edge nodes for asset pull
func (m *Manager) handleEdgesSelect(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Debugf("handle edges select , %s", info.Hash)

	needCount := info.EdgeReplicas - int64(len(info.EdgeReplicaSucceeds))

	if info.ReplenishReplicas > 0 {
		// Check to node offline while replenishing the temporary replica
		needCount = info.ReplenishReplicas
	}

	needBandwidth := info.Bandwidth - m.getCurBandwidthUp(info.EdgeReplicaSucceeds)
	if needCount < 1 && needBandwidth <= 0 {
		// The number of edge node replications and the total downlink bandwidth are met
		return ctx.Send(SkipStep{})
	}

	sources := m.getDownloadSources(info.Hash.String(), info.Note)
	if len(sources) < 1 && info.Note == "" {
		return ctx.Send(SelectFailed{error: xerrors.New("source node not found")})
	}

	nodes := make(map[string]*node.Node)

	nodeInfo := m.getNodesFromFillAsset(info.CID)
	if nodeInfo != nil && nodeInfo.edgeList != nil && len(nodeInfo.edgeList) > 0 {
		for _, n := range nodeInfo.edgeList {
			nodes[n.NodeID] = n
		}

		m.removeNodesFromFillAsset(info.CID, false)
	} else {
		// if info.Note == "" {
		// 	sLen := int64(len(sources))
		// 	if needCount > sLen {
		// 		needCount = sLen
		// 	}
		// }
		// find nodes
		str := ""
		nodes, str = m.chooseEdgeNodes(int(needCount), needBandwidth, info.EdgeReplicaSucceeds, float64(info.Size))
		if len(nodes) < 1 {
			return ctx.Send(SelectFailed{error: xerrors.Errorf("node not found; %s", str)})
		}
	}

	downloadSources, payloads, err := m.GenerateToken(info.CID, sources, nodes)
	if err != nil {
		return ctx.Send(SelectFailed{error: xerrors.Errorf("GenerateToken; %s", err.Error())})
	}

	// save to db
	err = m.saveReplicaInformation(nodes, info.Hash.String(), false)
	if err != nil {
		return ctx.Send(SelectFailed{error: xerrors.Errorf("saveReplicaInformation; %s", err.Error())})
	}

	m.startAssetTimeoutCounting(info.Hash.String(), 0, info.Size)

	// send a pull request to the node
	go func() {
		err = m.SaveTokenPayload(payloads)
		if err != nil {
			log.Errorf("%s len:%d SaveTokenPayload err:%s", info.Hash, len(payloads), err.Error())
		}

		for _, node := range nodes {
			err := node.PullAsset(ctx.Context(), info.CID, downloadSources[node.NodeID])
			if err != nil {
				log.Errorf("%s pull asset err:%s", node.NodeID, err.Error())
				continue
			}
		}
	}()

	return ctx.Send(PullRequestSent{})
}

// handleEdgesPulling handles the asset pulling process of edge nodes
func (m *Manager) handleEdgesPulling(ctx statemachine.Context, info AssetPullingInfo) error {
	needBandwidth := info.Bandwidth - m.getCurBandwidthUp(info.EdgeReplicaSucceeds)
	log.Debugf("handle edges pulling, %s ; %d>=%d , %d", info.Hash, int64(len(info.EdgeReplicaSucceeds)), info.EdgeReplicas, needBandwidth)

	if int64(len(info.EdgeReplicaSucceeds)) >= info.EdgeReplicas && needBandwidth <= 0 {
		return ctx.Send(PullSucceed{})
	}

	if info.EdgeWaitings == 0 {
		return ctx.Send(PullFailed{error: xerrors.New("node pull failed")})
	}

	return nil
}

// handleServicing asset pull completed and in service status
func (m *Manager) handleServicing(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Infof("handle servicing: %s", info.Hash)
	m.stopAssetTimeoutCounting(info.Hash.String())

	// remove fail replicas
	// return m.DeleteUnfinishedReplicas(info.Hash.String())
	return nil
}

// handlePullsFailed handles the failed state of asset pulling and retries if necessary
func (m *Manager) handlePullsFailed(ctx statemachine.Context, info AssetPullingInfo) error {
	m.stopAssetTimeoutCounting(info.Hash.String())

	if info.RetryCount >= int64(MaxRetryCount) {
		log.Infof("handle pulls failed: %s, retry count: %d", info.Hash.String(), info.RetryCount)

		err := m.DeleteReplenishBackup(info.Hash.String())
		if err != nil {
			log.Errorf("%s handle candidates DeleteReplenishBackup err, %s", info.Hash.String(), err.Error())
		}

		return nil
	}

	log.Debugf(": %s, retries: %d", info.Hash.String(), info.RetryCount)

	if err := failedCoolDown(ctx, info); err != nil {
		return err
	}

	return ctx.Send(AssetRePull{})
}

func (m *Manager) handleUploadFailed(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Infof("handle upload fail: %s", info.Hash)
	m.stopAssetTimeoutCounting(info.Hash.String())

	return nil
}

func (m *Manager) handleRemove(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Infof("handle remove: %s", info.Hash)
	m.stopAssetTimeoutCounting(info.Hash.String())
	defer m.AssetRemoveDone(info.Hash.String())

	hash := info.Hash.String()
	cid := info.CID

	cInfos, err := m.LoadReplicasByStatus(hash, types.ReplicaStatusAll)
	if err != nil {
		return xerrors.Errorf("RemoveAsset %s LoadAssetReplicas err:%s", info.Hash, err.Error())
	}

	// node info
	for _, cInfo := range cInfos {
		err = m.RemoveReplica(cid, hash, cInfo.NodeID)
		if err != nil {
			return xerrors.Errorf("RemoveAsset %s RemoveReplica err: %s", hash, err.Error())
		}
	}

	// remove user asset
	users, err := m.ListUsersForAsset(hash)
	for _, user := range users {
		err = m.DeleteAssetUser(hash, user)
		if err != nil {
			log.Errorf("DeleteAssetUser: %s", info.Hash)
		}
	}

	return nil
}

func (m *Manager) handleStop(ctx statemachine.Context, info AssetPullingInfo) error {
	log.Infof("handle stop: %s", info.Hash)
	m.stopAssetTimeoutCounting(info.Hash.String())

	// m.DeleteUnfinishedReplicas(info.Hash.String())

	return nil
}
