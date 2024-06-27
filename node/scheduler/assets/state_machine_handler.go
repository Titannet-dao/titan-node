package assets

import (
	"bytes"
	"crypto"
	"encoding/gob"
	"math"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/alecthomas/units"
	"github.com/filecoin-project/go-statemachine"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

var (
	// MinRetryTime defines the minimum time duration between retries
	MinRetryTime = 1 * time.Minute

	WaitTime = 5 * time.Second

	// MaxRetryCount defines the maximum number of retries allowed
	MaxRetryCount = 1
)

// failedCoolDown is called when a retry needs to be attempted and waits for the specified time duration
func failedCoolDown(ctx statemachine.Context, info AssetPullingInfo, t time.Duration) error {
	retryStart := time.Now().Add(t)
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
		nodeInfo := m.getNodesFromAWSAsset(info.CID)
		if nodeInfo != nil && nodeInfo.candidateList != nil && len(nodeInfo.candidateList) > 0 {
			seed := nodeInfo.candidateList[0]
			nodes[seed.NodeID] = seed
		} else {
			// find nodes
			str := ""
			nodes, str = m.chooseCandidateNodes(seedReplicaCount, info.CandidateReplicaSucceeds, float64(info.Size))
			if len(nodes) < 1 {
				return ctx.Send(SelectFailed{error: xerrors.Errorf("node not found; %s", str)})
			}
		}
	}

	var awsInfo *types.AWSDownloadSources
	if info.Source == AssetSourceAWS && info.Note != "" {
		awsInfo = &types.AWSDownloadSources{Bucket: info.Note}
	}

	for _, node := range nodes {
		cNode := node

		workloadID := m.createSeedWorkload(info, cNode.NodeID)

		log.Infof("workload PullAssetV2 nodeID:[%s] , %s\n", cNode.NodeID, workloadID)
		err := cNode.PullAssetV2(ctx.Context(), &types.AssetPullRequest{AssetCID: info.CID, WorkloadID: workloadID, Dss: &types.DownloadSources{AWS: awsInfo}})
		if err != nil {
			log.Errorf("%s PullAsset err:%s", cNode.NodeID, err.Error())
			continue
		}

		err = m.SaveReplicaStatus(&types.ReplicaInfo{
			NodeID:      cNode.NodeID,
			Status:      types.ReplicaStatusWaiting,
			Hash:        info.Hash.String(),
			IsCandidate: true,
		})
		if err != nil {
			log.Errorf("%s SaveReplicaStatus err:%s", cNode.NodeID, err.Error())
		}
	}

	m.startAssetTimeoutCounting(info.Hash.String(), 0, info.Size)

	return ctx.Send(PullRequestSent{})
}

func (m *Manager) createSeedWorkload(info AssetPullingInfo, nodeID string) string {
	sID := types.DownloadSourceIPFS.String()
	if info.Source == AssetSourceAWS {
		sID = types.DownloadSourceAWS.String()
	}

	ws := []*types.Workload{{SourceID: sID}}
	buffer := &bytes.Buffer{}
	enc := gob.NewEncoder(buffer)
	err := enc.Encode(ws)
	if err != nil {
		log.Errorf("encode error:%s", err.Error())
	} else {
		record := &types.WorkloadRecord{
			WorkloadID: uuid.NewString(),
			AssetCID:   info.CID,
			ClientID:   nodeID,
			AssetSize:  info.Size,
			Workloads:  buffer.Bytes(),
			Event:      types.WorkloadEventPull,
			Status:     types.WorkloadStatusCreate,
		}

		workloads := []*types.WorkloadRecord{record}

		err = m.SaveTokenPayload(workloads)
		if err != nil {
			log.Errorf("%s len:%d SaveTokenPayload err:%s", info.Hash, len(workloads), err.Error())
		}

		return record.WorkloadID
	}

	return ""
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

	err := m.SaveReplicaStatus(&types.ReplicaInfo{
		NodeID:      cNode.NodeID,
		Status:      types.ReplicaStatusWaiting,
		Hash:        info.Hash.String(),
		IsCandidate: true,
	})
	if err != nil {
		log.Errorf("%s SaveReplicaStatus err:%s", cNode.NodeID, err.Error())
		return ctx.Send(SelectFailed{error: err})
	}

	m.startAssetTimeoutCounting(info.Hash.String(), 0, info.Size)

	m.createSeedWorkload(info, cNode.NodeID)

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

	needCount := info.CandidateReplicas - int64(len(info.CandidateReplicaSucceeds))
	if needCount < 1 {
		err := m.DeleteReplenishBackup(info.Hash.String())
		if err != nil {
			log.Errorf("%s handle candidates DeleteReplenishBackup err, %s", info.Hash.String(), err.Error())
		}

		// The number of candidate node replicas has reached the requirement
		return ctx.Send(SkipStep{})
	}

	var awsInfo *types.AWSDownloadSources
	if info.Source == AssetSourceAWS && info.Note != "" {
		awsInfo = &types.AWSDownloadSources{Bucket: info.Note}
	}

	sources := m.getDownloadSources(info.Hash.String())
	if len(sources) < 1 && awsInfo == nil {
		return ctx.Send(SelectFailed{error: xerrors.New("source node not found")})
	}

	nodes := make(map[string]*node.Node)

	nodeInfo := m.getNodesFromAWSAsset(info.CID)
	if nodeInfo != nil && nodeInfo.candidateList != nil && len(nodeInfo.candidateList) > 1 {
		ns := nodeInfo.candidateList[1:]
		for _, n := range ns {
			nodes[n.NodeID] = n
		}

		m.removeNodesFromFillAsset(info.CID, true)
	} else {
		// find nodes
		str := ""
		nodes, str = m.chooseCandidateNodes(int(needCount), info.CandidateReplicaSucceeds, float64(info.Size))
		if len(nodes) < 1 {
			return ctx.Send(SelectFailed{error: xerrors.Errorf("node not found; %s", str)})
		}
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())

	for _, node := range nodes {
		cNode := node

		downloadSource, workload, err := m.GenerateToken(info.CID, sources, cNode, info.Size, titanRsa)
		if err != nil {
			log.Errorf("%s GenerateToken err:%s", cNode.NodeID, err.Error())
			continue
		}

		m.SaveWorkloadRecord([]*types.WorkloadRecord{workload})

		go func() {
			// err = cNode.PullAsset(ctx.Context(), info.CID, downloadSource)
			log.Infof("workload PullAssetV2 nodeID:[%s] , %s\n", cNode.NodeID, workload.WorkloadID)
			err := cNode.PullAssetV2(ctx.Context(), &types.AssetPullRequest{AssetCID: info.CID, WorkloadID: workload.WorkloadID, Dss: &types.DownloadSources{AWS: awsInfo, Nodes: downloadSource}})
			if err != nil {
				log.Errorf("%s PullAsset err:%s", cNode.NodeID, err.Error())
				return
			}

			err = m.SaveReplicaStatus(&types.ReplicaInfo{
				NodeID:      cNode.NodeID,
				Status:      types.ReplicaStatusWaiting,
				Hash:        info.Hash.String(),
				IsCandidate: true,
			})
			if err != nil {
				log.Errorf("%s SaveReplicaStatus err:%s", cNode.NodeID, err.Error())
			}
		}()
	}

	// Wait send to node
	if err := failedCoolDown(ctx, info, WaitTime); err != nil {
		return err
	}

	m.startAssetTimeoutCounting(info.Hash.String(), 0, info.Size)

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

	// B to KiB
	v := int64(math.Ceil(float64(bandwidthUp) / float64(units.KiB)))
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

	var awsInfo *types.AWSDownloadSources
	if info.Source == AssetSourceAWS && info.Note != "" {
		awsInfo = &types.AWSDownloadSources{Bucket: info.Note}
	}

	sources := m.getDownloadSources(info.Hash.String())
	if len(sources) < 1 && awsInfo == nil {
		return ctx.Send(SelectFailed{error: xerrors.New("source node not found")})
	}

	nodes := make(map[string]*node.Node)

	nodeInfo := m.getNodesFromAWSAsset(info.CID)
	if nodeInfo != nil && nodeInfo.edgeList != nil && len(nodeInfo.edgeList) > 0 {
		for _, n := range nodeInfo.edgeList {
			nodes[n.NodeID] = n
		}

		m.removeNodesFromFillAsset(info.CID, false)
	} else {
		str := ""
		nodes, str = m.chooseEdgeNodes(int(needCount), needBandwidth, info.EdgeReplicaSucceeds, float64(info.Size))
		if len(nodes) < 1 {
			return ctx.Send(SelectFailed{error: xerrors.Errorf("node not found; %s", str)})
		}
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	for _, node := range nodes {
		cNode := node

		downloadSource, workload, err := m.GenerateToken(info.CID, sources, cNode, info.Size, titanRsa)
		if err != nil {
			log.Errorf("%s GenerateToken err:%s", cNode.NodeID, err.Error())
			continue
		}

		m.SaveWorkloadRecord([]*types.WorkloadRecord{workload})

		go func() {
			// err := cNode.PullAsset(ctx.Context(), info.CID, downloadSource)
			log.Infof("workload PullAssetV2 nodeID:[%s] , %s\n", cNode.NodeID, workload.WorkloadID)
			err := cNode.PullAssetV2(ctx.Context(), &types.AssetPullRequest{AssetCID: info.CID, WorkloadID: workload.WorkloadID, Dss: &types.DownloadSources{AWS: awsInfo, Nodes: downloadSource}})
			if err != nil {
				log.Errorf("%s PullAsset err:%s", cNode.NodeID, err.Error())
				return
			}
			err = m.SaveReplicaStatus(&types.ReplicaInfo{
				NodeID:      cNode.NodeID,
				Status:      types.ReplicaStatusWaiting,
				Hash:        info.Hash.String(),
				IsCandidate: false,
			})
			if err != nil {
				log.Errorf("%s SaveReplicaStatus err:%s", cNode.NodeID, err.Error())
			}
		}()
	}

	// Wait send to node
	if err := failedCoolDown(ctx, info, WaitTime); err != nil {
		return err
	}

	m.startAssetTimeoutCounting(info.Hash.String(), 0, info.Size)

	return ctx.Send(PullRequestSent{})
}

// handleEdgesPulling handles the asset pulling process of edge nodes
func (m *Manager) handleEdgesPulling(ctx statemachine.Context, info AssetPullingInfo) error {
	needBandwidth := info.Bandwidth - m.getCurBandwidthUp(info.EdgeReplicaSucceeds)
	log.Debugf("handle edges pulling, %s ; %d>=%d , %d", info.Hash, int64(len(info.EdgeReplicaSucceeds)), info.EdgeReplicas, needBandwidth)

	if info.EdgeWaitings > 0 {
		return nil
	}

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

	if err := failedCoolDown(ctx, info, MinRetryTime); err != nil {
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
