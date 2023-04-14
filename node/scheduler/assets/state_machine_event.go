package assets

import (
	"github.com/Filecoin-Titan/titan/api/types"
	"golang.org/x/xerrors"
)

type mutator interface {
	apply(state *AssetPullingInfo)
}

// globalMutator is an event which can apply in every state
type globalMutator interface {
	// applyGlobal applies the event to the state. If if returns true,
	//  event processing should be interrupted
	applyGlobal(state *AssetPullingInfo) bool
}

// Ignorable Ignorable
type Ignorable interface {
	Ignore()
}

// Global events

// PullAssetRestart restarts asset pulling
type PullAssetRestart struct{}

func (evt PullAssetRestart) applyGlobal(state *AssetPullingInfo) bool {
	state.RetryCount = 0
	return false
}

// PullAssetFatalError represents a fatal error in asset pulling
type PullAssetFatalError struct{ error }

// FormatError Format error
func (evt PullAssetFatalError) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt PullAssetFatalError) applyGlobal(state *AssetPullingInfo) bool {
	log.Errorf("Fatal error on asset %s: %+v", state.CID, evt.error)
	return true
}

// AssetForceState forces an asset state
type AssetForceState struct {
	State AssetState
}

func (evt AssetForceState) applyGlobal(state *AssetPullingInfo) bool {
	state.State = evt.State
	return true
}

// AssetRemove removes an asset record
type AssetRemove struct{}

func (evt AssetRemove) applyGlobal(state *AssetPullingInfo) bool {
	*state = AssetPullingInfo{State: Remove}
	return true
}

// InfoUpdate update asset info
type InfoUpdate struct {
	Size   int64
	Blocks int64
}

func (evt InfoUpdate) applyGlobal(state *AssetPullingInfo) bool {
	if state.State == SeedPulling {
		state.Size = evt.Size
		state.Blocks = evt.Blocks
	}

	return true
}

// PulledResult represents the result of node pulling
type PulledResult struct {
	ResultInfo *NodePulledResult
}

func (evt PulledResult) apply(state *AssetPullingInfo) {
	rInfo := evt.ResultInfo
	if rInfo == nil {
		return
	}

	if state.State == SeedPulling {
		state.Size = rInfo.Size
		state.Blocks = rInfo.BlocksCount
	}

	nodeID := rInfo.NodeID
	if rInfo.Status == int64(types.ReplicaStatusSucceeded) {
		if rInfo.IsCandidate {
			state.CandidateReplicaSucceeds = append(state.CandidateReplicaSucceeds, nodeID)
		} else {
			state.EdgeReplicaSucceeds = append(state.EdgeReplicaSucceeds, nodeID)
		}
	} else if rInfo.Status == int64(types.ReplicaStatusFailed) {
		if rInfo.IsCandidate {
			state.CandidateReplicaFailures = append(state.CandidateReplicaFailures, nodeID)
		} else {
			state.EdgeReplicaFailures = append(state.EdgeReplicaFailures, nodeID)
		}
	}
}

// PullRequestSent indicates that a pull request has been sent
type PullRequestSent struct{}

func (evt PullRequestSent) apply(state *AssetPullingInfo) {
	state.CandidateReplicaFailures = make([]string, 0)
	state.EdgeReplicaFailures = make([]string, 0)
}

// Normal path

// AssetStartPulls start asset pulls
type AssetStartPulls struct {
	ID                string
	Hash              AssetHash
	Replicas          int64
	ServerID          string
	CreatedAt         int64
	Expiration        int64
	CandidateReplicas int // Number of candidate node replicas
}

func (evt AssetStartPulls) apply(state *AssetPullingInfo) {
	state.CID = evt.ID
	state.Hash = evt.Hash
	state.EdgeReplicas = evt.Replicas
	state.ServerID = evt.ServerID
	state.CreatedAt = evt.CreatedAt
	state.Expiration = evt.Expiration
	state.CandidateReplicas = int64(seedReplicaCount + evt.CandidateReplicas)
}

// ReplenishReplicas replenish asset replicas
type ReplenishReplicas struct {
	ID                       string
	State                    AssetState
	Hash                     AssetHash
	Replicas                 int64
	ServerID                 string
	CreatedAt                int64
	Expiration               int64
	CandidateReplicas        int // Number of candidate node replicas
	Size                     int64
	Blocks                   int64
	EdgeReplicaSucceeds      []string
	CandidateReplicaSucceeds []string
}

func (evt ReplenishReplicas) applyGlobal(state *AssetPullingInfo) bool {
	state.State = evt.State
	state.CID = evt.ID
	state.Hash = evt.Hash
	state.EdgeReplicas = evt.Replicas
	state.ServerID = evt.ServerID
	state.CreatedAt = evt.CreatedAt
	state.Expiration = evt.Expiration
	state.CandidateReplicas = int64(seedReplicaCount + evt.CandidateReplicas)
	state.Size = evt.Size
	state.Blocks = evt.Blocks
	state.EdgeReplicaSucceeds = evt.EdgeReplicaSucceeds
	state.CandidateReplicaSucceeds = evt.CandidateReplicaSucceeds
	return true
}

// AssetRePull re-pull the asset
type AssetRePull struct{}

func (evt AssetRePull) apply(state *AssetPullingInfo) {}

// PullSucceed indicates that a node has successfully pulled an asset
type PullSucceed struct{}

func (evt PullSucceed) apply(state *AssetPullingInfo) {
	state.RetryCount = 0
}

// SkipStep skips the current step
type SkipStep struct{}

func (evt SkipStep) apply(state *AssetPullingInfo) {}

// PullFailed indicates that a node has failed to pull an asset
type PullFailed struct{ error }

// FormatError Format error
func (evt PullFailed) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt PullFailed) apply(state *AssetPullingInfo) {
	state.RetryCount++
}

// SelectFailed  indicates that node selection has failed
type SelectFailed struct{ error }

// FormatError Format error
func (evt SelectFailed) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt SelectFailed) apply(state *AssetPullingInfo) {
	state.RetryCount++
}
