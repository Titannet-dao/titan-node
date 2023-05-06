package assets

import (
	"github.com/Filecoin-Titan/titan/api/types"
)

// AssetHash is an identifier for a asset.
type AssetHash string

func (c AssetHash) String() string {
	return string(c)
}

// NodePulledResult represents a result of a node pulling assets
type NodePulledResult struct {
	Status      int64
	BlocksCount int64
	Size        int64
	NodeID      string
	IsCandidate bool
}

// AssetPullingInfo represents asset pull information
type AssetPullingInfo struct {
	State             AssetState
	Hash              AssetHash
	CID               string
	Size              int64
	Blocks            int64
	EdgeReplicas      int64
	CandidateReplicas int64

	EdgeReplicaSucceeds      []string
	EdgeReplicaFailures      []string
	CandidateReplicaSucceeds []string
	CandidateReplicaFailures []string

	RetryCount        int64
	ReplenishReplicas int64
}

// ToAssetRecord converts AssetPullingInfo to types.AssetRecord
func (state *AssetPullingInfo) ToAssetRecord() *types.AssetRecord {
	return &types.AssetRecord{
		CID:                   state.CID,
		Hash:                  state.Hash.String(),
		NeedEdgeReplica:       state.EdgeReplicas,
		TotalSize:             state.Size,
		TotalBlocks:           state.Blocks,
		State:                 state.State.String(),
		NeedCandidateReplicas: state.CandidateReplicas,
		RetryCount:            state.RetryCount,
		ReplenishReplicas:     state.ReplenishReplicas,
	}
}

// assetPullingInfoFrom converts types.AssetRecord to AssetPullingInfo
func assetPullingInfoFrom(info *types.AssetRecord) *AssetPullingInfo {
	cInfo := &AssetPullingInfo{
		CID:               info.CID,
		State:             AssetState(info.State),
		Hash:              AssetHash(info.Hash),
		EdgeReplicas:      info.NeedEdgeReplica,
		Size:              info.TotalSize,
		Blocks:            info.TotalBlocks,
		CandidateReplicas: info.NeedCandidateReplicas,
		RetryCount:        info.RetryCount,
		ReplenishReplicas: info.ReplenishReplicas,
	}

	for _, r := range info.ReplicaInfos {
		if r.Status == types.ReplicaStatusSucceeded {
			if r.IsCandidate {
				cInfo.CandidateReplicaSucceeds = append(cInfo.CandidateReplicaSucceeds, r.NodeID)
			} else {
				cInfo.EdgeReplicaSucceeds = append(cInfo.EdgeReplicaSucceeds, r.NodeID)
			}
			continue
		}

		if r.Status == types.ReplicaStatusFailed {
			if r.IsCandidate {
				cInfo.CandidateReplicaFailures = append(cInfo.CandidateReplicaFailures, r.NodeID)
			} else {
				cInfo.EdgeReplicaFailures = append(cInfo.EdgeReplicaFailures, r.NodeID)
			}
		}
	}

	return cInfo
}
