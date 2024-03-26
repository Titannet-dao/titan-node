package assets

import (
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
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
	Bandwidth         int64

	EdgeReplicaSucceeds      []string
	CandidateReplicaSucceeds []string
	EdgeWaitings             int64
	CandidateWaitings        int64

	RetryCount        int64
	ReplenishReplicas int64

	Details    string
	SeedNodeID string

	Note string
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
		Note:                  state.Note,
	}
}

// assetPullingInfoFrom converts types.AssetRecord to AssetPullingInfo
func assetPullingInfoFrom(info *types.AssetRecord, assetDB *db.SQLDB) *AssetPullingInfo {
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
		Bandwidth:         info.NeedBandwidth,
		Note:              info.Note,
	}

	for _, r := range info.ReplicaInfos {
		switch r.Status {
		case types.ReplicaStatusSucceeded:
			if r.IsCandidate {
				deactivateTime, err := assetDB.LoadDeactivateNodeTime(r.NodeID)
				if err == nil && deactivateTime == 0 {
					cInfo.CandidateReplicaSucceeds = append(cInfo.CandidateReplicaSucceeds, r.NodeID)
				}
			} else {
				cInfo.EdgeReplicaSucceeds = append(cInfo.EdgeReplicaSucceeds, r.NodeID)
			}
		case types.ReplicaStatusPulling, types.ReplicaStatusWaiting:
			if r.IsCandidate {
				cInfo.CandidateWaitings++
			} else {
				cInfo.EdgeWaitings++
			}
		}
	}

	return cInfo
}
