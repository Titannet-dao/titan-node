package projects

import (
	"github.com/Filecoin-Titan/titan/api/types"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// ProjectID is an identifier for a project.
type ProjectID string

func (c ProjectID) String() string {
	return string(c)
}

type ProjectState string

const (
	NodeSelect ProjectState = "NodeSelect"
	Update     ProjectState = "Update"
	Deploying  ProjectState = "Deploying"
	Servicing  ProjectState = "Servicing"
	Failed     ProjectState = "Failed"
	Remove     ProjectState = "Remove"
)

// String status to string
func (ps ProjectState) String() string {
	return string(ps)
}

// ProjectInfo
type ProjectInfo struct {
	// uuid
	UUID      ProjectID
	State     ProjectState
	Name      string
	BundleURL string
	Replicas  int64

	CPUCores int64
	Memory   int64
	AreaID   string

	UserID string

	DetailsList []string

	EdgeReplicaSucceeds []string
	EdgeWaitings        int64
	RetryCount          int64
	ReplenishReplicas   int64

	NodeIDs []string
	Event   int64
}

// ToProjectInfo converts ProjectInfo to types.ProjectInfo
func (state *ProjectInfo) ToProjectInfo() *types.ProjectInfo {
	return &types.ProjectInfo{
		UUID:              state.UUID.String(),
		State:             state.State.String(),
		Name:              state.Name,
		BundleURL:         state.BundleURL,
		Replicas:          state.Replicas,
		UserID:            state.UserID,
		RetryCount:        state.RetryCount,
		ReplenishReplicas: state.ReplenishReplicas,
		CPUCores:          state.CPUCores,
		Memory:            float64(state.Memory),
		AreaID:            state.AreaID,
	}
}

// projectInfoFrom converts types.ProjectInfo to ProjectInfo
func projectInfoFrom(info *types.ProjectInfo) *ProjectInfo {
	cInfo := &ProjectInfo{
		UUID:              ProjectID(info.UUID),
		State:             ProjectState(info.State),
		Name:              info.Name,
		BundleURL:         info.BundleURL,
		Replicas:          info.Replicas,
		UserID:            info.UserID,
		RetryCount:        info.RetryCount,
		ReplenishReplicas: info.ReplenishReplicas,
		CPUCores:          info.CPUCores,
		Memory:            int64(info.Memory),
		AreaID:            info.AreaID,
	}

	for _, r := range info.DetailsList {
		switch r.Status {
		case types.ProjectReplicaStatusStarted:
			if len(cInfo.EdgeReplicaSucceeds) < cbg.MaxLength {
				cInfo.EdgeReplicaSucceeds = append(cInfo.EdgeReplicaSucceeds, r.NodeID)
			}
		case types.ProjectReplicaStatusStarting:
			cInfo.EdgeWaitings++
		}
	}

	return cInfo
}
