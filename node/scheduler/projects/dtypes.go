package projects

import (
	"bytes"
	"encoding/gob"

	"github.com/Filecoin-Titan/titan/api/types"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// ProjectID is an identifier for a project.
type ProjectID string

func (c ProjectID) String() string {
	return string(c)
}

// ProjectState represents the state of a project in the scheduler.
type ProjectState string

// NodeSelect indicates the project state when a node is selected.
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

// ProjectRequirement represents the resource requirements for a project.
type ProjectRequirement struct {
	CPUCores int64
	Memory   int64
	AreaID   string
	Version  int64

	NodeIDs []string
}

// ProjectInfo represents information about a project.
type ProjectInfo struct {
	// uuid
	UUID      ProjectID
	State     ProjectState
	Name      string
	BundleURL string
	Replicas  int64

	Type int64
	// CPUCores int64
	// Memory   int64
	// AreaID   string

	UserID string

	DetailsList []string

	Requirement ProjectRequirement

	EdgeReplicaSucceeds []string
	EdgeWaitings        int64
	RetryCount          int64
	ReplenishReplicas   int64

	// NodeIDs []string
	Event int64
}

// projectInfoFrom converts types.ProjectInfo to ProjectInfo
func projectInfoFrom(info *types.ProjectInfo) *ProjectInfo {
	pr := ProjectRequirement{}
	if info.RequirementByte != nil {
		dec := gob.NewDecoder(bytes.NewBuffer(info.RequirementByte))
		err := dec.Decode(&pr)
		if err != nil {
			log.Errorf("decode data to []*types.Workload error: %s", err.Error())
			return nil
		}
	}

	cInfo := &ProjectInfo{
		UUID:              ProjectID(info.UUID),
		State:             ProjectState(info.State),
		Name:              info.Name,
		BundleURL:         info.BundleURL,
		Replicas:          info.Replicas,
		UserID:            info.UserID,
		RetryCount:        info.RetryCount,
		ReplenishReplicas: info.ReplenishReplicas,
		Requirement:       pr,
		Type:              int64(info.Type),
		// CPUCores:          info.CPUCores,
		// Memory:            int64(info.Memory),
		// AreaID:            info.AreaID,
	}

	// log.Debugf("projectInfoFrom : %s ,area:%s nodes : %s", info.UUID, cInfo.Requirement.AreaID, cInfo.Requirement.NodeIDs)

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
