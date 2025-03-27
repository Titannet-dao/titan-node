package types

import (
	"time"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
)

// Project represents a project with an ID and a name.
type Project struct {
	ID        string // Id
	Name      string
	Status    ProjectReplicaStatus
	BundleURL string
	Port      int

	Msg string
}

// ProjectType represents the type of a project.
type ProjectType int64

// ProjectTypeTunnel represents a tunnel project type.
const (
	ProjectTypeTunnel ProjectType = iota
)

// ProjectReplicaStatus represents the status of a project replica.
type ProjectReplicaStatus int

// ProjectReplicaStatusStarting indicates that the project replica is starting.
const (
	ProjectReplicaStatusStarting ProjectReplicaStatus = iota
	ProjectReplicaStatusStarted
	ProjectReplicaStatusError
	ProjectReplicaStatusOffline
)

// String status to string
func (ps ProjectReplicaStatus) String() string {
	switch ps {
	case ProjectReplicaStatusStarting:
		return "starting"
	case ProjectReplicaStatusStarted:
		return "started"
	case ProjectReplicaStatusError:
		return "error"
	case ProjectReplicaStatusOffline:
		return "offline"
	default:
		return "invalidStatus"
	}
}

// ProjectReq represents a request for a project.
type ProjectReq struct {
	UUID   string
	NodeID string
	UserID string

	Name      string
	BundleURL string
	Replicas  int64
}

// DeployProjectReq represents a request to deploy a project.
type DeployProjectReq struct {
	UUID       string
	Name       string
	BundleURL  string
	UserID     string
	Replicas   int64
	Expiration time.Time
	Type       ProjectType

	Requirement ProjectRequirement
}

// ProjectRequirement represents the requirements for a project.
type ProjectRequirement struct {
	CPUCores int64
	Memory   int64
	AreaID   string
	Version  int64

	NodeIDs []string
}

// ProjectInfo holds information about a project.
type ProjectInfo struct {
	// uuid
	UUID        string          `db:"id"`
	State       string          `db:"state"`
	Name        string          `db:"name"`
	BundleURL   string          `db:"bundle_url"`
	Replicas    int64           `db:"replicas"`
	ServerID    dtypes.ServerID `db:"scheduler_sid"`
	Expiration  time.Time       `db:"expiration"`
	CreatedTime time.Time       `db:"created_time"`
	UserID      string          `db:"user_id"`
	Type        ProjectType     `db:"type"`

	RequirementByte []byte `db:"requirement"`
	Requirement     ProjectRequirement

	DetailsList       []*ProjectReplicas
	RetryCount        int64 `db:"retry_count"`
	ReplenishReplicas int64 `db:"replenish_replicas"`
}

// ProjectReplicas represents the replicas of a project.
type ProjectReplicas struct {
	Id            string               `db:"id"`
	Status        ProjectReplicaStatus `db:"status"`
	NodeID        string               `db:"node_id"`
	CreatedTime   time.Time            `db:"created_time"`
	EndTime       time.Time            `db:"end_time"`
	Type          ProjectType          `db:"type"`
	UploadTraffic int64                `db:"upload_traffic"`
	DownTraffic   int64                `db:"download_traffic"`
	Time          int64                `db:"time"`
	MaxDelay      int64                `db:"max_delay"`
	MinDelay      int64                `db:"min_delay"`
	AvgDelay      int64                `db:"avg_delay"`

	WsURL     string
	BundleURL string
	IP        string
	GeoID     string
}

// ProjectStateInfo represents information about an project state
type ProjectStateInfo struct {
	ID                string `db:"id"`
	State             string `db:"state"`
	RetryCount        int64  `db:"retry_count"`
	ReplenishReplicas int64  `db:"replenish_replicas"`
}

// ProjectEvent represents the different types of events for a project.
type ProjectEvent int

// ProjectEventRemove indicates that a project has been removed.
const (
	ProjectEventRemove ProjectEvent = iota
	ProjectEventAdd
	ProjectEventNodeOffline
	ProjectEventExpiration
	ProjectEventFailed
)

// ProjectOverview represents an overview of a project.
type ProjectOverview struct {
	NodeID          string `db:"node_id"`
	UploadTraffic   int64  `db:"sum_upload_traffic"`
	DownloadTraffic int64  `db:"sum_download_traffic"`
	Time            int64  `db:"sum_time"`
	AvgDelay        int64  `db:"avg_delay"`
}

// ListProjectOverviewRsp list replica events
type ListProjectOverviewRsp struct {
	Total int                `json:"total"`
	List  []*ProjectOverview `json:"list"`
}

// ListProjectReplicaRsp list replica info
type ListProjectReplicaRsp struct {
	Total int                `json:"total"`
	List  []*ProjectReplicas `json:"list"`
}

// NodeProjectReq represents a request for a node project.
type NodeProjectReq struct {
	NodeID    string
	Limit     int
	Offset    int
	ProjectID string
	Statuses  []ProjectReplicaStatus
}
