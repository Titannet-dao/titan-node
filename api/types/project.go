package types

import (
	"time"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
)

type Project struct {
	ID        string // Id
	Name      string
	Status    ProjectReplicaStatus
	BundleURL string
	Port      int

	Msg string
}

type ProjectReplicaStatus int

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

type ProjectReq struct {
	UUID   string
	NodeID string
	UserID string

	Name      string
	BundleURL string
	Replicas  int64
}

type DeployProjectReq struct {
	UUID       string
	Name       string
	BundleURL  string
	UserID     string
	Replicas   int64
	Expiration time.Time

	CPUCores int64
	Memory   float64
	AreaID   string
	NodeIDs  []string
}

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

	CPUCores int64   `db:"cpu_cores"`
	Memory   float64 `db:"memory"`
	AreaID   string  `db:"area_id"`

	DetailsList       []*ProjectReplicas
	RetryCount        int64 `db:"retry_count"`
	ReplenishReplicas int64 `db:"replenish_replicas"`
}

type ProjectReplicas struct {
	Id          string               `db:"id"`
	Status      ProjectReplicaStatus `db:"status"`
	NodeID      string               `db:"node_id"`
	CreatedTime time.Time            `db:"created_time"`
	EndTime     time.Time            `db:"end_time"`

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

type ProjectEvent int

const (
	ProjectEventRemove ProjectEvent = iota
	ProjectEventAdd
	ProjectEventNodeOffline

	ProjectEventStatusChange
	ProjectEventExpiration
)
