package types

import (
	"time"

	"github.com/filecoin-project/go-jsonrpc/auth"
)

// OpenRPCDocument represents a document in the OpenRPC format.
type OpenRPCDocument map[string]interface{}

// EventTopics represents topics for pub/sub events
type EventTopics string

const (
	// EventNodeOnline node online event
	EventNodeOnline EventTopics = "node_online"
	// EventNodeOffline node offline event
	EventNodeOffline EventTopics = "node_offline"
)

func (t EventTopics) String() string {
	return string(t)
}

// ValidationInfo Validation, election related information
type ValidationInfo struct {
	NextElectionTime time.Time
}

// JWTPayload represents the payload for JWT authentication.
type JWTPayload struct {
	// role base access controller permission
	Allow []auth.Permission
	ID    string
	// TODO remove NodeID later, any role id replace as ID
	NodeID string
	// Extend is json string
	Extend string
	// The sub permission of user
	AccessControlList []UserAccessControl
	// Password for encrypt and decrypt the upload file
	FilePassNonce string

	TraceID string
}

// StorageStats storage stats of user
type StorageStats struct {
	TotalSize    int64 `db:"total_storage_size"`
	UsedSize     int64 `db:"used_storage_size"`
	TotalTraffic int64 `db:"total_traffic"`
	EnableVIP    bool  `db:"enable_vip"`
	AssetCount   int   `db:"asset_count"`
}

// ListStorageStatsRsp list storage stats records
type ListStorageStatsRsp struct {
	Total    int             `json:"total"`
	Storages []*StorageStats `json:"infos"`
}
