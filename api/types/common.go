package types

import (
	"time"

	"github.com/filecoin-project/go-jsonrpc/auth"
)

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

// AssetGroup user asset group
type AssetGroup struct {
	ID          int       `db:"id"`
	UserID      string    `db:"user_id"`
	Name        string    `db:"name"`
	Parent      int       `db:"parent"`
	AssetCount  int       `db:"asset_count"`
	AssetSize   int64     `db:"asset_size"`
	CreatedTime time.Time `db:"created_time"`
}

// ListAssetGroupRsp list  asset group records
type ListAssetGroupRsp struct {
	Total       int           `json:"total"`
	AssetGroups []*AssetGroup `json:"infos"`
}

// UserAssetSummary user asset and group
type UserAssetSummary struct {
	AssetOverview *AssetOverview
	AssetGroup    *AssetGroup
}

// ListAssetSummaryRsp list asset and group
type ListAssetSummaryRsp struct {
	Total int                 `json:"total"`
	List  []*UserAssetSummary `json:"list"`
}
