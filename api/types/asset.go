package types

import (
	"time"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
)

// AssetPullProgress represents the progress of pulling an asset
type AssetPullProgress struct {
	CID             string
	Status          ReplicaStatus
	Msg             string
	BlocksCount     int
	DoneBlocksCount int
	Size            int64
	DoneSize        int64
}

// PullResult contains information about the result of a data pull
type PullResult struct {
	Progresses       []*AssetPullProgress
	DiskUsage        float64
	TotalBlocksCount int
	AssetCount       int
}

// RemoveAssetResult contains information about the result of removing an asset
type RemoveAssetResult struct {
	BlocksCount int
	DiskUsage   float64
}

// AssetRecord represents information about an asset record
type AssetRecord struct {
	CID                   string          `db:"cid"`
	Hash                  string          `db:"hash"`
	NeedEdgeReplica       int64           `db:"edge_replicas"`
	TotalSize             int64           `db:"total_size"`
	TotalBlocks           int64           `db:"total_blocks"`
	Expiration            time.Time       `db:"expiration"`
	CreatedTime           time.Time       `db:"created_time"`
	EndTime               time.Time       `db:"end_time"`
	NeedCandidateReplicas int64           `db:"candidate_replicas"`
	ServerID              dtypes.ServerID `db:"scheduler_sid"`
	State                 string          `db:"state"`
	NeedBandwidth         int64           `db:"bandwidth"` // unit:MiB/s

	AssetName string `db:"asset_name"`

	RetryCount        int64 `db:"retry_count"`
	ReplenishReplicas int64 `db:"replenish_replicas"`
	ReplicaInfos      []*ReplicaInfo
}

// AssetStateInfo represents information about an asset state
type AssetStateInfo struct {
	State             string `db:"state"`
	RetryCount        int64  `db:"retry_count"`
	Hash              string `db:"hash"`
	ReplenishReplicas int64  `db:"replenish_replicas"`
}

// ReplicaInfo represents information about an asset replica
type ReplicaInfo struct {
	Hash        string        `db:"hash"`
	NodeID      string        `db:"node_id"`
	Status      ReplicaStatus `db:"status"`
	IsCandidate bool          `db:"is_candidate"`
	EndTime     time.Time     `db:"end_time"`
	DoneSize    int64         `db:"done_size"`
}

// PullAssetReq represents a request to pull an asset to Titan
type PullAssetReq struct {
	ID         string
	CID        string
	Hash       string
	Replicas   int64
	Expiration time.Time
	Bandwidth  int64 // unit:MiB/s
}

// AssetType represents the type of a asset
type AssetType int

const (
	// AssetTypeCarfile type
	AssetTypeCarfile AssetType = iota
	// AssetTypeFile type
	AssetTypeFile
)

type ReplicaEvent int

const (
	// ReplicaEventRemove event
	ReplicaEventRemove ReplicaEvent = iota
	// ReplicaEventAdd event
	ReplicaEventAdd
)

// ReplicaStatus represents the status of a replica pull
type ReplicaStatus int

const (
	// ReplicaStatusWaiting status
	ReplicaStatusWaiting ReplicaStatus = iota
	// ReplicaStatusPulling status
	ReplicaStatusPulling
	// ReplicaStatusFailed status
	ReplicaStatusFailed
	// ReplicaStatusSucceeded status
	ReplicaStatusSucceeded
)

// String status to string
func (c ReplicaStatus) String() string {
	switch c {
	case ReplicaStatusWaiting:
		return "Waiting"
	case ReplicaStatusFailed:
		return "Failed"
	case ReplicaStatusPulling:
		return "Pulling"
	case ReplicaStatusSucceeded:
		return "Succeeded"
	default:
		return "Unknown"
	}
}

// ReplicaStatusAll contains all possible replica statuses
var ReplicaStatusAll = []ReplicaStatus{
	ReplicaStatusWaiting,
	ReplicaStatusPulling,
	ReplicaStatusFailed,
	ReplicaStatusSucceeded,
}

// AssetStats contains statistics about assets
type AssetStats struct {
	TotalAssetCount     int
	TotalBlockCount     int
	WaitCacheAssetCount int
	InProgressAssetCID  string
	DiskUsage           float64
}

// InProgressAsset represents an asset that is currently being fetched, including its progress details.
type InProgressAsset struct {
	CID       string
	TotalSize int64
	DoneSize  int64
}

// AssetHash is an identifier for a asset.
type AssetHash string

func (c AssetHash) String() string {
	return string(c)
}

// AssetStatistics Statistics on asset pulls and downloads
type AssetStatistics struct {
	ReplicaCount      int
	UserDownloadCount int
}

// NodeAssetInfo node asset info of web
type NodeAssetInfo struct {
	Hash       string    `db:"hash"`
	Cid        string    `db:"cid"`
	TotalSize  int64     `db:"total_size"`
	Expiration time.Time `db:"expiration"`
	EndTime    time.Time `db:"end_time"`
}

// ListNodeAssetRsp list node assets
type ListNodeAssetRsp struct {
	Total          int              `json:"total"`
	NodeAssetInfos []*NodeAssetInfo `json:"asset_infos"`
}

// ReplicaEventInfo replica event info
type ReplicaEventInfo struct {
	NodeID  string       `db:"node_id"`
	Event   ReplicaEvent `db:"event"`
	Hash    string       `db:"hash"`
	EndTime time.Time    `db:"end_time"`

	Cid        string    `db:"cid"`
	TotalSize  int64     `db:"total_size"`
	Expiration time.Time `db:"expiration"`
}

// ListReplicaEventRsp list replica events
type ListReplicaEventRsp struct {
	Total         int                 `json:"total"`
	ReplicaEvents []*ReplicaEventInfo `json:"replica_events"`
}
