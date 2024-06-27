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
	NeedBandwidth         int64           `db:"bandwidth"` // unit:MiB/
	Note                  string          `db:"note"`
	Source                int64           `db:"source"`

	RetryCount        int64 `db:"retry_count"`
	ReplenishReplicas int64 `db:"replenish_replicas"`
	ReplicaInfos      []*ReplicaInfo

	SPCount int64
}

type UserAssetDetail struct {
	UserID      string    `db:"user_id"`
	Hash        string    `db:"hash"`
	AssetName   string    `db:"asset_name"`
	AssetType   string    `db:"asset_type"`
	ShareStatus int64     `db:"share_status"`
	Expiration  time.Time `db:"expiration"`
	CreatedTime time.Time `db:"created_time"`
	TotalSize   int64     `db:"total_size"`
	Password    string    `db:"password"`
	GroupID     int       `db:"group_id"`
}

type AssetOverview struct {
	AssetRecord      *AssetRecord
	UserAssetDetail  *UserAssetDetail
	VisitCount       int
	RemainVisitCount int
}

// ListAssetRecordRsp list asset records
type ListAssetRecordRsp struct {
	Total          int              `json:"total"`
	AssetOverviews []*AssetOverview `json:"asset_infos"`
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
	StartTime   time.Time     `db:"start_time"`
}

// PullAssetReq represents a request to pull an asset to Titan
type PullAssetReq struct {
	CID        string
	Replicas   int64
	Expiration time.Time

	Bucket    string
	Hash      string
	Bandwidth int64 // unit:MiB/s

	SeedNodeID        string
	CandidateReplicas int64

	CandidateNodeList []string
	EdgeNodeList      []string
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
	// MinioEventAdd event
	MinioEventAdd
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

// AssetSource aws or storage
type AssetSource int64

const (
	// AssetSourceAdminPull
	AssetSourceAdminPull AssetSource = iota
	// AssetSourceAWS status
	AssetSourceAWS
	// AssetSourceStorage status
	AssetSourceStorage
	// AssetSourceMinio status
	AssetSourceMinio
)

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

// NodeReplicaInfo node replica info of web
type NodeReplicaInfo struct {
	Hash      string        `db:"hash"`
	Cid       string        `db:"cid"`
	TotalSize int64         `db:"total_size"`
	Status    ReplicaStatus `db:"status"`
	DoneSize  int64         `db:"done_size"`
	StartTime time.Time     `db:"start_time"`
	EndTime   time.Time     `db:"end_time"`
}

// ListNodeReplicaRsp list node assets
type ListNodeReplicaRsp struct {
	Total            int                `json:"total"`
	NodeReplicaInfos []*NodeReplicaInfo `json:"infos"`
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
	Source  int64        `db:"source"`

	Cid        string    `db:"cid"`
	TotalSize  int64     `db:"total_size"`
	Expiration time.Time `db:"expiration"`
}

// ListReplicaEventRsp list replica events
type ListReplicaEventRsp struct {
	Total         int                 `json:"total"`
	ReplicaEvents []*ReplicaEventInfo `json:"replica_events"`
}

// ListReplicaRsp list asset replicas
type ListReplicaRsp struct {
	Total        int            `json:"total"`
	ReplicaInfos []*ReplicaInfo `json:"replica_infos"`
}

type AssetStatus struct {
	IsExist           bool
	IsExpiration      bool
	IsVisitOutOfLimit bool
}

type MinioUploadFileEvent struct {
	AssetCID   string
	Size       int64
	CreateTime time.Time
	Expiration time.Time
}

type AssetView struct {
	TopHash string
	// key bucketID, value bucketHash
	BucketHashes map[uint32]string
}

type FreeUpDiskResp struct {
	Hashes   []string
	NextTime int64
}

type FreeUpDiskStateResp struct {
	Hashes   []*FreeUpDiskState
	NextTime int64
}

type FreeUpDiskState struct {
	Hash   string
	ErrMsg string
}

type AWSDownloadSources struct {
	Bucket string
	Key    string
}

type DownloadSources struct {
	Nodes []*SourceDownloadInfo
	AWS   *AWSDownloadSources
}

type AssetPullRequest struct {
	AssetCID   string
	Dss        *DownloadSources
	WorkloadID string
}
