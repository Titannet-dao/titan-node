package types

import "time"

type AssetProperty struct {
	AssetCID  string
	AssetName string
	AssetSize int64
	AssetType string
	NodeID    string
}

type CreateAssetReq struct {
	UserID string
	AssetProperty
}

type CreateAssetRsp struct {
	UploadURL     string
	Token         string
	AlreadyExists bool
}

type AuthUserUploadDownloadAsset struct {
	UserID     string
	AssetCID   string
	AssetSize  int64
	Expiration time.Time
}

type UploadProgress struct {
	TotalSize int64
	DoneSize  int64
}

type UploadingAsset struct {
	UserID          string
	TokenExpiration time.Time
	Progress        *UploadProgress
}

type UserInfo struct {
	TotalSize     int64 `db:"total_storage_size"`
	UsedSize      int64 `db:"used_storage_size"`
	TotalTraffic  int64 `db:"total_traffic"`
	PeakBandwidth int64 `db:"peak_bandwidth"`
	DownloadCount int64 `db:"download_count"`
	EnableVIP     bool  `db:"enable_vip"`

	UpdateTime time.Time `db:"update_peak_time"`
}

type UserAPIKeysInfo struct {
	CreatedTime time.Time
	APIKey      string
}

type UserAssetShareStatus int

const (
	UserAssetShareStatusUnshare UserAssetShareStatus = iota
	UserAssetShareStatusShared
	UserAssetShareStatusForbid
)
