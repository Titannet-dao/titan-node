package types

import "time"

type CreateAssetReq struct {
	UserID    string
	AssetCID  string
	AssetName string
	AssetSize int64
}

type CreateAssetRsp struct {
	CandidateAddr string
	Token         string
}

type AssetProperty struct {
	CID    string
	Name   string
	Status int
	Size   int64
}

type AuthUserUploadAsset struct {
	UserID     string
	AssetCID   string
	AssetSize  int64
	Expiration time.Time
}

type AuthUserDownloadAsset struct {
	UserID   string
	AssetCID string
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

type StorageSize struct {
	TotalSize int64 `db:"total_storage_size"`
	UsedSize  int64 `db:"used_storage_size"`
}
