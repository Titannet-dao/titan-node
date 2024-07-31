package types

import "time"

type CreateSyncAssetReq struct {
	AssetCID      string
	AssetSize     int64
	ReplicaCount  int64
	ExpirationDay int

	DownloadInfo *SourceDownloadInfo
}

type CreateAssetReq struct {
	UserID        string
	AssetCID      string
	AssetSize     int64
	NodeID        string
	ReplicaCount  int64
	ExpirationDay int
}

type UploadInfo struct {
	List          []*NodeUploadInfo
	AlreadyExists bool
}

type NodeUploadInfo struct {
	UploadURL string
	Token     string
	NodeID    string
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

type UserAccessControl string

const (
	UserAPIKeyReadFile     UserAccessControl = "readFile"
	UserAPIKeyCreateFile   UserAccessControl = "createFile"
	UserAPIKeyDeleteFile   UserAccessControl = "deleteFile"
	UserAPIKeyReadFolder   UserAccessControl = "readFolder"
	UserAPIKeyCreateFolder UserAccessControl = "createFolder"
	UserAPIKeyDeleteFolder UserAccessControl = "deleteFolder"
)

var UserAccessControlAll = []UserAccessControl{
	UserAPIKeyReadFile,
	UserAPIKeyCreateFile,
	UserAPIKeyDeleteFile,
	UserAPIKeyReadFolder,
	UserAPIKeyCreateFolder,
	UserAPIKeyDeleteFolder,
}

// key is function name, value is permission name
var FuncAccessControlMap = map[string]UserAccessControl{
	"CreateAsset":      UserAPIKeyCreateFile,
	"ListAssets":       UserAPIKeyReadFile,
	"DeleteAsset":      UserAPIKeyDeleteFile,
	"ShareAssets":      UserAPIKeyReadFile,
	"CreateAssetGroup": UserAPIKeyCreateFolder,
	"ListAssetGroup":   UserAPIKeyReadFolder,
	"DeleteAssetGroup": UserAPIKeyDeleteFolder,
	"RenameAssetGroup": UserAPIKeyCreateFolder,
}
