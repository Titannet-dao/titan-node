package types

import "time"

// CreateSyncAssetReq represents a request to create a synchronized asset.
type CreateSyncAssetReq struct {
	AssetCID      string
	AssetSize     int64
	ReplicaCount  int64
	ExpirationDay int
	Owner         string

	DownloadInfos []*SourceDownloadInfo
	DownloadInfo  *SourceDownloadInfo
}

// CreateAssetReq represents a request to create an asset.
type CreateAssetReq struct {
	UserID        string
	AssetCID      string
	AssetSize     int64
	NodeID        string
	ReplicaCount  int64
	ExpirationDay int
	Owner         string
	TraceID       string
}

// GetUploadInfoReq represents a request to get upload information.
type GetUploadInfoReq struct {
	UserID  string
	URLMode bool
	TraceID string
}

// UploadInfo holds information about file uploads.
type UploadInfo struct {
	List          []*NodeUploadInfo
	AlreadyExists bool
}

// NodeUploadInfo contains information for uploading a node.
type NodeUploadInfo struct {
	UploadURL string
	Token     string
	NodeID    string
}

// AuthUserUploadDownloadAsset represents the user asset upload and download information.
type AuthUserUploadDownloadAsset struct {
	UserID     string
	AssetCID   string
	AssetSize  int64
	Expiration time.Time
	TraceID    string
}

// UploadProgress represents the progress of an upload operation.
type UploadProgress struct {
	TotalSize int64
	DoneSize  int64
}

// UploadingAsset represents an asset being uploaded by a user.
type UploadingAsset struct {
	UserID          string
	TokenExpiration time.Time
	Progress        *UploadProgress
}

// UserInfo holds information about the user's storage.
type UserInfo struct {
	TotalSize     int64 `db:"total_storage_size"`
	UsedSize      int64 `db:"used_storage_size"`
	TotalTraffic  int64 `db:"total_traffic"`
	PeakBandwidth int64 `db:"peak_bandwidth"`
	DownloadCount int64 `db:"download_count"`
	EnableVIP     bool  `db:"enable_vip"`

	UpdateTime time.Time `db:"update_peak_time"`
}

// UserAPIKeysInfo holds information about the user's API keys.
type UserAPIKeysInfo struct {
	CreatedTime time.Time
	APIKey      string
}

// UserAssetShareStatus represents the sharing status of a user asset.
type UserAssetShareStatus int

const (
	// UserAssetShareStatusUnshare indicates that the asset is not shared.
	UserAssetShareStatusUnshare UserAssetShareStatus = iota
	// UserAssetShareStatusShared indicates that the asset is shared.
	UserAssetShareStatusShared
	// UserAssetShareStatusForbid indicates that sharing of the asset is forbidden.
	UserAssetShareStatusForbid
)

// UserAccessControl represents the access level of a user.
type UserAccessControl string

// UserAPIKeyReadFile allows reading files.
const (
	UserAPIKeyReadFile     UserAccessControl = "readFile"
	UserAPIKeyCreateFile   UserAccessControl = "createFile"
	UserAPIKeyDeleteFile   UserAccessControl = "deleteFile"
	UserAPIKeyReadFolder   UserAccessControl = "readFolder"
	UserAPIKeyCreateFolder UserAccessControl = "createFolder"
	UserAPIKeyDeleteFolder UserAccessControl = "deleteFolder"
)

// UserAccessControlAll contains all user access control permissions.
var UserAccessControlAll = []UserAccessControl{
	UserAPIKeyReadFile,
	UserAPIKeyCreateFile,
	UserAPIKeyDeleteFile,
}

// FuncAccessControlMap key is function name, value is permission name
var FuncAccessControlMap = map[string]UserAccessControl{
	"CreateAsset":      UserAPIKeyCreateFile,
	"ListAssets":       UserAPIKeyReadFile,
	"DeleteAsset":      UserAPIKeyDeleteFile,
	"ShareAssets":      UserAPIKeyReadFile,
	"CreateAssetGroup": UserAPIKeyCreateFile,
	"ListAssetGroup":   UserAPIKeyReadFile,
	"DeleteAssetGroup": UserAPIKeyDeleteFile,
	"RenameAssetGroup": UserAPIKeyCreateFile,
}
