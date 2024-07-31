package api

import (
	"context"
	"io"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/google/uuid"
)

// AssetAPI is an interface for asset
type AssetAPI interface {
	// Asset-related methods
	// PullAsset Pull an asset based on the provided PullAssetReq structure.
	PullAsset(ctx context.Context, info *types.PullAssetReq) error //perm:web,admin
	// RemoveAssetRecord removes the asset record with the specified CID from the scheduler
	RemoveAssetRecord(ctx context.Context, cid string) error //perm:admin,web
	// StopAssetRecord stop asset
	StopAssetRecord(ctx context.Context, cids []string) error //perm:admin
	// RemoveAssetReplica deletes an asset replica with the specified CID and node from the scheduler
	RemoveAssetReplica(ctx context.Context, cid, nodeID string) error //perm:admin
	// GetAssetRecord retrieves the asset record with the specified CID
	GetAssetRecord(ctx context.Context, cid string) (*types.AssetRecord, error) //perm:web,admin
	// GetAssetRecords retrieves a list of asset records with pagination using the specified limit, offset, and states
	GetAssetRecords(ctx context.Context, limit, offset int, states []string, serverID dtypes.ServerID) ([]*types.AssetRecord, error) //perm:web,admin
	// GetReplicas retrieves a list of asset replicas with pagination using the specified limit, offset
	GetReplicas(ctx context.Context, cid string, limit, offset int) (*types.ListReplicaRsp, error) //perm:web,admin
	// RePullFailedAssets retries the pull process for a list of failed assets
	RePullFailedAssets(ctx context.Context, hashes []types.AssetHash) error //perm:admin
	// UpdateAssetExpiration updates the expiration time for an asset with the specified CID
	UpdateAssetExpiration(ctx context.Context, cid string, time time.Time) error //perm:admin
	// NodeRemoveAssetResult the result of an asset removal operation
	NodeRemoveAssetResult(ctx context.Context, resultInfo types.RemoveAssetResult) error //perm:edge,candidate
	// GetAssetListForBucket retrieves a list of asset hashes for a bucket associated with the specified bucket ID (bucketID is hash code)
	GetAssetListForBucket(ctx context.Context, bucketID uint32) ([]string, error) //perm:edge,candidate
	// GetAssetCount retrieves a count of asset
	GetAssetCount(ctx context.Context) (int, error) //perm:web,admin
	// GetAssetsForNode retrieves a asset list of node
	GetAssetsForNode(ctx context.Context, nodeID string, limit, offset int) (*types.ListNodeAssetRsp, error) //perm:web,admin
	// GetReplicasForNode retrieves a replica list of node
	GetReplicasForNode(ctx context.Context, nodeID string, limit, offset int, statuses []types.ReplicaStatus) (*types.ListNodeReplicaRsp, error) //perm:web,admin
	// GetReplicaEventsForNode retrieves a replica event list of node
	GetReplicaEventsForNode(ctx context.Context, nodeID string, limit, offset int) (*types.ListReplicaEventRsp, error) //perm:web,admin
	// GetReplicaEvents retrieves a replica event list of node
	GetReplicaEvents(ctx context.Context, start, end time.Time, limit, offset int) (*types.ListReplicaEventRsp, error) //perm:web,admin
	// CreateAsset creates an asset with car CID, car name, and car size.
	CreateAsset(ctx context.Context, req *types.CreateAssetReq) (*types.UploadInfo, error) //perm:web,admin,user
	// CreateSyncAsset Synchronizing assets from other schedulers
	CreateSyncAsset(ctx context.Context, req *types.CreateSyncAssetReq) error //perm:web,admin,user
	// GenerateTokenForDownloadSource Generate Token For Download Source
	GenerateTokenForDownloadSource(ctx context.Context, nodeID string, cid string) (*types.SourceDownloadInfo, error) //perm:web,admin,user
	// // ListAssets lists the assets of the user.
	// ListAssets(ctx context.Context, userID string, limit, offset, groupID int) (*types.ListAssetRecordRsp, error) //perm:web,admin,user
	// // DeleteAsset deletes the asset of the user.
	// DeleteAsset(ctx context.Context, userID, assetCID string) error //perm:web,admin,user
	// ShareAssets shares the assets of the user.
	ShareAssets(ctx context.Context, userID string, assetCID []string) (map[string][]string, error) //perm:web,admin,user
	// // UpdateShareStatus update share status of the user asset
	// UpdateShareStatus(ctx context.Context, userID, assetCID string) error //perm:web,admin
	// // GetAssetStatus retrieves a asset status
	// GetAssetStatus(ctx context.Context, userID, assetCID string) (*types.AssetStatus, error) //perm:web,admin
	// MinioUploadFileEvent the event of minio upload file
	MinioUploadFileEvent(ctx context.Context, event *types.MinioUploadFileEvent) error //perm:candidate
	// AddAWSData add aws resource information
	AddAWSData(ctx context.Context, list []types.AWSDataInfo) error //perm:web,admin
	// SwitchFillDiskTimer  switches the timer between ON and OFF states
	SwitchFillDiskTimer(ctx context.Context, open bool) error //perm:web,admin
	// LoadAWSData load data
	LoadAWSData(ctx context.Context, limit, offset int, isDistribute bool) ([]*types.AWSDataInfo, error) //perm:web,admin
	// RemoveNodeFailedReplica
	RemoveNodeFailedReplica(ctx context.Context) error //perm:web,admin
}

// NodeAPI is an interface for node
type NodeAPI interface {
	// Node-related methods
	// GetOnlineNodeCount returns the count of online nodes for a given node type
	GetOnlineNodeCount(ctx context.Context, nodeType types.NodeType) (int, error) //perm:web,admin
	// RegisterNode adds new node to the scheduler
	RegisterNode(ctx context.Context, nodeID, publicKey string, nodeType types.NodeType) (*types.ActivationDetail, error) //perm:default
	// RegisterEdgeNode adds new edge node to the scheduler
	RegisterEdgeNode(ctx context.Context, nodeID, publicKey string) (*types.ActivationDetail, error) //perm:default
	// RegisterCandidateNode adds new node to the scheduler
	RegisterCandidateNode(ctx context.Context, nodeID, publicKey, code string) (*types.ActivationDetail, error) //perm:default
	// DeactivateNode is used to deactivate a node in the titan server.
	// It stops the node from serving any requests and marks it as inactive.
	// - nodeID: The ID of the node to deactivate.
	// - hours: The deactivation countdown time in hours. It specifies the duration
	// before the deactivation is executed. If the deactivation is canceled within
	// this period, the node will remain active.
	DeactivateNode(ctx context.Context, nodeID string, hours int) error //perm:web,admin,candidate
	// CalculateExitProfit
	CalculateExitProfit(ctx context.Context, nodeID string) (types.ExitProfitRsp, error) //perm:web,admin,candidate
	// UndoNodeDeactivation is used to undo the deactivation of a node in the titan server.
	// It allows the previously deactivated node to start serving requests again.
	UndoNodeDeactivation(ctx context.Context, nodeID string) error //perm:web,admin
	// UpdateNodePort updates the port for the node with the specified node
	UpdateNodePort(ctx context.Context, nodeID, port string) error //perm:web,admin
	// EdgeConnect edge node login to the scheduler
	EdgeConnect(ctx context.Context, opts *types.ConnectOptions) error //perm:edge
	// CandidateConnect candidate node login to the scheduler
	CandidateConnect(ctx context.Context, opts *types.ConnectOptions) error //perm:candidate
	// L5Connect l5 node login to the scheduler
	L5Connect(ctx context.Context, opts *types.ConnectOptions) error //perm:l5
	// GetExternalAddress retrieves the external address of the caller.
	GetExternalAddress(ctx context.Context) (string, error) //perm:default
	// NodeLogin generates an authentication token for a node with the specified node ID and signature
	NodeLogin(ctx context.Context, nodeID, sign string) (string, error) //perm:default
	// GetNodeInfo get information for node
	GetNodeInfo(ctx context.Context, nodeID string) (types.NodeInfo, error) //perm:web,admin
	// GetNodeList retrieves a list of nodes with pagination using the specified cursor and count
	GetNodeList(ctx context.Context, cursor int, count int) (*types.ListNodesRsp, error) //perm:web,admin
	// GetNodesFromRegion retrieves a list of nodes with pagination using the specified cursor and count
	GetNodesFromRegion(ctx context.Context, areaID string) ([]*types.NodeInfo, error) //perm:web,admin
	// GetCurrentRegionInfos retrieves a list of nodes with pagination using the specified cursor and count
	GetCurrentRegionInfos(ctx context.Context, areaID string) (map[string]int, error) //perm:web,admin
	// GetCandidateURLsForDetectNat Get the rpc url of the specified number of candidate nodes
	GetCandidateURLsForDetectNat(ctx context.Context) ([]string, error) //perm:default
	// GetEdgeExternalServiceAddress nat travel, get edge external addr with different candidate
	GetEdgeExternalServiceAddress(ctx context.Context, nodeID, candidateURL string) (string, error) //perm:admin
	// NatPunch nat punch between user and node
	NatPunch(ctx context.Context, target *types.NatPunchReq) error //perm:default
	// GetEdgeDownloadInfos retrieves download information for the edge with the asset with the specified CID.
	GetEdgeDownloadInfos(ctx context.Context, cid string) (*types.EdgeDownloadInfoList, error) //perm:default
	// GetCandidateDownloadInfos retrieves download information for the candidate with the asset with the specified CID.
	GetCandidateDownloadInfos(ctx context.Context, cid string) ([]*types.CandidateDownloadInfo, error) //perm:edge,candidate,web,locator
	// GetAssetSourceDownloadInfo
	GetAssetSourceDownloadInfo(ctx context.Context, cid string) (*types.AssetSourceDownloadInfoRsp, error) //perm:edge,candidate,web,locator
	// NodeExists checks if the node with the specified ID exists.
	NodeExists(ctx context.Context, nodeID string) error //perm:web
	// NodeKeepalive
	NodeKeepalive(ctx context.Context) (uuid.UUID, error) //perm:edge,candidate
	// NodeKeepaliveV2 fix the problem of NodeKeepalive, Maintaining old device connections
	NodeKeepaliveV2(ctx context.Context) (uuid.UUID, error) //perm:edge,candidate,l5
	// RequestActivationCodes Get the device's encrypted activation code
	RequestActivationCodes(ctx context.Context, nodeType types.NodeType, count int) ([]*types.NodeActivation, error) //perm:web,admin
	// VerifyTokenWithLimitCount verify token in limit count
	VerifyTokenWithLimitCount(ctx context.Context, token string) (*types.JWTPayload, error) //perm:edge,candidate
	// UpdateBandwidths update node bandwidthDown and bandwidthUp
	UpdateBandwidths(ctx context.Context, bandwidthDown, bandwidthUp int64) error //perm:edge,candidate
	// UpdateNetFlows update node net flow total,up,down usage
	UpdateNetFlows(ctx context.Context, total, up, down int64) error //perm:edge
	// GetCandidateNodeIP get candidate ip for locator
	GetCandidateNodeIP(ctx context.Context, nodeID string) (string, error) //perm:web,admin
	// GetMinioConfigFromCandidate get minio config from candidate
	GetMinioConfigFromCandidate(ctx context.Context, nodeID string) (*types.MinioConfig, error) //perm:default
	// GetCandidateIPs get candidate ips
	GetCandidateIPs(ctx context.Context) ([]*types.NodeIPInfo, error) //perm:web,user,admin
	// GetNodeOnlineState get node online state
	GetNodeOnlineState(ctx context.Context) (bool, error) //perm:edge
	// DownloadDataResult node download data from AWS result
	DownloadDataResult(ctx context.Context, bucket, cid string, size int64) error //perm:edge,candidate
	// GetNodeToken get node token
	GetNodeToken(ctx context.Context, nodeID string) (string, error) //perm:admin
	// CheckIpUsage
	CheckIpUsage(ctx context.Context, ip string) (bool, error) //perm:admin,web,locator
	// GetAssetView get the asset view of node
	GetAssetView(ctx context.Context, nodeID string, isFromNode bool) (*types.AssetView, error) //perm:admin
	// GetAssetInBucket get the assets of the bucket
	GetAssetsInBucket(ctx context.Context, nodeID string, bucketID int, isFromNode bool) ([]string, error) //perm:admin
	// GetNodeOfIP get nodes
	GetNodeOfIP(ctx context.Context, ip string) ([]string, error) //perm:admin,web,locator
	// PerformSyncData sync the assetView of scheduler and node
	PerformSyncData(ctx context.Context, nodeID string) error //perm:admin
	// GetProfitDetailsForNode retrieves a profit list of node
	GetProfitDetailsForNode(ctx context.Context, nodeID string, limit, offset int, ts []int) (*types.ListNodeProfitDetailsRsp, error) //perm:web,admin
	// FreeUpDiskSpace  Request to free up disk space, returns free hashes and next time
	FreeUpDiskSpace(ctx context.Context, nodeID string, size int64) (*types.FreeUpDiskResp, error) //perm:edge,candidate,admin
	// GetNextFreeTime returns the next free up time
	GetNextFreeTime(ctx context.Context, nodeID string) (int64, error) //perm:edge,candidate,admin
	// UpdateNodeDynamicInfo
	UpdateNodeDynamicInfo(ctx context.Context, info *types.NodeDynamicInfo) error //perm:admin
	// ReDetermineNodeNATType
	ReDetermineNodeNATType(ctx context.Context, nodeID string) error //perm:admin,web,locator
	// AssignTunserverURL
	AssignTunserverURL(ctx context.Context) (*types.TunserverRsp, error) //perm:edge
	// UpdateTunserverURL
	UpdateTunserverURL(ctx context.Context, nodeID string) error //perm:edge
	// SetTunserverURL
	SetTunserverURL(ctx context.Context, nodeID, wsNodeID string) error //perm:admin,web,locator
	// ReimburseNodeProfit
	ReimburseNodeProfit(ctx context.Context, nodeID, note string, profit float64) error //perm:admin,web,locator
	// GetTunserverURLFromUser
	GetTunserverURLFromUser(ctx context.Context, req *types.TunserverReq) (*types.TunserverRsp, error) //perm:admin,web,locator
	// CreateTunnel create tunnel for workerd communication
	CreateTunnel(ctx context.Context, req *types.CreateTunnelReq) error // perm:candidate
}

// UserAPI is an interface for user
type UserAPI interface {
	// // UserAPIKeysExists checks if the user api key exists.
	// UserAPIKeysExists(ctx context.Context, userID string) error //perm:web

	// // User-related methods
	// // AllocateStorage allocates storage space.
	// AllocateStorage(ctx context.Context, userID string) (*types.UserInfo, error) //perm:web,admin
	// // GetUserInfo get user info
	// GetUserInfo(ctx context.Context, userID string) (*types.UserInfo, error) // perm:web,admin
	// // GetUserInfos get user infos
	// GetUserInfos(ctx context.Context, userIDs []string) (map[string]*types.UserInfo, error) // perm:web,admin
	// // CreateAPIKey creates a key for the client API.
	// CreateAPIKey(ctx context.Context, userID, keyName string, acl []types.UserAccessControl) (string, error) //perm:web,admin
	// // GetAPIKeys get all api key for user.
	// GetAPIKeys(ctx context.Context, userID string) (map[string]types.UserAPIKeysInfo, error) //perm:web,admin
	// // DeleteAPIKey delete a api key for user
	// DeleteAPIKey(ctx context.Context, userID, name string) error //perm:web,admin
	// UserAssetDownloadResult After a user downloads a resource from a candidate node, the candidate node reports the download result
	UserAssetDownloadResult(ctx context.Context, userID, cid string, totalTraffic, peakBandwidth int64) error //perm:candidate

	// // SetUserVIP set user vip state
	// SetUserVIP(ctx context.Context, userID string, enableVIP bool) error //perm:admin
	// // GetUserAccessToken get access token for user
	// GetUserAccessToken(ctx context.Context, userID string) (string, error) //perm:web,admin
	// // GetUserStorageStats
	// GetUserStorageStats(ctx context.Context, userID string) (*types.StorageStats, error) //perm:web,admin
	// // GetUsersStorageStatistics
	// ListUserStorageStats(ctx context.Context, limit, offset int) (*types.ListStorageStatsRsp, error) //perm:web,admin

	// // CreateAssetGroup create Asset group
	// CreateAssetGroup(ctx context.Context, userID, name string, parent int) (*types.AssetGroup, error) //perm:user,web,admin
	// // ListAssetGroup list Asset group
	// ListAssetGroup(ctx context.Context, userID string, parent, limit, offset int) (*types.ListAssetGroupRsp, error) //perm:user,web,admin
	// // ListAssetSummary list Asset and group
	// ListAssetSummary(ctx context.Context, userID string, parent, limit, offset int) (*types.ListAssetSummaryRsp, error) //perm:user,web,admin
	// // DeleteAssetGroup delete Asset group
	// DeleteAssetGroup(ctx context.Context, userID string, gid int) error //perm:user,web,admin
	// // RenameAssetGroup rename group
	// RenameAssetGroup(ctx context.Context, userID, newName string, groupID int) error //perm:user,web,admin
	// // MoveAssetToGroup move a asset to group
	// MoveAssetToGroup(ctx context.Context, userID, cid string, groupID int) error //perm:user,web,admin
	// // MoveAssetGroup move a asset group
	// MoveAssetGroup(ctx context.Context, userID string, groupID, targetGroupID int) error //perm:user,web,admin
	// // GetAPPKeyPermissions get the permissions of user app key
	// GetAPPKeyPermissions(ctx context.Context, userID, keyName string) ([]string, error) //perm:user,web,admin
	// GetNodeUploadInfo
	GetNodeUploadInfo(ctx context.Context, userID string) (*types.UploadInfo, error) //perm:user,web,admin
}

// ProjectAPI is an interface for project
type ProjectAPI interface {
	// RedeployFailedProjects retries the pull process for a list of failed assets
	RedeployFailedProjects(ctx context.Context, ids []string) error //perm:admin
	// UpdateProjectStatus
	UpdateProjectStatus(ctx context.Context, list []*types.Project) error //perm:edge,candidate
	// GetProjectsForNode
	GetProjectsForNode(ctx context.Context, nodeID string) ([]*types.ProjectReplicas, error) //perm:edge,candidate,web,locator

	DeployProject(ctx context.Context, req *types.DeployProjectReq) error                              //perm:user,web,admin
	DeleteProject(ctx context.Context, req *types.ProjectReq) error                                    //perm:user,web,admin
	UpdateProject(ctx context.Context, req *types.ProjectReq) error                                    //perm:user,web,admin
	GetProjectInfo(ctx context.Context, uuid string) (*types.ProjectInfo, error)                       //perm:user,web,admin
	GetProjectInfos(ctx context.Context, user string, limit, offset int) ([]*types.ProjectInfo, error) //perm:user,web,admin
}

// Scheduler is an interface for scheduler
type Scheduler interface {
	Common
	AssetAPI
	NodeAPI
	UserAPI
	ProjectAPI
	ContainerAPI

	// NodeValidationResult processes the validation result for a node
	NodeValidationResult(ctx context.Context, r io.Reader, sign string) error //perm:edge,candidate
	// GetValidationResults retrieves a list of validation results with pagination using the specified node, page number, and page size
	GetValidationResults(ctx context.Context, nodeID string, limit, offset int) (*types.ListValidationResultRsp, error) //perm:web,admin
	// SubmitProjectReport
	SubmitProjectReport(ctx context.Context, req *types.ProjectRecordReq) error //perm:candidate
	// SubmitWorkloadReport
	SubmitWorkloadReport(ctx context.Context, workload *types.WorkloadRecordReq) error //perm:default
	// SubmitWorkloadReportV2
	SubmitWorkloadReportV2(ctx context.Context, workload *types.WorkloadRecordReq) error //perm:default
	// GetWorkloadRecords retrieves a list of workload results with pagination using the specified limit, offset, and node
	GetWorkloadRecords(ctx context.Context, nodeID string, limit, offset int) (*types.ListWorkloadRecordRsp, error) //perm:web,admin
	// GetWorkloadRecord retrieves a list of workload results with pagination using the specified limit, offset, and node
	GetWorkloadRecord(ctx context.Context, id string) (*types.WorkloadRecord, error) //perm:web,admin
	// GetWorkloadRecord retrieves result with tokenID
	// GetWorkloadRecord(ctx context.Context, tokenID string) (*types.WorkloadRecord, error) //perm:web,admin
	// GetRetrieveEventRecords retrieves a list of retrieve event with pagination using the specified limit, offset, and node
	GetRetrieveEventRecords(ctx context.Context, nodeID string, limit, offset int) (*types.ListRetrieveEventRsp, error) //perm:web,admin

	// Server-related methods
	// GetSchedulerPublicKey retrieves the scheduler's public key in PEM format
	GetSchedulerPublicKey(ctx context.Context) (string, error) //perm:edge,candidate
	// GetNodePublicKey retrieves the node's public key in PEM format
	GetNodePublicKey(ctx context.Context, nodeID string) (string, error) //perm:web,admin
	// TriggerElection starts a new election process
	TriggerElection(ctx context.Context) error //perm:admin
	// GetEdgeUpdateConfigs retrieves edge update configurations for different node types
	GetEdgeUpdateConfigs(ctx context.Context) (map[int]*EdgeUpdateConfig, error) //perm:edge
	// SetEdgeUpdateConfig updates the edge update configuration for a specific node type with the provided information
	SetEdgeUpdateConfig(ctx context.Context, info *EdgeUpdateConfig) error //perm:admin
	// DeleteEdgeUpdateConfig deletes the edge update configuration for the specified node type
	DeleteEdgeUpdateConfig(ctx context.Context, nodeType int) error //perm:admin

	// code
	// GenerateCandidateCodes
	GenerateCandidateCodes(ctx context.Context, count int, nodeType types.NodeType, isTest bool) ([]string, error) //perm:admin
	// CandidateCodeExist
	CandidateCodeExist(ctx context.Context, code string) (bool, error) //perm:admin,web,locator
	// GetCandidateCodeInfos
	GetCandidateCodeInfos(ctx context.Context, nodeID, code string) ([]*types.CandidateCodeInfo, error) //perm:admin,web,locator
	ResetCandidateCode(ctx context.Context, nodeID, code string) error                                  //perm:admin,web,locator
	RemoveCandidateCode(ctx context.Context, code string) error                                         //perm:admin,web,locator
}
