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
	// RemoveAssetRecords removes the asset record with the specified CID from the scheduler
	RemoveAssetRecords(ctx context.Context, cids []string) error //perm:admin,web
	// StopAssetRecord stop asset
	StopAssetRecord(ctx context.Context, cids []string) error //perm:admin
	// RemoveAssetReplica deletes an asset replica with the specified CID and node from the scheduler
	RemoveAssetReplica(ctx context.Context, cid, nodeID string) error //perm:admin
	// GetAssetRecord retrieves the asset record with the specified CID
	GetAssetRecord(ctx context.Context, cid string) (*types.AssetRecord, error) //perm:web,admin
	// GetAssetRecords retrieves a list of asset records with pagination using the specified limit, offset, and states
	GetAssetRecords(ctx context.Context, limit, offset int, states []string, serverID dtypes.ServerID) ([]*types.AssetRecord, error) //perm:web,admin
	// GetAssetRecordsWithCIDs retrieves a list of asset records with cid
	GetAssetRecordsWithCIDs(ctx context.Context, cids []string) ([]*types.AssetRecord, error) //perm:web,admin
	// GetReplicas retrieves a list of asset replicas with pagination using the specified limit, offset
	GetReplicas(ctx context.Context, cid string, limit, offset int) (*types.ListReplicaRsp, error) //perm:web,admin
	// RePullFailedAssets retries the pull process for a list of failed assets
	RePullFailedAssets(ctx context.Context, hashes []types.AssetHash) error //perm:admin
	// UpdateAssetExpiration updates the expiration time for an asset with the specified CID
	UpdateAssetExpiration(ctx context.Context, cid string, time time.Time) error //perm:web,admin
	// ResetAssetReplicaCount updates the replica count for an asset with the specified CID
	ResetAssetReplicaCount(ctx context.Context, cid string, count int) error //perm:web,admin
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
	GetReplicaEventsForNode(ctx context.Context, nodeID string, limit, offset int) (*types.ListAssetReplicaEventRsp, error) //perm:web,admin
	// GetAssetDownloadResults retrieves a asset download list
	GetAssetDownloadResults(ctx context.Context, hash string, start, end time.Time) (*types.ListAssetDownloadRsp, error) //perm:web,admin
	// GetDownloadResultsFromAssets
	GetDownloadResultsFromAssets(ctx context.Context, hashes []string, start, end time.Time) ([]*types.AssetDownloadResultRsp, error) //perm:web,admin
	// CreateAsset creates an asset with car CID, car name, and car size.
	CreateAsset(ctx context.Context, req *types.CreateAssetReq) (*types.UploadInfo, error) //perm:web,admin,user
	// CreateSyncAsset Synchronizing assets from other schedulers
	CreateSyncAsset(ctx context.Context, req *types.CreateSyncAssetReq) error //perm:web,admin,user
	// GenerateTokenForDownloadSource Generate Token For Download Source
	GenerateTokenForDownloadSource(ctx context.Context, nodeID string, cid string) (*types.SourceDownloadInfo, error) //perm:web,admin,user
	// GenerateTokenForDownloadSources Generate Token For Download Source
	GenerateTokenForDownloadSources(ctx context.Context, cid string) ([]*types.SourceDownloadInfo, error) //perm:web,admin,user
	// ShareAssets shares the assets of the user.
	// ShareAssets(ctx context.Context, userID string, assetCID []string, expireTime time.Time) (map[string][]string, error) //perm:web,admin,user
	// ShareEncryptedAsset shares the encrypted file
	// ShareEncryptedAsset(ctx context.Context, userID, assetCID, filePass string, expireTime time.Time) ([]string, error) // perm:web,admin,user
	// ShareAssetV2 shares the assets of the user.
	ShareAssetV2(ctx context.Context, info *types.ShareAssetReq) (*types.ShareAssetRsp, error) //perm:web,admin,user
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
	// GetActiveAssetRecords
	GetActiveAssetRecords(ctx context.Context, offset int, limit int) (*types.ListAssetRecordRsp, error) //perm:web,admin
	// GetAssetRecordsByDateRange
	GetAssetRecordsByDateRange(ctx context.Context, offset int, limit int, start time.Time, end time.Time) (*types.ListAssetRecordRsp, error) //perm:web,admin
	// GetSucceededReplicaByCID
	GetSucceededReplicaByCID(ctx context.Context, cid string, limit, offset int) (*types.ListReplicaRsp, error) //perm:web,admin
	// GetFailedReplicaByCID
	GetFailedReplicaByCID(ctx context.Context, cid string, limit, offset int) (*types.ListAssetReplicaEventRsp, error) //perm:web,admin
	// GetSucceededReplicaByNode
	GetSucceededReplicaByNode(ctx context.Context, nodeID string, limit, offset int) (*types.ListReplicaRsp, error) //perm:web,admin
	// GetFailedReplicaByNode
	GetFailedReplicaByNode(ctx context.Context, nodeID string, limit, offset int) (*types.ListAssetReplicaEventRsp, error) //perm:web,admin
	// GetNodeAssetReplicasByHashes
	GetNodeAssetReplicasByHashes(ctx context.Context, nodeID string, hashes []string) ([]*types.ReplicaInfo, error) //perm:web,admin
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
	// RegisterNodeV2 adds new edge node to the scheduler
	RegisterNodeV2(ctx context.Context, info types.NodeRegister) (*types.ActivationDetail, error) //perm:default
	// DeactivateNode is used to deactivate a node in the titan server.
	// It stops the node from serving any requests and marks it as inactive.
	// - nodeID: The ID of the node to deactivate.
	// - hours: The deactivation countdown time in hours. It specifies the duration
	// before the deactivation is executed. If the deactivation is canceled within
	// this period, the node will remain active.
	DeactivateNode(ctx context.Context, nodeID string, hours int) error //perm:web,admin,candidate
	// ForceNodeOffline changes the online status of a node identified by nodeID.
	// If the status is true, it forces the node offline. If the status is false, it brings the node back online.
	ForceNodeOffline(ctx context.Context, nodeID string, forceOffline bool) error //perm:web,admin
	// MigrateNodeOut Migrate out the node
	MigrateNodeOut(ctx context.Context, nodeID string) (*types.NodeMigrateInfo, error) //perm:web,admin
	// MigrateNodeIn Migrate in the node
	MigrateNodeIn(ctx context.Context, info *types.NodeMigrateInfo) error //perm:web,admin
	// CleanupNode removes residual data from the source server after a node has been migrated.
	CleanupNode(ctx context.Context, nodeID, key string) error //perm:web,admin
	// CalculateExitProfit please use CalculateDowntimePenalty
	CalculateExitProfit(ctx context.Context, nodeID string) (types.ExitProfitRsp, error) //perm:web,admin,candidate
	// CalculateDowntimePenalty calculates the penalty points to be deducted for a node's requested downtime.
	CalculateDowntimePenalty(ctx context.Context, nodeID string) (types.ExitProfitRsp, error) //perm:web,admin,candidate
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
	// L3Connect l3 node login to the scheduler
	L3Connect(ctx context.Context, opts *types.ConnectOptions) error //perm:edge
	// GetExternalAddress retrieves the external address of the caller.
	GetExternalAddress(ctx context.Context) (string, error) //perm:default
	// NodeLogin generates an authentication token for a node with the specified node ID and signature
	NodeLogin(ctx context.Context, nodeID, sign string) (string, error) //perm:default
	// GetNodeInfo get information for node
	GetNodeInfo(ctx context.Context, nodeID string) (*types.NodeInfo, error) //perm:web,admin
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
	// GetAssetSourceDownloadInfo retrieves the download details for a specified asset.
	GetAssetSourceDownloadInfo(ctx context.Context, cid string) (*types.AssetSourceDownloadInfoRsp, error) //perm:edge,candidate,web,locator
	// NodeExists checks if the node with the specified ID exists.
	NodeExists(ctx context.Context, nodeID string) error //perm:web
	// NodeKeepalive
	NodeKeepalive(ctx context.Context) (*types.KeepaliveRsp, error) //perm:edge,candidate
	// NodeKeepaliveV2 fix the problem of NodeKeepalive, Maintaining old device connections
	NodeKeepaliveV2(ctx context.Context) (uuid.UUID, error) //perm:edge,candidate,l5
	// NodeKeepaliveV3
	NodeKeepaliveV3(ctx context.Context, req *types.KeepaliveReq) (*types.KeepaliveRsp, error) //perm:edge,candidate,l5
	// RequestActivationCodes Get the device's encrypted activation code
	RequestActivationCodes(ctx context.Context, nodeType types.NodeType, count int) ([]*types.NodeActivation, error) //perm:web,admin
	// VerifyTokenWithLimitCount verify token in limit count
	VerifyTokenWithLimitCount(ctx context.Context, token string) (*types.JWTPayload, error) //perm:edge,candidate
	// UpdateBandwidths update node bandwidthDown and bandwidthUp
	UpdateBandwidths(ctx context.Context, bandwidthDown, bandwidthUp int64) error //perm:edge,candidate
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
	// CheckIpUsage  checks if a specific IP address is present on the server.
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

	// ReDetermineNodeNATType
	ReDetermineNodeNATType(ctx context.Context, nodeID string) error //perm:admin,web,locator
	// SetTunserverURL
	SetTunserverURL(ctx context.Context, nodeID, wsNodeID string) error //perm:admin,web,locator
	// RecompenseNodeProfit
	RecompenseNodeProfit(ctx context.Context, nodeID, note string, profit float64) error //perm:admin,web,locator
	// GetTunserverURLFromUser
	GetTunserverURLFromUser(ctx context.Context, req *types.TunserverReq) (*types.TunserverRsp, error) //perm:admin,web,locator
	// CreateTunnel create tunnel for workerd communication
	CreateTunnel(ctx context.Context, req *types.CreateTunnelReq) error // perm:candidate
	// UserAssetDownloadResult After a user downloads a resource from a candidate node, the candidate node reports the download result
	UserAssetDownloadResultV2(ctx context.Context, info *types.RetrieveEvent) error //perm:candidate
	// LoadNodeBandwidthScores
	LoadNodeBandwidthScores(ctx context.Context, nodeID string, start, end time.Time, limit int, offset int) (*types.ListBandwidthScoreRsp, error) //perm:user,web,admin
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

	GetProjectOverviewByNode(ctx context.Context, req *types.NodeProjectReq) (*types.ListProjectOverviewRsp, error) //perm:web,admin
	GetProjectReplicasForNode(ctx context.Context, req *types.NodeProjectReq) (*types.ListProjectReplicaRsp, error) //perm:web,admin
}

// Scheduler is an interface for scheduler
type Scheduler interface {
	Common
	AssetAPI
	NodeAPI
	ProjectAPI
	// ContainerAPI

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

	// Server-related methods
	// GetSchedulerPublicKey retrieves the scheduler's public key in PEM format
	GetSchedulerPublicKey(ctx context.Context) (string, error) //perm:edge,candidate
	// GetNodePublicKey retrieves the node's public key in PEM format
	GetNodePublicKey(ctx context.Context, nodeID string) (string, error) //perm:web,admin
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

	// GetNodeUploadInfo
	GetNodeUploadInfoV2(ctx context.Context, info *types.GetUploadInfoReq) (*types.UploadInfo, error) //perm:user,web,admin

	GetValidators(ctx context.Context) ([]string, error) //perm:web,admin

	GetReplicaEvents(ctx context.Context, start, end time.Time, limit, offset int) (*types.ListAssetReplicaEventRsp, error) //perm:web,admin

	AddNodeServiceEvent(ctx context.Context, event *types.ServiceEvent) error //perm:web,admin

	// Deprecated api
	// AssignTunserverURL
	AssignTunserverURL(ctx context.Context) (*types.TunserverRsp, error)                                        //perm:edge
	GetNodeUploadInfo(ctx context.Context, userID string, pass string, urlMode bool) (*types.UploadInfo, error) //perm:user,web,admin
	UserAssetDownloadResult(ctx context.Context, userID, cid string, totalTraffic, peakBandwidth int64) error   //perm:candidate
	GetDeploymentProviderIP(ctx context.Context, id types.DeploymentID) (string, error)                         //perm:edge,candidate,web,locator,admin
}
