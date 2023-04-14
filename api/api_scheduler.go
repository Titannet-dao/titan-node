package api

import (
	"context"
	"time"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/Filecoin-Titan/titan/api/types"
)

// Scheduler is an interface for scheduler
type Scheduler interface {
	Common

	// Node-related methods
	// GetOnlineNodeCount returns the count of online nodes for a given node type
	GetOnlineNodeCount(ctx context.Context, nodeType types.NodeType) (int, error) //perm:read
	// RegisterNode adds a new node to the scheduler with the specified public key, and node type , and returns node id
	RegisterNode(ctx context.Context, publicKey string, nodeType types.NodeType) (string, error) //perm:admin
	// UnregisterNode removes a node from the scheduler with the specified node ID
	UnregisterNode(ctx context.Context, nodeID string) error //perm:admin
	// UpdateNodePort updates the port for the node with the specified node
	UpdateNodePort(ctx context.Context, nodeID, port string) error //perm:admin
	// EdgeConnect edge node login to the scheduler
	EdgeConnect(ctx context.Context, opts *types.ConnectOptions) error //perm:write
	// NodeValidationResult processes the validation result for a node
	NodeValidationResult(ctx context.Context, vr ValidationResult) error //perm:write
	// CandidateConnect candidate node login to the scheduler
	CandidateConnect(ctx context.Context, opts *types.ConnectOptions) error //perm:write
	// NodeRemoveAssetResult the result of an asset removal operation
	NodeRemoveAssetResult(ctx context.Context, resultInfo types.RemoveAssetResult) error //perm:write
	// GetExternalAddress retrieves the external address of the caller.
	GetExternalAddress(ctx context.Context) (string, error) //perm:read
	// VerifyNodeAuthToken checks the authenticity of a node's authentication token and returns the associated permissions
	VerifyNodeAuthToken(ctx context.Context, token string) ([]auth.Permission, error) //perm:read
	// NodeLogin generates an authentication token for a node with the specified node ID and signature
	NodeLogin(ctx context.Context, nodeID, sign string) (string, error) //perm:read
	// GetNodeInfo get information for node
	GetNodeInfo(ctx context.Context, nodeID string) (types.NodeInfo, error) //perm:read
	// GetNodeList retrieves a list of nodes with pagination using the specified cursor and count
	GetNodeList(ctx context.Context, cursor int, count int) (*types.ListNodesRsp, error) //perm:read
	// GetAssetListForBucket retrieves a list of asset CIDs for a bucket associated with the specified bucket ID (bucketID is 'nodeID + bucketNumber')
	GetAssetListForBucket(ctx context.Context, bucketID string) ([]string, error) //perm:write
	// GetEdgeExternalServiceAddress nat travel, get edge external addr with different scheduler
	GetEdgeExternalServiceAddress(ctx context.Context, nodeID, schedulerURL string) (string, error) //perm:write
	// GetNodeNATType returns the NAT type for a node with the specified node
	GetNodeNATType(ctx context.Context, nodeID string) (types.NatType, error) //perm:write
	// NatPunch between user and node
	NatPunch(ctx context.Context, target *types.NatPunchReq) error //perm:read
	// CheckNetworkConnectivity check tcp or udp network connectivity , network is "tcp" or "udp"
	CheckNetworkConnectivity(ctx context.Context, network, targetURL string) error //perm:read
	// GetEdgeDownloadInfos retrieves download information for the edge with the asset with the specified CID.
	GetEdgeDownloadInfos(ctx context.Context, cid string) (*types.EdgeDownloadInfoList, error) //perm:read
	// GetCandidateDownloadInfos retrieves download information for the candidate with the asset with the specified CID.
	GetCandidateDownloadInfos(ctx context.Context, cid string) ([]*types.CandidateDownloadInfo, error) //perm:read
	// NodeExists checks if the node with the specified ID exists.
	NodeExists(ctx context.Context, nodeID string) error //perm:write

	// Asset-related methods
	// PullAsset Pull an asset based on the provided PullAssetReq structure.
	PullAsset(ctx context.Context, info *types.PullAssetReq) error //perm:admin
	// RemoveAssetRecord removes the asset record with the specified CID from the scheduler
	RemoveAssetRecord(ctx context.Context, cid string) error //perm:admin
	// RemoveAssetReplica deletes an asset replica with the specified CID and node from the scheduler
	RemoveAssetReplica(ctx context.Context, cid, nodeID string) error //perm:admin
	// GetAssetRecord retrieves the asset record with the specified CID
	GetAssetRecord(ctx context.Context, cid string) (*types.AssetRecord, error) //perm:read
	// GetAssetRecords retrieves a list of asset records with pagination using the specified limit, offset, and states
	GetAssetRecords(ctx context.Context, limit, offset int, states []string) ([]*types.AssetRecord, error) //perm:read
	// RePullFailedAssets retries the pull process for a list of failed assets
	RePullFailedAssets(ctx context.Context, hashes []types.AssetHash) error //perm:admin
	// UpdateAssetExpiration updates the expiration time for an asset with the specified CID
	UpdateAssetExpiration(ctx context.Context, cid string, time time.Time) error //perm:admin
	// GetAssetReplicaInfos retrieves a list of asset replica information using the specified request parameters
	GetAssetReplicaInfos(ctx context.Context, req types.ListReplicaInfosReq) (*types.ListReplicaInfosRsp, error) //perm:read
	// GetValidationResults retrieves a list of validation results with pagination using the specified time range, page number, and page size
	GetValidationResults(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*types.ListValidationResultRsp, error) //perm:read
	// SubmitUserProofsOfWork submits Proof of Work for User Asset Download
	SubmitUserProofsOfWork(ctx context.Context, proofs []*types.UserProofOfWork) error //perm:read

	// Server-related methods
	// GetSchedulerPublicKey retrieves the scheduler's public key in PEM format
	GetSchedulerPublicKey(ctx context.Context) (string, error) //perm:write
	// TriggerElection starts a new election process
	TriggerElection(ctx context.Context) error //perm:admin
	// GetEdgeUpdateConfigs retrieves edge update configurations for different node types
	GetEdgeUpdateConfigs(ctx context.Context) (map[int]*EdgeUpdateConfig, error) //perm:read
	// SetEdgeUpdateConfig updates the edge update configuration for a specific node type with the provided information
	SetEdgeUpdateConfig(ctx context.Context, info *EdgeUpdateConfig) error //perm:admin
	// DeleteEdgeUpdateConfig deletes the edge update configuration for the specified node type
	DeleteEdgeUpdateConfig(ctx context.Context, nodeType int) error //perm:admin
}
