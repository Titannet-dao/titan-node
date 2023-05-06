package scheduler

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"database/sql"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/scheduler/nat"
	"github.com/Filecoin-Titan/titan/node/scheduler/validation"
	"github.com/google/uuid"

	"go.uber.org/fx"

	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/scheduler/assets"
	"github.com/gbrlsnchs/jwt/v3"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/common"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/filecoin-project/go-jsonrpc/auth"
	logging "github.com/ipfs/go-log/v2"

	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/Filecoin-Titan/titan/node/scheduler/sync"
	"golang.org/x/xerrors"
)

var log = logging.Logger("scheduler")

// Scheduler represents a scheduler node in a distributed system.
type Scheduler struct {
	fx.In

	*common.CommonAPI
	*EdgeUpdateManager
	dtypes.ServerID

	NodeManager            *node.Manager
	ValidationMgr          *validation.Manager
	AssetManager           *assets.Manager
	NatManager             *nat.Manager
	DataSync               *sync.DataSync
	SchedulerCfg           *config.SchedulerCfg
	SetSchedulerConfigFunc dtypes.SetSchedulerConfigFunc
	GetSchedulerConfigFunc dtypes.GetSchedulerConfigFunc

	PrivateKey *rsa.PrivateKey
}

var _ api.Scheduler = &Scheduler{}

type jwtPayload struct {
	Allow  []auth.Permission
	NodeID string
}

// VerifyNodeAuthToken verifies the JWT token for a node.
func (s *Scheduler) VerifyNodeAuthToken(ctx context.Context, token string) ([]auth.Permission, error) {
	nodeID := handler.GetNodeID(ctx)

	var payload jwtPayload
	if _, err := jwt.Verify([]byte(token), s.APISecret, &payload); err != nil {
		return nil, xerrors.Errorf("node:%s JWT Verify failed: %w", nodeID, err)
	}

	if payload.NodeID != nodeID {
		return nil, xerrors.Errorf("node id %s not match", nodeID)
	}

	return payload.Allow, nil
}

// NodeLogin creates a new JWT token for a node.
func (s *Scheduler) NodeLogin(ctx context.Context, nodeID, sign string) (string, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	oldNode := s.NodeManager.GetNode(nodeID)
	if oldNode != nil {
		oAddr := oldNode.RemoteAddr()
		if oAddr != remoteAddr {
			return "", xerrors.Errorf("node already login, addr : %s", oAddr)
		}
	}

	pem, err := s.NodeManager.LoadNodePublicKey(nodeID)
	if err != nil {
		return "", xerrors.Errorf("%s load node public key failed: %w", nodeID, err)
	}

	nType, err := s.NodeManager.LoadNodeType(nodeID)
	if err != nil {
		return "", xerrors.Errorf("%s load node type failed: %w", nodeID, err)
	}

	publicKey, err := titanrsa.Pem2PublicKey([]byte(pem))
	if err != nil {
		return "", err
	}

	signBuf, err := hex.DecodeString(sign)
	if err != nil {
		return "", err
	}

	rsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	err = rsa.VerifySign(publicKey, signBuf, []byte(nodeID))
	if err != nil {
		return "", err
	}

	p := jwtPayload{
		NodeID: nodeID,
	}

	if nType == types.NodeEdge {
		p.Allow = append(p.Allow, api.RoleEdge)
	} else if nType == types.NodeCandidate {
		p.Allow = append(p.Allow, api.RoleCandidate)
	} else {
		return "", xerrors.Errorf("Node type mismatch [%d]", nType)
	}

	tk, err := jwt.Sign(&p, s.APISecret)
	if err != nil {
		return "", xerrors.Errorf("node %s sign err:%s", nodeID, err.Error())
	}

	return string(tk), nil
}

// nodeConnect processes a node connect request with the given options and node type.
func (s *Scheduler) nodeConnect(ctx context.Context, opts *types.ConnectOptions, nodeType types.NodeType) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	nodeID := handler.GetNodeID(ctx)

	alreadyConnect := true

	cNode := s.NodeManager.GetNode(nodeID)
	if cNode == nil {
		if err := s.NodeManager.NodeExists(nodeID, nodeType); err != nil {
			return xerrors.Errorf("node: %s, type: %d, error: %w", nodeID, nodeType, err)
		}
		cNode = node.New()
		alreadyConnect = false
	}
	cNode.SetToken(opts.Token)

	log.Infof("node connected %s, address:%s", nodeID, remoteAddr)

	err := cNode.ConnectRPC(remoteAddr, nodeType)
	if err != nil {
		return xerrors.Errorf("nodeConnect ConnectRPC err:%s", err.Error())
	}

	if !alreadyConnect {
		// init node info
		nodeInfo, err := cNode.API.GetNodeInfo(ctx)
		if err != nil {
			log.Errorf("nodeConnect NodeInfo err:%s", err.Error())
			return err
		}

		if nodeID != nodeInfo.NodeID {
			return xerrors.Errorf("nodeID mismatch %s, %s", nodeID, nodeInfo.NodeID)
		}

		nodeInfo.Type = nodeType
		nodeInfo.SchedulerID = s.ServerID

		port, err := s.NodeManager.LoadPortMapping(nodeID)
		if err != nil && err != sql.ErrNoRows {
			return xerrors.Errorf("load node port %s err : %s", nodeID, err.Error())
		}

		pStr, err := s.NodeManager.LoadNodePublicKey(nodeID)
		if err != nil && err != sql.ErrNoRows {
			return xerrors.Errorf("load node port %s err : %s", nodeID, err.Error())
		}

		onlineDuration, err := s.NodeManager.LoadNodeOnlineDuration(nodeID)
		if err != nil && err != sql.ErrNoRows {
			return xerrors.Errorf("load node online duration %s err : %s", nodeID, err.Error())
		}

		publicKey, err := titanrsa.Pem2PublicKey([]byte(pStr))
		if err != nil {
			return xerrors.Errorf("load node port %s err : %s", nodeID, err.Error())
		}

		// init node info
		nodeInfo.OnlineDuration = onlineDuration
		nodeInfo.PortMapping = port
		nodeInfo.ExternalIP, _, err = net.SplitHostPort(remoteAddr)
		if err != nil {
			return xerrors.Errorf("SplitHostPort err:%s", err.Error())
		}

		cNode.SetPublicKey(publicKey)
		cNode.SetTCPPort(opts.TcpServerPort)
		cNode.SetRemoteAddr(remoteAddr)

		cNode.NodeInfo = &nodeInfo

		err = s.NodeManager.NodeOnline(cNode)
		if err != nil {
			log.Errorf("nodeConnect err:%s,nodeID:%s", err.Error(), nodeID)
			return err
		}
	}

	if nodeType == types.NodeEdge {
		go s.NatManager.DetermineEdgeNATType(context.Background(), nodeID)
	}

	s.DataSync.AddNodeToList(nodeID)

	return nil
}

// CandidateConnect candidate node login to the scheduler
func (s *Scheduler) CandidateConnect(ctx context.Context, opts *types.ConnectOptions) error {
	return s.nodeConnect(ctx, opts, types.NodeCandidate)
}

// EdgeConnect edge node login to the scheduler
func (s *Scheduler) EdgeConnect(ctx context.Context, opts *types.ConnectOptions) error {
	return s.nodeConnect(ctx, opts, types.NodeEdge)
}

// GetExternalAddress retrieves the external address of the caller.
func (s *Scheduler) GetExternalAddress(ctx context.Context) (string, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	return remoteAddr, nil
}

// NodeValidationResult processes the validation result for a node
func (s *Scheduler) NodeValidationResult(ctx context.Context, result api.ValidationResult, sign string) error {
	validator := handler.GetNodeID(ctx)
	node := s.NodeManager.GetNode(validator)
	if node == nil {
		return fmt.Errorf("node %s not online", validator)
	}

	signBuf, err := hex.DecodeString(sign)
	if err != nil {
		return err
	}

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err = enc.Encode(result)
	if err != nil {
		return err
	}

	rsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	err = rsa.VerifySign(node.PublicKey(), signBuf, buffer.Bytes())
	if err != nil {
		return err
	}

	vs := &result
	vs.Validator = validator

	s.ValidationMgr.HandleResult(vs)

	return nil
}

// RegisterNode adds a new node to the scheduler with the specified node ID, public key, and node type
func (s *Scheduler) RegisterNode(ctx context.Context, pKey string, nodeType types.NodeType) (nodeID string, err error) {
	nodeID, err = s.NodeManager.NewNodeID(nodeType)
	if err != nil {
		return
	}

	err = s.NodeManager.SaveNodeRegisterInfo(pKey, nodeID, nodeType)
	return
}

// UnregisterNode removes a node from the scheduler with the specified node ID
func (s *Scheduler) UnregisterNode(ctx context.Context, nodeID string) error {
	return s.db.DeleteNodeInfo(nodeID)
}

// GetOnlineNodeCount returns the count of online nodes for a given node type
func (s *Scheduler) GetOnlineNodeCount(ctx context.Context, nodeType types.NodeType) (int, error) {
	if nodeType == types.NodeValidator {
		list, err := s.NodeManager.LoadValidators(s.ServerID)
		if err != nil {
			return 0, err
		}

		i := 0
		for _, nodeID := range list {
			node := s.NodeManager.GetCandidateNode(nodeID)
			if node != nil {
				i++
			}
		}

		return i, nil
	}

	return s.NodeManager.GetOnlineNodeCount(nodeType), nil
}

// TriggerElection triggers a single election for validators.
func (s *Scheduler) TriggerElection(ctx context.Context) error {
	s.ValidationMgr.StartElection()
	return nil
}

// GetNodeInfo returns information about the specified node.
func (s *Scheduler) GetNodeInfo(ctx context.Context, nodeID string) (types.NodeInfo, error) {
	nodeInfo := types.NodeInfo{}
	nodeInfo.IsOnline = false

	info := s.NodeManager.GetNode(nodeID)
	if info != nil {
		nodeInfo = *info.NodeInfo
		nodeInfo.IsOnline = true
	} else {
		dbInfo, err := s.NodeManager.LoadNodeInfo(nodeID)
		if err != nil {
			log.Errorf("getNodeInfo: %s ,nodeID : %s", err.Error(), nodeID)
			return types.NodeInfo{}, err
		}

		nodeInfo = *dbInfo
	}

	return nodeInfo, nil
}

// UpdateNodePort sets the port for the specified node.
func (s *Scheduler) UpdateNodePort(ctx context.Context, nodeID, port string) error {
	baseInfo := s.NodeManager.GetNode(nodeID)
	if baseInfo != nil {
		baseInfo.UpdateNodePort(port)
	}

	return s.NodeManager.UpdatePortMapping(nodeID, port)
}

// NodeExists checks if the node with the specified ID exists.
func (s *Scheduler) NodeExists(ctx context.Context, nodeID string) error {
	if err := s.NodeManager.NodeExists(nodeID, types.NodeEdge); err != nil {
		return s.NodeManager.NodeExists(nodeID, types.NodeCandidate)
	}

	return nil
}

// GetNodeList retrieves a list of nodes with pagination.
func (s *Scheduler) GetNodeList(ctx context.Context, offset int, limit int) (*types.ListNodesRsp, error) {
	rsp := &types.ListNodesRsp{Data: make([]types.NodeInfo, 0)}

	rows, total, err := s.NodeManager.LoadNodeInfos(limit, offset)
	if err != nil {
		return rsp, err
	}
	defer rows.Close()

	validator := make(map[string]struct{})
	validatorList, err := s.NodeManager.LoadValidators(s.NodeManager.ServerID)
	if err != nil {
		log.Errorf("get validator list: %v", err)
	}
	for _, id := range validatorList {
		validator[id] = struct{}{}
	}

	nodeInfos := make([]types.NodeInfo, 0)
	for rows.Next() {
		nodeInfo := &types.NodeInfo{}
		err = rows.StructScan(nodeInfo)
		if err != nil {
			log.Errorf("NodeInfo StructScan err: %s", err.Error())
			continue
		}

		_, exist := validator[nodeInfo.NodeID]
		if exist {
			nodeInfo.Type = types.NodeValidator
		}

		nInfo := s.NodeManager.GetNode(nodeInfo.NodeID)
		if nInfo != nil {
			nodeInfo.IsOnline = true
			nodeInfo.ExternalIP = nInfo.ExternalIP
			nodeInfo.InternalIP = nInfo.InternalIP
		}

		nodeInfos = append(nodeInfos, *nodeInfo)
	}

	rsp.Data = nodeInfos
	rsp.Total = total

	return rsp, nil
}

// GetValidationResults retrieves a list of validation results.
func (s *Scheduler) GetValidationResults(ctx context.Context, startTime, endTime time.Time, pageNumber, pageSize int) (*types.ListValidationResultRsp, error) {
	svm, err := s.NodeManager.LoadValidationResultInfos(startTime, endTime, pageNumber, pageSize)
	if err != nil {
		return nil, err
	}

	return svm, nil
}

// GetSchedulerPublicKey get server publicKey
func (s *Scheduler) GetSchedulerPublicKey(ctx context.Context) (string, error) {
	if s.PrivateKey == nil {
		return "", fmt.Errorf("scheduler private key not exist")
	}

	publicKey := s.PrivateKey.PublicKey
	pem := titanrsa.PublicKey2Pem(&publicKey)
	return string(pem), nil
}

// GetCandidateDownloadInfos finds candidate download info for the given CID.
func (s *Scheduler) GetCandidateDownloadInfos(ctx context.Context, cid string) ([]*types.CandidateDownloadInfo, error) {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sources := make([]*types.CandidateDownloadInfo, 0)

	rows, err := s.NodeManager.LoadReplicasByHash(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tkPayloads := make([]*types.TokenPayload, 0)

	for rows.Next() {
		rInfo := &types.ReplicaInfo{}
		err = rows.StructScan(rInfo)
		if err != nil {
			log.Errorf("replica StructScan err: %s", err.Error())
			continue
		}

		if !rInfo.IsCandidate {
			continue
		}

		nodeID := rInfo.NodeID
		cNode := s.NodeManager.GetCandidateNode(nodeID)
		if cNode == nil {
			continue
		}

		token, tkPayload, err := cNode.Token(cid, titanRsa, s.NodeManager.PrivateKey)
		if err != nil {
			continue
		}
		tkPayloads = append(tkPayloads, tkPayload)

		source := &types.CandidateDownloadInfo{
			URL: cNode.DownloadAddr(),
			Tk:  token,
		}

		sources = append(sources, source)
	}

	if len(tkPayloads) > 0 {
		if err = s.NodeManager.SaveTokenPayload(tkPayloads); err != nil {
			return nil, err
		}
	}

	return sources, nil
}

// GetAssetListForBucket retrieves a list of asset hashes for the specified node's bucket.
func (s *Scheduler) GetAssetListForBucket(ctx context.Context, bucketID uint32) ([]string, error) {
	nodeID := handler.GetNodeID(ctx)
	id := fmt.Sprintf("%s:%d", nodeID, bucketID)
	hashBytes, err := s.NodeManager.LoadBucket(id)
	if err != nil {
		return nil, err
	}

	if len(hashBytes) == 0 {
		return make([]string, 0), nil
	}

	buffer := bytes.NewBuffer(hashBytes)
	dec := gob.NewDecoder(buffer)

	out := make([]string, 0)
	if err = dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetValidationInfo  get information related to validation and election
func (s *Scheduler) GetValidationInfo(ctx context.Context) (*types.ValidationInfo, error) {
	eTime := s.ValidationMgr.GetNextElectionTime()

	return &types.ValidationInfo{
		NextElectionTime: eTime,
	}, nil
}

// GetEdgeExternalServiceAddress returns the external service address of an edge node
func (s *Scheduler) GetEdgeExternalServiceAddress(ctx context.Context, nodeID, candidateURL string) (string, error) {
	eNode := s.NodeManager.GetEdgeNode(nodeID)
	if eNode != nil {
		return eNode.ExternalServiceAddress(ctx, candidateURL)
	}

	return "", fmt.Errorf("node %s offline or not exist", nodeID)
}

// NatPunch performs NAT traversal
func (s *Scheduler) NatPunch(ctx context.Context, target *types.NatPunchReq) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	sourceURL := fmt.Sprintf("https://%s/ping", remoteAddr)

	eNode := s.NodeManager.GetEdgeNode(target.NodeID)
	if eNode == nil {
		return xerrors.Errorf("edge %n not exist", target.NodeID)
	}

	return eNode.UserNATPunch(context.Background(), sourceURL, target)
}

func (s *Scheduler) GetCandidateURLsForDetectNat(ctx context.Context) ([]string, error) {
	return s.NatManager.GetCandidateURLsForDetectNat(ctx)
}

// NodeKeepalive candidate and edge keepalive
func (s *Scheduler) NodeKeepalive(ctx context.Context) (uuid.UUID, error) {
	uuid, err := s.CommonAPI.Session(ctx)

	remoteAddr := handler.GetRemoteAddr(ctx)
	nodeID := handler.GetNodeID(ctx)
	if nodeID != "" && remoteAddr != "" {
		lastTime := time.Now()

		node := s.NodeManager.GetNode(nodeID)
		if node != nil {
			node.SetLastRequestTime(lastTime)

			if remoteAddr != node.RemoteAddr() {
				return uuid, xerrors.New("remoteAddr inconsistent")
			}
		}
	}

	return uuid, err
}
