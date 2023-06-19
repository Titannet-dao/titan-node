package scheduler

import (
	"context"
	"crypto"
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/handler"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

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

// RegisterNode register node
func (s *Scheduler) RegisterNode(ctx context.Context, nodeID, publicKey, key string) error {
	if publicKey == "" {
		return xerrors.New("public key is nil")
	}

	_, err := titanrsa.Pem2PublicKey([]byte(publicKey))
	if err != nil {
		return xerrors.Errorf("pem to publicKey err : %s", err.Error())
	}

	activationKey, err := s.db.LoadNodeActivationKey(nodeID)
	if err != nil {
		return xerrors.Errorf("LoadNodeActivationKey err : %s", err.Error())
	}

	if activationKey != key {
		return xerrors.New("activationKey Mismatch ")
	}

	pKey, err := s.db.LoadNodePublicKey(nodeID)
	if err != nil {
		return xerrors.Errorf("LoadNodePublicKey err : %s", err.Error())
	}

	if pKey != "" {
		return xerrors.New("node public key already exists ")
	}

	return s.db.SaveNodePublicKey(publicKey, nodeID)
}

// RequestActivationCodes request node activation codes
func (s *Scheduler) RequestActivationCodes(ctx context.Context, nodeType types.NodeType, count int) ([]*types.NodeActivation, error) {
	if count < 1 {
		return nil, xerrors.New("count is 0")
	}

	areaID := s.SchedulerCfg.AreaID
	out := make([]*types.NodeActivation, 0)
	details := make([]*types.ActivationDetail, 0)

	for i := 0; i < count; i++ {
		nodeID, err := newNodeID(nodeType)
		if err != nil {
			return nil, err
		}

		detail := &types.ActivationDetail{
			NodeID:        nodeID,
			AreaID:        areaID,
			ActivationKey: newNodeKey(),
			NodeType:      nodeType,
		}

		code, err := detail.Marshal()
		if err != nil {
			return nil, err
		}

		info := &types.NodeActivation{
			NodeID:         nodeID,
			ActivationCode: code,
		}

		out = append(out, info)
		details = append(details, detail)
	}

	err := s.db.SaveNodeRegisterInfos(details)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// UnregisterNode removes a node from the scheduler with the specified node ID
func (s *Scheduler) UnregisterNode(ctx context.Context, nodeID string) error {
	// return s.db.DeleteNodeInfo(nodeID)
	return xerrors.New("Function not implemented")
}

// UpdateNodePort sets the port for the specified node.
func (s *Scheduler) UpdateNodePort(ctx context.Context, nodeID, port string) error {
	baseInfo := s.NodeManager.GetNode(nodeID)
	if baseInfo != nil {
		baseInfo.UpdateNodePort(port)
	}

	return s.NodeManager.UpdatePortMapping(nodeID, port)
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

// NodeLogin creates a new JWT token for a node.
func (s *Scheduler) NodeLogin(ctx context.Context, nodeID, sign string) (string, error) {
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

	p := types.JWTPayload{
		ID: nodeID,
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

// GetNodeInfo returns information about the specified node.
func (s *Scheduler) GetNodeInfo(ctx context.Context, nodeID string) (types.NodeInfo, error) {
	nodeInfo := types.NodeInfo{}
	nodeInfo.Status = types.NodeOffine

	dbInfo, err := s.NodeManager.LoadNodeInfo(nodeID)
	if err != nil {
		log.Errorf("getNodeInfo: %s ,nodeID : %s", err.Error(), nodeID)
		return types.NodeInfo{}, err
	}
	nodeInfo = *dbInfo

	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		nodeInfo.Status = nodeStatus(node)
		nodeInfo.NATType = node.NATType.String()
		nodeInfo.Type = node.Type
		nodeInfo.ExternalIP = node.ExternalIP
		nodeInfo.MemoryUsage = node.MemoryUsage
		nodeInfo.CPUUsage = node.CPUUsage
		nodeInfo.DiskUsage = node.DiskUsage

		log.Debugf("%s node select codes:%v", nodeID, node.SelectWeights())
	}

	return nodeInfo, nil
}

// GetNodeList retrieves a list of nodes with pagination.
func (s *Scheduler) GetNodeList(ctx context.Context, offset int, limit int) (*types.ListNodesRsp, error) {
	log.Debugf("GetNodeList start time %s", time.Now().Format("2006-01-02 15:04:05"))
	defer log.Debugf("GetNodeList end time %s", time.Now().Format("2006-01-02 15:04:05"))

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

		node := s.NodeManager.GetNode(nodeInfo.NodeID)
		if node != nil {
			nodeInfo.Status = nodeStatus(node)
			nodeInfo.ExternalIP = node.ExternalIP
			nodeInfo.NATType = node.NATType.String()
			nodeInfo.Type = node.Type
			nodeInfo.ExternalIP = node.ExternalIP
			nodeInfo.MemoryUsage = node.MemoryUsage
			nodeInfo.CPUUsage = node.CPUUsage
			nodeInfo.DiskUsage = node.DiskUsage
		}

		nodeInfos = append(nodeInfos, *nodeInfo)
	}

	rsp.Data = nodeInfos
	rsp.Total = total

	return rsp, nil
}

func (s *Scheduler) GetCandidateURLsForDetectNat(ctx context.Context) ([]string, error) {
	return s.NatManager.GetCandidateURLsForDetectNat(ctx)
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

// GetEdgeDownloadInfos finds edge download information for a given CID
func (s *Scheduler) GetEdgeDownloadInfos(ctx context.Context, cid string) (*types.EdgeDownloadInfoList, error) {
	if cid == "" {
		return nil, xerrors.New("cids is nil")
	}

	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	replicas, err := s.NodeManager.LoadReplicasByStatus(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		return nil, err
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	infos := make([]*types.EdgeDownloadInfo, 0)
	workloadRecords := make([]*types.WorkloadRecord, 0)

	for _, rInfo := range replicas {
		if rInfo.IsCandidate {
			continue
		}

		nodeID := rInfo.NodeID
		eNode := s.NodeManager.GetEdgeNode(nodeID)
		if eNode == nil {
			continue
		}

		if eNode.NATType == types.NatTypeSymmetric {
			continue
		}

		token, tkPayload, err := eNode.Token(cid, uuid.NewString(), titanRsa, s.NodeManager.PrivateKey)
		if err != nil {
			continue
		}

		workloadRecord := &types.WorkloadRecord{TokenPayload: *tkPayload, Status: types.WorkloadStatusCreate}
		workloadRecords = append(workloadRecords, workloadRecord)

		info := &types.EdgeDownloadInfo{
			Address: eNode.DownloadAddr(),
			NodeID:  nodeID,
			Tk:      token,
			NatType: eNode.NATType.String(),
		}
		infos = append(infos, info)
	}

	if len(infos) == 0 {
		return nil, nil
	}

	if len(workloadRecords) > 0 {
		if err = s.NodeManager.SaveWorkloadRecord(workloadRecords); err != nil {
			return nil, err
		}
	}

	pk, err := s.GetSchedulerPublicKey(ctx)
	if err != nil {
		return nil, err
	}

	edgeDownloadRatio := s.getEdgeDownloadRatio()
	log.Debugln("getEdgeDownloadRatio : ", edgeDownloadRatio)
	if edgeDownloadRatio >= 1 {
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].NodeID < infos[j].NodeID
		})
		edgeDownloadRatio = 1
	} else {
		// random return edge
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(infos), func(i, j int) {
			infos[i], infos[j] = infos[j], infos[i]
		})
	}

	size := int(math.Ceil(float64(len(infos)) * edgeDownloadRatio))
	infos = infos[:size]

	ret := &types.EdgeDownloadInfoList{
		Infos:        infos,
		SchedulerURL: s.SchedulerCfg.ExternalURL,
		SchedulerKey: pk,
	}

	return ret, nil
}

func (s *Scheduler) getEdgeDownloadRatio() float64 {
	return s.SchedulerCfg.EdgeDownloadRatio
}

// GetCandidateDownloadInfos finds candidate download info for the given CID.
func (s *Scheduler) GetCandidateDownloadInfos(ctx context.Context, cid string) ([]*types.CandidateDownloadInfo, error) {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sources := make([]*types.CandidateDownloadInfo, 0)

	replicas, err := s.NodeManager.LoadReplicasByStatus(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		return nil, err
	}

	workloadRecords := make([]*types.WorkloadRecord, 0)

	for _, rInfo := range replicas {
		if !rInfo.IsCandidate {
			continue
		}

		nodeID := rInfo.NodeID
		cNode := s.NodeManager.GetCandidateNode(nodeID)
		if cNode == nil {
			continue
		}

		token, tkPayload, err := cNode.Token(cid, uuid.NewString(), titanRsa, s.NodeManager.PrivateKey)
		if err != nil {
			continue
		}

		workloadRecord := &types.WorkloadRecord{TokenPayload: *tkPayload, Status: types.WorkloadStatusCreate}
		workloadRecords = append(workloadRecords, workloadRecord)

		source := &types.CandidateDownloadInfo{
			NodeID:  nodeID,
			Address: cNode.DownloadAddr(),
			Tk:      token,
		}

		sources = append(sources, source)
	}

	if len(workloadRecords) > 0 {
		if err = s.NodeManager.SaveWorkloadRecord(workloadRecords); err != nil {
			return nil, err
		}
	}

	return sources, nil
}

// NodeExists checks if the node with the specified ID exists.
func (s *Scheduler) NodeExists(ctx context.Context, nodeID string) error {
	if err := s.NodeManager.NodeExists(nodeID, types.NodeEdge); err != nil {
		return s.NodeManager.NodeExists(nodeID, types.NodeCandidate)
	}

	return nil
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
			if remoteAddr != node.RemoteAddr {
				return uuid, xerrors.Errorf("node %s remoteAddr inconsistent, new addr %s ,old addr %s", nodeID, remoteAddr, node.RemoteAddr)
			}

			node.SetLastRequestTime(lastTime)
		}
	} else {
		return uuid, xerrors.Errorf("nodeID %s or remoteAddr %s is nil", nodeID, remoteAddr)
	}

	return uuid, err
}

// create a node id
func newNodeID(nType types.NodeType) (string, error) {
	nodeID := ""
	switch nType {
	case types.NodeEdge:
		nodeID = "e_"
	case types.NodeCandidate:
		nodeID = "c_"
	default:
		return nodeID, xerrors.Errorf("node type %s is error", nType.String())
	}

	uid := uuid.NewString()
	uid = strings.Replace(uid, "-", "", -1)

	return fmt.Sprintf("%s%s", nodeID, uid), nil
}

// create a node key
func newNodeKey() string {
	randomString := make([]byte, 16)
	_, err := crand.Read(randomString)
	if err != nil {
		uid := uuid.NewString()
		return strings.Replace(uid, "-", "", -1)
	}

	return hex.EncodeToString(randomString)
}

func nodeStatus(node *node.Node) types.NodeStatus {
	if node.NATType == types.NatTypeSymmetric {
		return types.NodeNatSymmetric
	}

	return types.NodeServicing
}
