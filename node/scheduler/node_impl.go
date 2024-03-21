package scheduler

import (
	"bytes"
	"context"
	"crypto"
	crand "crypto/rand"
	"database/sql"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/terrors"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/handler"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

const (
	connectivityCheckTimeout = 2 * time.Second
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
func (s *Scheduler) RegisterNode(ctx context.Context, nodeID, publicKey string, nodeType types.NodeType) (*types.ActivationDetail, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return nil, err
	}

	// check params
	if nodeType != types.NodeEdge && nodeType != types.NodeCandidate {
		return nil, xerrors.New("invalid node type")
	}

	if nodeType == types.NodeEdge && !strings.HasPrefix(nodeID, "e_") {
		return nil, xerrors.New("invalid edge node id")
	}

	if nodeType == types.NodeCandidate && !strings.HasPrefix(nodeID, "c_") {
		return nil, xerrors.New("invalid candidate node id")
	}

	if publicKey == "" {
		return nil, xerrors.New("public key is nil")
	}

	_, err = titanrsa.Pem2PublicKey([]byte(publicKey))
	if err != nil {
		return nil, xerrors.Errorf("pem to publicKey err : %s", err.Error())
	}

	if err = s.db.NodeExists(nodeID, nodeType); err == nil {
		return nil, xerrors.Errorf("Node %s aready exist", nodeID)
	}

	if count, err := s.db.RegisterCount(ip); err != nil {
		return nil, xerrors.Errorf("RegisterCount %w", err)
	} else if count >= s.SchedulerCfg.MaxNumberOfRegistrations &&
		!isInIPWhitelist(ip, s.SchedulerCfg.IPWhitelist) {
		return nil, xerrors.New("Registrations exceeded the number")
	}

	detail := &types.ActivationDetail{
		NodeID:        nodeID,
		AreaID:        s.SchedulerCfg.AreaID,
		ActivationKey: newNodeKey(),
		NodeType:      nodeType,
		IP:            ip,
	}

	if err = s.db.SaveNodeRegisterInfos([]*types.ActivationDetail{detail}); err != nil {
		return nil, xerrors.Errorf("SaveNodeRegisterInfos %w", err)
	}

	if err = s.db.SaveNodePublicKey(publicKey, nodeID); err != nil {
		return nil, xerrors.Errorf("SaveNodePublicKey %w", err)
	}

	return detail, nil
}

func isInIPWhitelist(ip string, ipWhiteList []string) bool {
	for _, allowIP := range ipWhiteList {
		if ip == allowIP {
			return true
		}
	}

	return false
}

// RegisterEdgeNode register edge node, return key
func (s *Scheduler) RegisterEdgeNode(ctx context.Context, nodeID, publicKey string) (*types.ActivationDetail, error) {
	return s.RegisterNode(ctx, nodeID, publicKey, types.NodeEdge)
}

// DeactivateNode is used to deactivate a node in the titan server.
// It stops the node from serving any requests and marks it as inactive.
// - nodeID: The ID of the node to deactivate.
// - hours: The deactivation countdown time in hours. It specifies the duration
// before the deactivation is executed. If the deactivation is canceled within
// this period, the node will remain active.
func (s *Scheduler) DeactivateNode(ctx context.Context, nodeID string, hours int) error {
	deactivateTime, err := s.db.LoadDeactivateNodeTime(nodeID)
	if err != nil {
		return xerrors.Errorf("LoadDeactivateNodeTime %s err : %s", nodeID, err.Error())
	}

	if deactivateTime > 0 {
		return xerrors.Errorf("node %s is waiting to deactivate", nodeID)
	}

	deactivateTime = time.Now().Add(time.Duration(hours) * time.Hour).Unix()
	err = s.db.SaveDeactivateNode(nodeID, deactivateTime)
	if err != nil {
		return xerrors.Errorf("SaveDeactivateNode %s err : %s", nodeID, err.Error())
	}

	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.DeactivateTime = deactivateTime
		s.NodeManager.RepayNodeWeight(node)

		// remove from validation
	}

	err = s.db.NodeExists(nodeID, types.NodeCandidate)
	if err == nil {
		// if node is candidate , need to backup asset
		return s.AssetManager.CandidateDeactivate(nodeID)
	}

	return nil
}

// UndoNodeDeactivation is used to undo the deactivation of a node in the titan server.
// It allows the previously deactivated node to start serving requests again.
func (s *Scheduler) UndoNodeDeactivation(ctx context.Context, nodeID string) error {
	deactivateTime, err := s.db.LoadDeactivateNodeTime(nodeID)
	if err != nil {
		return xerrors.Errorf("LoadDeactivateNodeTime %s err : %s", nodeID, err.Error())
	}

	if time.Now().Unix() > deactivateTime {
		return xerrors.New("Node has been deactivation")
	}

	err = s.db.SaveDeactivateNode(nodeID, 0)
	if err != nil {
		return xerrors.Errorf("DeleteDeactivateNode %s err : %s", nodeID, err.Error())
	}

	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.DeactivateTime = 0
		s.NodeManager.DistributeNodeWeight(node)

		// add to validation
	}

	return nil
}

// RequestActivationCodes request node activation codes
func (s *Scheduler) RequestActivationCodes(ctx context.Context, nodeType types.NodeType, count int) ([]*types.NodeActivation, error) {
	if count < 1 {
		return nil, nil
	}

	areaID := s.SchedulerCfg.AreaID
	out := make([]*types.NodeActivation, 0)
	details := make([]*types.ActivationDetail, 0)

	for i := 0; i < count; i++ {
		nodeID, err := newNodeID(nodeType)
		if err != nil {
			return nil, xerrors.Errorf("newNodeID err:%s", err.Error())
		}

		detail := &types.ActivationDetail{
			NodeID:        nodeID,
			AreaID:        areaID,
			ActivationKey: newNodeKey(),
			NodeType:      nodeType,
			IP:            "localhost",
		}

		code, err := detail.Marshal()
		if err != nil {
			return nil, xerrors.Errorf("Marshal err:%s", err.Error())
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
		return nil, xerrors.Errorf("SaveNodeRegisterInfos err:%s", err.Error())
	}

	return out, nil
}

// UpdateNodePort sets the port for the specified node.
func (s *Scheduler) UpdateNodePort(ctx context.Context, nodeID, port string) error {
	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.PortMapping = port
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
	nodeInfo.Status = types.NodeOffline

	dbInfo, err := s.NodeManager.LoadNodeInfo(nodeID)
	if err != nil {
		return nodeInfo, xerrors.Errorf("nodeID %s LoadNodeInfo err:%s", nodeID, err.Error())
	}
	nodeInfo = *dbInfo

	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		nodeInfo.Status = nodeStatus(node)
		nodeInfo.NATType = node.NATType.String()
		nodeInfo.Type = node.Type
		nodeInfo.MemoryUsage = node.MemoryUsage
		nodeInfo.CPUUsage = node.CPUUsage
		nodeInfo.DiskUsage = node.DiskUsage
		nodeInfo.BandwidthDown = node.BandwidthDown
		nodeInfo.BandwidthUp = node.BandwidthUp
		nodeInfo.ExternalIP = node.ExternalIP
		nodeInfo.IncomeIncr = node.IncomeIncr
		nodeInfo.TitanDiskUsage = node.TitanDiskUsage

		log.Debugf("%s node select codes:%v", nodeID, node.SelectWeights())
	}

	// isValidator, err := s.db.IsValidator(nodeID)
	// if err != nil {
	// 	log.Errorf("IsValidator %s err:%s", node.NodeID, err.Error())
	// }

	// if isValidator {
	// 	nodeInfo.Type = types.NodeValidator
	// }

	return nodeInfo, nil
}

// GetNodeList retrieves a list of nodes with pagination.
func (s *Scheduler) GetNodeList(ctx context.Context, offset int, limit int) (*types.ListNodesRsp, error) {
	info := &types.ListNodesRsp{Data: make([]types.NodeInfo, 0)}

	rows, total, err := s.NodeManager.LoadNodeInfos(limit, offset)
	if err != nil {
		return nil, xerrors.Errorf("LoadNodeInfos err:%s", err.Error())
	}
	defer rows.Close()

	// validator := make(map[string]struct{})
	// validatorList, err := s.NodeManager.LoadValidators(s.NodeManager.ServerID)
	// if err != nil {
	// 	log.Errorf("get validator list: %v", err)
	// }
	// for _, id := range validatorList {
	// 	validator[id] = struct{}{}
	// }

	nodeInfos := make([]types.NodeInfo, 0)
	for rows.Next() {
		nodeInfo := &types.NodeInfo{}
		err = rows.StructScan(nodeInfo)
		if err != nil {
			log.Errorf("NodeInfo StructScan err: %s", err.Error())
			continue
		}

		node := s.NodeManager.GetNode(nodeInfo.NodeID)
		if node != nil {
			nodeInfo.Status = nodeStatus(node)
			nodeInfo.NATType = node.NATType.String()
			nodeInfo.Type = node.Type
			nodeInfo.MemoryUsage = node.MemoryUsage
			nodeInfo.CPUUsage = node.CPUUsage
			nodeInfo.DiskUsage = node.DiskUsage
			nodeInfo.BandwidthDown = node.BandwidthDown
			nodeInfo.BandwidthUp = node.BandwidthUp
			nodeInfo.ExternalIP = node.ExternalIP
			nodeInfo.IncomeIncr = node.IncomeIncr
			nodeInfo.TitanDiskUsage = node.TitanDiskUsage
		}

		// _, exist := validator[nodeInfo.NodeID]
		// if exist {
		// 	nodeInfo.Type = types.NodeValidator
		// }

		nodeInfos = append(nodeInfos, *nodeInfo)
	}

	info.Data = nodeInfos
	info.Total = total

	return info, nil
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

		workloadRecord := &types.WorkloadRecord{TokenPayload: *tkPayload, Status: types.WorkloadStatusCreate, ClientEndTime: tkPayload.Expiration.Unix()}
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

func (s *Scheduler) GetNodeToken(ctx context.Context, nodeID string) (string, error) {
	node := s.NodeManager.GetNode(nodeID)
	if node == nil {
		return "", xerrors.Errorf("node %s not find ", nodeID)
	}

	return node.GetToken(), nil
}

func (s *Scheduler) CheckIpUsage(ctx context.Context, ip string) (bool, error) {
	if s.NodeManager.CheckIPExist(ip) {
		return true, nil
	}

	count, err := s.db.RegisterCount(ip)
	return count > 0, err
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

	limit := 50

	for _, rInfo := range replicas {
		if len(sources) > limit {
			break
		}

		// if !rInfo.IsCandidate {
		// 	continue
		// }

		nodeID := rInfo.NodeID
		cNode := s.NodeManager.GetNode(nodeID)
		if cNode == nil {
			continue
		}

		if cNode.Type == types.NodeValidator {
			continue
		}

		// edge
		if cNode.Type != types.NodeCandidate {
			if cNode.NATType != types.NatTypeNo || cNode.ExternalIP == "" {
				continue
			}
		}

		token, tkPayload, err := cNode.Token(cid, uuid.NewString(), titanRsa, s.NodeManager.PrivateKey)
		if err != nil {
			continue
		}

		workloadRecord := &types.WorkloadRecord{TokenPayload: *tkPayload, Status: types.WorkloadStatusCreate, ClientEndTime: tkPayload.Expiration.Unix()}
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
				log.Debugf("node %s remoteAddr inconsistent, new addr %s ,old addr %s", nodeID, remoteAddr, node.RemoteAddr)
			}

			if node.DeactivateTime > 0 && node.DeactivateTime < time.Now().Unix() {
				return uuid, xerrors.Errorf("The node %s has been deactivate and cannot be logged in", nodeID)
			}

			node.SetLastRequestTime(lastTime)
		}
	} else {
		return uuid, xerrors.Errorf("nodeID %s or remoteAddr %s is nil", nodeID, remoteAddr)
	}

	return uuid, err
}

// NodeKeepaliveV2 candidate and edge keepalive
func (s *Scheduler) NodeKeepaliveV2(ctx context.Context) (uuid.UUID, error) {
	uuid, err := s.CommonAPI.Session(ctx)

	remoteAddr := handler.GetRemoteAddr(ctx)
	nodeID := handler.GetNodeID(ctx)
	if nodeID != "" && remoteAddr != "" {
		lastTime := time.Now()

		node := s.NodeManager.GetNode(nodeID)
		if node != nil {
			if remoteAddr != node.RemoteAddr {
				log.Debugf("node %s remoteAddr inconsistent, new addr %s ,old addr %s", nodeID, remoteAddr, node.RemoteAddr)
				return uuid, &api.ErrNode{Code: int(terrors.NodeIPInconsistent), Message: fmt.Sprintf("node %s new ip %s, old ip %s", nodeID, remoteAddr, node.RemoteAddr)}
			}

			if node.DeactivateTime > 0 && node.DeactivateTime < time.Now().Unix() {
				return uuid, &api.ErrNode{Code: int(terrors.NodeDeactivate), Message: fmt.Sprintf("The node %s has been deactivate and cannot be logged in", nodeID)}
			}

			node.SetLastRequestTime(lastTime)
		} else {
			return uuid, &api.ErrNode{Code: int(terrors.NodeOffline), Message: fmt.Sprintf("node %s offline or not exist", nodeID)}
		}
	} else {
		return uuid, &api.ErrNode{Code: terrors.Unknown, Message: fmt.Sprintf("nodeID %s or remoteAddr %s is nil", nodeID, remoteAddr)}
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

// VerifyTokenWithLimitCount verify token in limit count
func (s *Scheduler) VerifyTokenWithLimitCount(ctx context.Context, token string) (*types.JWTPayload, error) {
	jwtPayload, err := s.AuthVerify(ctx, token)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.VerifyTokenError.Int(), Message: fmt.Sprintf("verify token error %s", err.Error())}
	}

	if len(jwtPayload.Extend) == 0 {
		return nil, fmt.Errorf("JWTPayload.Extend can not empty")
	}

	payload := &types.AuthUserUploadDownloadAsset{}
	if err = json.Unmarshal([]byte(jwtPayload.Extend), payload); err != nil {
		return nil, err
	}

	if !payload.Expiration.IsZero() && payload.Expiration.Before(time.Now()) {
		return nil, fmt.Errorf("token is expiration")
	}

	assetHash, err := cidutil.CIDToHash(payload.AssetCID)
	if err != nil {
		return nil, err
	}

	if _, err = s.db.GetAssetName(assetHash, payload.UserID); err == sql.ErrNoRows {
		return nil, fmt.Errorf("asset %s does not exist", payload.AssetCID)
	}

	userInfo, err := s.loadUserInfo(payload.UserID)
	if err != nil {
		return nil, err
	}

	if userInfo.EnableVIP {
		return jwtPayload, nil
	}

	count, err := s.db.GetAssetVisitCount(assetHash)
	if err != nil {
		return nil, err
	}

	if count >= s.SchedulerCfg.MaxCountOfVisitShareLink {
		return nil, &api.ErrWeb{Code: terrors.VisitShareLinkOutOfMaxCount.Int(), Message: fmt.Sprintf("visit share link is out of max count %d", s.SchedulerCfg.MaxCountOfVisitShareLink)}
	}

	if err = s.db.UpdateAssetVisitCount(assetHash); err != nil {
		return nil, err
	}

	return jwtPayload, nil
}

// UpdateBandwidths update bandwidths
func (s *Scheduler) UpdateBandwidths(ctx context.Context, bandwidthDown, bandwidthUp int64) error {
	// nodeID := handler.GetNodeID(ctx)
	// s.NodeManager.UpdateNodeBandwidths(nodeID, bandwidthDown, bandwidthUp)

	return nil
}

// DownloadDataResult node download data result
func (s *Scheduler) DownloadDataResult(ctx context.Context, bucket, cid string, size int64) error {
	nodeID := handler.GetNodeID(ctx)

	log.Infof("awsTask DownloadDataResult %s : %s : %s : %d", nodeID, cid, bucket, size)

	s.AssetManager.UpdateFillAssetResponseCount(bucket, cid, nodeID)

	return nil

	// node := s.NodeManager.GetCandidateNode(nodeID)
	// if node == nil {
	// 	return xerrors.Errorf("node %s not exists", nodeID)
	// }

	// if cid == "" {
	// 	return s.db.UpdateAWSData(&types.AWSDataInfo{Bucket: bucket, Cid: cid, IsDistribute: false})
	// }

	// err := s.db.UpdateAWSData(&types.AWSDataInfo{Bucket: bucket, Cid: cid, IsDistribute: true})
	// if err != nil {
	// 	return err
	// }

	// info, err := s.db.LoadAWSData(bucket)
	// if err != nil {
	// 	return err
	// }

	// return s.AssetManager.CreateBaseAsset(cid, nodeID, size, int64(info.Replicas))
}

// GetCandidateNodeIP get candidate ip for locator
func (s *Scheduler) GetCandidateNodeIP(ctx context.Context, nodeID string) (string, error) {
	node := s.NodeManager.GetCandidateNode(nodeID)
	if node == nil {
		return "", fmt.Errorf("node %s does not exist", nodeID)
	}

	ip, _, err := net.SplitHostPort(node.RemoteAddr)
	if err != nil {
		return "", err
	}
	return ip, nil
}

func (s *Scheduler) verifyTCPConnectivity(targetURL string) error {
	conn, err := net.DialTimeout("tcp", targetURL, connectivityCheckTimeout)
	if err != nil {
		return xerrors.Errorf("dial tcp %w, addr %s", err, targetURL)
	}
	defer conn.Close()

	return nil
}

func (s *Scheduler) GetMinioConfigFromCandidate(ctx context.Context, nodeID string) (*types.MinioConfig, error) {
	node := s.NodeManager.GetCandidateNode(nodeID)
	if node == nil {
		return nil, fmt.Errorf("node %s does not exist", nodeID)
	}

	return node.API.GetMinioConfig(ctx)
}

func (s *Scheduler) GetCandidateIPs(ctx context.Context) ([]*types.NodeIPInfo, error) {
	list := make([]*types.NodeIPInfo, 0)

	_, cNodes := s.NodeManager.GetAllValidCandidateNodes()
	if len(cNodes) == 0 {
		return list, &api.ErrWeb{Code: terrors.NotFoundNode.Int(), Message: terrors.NotFoundNode.String()}
	}

	for _, n := range cNodes {
		if n.IsPrivateMinioOnly {
			continue
		}

		externalURL := n.ExternalURL
		if len(externalURL) == 0 {
			externalURL = fmt.Sprintf("http://%s", n.RemoteAddr)
		}
		list = append(list, &types.NodeIPInfo{NodeID: n.NodeID, IP: n.ExternalIP, ExternalURL: externalURL})
	}

	return list, nil
}

func (s *Scheduler) GetNodeOnlineState(ctx context.Context) (bool, error) {
	nodeID := handler.GetNodeID(ctx)
	if len(nodeID) == 0 {
		return false, fmt.Errorf("invalid request")
	}
	if node := s.NodeManager.GetNode(nodeID); node != nil {
		return true, nil
	}
	return false, nil
}

// GetAssetViwe get the asset view of node
func (s *Scheduler) GetAssetView(ctx context.Context, nodeID string, isFromNode bool) (*types.AssetView, error) {
	if isFromNode {
		fmt.Println("from node")
		node := s.NodeManager.GetNode(nodeID)
		if node == nil {
			return nil, fmt.Errorf("node %s offline or not exist", nodeID)
		}
		return node.GetAssetView(ctx)
	}

	topHash, err := s.AssetManager.LoadTopHash(nodeID)
	if err != nil {
		return nil, err
	}

	hashesBytes, err := s.AssetManager.LoadBucketHashes(nodeID)
	if err != nil {
		return nil, err
	}

	if len(hashesBytes) == 0 {
		return nil, fmt.Errorf("node %s not exist any asset", nodeID)
	}

	bucketHashMap := make(map[uint32]string)
	buffer := bytes.NewBuffer(hashesBytes)
	dec := gob.NewDecoder(buffer)
	if err := dec.Decode(&bucketHashMap); err != nil {
		return nil, err
	}

	return &types.AssetView{TopHash: topHash, BucketHashes: bucketHashMap}, nil
}

// GetAssetInBucket get the assets of the bucket
func (s *Scheduler) GetAssetsInBucket(ctx context.Context, nodeID string, bucketID int, isFromNode bool) ([]string, error) {
	if isFromNode {
		fmt.Println("from node")
		node := s.NodeManager.GetNode(nodeID)
		if node == nil {
			return nil, fmt.Errorf("node %s offline or not exist", nodeID)
		}
		return node.GetAssetsInBucket(ctx, bucketID)
	}

	id := fmt.Sprintf("%s:%d", nodeID, bucketID)
	hashesBytes, err := s.AssetManager.LoadBucket(id)
	if err != nil {
		return nil, err
	}

	if len(hashesBytes) == 0 {
		return nil, fmt.Errorf("bucket %s not exist any asset", bucketID)
	}

	assetHashes := make([]string, 0)
	buffer := bytes.NewBuffer(hashesBytes)
	dec := gob.NewDecoder(buffer)
	if err := dec.Decode(&assetHashes); err != nil {
		return nil, err
	}

	return assetHashes, nil
}
