package scheduler

import (
	"bytes"
	"context"
	"crypto"
	cRand "crypto/rand"
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
	"github.com/Filecoin-Titan/titan/region"
	"github.com/docker/go-units"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

const (
	connectivityCheckTimeout = 2 * time.Second
)

// GetOnlineNodeCount returns the count of online nodes for a given node type
func (s *Scheduler) GetOnlineNodeCount(ctx context.Context, nodeType types.NodeType) (int, error) {
	if nodeType == types.NodeUnknown || nodeType == types.NodeEdge {
		return s.NodeManager.GetOnlineNodeCount(nodeType), nil
	}

	i := 0
	_, nodes := s.NodeManager.GetAllCandidateNodes()
	for _, node := range nodes {
		if node == nil {
			continue
		}

		if nodeType == node.Type {
			i++
		}
	}

	return i, nil
}

// RegisterCandidateNode register node
func (s *Scheduler) RegisterCandidateNode(ctx context.Context, nodeID, publicKey, code string) (*types.ActivationDetail, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return nil, err
	}

	info, err := s.db.GetCandidateCodeInfo(code)
	if err != nil {
		return nil, err
	}

	if info.NodeID != "" {
		return nil, xerrors.New("invalid code")
	}

	nodeType := info.NodeType

	// check params
	if nodeType != types.NodeCandidate && nodeType != types.NodeValidator {
		return nil, xerrors.New("invalid node type")
	}

	if !strings.HasPrefix(nodeID, "c_") {
		return nil, xerrors.New("invalid candidate node id")
	}

	if publicKey == "" {
		return nil, xerrors.New("public key is nil")
	}

	_, err = titanrsa.Pem2PublicKey([]byte(publicKey))
	if err != nil {
		return nil, xerrors.Errorf("pem to publicKey err : %s", err.Error())
	}

	isValidator := false
	if nodeType == types.NodeValidator {
		isValidator = true
		nodeType = types.NodeCandidate
	}

	if err = s.db.NodeExists(nodeID, nodeType); err == nil {
		return nil, xerrors.Errorf("Node %s are exist", nodeID)
	}

	err = s.db.UpdateCandidateCodeInfo(code, nodeID)
	if err != nil {
		return nil, xerrors.Errorf("UpdateCandidateCodeInfo %s err : %s", nodeID, err.Error())
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

	if isValidator {
		err = s.db.UpdateValidators([]string{nodeID}, s.ServerID, false)
		if err != nil {
			log.Errorf("RegisterNode UpdateValidators %s err:%s", nodeID, err.Error())
		}
	}

	return detail, nil
}

// RegisterNode register node
func (s *Scheduler) RegisterNode(ctx context.Context, nodeID, publicKey string, nodeType types.NodeType) (*types.ActivationDetail, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return nil, err
	}

	// check params
	if nodeType != types.NodeEdge {
		return nil, xerrors.New("invalid node type")
	}

	if !strings.HasPrefix(nodeID, "e_") {
		return nil, xerrors.New("invalid edge node id")
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
	nID := handler.GetNodeID(ctx)
	if len(nID) > 0 {
		nodeID = nID
	}

	deactivateTime, err := s.db.LoadDeactivateNodeTime(nodeID)
	if err != nil {
		return xerrors.Errorf("LoadDeactivateNodeTime %s err : %s", nodeID, err.Error())
	}

	if deactivateTime > 0 {
		return xerrors.Errorf("node %s is waiting to deactivate", nodeID)
	}

	if hours <= 0 {
		hours = 24
	}

	penaltyPoint := 0.0

	err = s.db.NodeExists(nodeID, types.NodeCandidate)
	if err == nil {
		info, err := s.db.LoadNodeInfo(nodeID)
		if err != nil {
			return err
		}
		// if node is candidate , need to backup asset
		s.AssetManager.CandidateDeactivate(nodeID)

		pe, _ := s.NodeManager.CalculateExitProfit(info.Profit)
		penaltyPoint = info.Profit - pe
	}

	deactivateTime = time.Now().Add(time.Duration(hours) * time.Hour).Unix()
	err = s.db.SaveDeactivateNode(nodeID, deactivateTime, penaltyPoint)
	if err != nil {
		return xerrors.Errorf("SaveDeactivateNode %s err : %s", nodeID, err.Error())
	}

	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.DeactivateTime = deactivateTime
		s.NodeManager.RepayNodeWeight(node)

		// remove from validation
	}

	return nil
}

func (s *Scheduler) CalculateExitProfit(ctx context.Context, nodeID string) (types.ExitProfitRsp, error) {
	nID := handler.GetNodeID(ctx)
	if len(nID) > 0 {
		nodeID = nID
	}

	err := s.db.NodeExists(nodeID, types.NodeCandidate)
	if err != nil {
		return types.ExitProfitRsp{}, nil
	}

	info, err := s.db.LoadNodeInfo(nodeID)
	if err != nil {
		return types.ExitProfitRsp{}, err
	}

	pe, exitRate := s.NodeManager.CalculateExitProfit(info.Profit)
	return types.ExitProfitRsp{
		CurrentPoint:   info.Profit,
		RemainingPoint: pe,
		PenaltyRate:    exitRate,
	}, err
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

	err = s.db.SaveDeactivateNode(nodeID, 0, 0)
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
		nodeInfo.NATType = node.NATType
		nodeInfo.Type = node.Type
		nodeInfo.CPUUsage = node.CPUUsage
		nodeInfo.DiskUsage = node.DiskUsage
		nodeInfo.ExternalIP = node.ExternalIP
		nodeInfo.IncomeIncr = node.IncomeIncr
		nodeInfo.IsTestNode = node.IsTestNode
		nodeInfo.GeoInfo = node.GeoInfo
		nodeInfo.RemoteAddr = node.RemoteAddr

		log.Debugf("%s node select codes:%v , url:%s", nodeID, node.SelectWeights(), node.ExternalURL)
	}

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
			nodeInfo.NATType = node.NATType
			nodeInfo.Type = node.Type
			nodeInfo.CPUUsage = node.CPUUsage
			nodeInfo.DiskUsage = node.DiskUsage
			nodeInfo.ExternalIP = node.ExternalIP
			nodeInfo.IncomeIncr = node.IncomeIncr
			nodeInfo.IsTestNode = node.IsTestNode
			nodeInfo.GeoInfo = node.GeoInfo
			nodeInfo.RemoteAddr = node.RemoteAddr
		}

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

	// ws := make([]*types.Workload, 0)

	for _, rInfo := range replicas {
		if rInfo.IsCandidate {
			continue
		}

		nodeID := rInfo.NodeID
		eNode := s.NodeManager.GetEdgeNode(nodeID)
		if eNode == nil {
			continue
		}

		if eNode.NATType == types.NatTypeSymmetric.String() {
			continue
		}

		if eNode.NetFlowUpExcess(float64(rInfo.DoneSize)) {
			continue
		}

		token, _, err := eNode.Token(cid, uuid.NewString(), titanRsa, s.NodeManager.PrivateKey)
		if err != nil {
			continue
		}

		// workloadRecord := &types.WorkloadRecord{TokenPayload: *tkPayload, Status: types.WorkloadStatusCreate, ClientEndTime: tkPayload.Expiration.Unix()}
		// workloadRecords = append(workloadRecords, workloadRecord)

		info := &types.EdgeDownloadInfo{
			Address: eNode.DownloadAddr(),
			NodeID:  nodeID,
			Tk:      token,
			NatType: eNode.NATType,
		}
		infos = append(infos, info)
	}

	if len(infos) == 0 {
		return nil, nil
	}

	// if len(ws) > 0 {
	// 	buffer := &bytes.Buffer{}
	// 	enc := gob.NewEncoder(buffer)
	// 	err := enc.Encode(ws)
	// 	if err != nil {
	// 		log.Errorf("encode error:%s", err.Error())
	// 	} else {
	// 		record = &types.WorkloadRecord{
	// 			WorkloadID: uuid.NewString(),
	// 			AssetCID:   cid,
	// 			ClientID:   clientID,
	// 			AssetSize:  size,
	// 			Workloads:  buffer.Bytes(),
	// 		}

	// 		if err = s.NodeManager.SaveWorkloadRecord(workloadRecords); err != nil {
	// 			return nil, err
	// 		}
	// 	}
	// }

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

func (s *Scheduler) GetNodeOfIP(ctx context.Context, ip string) ([]string, error) {
	return s.NodeManager.GetNodeOfIP(ip), nil
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

func (s *Scheduler) getSource2(cNode *node.Node, cid string, titanRsa *titanrsa.Rsa) *types.SourceDownloadInfo {
	token, _, err := cNode.Token(cid, uuid.NewString(), titanRsa, s.NodeManager.PrivateKey)
	if err != nil {
		return nil
	}

	source := &types.SourceDownloadInfo{
		NodeID:  cNode.NodeID,
		Address: cNode.DownloadAddr(),
		Tk:      token,
	}

	return source
}

func (s *Scheduler) getSource(cNode *node.Node, cid, bucket string, titanRsa *titanrsa.Rsa) *types.CandidateDownloadInfo {
	token, _, err := cNode.Token(cid, uuid.NewString(), titanRsa, s.NodeManager.PrivateKey)
	if err != nil {
		return nil
	}

	source := &types.CandidateDownloadInfo{
		NodeID:    cNode.NodeID,
		Address:   cNode.DownloadAddr(),
		AWSBucket: bucket,
		Tk:        token,
	}

	return source
}

func (s *Scheduler) GetAssetSourceDownloadInfo(ctx context.Context, cid string) (*types.AssetSourceDownloadInfoRsp, error) {
	out := &types.AssetSourceDownloadInfoRsp{}
	// from app
	clientID := ""

	event := types.WorkloadEventRetrieve

	nodeID := handler.GetNodeID(ctx)
	if len(nodeID) > 0 {
		clientID = nodeID
		event = types.WorkloadEventSync
	} else {
		uID := handler.GetUserID(ctx)
		if len(uID) > 0 {
			clientID = uID
		}
	}

	log.Infof("GetAssetSourceDownloadInfo clientID:%s, cid:%s", clientID, cid)

	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, xerrors.Errorf("GetAssetSourceDownloadInfo %s cid to hash err:%s", cid, err.Error())
	}

	replicas, err := s.db.LoadReplicasByStatus(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		return nil, err
	}

	aInfo, err := s.db.LoadAssetRecord(hash)
	if err != nil {
		return nil, err
	}

	if aInfo.Source == int64(types.AssetSourceAWS) {
		out.AWSBucket = aInfo.Note
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sources := make([]*types.SourceDownloadInfo, 0)

	// Shuffle array
	for i := len(replicas) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		replicas[i], replicas[j] = replicas[j], replicas[i]
	}

	limit := 5
	for _, rInfo := range replicas {
		nodeID := rInfo.NodeID
		cNode := s.NodeManager.GetNode(nodeID)
		if cNode == nil {
			continue
		}

		if cNode.NetFlowUpExcess(float64(rInfo.DoneSize)) {
			continue
		}

		if rInfo.IsCandidate {
			// if aInfo.Source == int64(types.AssetSourceStorage) {
			// 	if !cNode.IsStorageOnly {
			// 		continue
			// 	}
			// }

			source := s.getSource2(cNode, cid, titanRsa)
			if source != nil {
				sources = append(sources, source)
			}

			continue
		}

		// limit edge count
		if len(sources) >= limit {
			continue
		}

		if (cNode.NATType != types.NatTypeNo.String() && cNode.NATType != types.NatTypeFullCone.String()) || cNode.ExternalIP == "" {
			continue
		}

		source := s.getSource2(cNode, cid, titanRsa)
		if source != nil {
			sources = append(sources, source)
		}
	}

	out.SourceList = sources

	// init workload
	if len(sources) > 0 {
		out.WorkloadID = uuid.NewString()

		ws := make([]*types.Workload, 0)
		for _, info := range sources {
			ws = append(ws, &types.Workload{SourceID: info.NodeID})
		}

		buffer := &bytes.Buffer{}
		enc := gob.NewEncoder(buffer)
		err := enc.Encode(ws)
		if err != nil {
			log.Errorf("GetAssetSourceDownloadInfo encode error:%s", err.Error())
			return out, nil
		}

		if event == types.WorkloadEventSync {
			record := &types.WorkloadRecord{
				WorkloadID: out.WorkloadID,
				AssetCID:   cid,
				ClientID:   clientID,
				AssetSize:  aInfo.TotalSize,
				Workloads:  buffer.Bytes(),
				Event:      event,
				Status:     types.WorkloadStatusCreate,
			}

			if err = s.NodeManager.SaveWorkloadRecord([]*types.WorkloadRecord{record}); err != nil {
				log.Errorf("GetAssetSourceDownloadInfo SaveWorkloadRecord error:%s", err.Error())
				return out, nil
			}
		}
	}

	return out, nil
}

// GetCandidateDownloadInfos finds candidate download info for the given CID.
func (s *Scheduler) GetCandidateDownloadInfos(ctx context.Context, cid string) ([]*types.CandidateDownloadInfo, error) {
	return nil, xerrors.New("The interface has been deprecated")
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
	uuid, _ := s.CommonAPI.Session(ctx)
	return uuid, xerrors.New("The interface has been deprecated")
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
				count, lastTime := node.GetNumberOfIPChanges()
				duration := time.Now().Sub(lastTime)
				seconds := duration.Seconds()

				if seconds > 10*6*20 {
					node.SetNumberOfIPChanges(0)

					if count > 120 {
						log.Infof("NodeKeepaliveV2 Exceeded expectations %s , ip:%s : %s, count:%d ,resetSeconds:%.2f ", nodeID, remoteAddr, node.RemoteAddr, count, seconds)
					}

					return uuid, &api.ErrNode{Code: int(terrors.NodeIPInconsistent), Message: fmt.Sprintf("node %s new ip %s, old ip %s, resetSeconds:%.2f , resetCount:%d", nodeID, remoteAddr, node.RemoteAddr, seconds, count)}
				}

				count++
				node.SetNumberOfIPChanges(count)
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
	_, err := cRand.Read(randomString)
	if err != nil {
		uid := uuid.NewString()
		return strings.Replace(uid, "-", "", -1)
	}

	return hex.EncodeToString(randomString)
}

func nodeStatus(node *node.Node) types.NodeStatus {
	if node.NATType == types.NatTypeSymmetric.String() {
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

	s.AssetManager.UpdateFillAssetResponseCount(bucket, cid, nodeID, size)

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

	_, cNodes := s.NodeManager.GetAllCandidateNodes()
	if len(cNodes) == 0 {
		return list, &api.ErrWeb{Code: terrors.NotFoundNode.Int(), Message: terrors.NotFoundNode.String()}
	}

	for _, n := range cNodes {
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

// GetAssetView get the asset view of node
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
		return nil, fmt.Errorf("bucket %d not exist any asset", bucketID)
	}

	assetHashes := make([]string, 0)
	buffer := bytes.NewBuffer(hashesBytes)
	dec := gob.NewDecoder(buffer)
	if err := dec.Decode(&assetHashes); err != nil {
		return nil, err
	}

	return assetHashes, nil
}

func (s *Scheduler) PerformSyncData(ctx context.Context, nodeID string) error {
	node := s.NodeManager.GetNode(nodeID)
	if node == nil {
		return xerrors.Errorf("node %s is offline or not exist", nodeID)
	}

	view, err := s.GetAssetView(ctx, nodeID, false)
	if err != nil {
		return err
	}

	mismatchBuckets, err := node.CompareBucketHashes(ctx, view.BucketHashes)
	if err != nil {
		return xerrors.Errorf("compare bucket hashes %w", err)
	}

	log.Warnf("node %s mismatch buckets len:%d", nodeID, len(mismatchBuckets))
	return nil
}

// GetReplicasForNode retrieves a asset list of node
func (s *Scheduler) GetProfitDetailsForNode(ctx context.Context, nodeID string, limit, offset int, ts []int) (*types.ListNodeProfitDetailsRsp, error) {
	if len(ts) == 0 {
		return nil, nil
	}

	info, err := s.db.LoadNodeProfits(nodeID, limit, offset, ts)
	if err != nil {
		return nil, xerrors.Errorf("LoadNodeProfits err:%s", err.Error())
	}

	return info, nil
}

// Interval for initiating free space release
var FreeUpDayInterval = 1

// FreeUpDiskSpace Request to free up disk space, returns file hashes and next time
func (s *Scheduler) FreeUpDiskSpace(ctx context.Context, nodeID string, size int64) (*types.FreeUpDiskResp, error) {
	nID := handler.GetNodeID(ctx)
	if nID != "" {
		nodeID = nID
	}

	if size <= 0 {
		return nil, xerrors.Errorf("size is %d", size)
	}

	// limit
	t, err := s.db.LoadFreeUpDiskTime(nodeID)
	if err != nil {
		return &types.FreeUpDiskResp{}, err
	}

	now := time.Now()
	fiveDaysAgo := now.AddDate(0, 0, -FreeUpDayInterval)
	nextReleaseTime := t.AddDate(0, 0, +FreeUpDayInterval)
	if !t.Before(fiveDaysAgo) {
		return &types.FreeUpDiskResp{NextTime: nextReleaseTime.Unix()}, xerrors.Errorf("Less than %d days have passed since the last release", FreeUpDayInterval)
	}

	// todo
	hashes, err := s.db.LoadAllHashesOfNode(nodeID)
	if err != nil {
		return nil, err
	}

	removeList := make([]string, 0)

	for _, hash := range hashes {
		asset, err := s.db.LoadAssetRecord(hash)
		if err != nil {
			continue
		}

		err = s.AssetManager.RemoveReplica(asset.CID, asset.Hash, nodeID)
		if err != nil {
			log.Errorf("FreeUpDiskSpace %s RemoveReplica %s err:%s", nodeID, asset.CID, err.Error())
			continue
		}

		removeList = append(removeList, asset.Hash)

		size -= asset.TotalSize
		if size <= 0 {
			break
		}
	}

	if len(removeList) > 0 {
		err = s.db.SaveFreeUpDiskTime(nodeID, now)
		if err != nil {
			return &types.FreeUpDiskResp{NextTime: now.AddDate(0, 0, +FreeUpDayInterval).Unix()}, err
		}
	}

	err = s.db.SaveReplenishBackup(removeList)
	if err != nil {
		log.Errorf("FreeUpDiskSpace %s SaveReplenishBackup err:%s", nodeID, err.Error())
	}

	return &types.FreeUpDiskResp{Hashes: removeList, NextTime: now.AddDate(0, 0, +FreeUpDayInterval).Unix()}, nil
}

func (s *Scheduler) GetNextFreeTime(ctx context.Context, nodeID string) (int64, error) {
	nID := handler.GetNodeID(ctx)
	if nID != "" {
		nodeID = nID
	}

	t, err := s.db.LoadFreeUpDiskTime(nodeID)
	if err != nil {
		return 0, err
	}

	return t.AddDate(0, 0, +FreeUpDayInterval).Unix(), nil
}

func (s *Scheduler) UpdateNodeDynamicInfo(ctx context.Context, info *types.NodeDynamicInfo) error {
	node := s.NodeManager.GetNode(info.NodeID)
	if node == nil {
		return xerrors.Errorf("node %s not found", info.NodeID)
	}

	if node.DownloadTraffic < info.DownloadTraffic {
		node.DownloadTraffic += info.DownloadTraffic
	}

	if node.UploadTraffic < info.UploadTraffic {
		node.UploadTraffic += info.UploadTraffic
	}

	return nil
}

func (s *Scheduler) GenerateCandidateCode(ctx context.Context, count int, nodeType types.NodeType, isTest bool) ([]string, error) {
	infos := make([]*types.CandidateCodeInfo, 0)
	out := make([]string, 0)
	for i := 0; i < count; i++ {
		code := uuid.NewString()
		code = strings.Replace(code, "-", "", -1)

		infos = append(infos, &types.CandidateCodeInfo{
			Code:       code,
			NodeType:   nodeType,
			Expiration: time.Now().Add(time.Hour * 24),
			IsTest:     isTest,
		})
		out = append(out, code)
	}

	return out, s.db.SaveCandidateCodeInfo(infos)
}

func (s *Scheduler) GetCandidateCodeInfos(ctx context.Context, nodeID, code string) ([]*types.CandidateCodeInfo, error) {
	if nodeID != "" {
		info, err := s.db.GetCandidateCodeInfoForNodeID(nodeID)
		if err != nil {
			return nil, err
		}

		return []*types.CandidateCodeInfo{info}, nil
	}

	if code != "" {
		info, err := s.db.GetCandidateCodeInfo(code)
		if err != nil {
			return nil, err
		}

		return []*types.CandidateCodeInfo{info}, nil
	}

	return s.db.GetCandidateCodeInfos()
}

func (s *Scheduler) CandidateCodeExist(ctx context.Context, code string) (bool, error) {
	info, err := s.db.GetCandidateCodeInfo(code)
	if err != nil {
		return false, err
	}

	return info.Code == code, nil
}

func generateRandomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]rune, n) //
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))] //
	}

	return string(b)
}

// dpGetRemoveAssetsClosetSize returns a certain MiB size that is closest to the demand MiB size based on Dynamic Programming
func dpGetRemoveAssetsClosetSize(assets []*types.AssetRecord, target int64) int64 {
	n := int64(len(assets))

	maxPossibleSum := int64(0)
	for _, asset := range assets {
		maxPossibleSum += asset.TotalSize / units.MiB
	}

	if target > maxPossibleSum {
		return maxPossibleSum
	}

	subset := make([][]bool, n+1)
	for i := range subset {
		subset[i] = make([]bool, maxPossibleSum+1)
	}

	for i := int64(0); i <= n; i++ {
		subset[i][0] = true
	}

	for i := int64(1); i <= int64(maxPossibleSum); i++ {
		subset[0][i] = false
	}

	for i := int64(1); i <= n; i++ {
		for j := int64(1); j <= int64(maxPossibleSum); j++ {
			if j < assets[i-1].TotalSize/units.MiB {
				subset[i][j] = subset[i-1][j]
			}
			if j >= assets[i-1].TotalSize/units.MiB {
				subset[i][j] = subset[i-1][j] || subset[i-1][j-assets[i-1].TotalSize/units.MiB]
			}
		}
	}

	if subset[n][target] {
		return target
	}

	closestSum := int64(0)
	if subset[n][target] {
		closestSum = target
	} else {
		for i := 1; target-int64(i) >= 0 || target+int64(i) <= maxPossibleSum; i++ {
			lf := target-int64(i) >= 0 && subset[n][target-int64(i)]
			rf := target+int64(i) <= maxPossibleSum && subset[n][target+int64(i)]
			if lf || rf {
				if lf && !rf {
					closestSum = target - int64(i)
				}
				if !lf && rf {
					closestSum = target + int64(i)
				}
				if lf && rf {
					closestSum = target - int64(i)
				}
				break
			}
		}
	}

	return closestSum
}

// dpGetRemoveAssets returns the combination of to-remove list with a specified size
func dpGetRemoveAssets(assets []*types.AssetRecord, sum int64) []*types.AssetRecord {
	n := int64(len(assets))

	subset := make([][]bool, n+1)
	for i := range subset {
		subset[i] = make([]bool, sum+1)
	}

	for i := int64(0); i <= n; i++ {
		subset[i][0] = true
	}

	for i := int64(1); i <= sum; i++ {
		subset[0][i] = false
	}

	for i := int64(1); i <= n; i++ {
		for j := int64(1); j <= sum; j++ {
			if j < assets[i-1].TotalSize/units.MiB {
				subset[i][j] = subset[i-1][j]
			}
			if j >= assets[i-1].TotalSize/units.MiB {
				subset[i][j] = subset[i-1][j] || subset[i-1][j-assets[i-1].TotalSize/units.MiB]
			}
		}
	}

	if !subset[n][sum] {
		return nil
	}

	sub := make([]*types.AssetRecord, 0)
	i, j := n, sum
	for i > 0 && j > 0 {
		if subset[i][j] != subset[i-1][j] {
			sub = append(sub, assets[i-1])
			j -= assets[i-1].TotalSize / units.MiB
		}
		i--
	}

	return sub
}

// AssignTunserverURL
func (s *Scheduler) AssignTunserverURL(ctx context.Context) (*types.TunserverRsp, error) {
	nodeID := handler.GetNodeID(ctx)
	if len(nodeID) == 0 {
		return nil, fmt.Errorf("invalid request")
	}

	var vNode *node.Node
	wID, err := s.db.LoadWSServerID(nodeID)
	if err == nil && wID != "" {
		vNode = s.NodeManager.GetCandidateNode(wID)
	}

	if vNode == nil {
		// select candidate
		_, list := s.NodeManager.GetAllCandidateNodes()
		if len(list) > 0 {
			index := rand.Intn(len(list))
			vNode = list[index]
		}
	}

	if vNode == nil {
		return nil, fmt.Errorf("node not found")
	}

	wsURL := vNode.WsURL()
	vID := vNode.NodeID

	return &types.TunserverRsp{URL: wsURL, NodeID: vID}, nil
}

func (s *Scheduler) UpdateTunserverURL(ctx context.Context, nodeID string) error {
	nID := handler.GetNodeID(ctx)
	if len(nID) == 0 {
		return fmt.Errorf("invalid request")
	}

	node := s.NodeManager.GetEdgeNode(nID)
	if node != nil {
		node.WSServerID = nodeID
	}

	return s.db.SaveWSServerID(nID, nodeID)
}

// GetProjectsForNode
func (s *Scheduler) GetProjectsForNode(ctx context.Context, nodeID string) ([]*types.ProjectReplicas, error) {
	nID := handler.GetNodeID(ctx)
	if nID != "" {
		nodeID = nID
	}

	list, err := s.db.LoadProjectReplicasForNode(nodeID)
	if err != nil {
		return nil, err
	}

	for _, info := range list {
		pInfo, err := s.db.LoadProjectInfo(info.Id)
		if err != nil {
			continue
		}

		info.BundleURL = pInfo.BundleURL
	}

	return list, nil
}

func (s *Scheduler) GetNodesFromRegion(ctx context.Context, areaID string) ([]*types.NodeInfo, error) {
	continent, country, province, city := region.DecodeAreaID(areaID)
	if continent != "" {
		return s.NodeManager.FindNodesFromGeo(continent, country, province, city), nil
	}

	return nil, xerrors.Errorf("continent is nil ; %s", areaID)
}

func (s *Scheduler) GetCurrentRegionInfos(ctx context.Context, areaID string) (map[string]int, error) {
	continent, country, province, _ := region.DecodeAreaID(areaID)
	return s.NodeManager.GetGeoKey(continent, country, province), nil
}
