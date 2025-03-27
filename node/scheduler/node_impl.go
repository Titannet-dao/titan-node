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

	// Interval for initiating free space release
	freeUpDayInterval = 1
)

// GetOnlineNodeCount returns the count of online nodes for a given node type
func (s *Scheduler) GetOnlineNodeCount(ctx context.Context, nodeType types.NodeType) (int, error) {
	if nodeType == types.NodeUnknown || nodeType == types.NodeEdge {
		return s.NodeManager.GetOnlineNodeCount(nodeType), nil
	}

	i := 0
	_, nodes := s.NodeManager.GetValidCandidateNodes()
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
	if nodeType != types.NodeCandidate {
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

	// isValidator := false
	// if nodeType == types.NodeValidator {
	// 	isValidator = true
	// 	// nodeType = types.NodeCandidate
	// }

	if err = s.db.NodeExistsFromType(nodeID, nodeType); err == nil {
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

	// if isValidator {
	// err = s.db.UpdateValidators([]string{nodeID}, s.ServerID, false)
	// if err != nil {
	// 	log.Errorf("RegisterNode UpdateValidators %s err:%s", nodeID, err.Error())
	// }
	// }

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
	if nodeType != types.NodeEdge && nodeType != types.NodeL5 && nodeType != types.NodeL3 {
		return nil, xerrors.New("invalid node type")
	}

	if !strings.HasPrefix(nodeID, "e_") && !strings.HasPrefix(nodeID, "l5_") {
		return nil, xerrors.New("invalid edge node id")
	}

	if publicKey == "" {
		return nil, xerrors.New("public key is nil")
	}

	_, err = titanrsa.Pem2PublicKey([]byte(publicKey))
	if err != nil {
		return nil, xerrors.Errorf("pem to publicKey err : %s", err.Error())
	}

	if err = s.db.NodeExistsFromType(nodeID, nodeType); err == nil {
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

// RegisterNodeV2 register edge node, return key
func (s *Scheduler) RegisterNodeV2(ctx context.Context, info types.NodeRegister) (*types.ActivationDetail, error) {
	if info.NodeType == types.NodeEdge {
		if !s.areaMatch(info.AreaID) {
			return nil, xerrors.Errorf("%s area is not match %s", info.NodeID, info.AreaID)
		}

		return s.RegisterNode(ctx, info.NodeID, info.PublicKey, types.NodeEdge)
	} else if info.NodeType == types.NodeCandidate {
		if !s.areaMatch(info.AreaID) {
			cInfo, err := s.db.GetCandidateCodeInfo(info.Code)
			if err != nil {
				return nil, err
			}

			if !cInfo.IsTest {
				return nil, xerrors.Errorf("%s area is not match %s", info.NodeID, info.AreaID)
			}
		}

		return s.RegisterCandidateNode(ctx, info.NodeID, info.PublicKey, info.Code)
	}

	return s.RegisterNode(ctx, info.NodeID, info.PublicKey, info.NodeType)
}

func (s *Scheduler) areaMatch(nodeArea string) bool {
	sID, isC := s.getAreaInfo()
	if isC {
		return true
	}

	parts := strings.Split(nodeArea, "-")
	if len(parts) < 2 {
		return false
	}

	continent := strings.ToLower(strings.Replace(parts[0], " ", "", -1))
	country := strings.ToLower(strings.Replace(parts[1], " ", "", -1))
	areaID := fmt.Sprintf("%s-%s", continent, country)

	return sID == areaID
}

// RegisterEdgeNode register edge node, return key
func (s *Scheduler) RegisterEdgeNode(ctx context.Context, nodeID, publicKey string) (*types.ActivationDetail, error) {
	return s.RegisterNode(ctx, nodeID, publicKey, types.NodeEdge)
}

// ForceNodeOffline changes the online status of a node identified by nodeID.
// If the status is true, it forces the node offline. If the status is false, it brings the node back online.
func (s *Scheduler) ForceNodeOffline(ctx context.Context, nodeID string, forceOffline bool) error {
	if forceOffline {
		// is candidate
		err := s.db.NodeExistsFromType(nodeID, types.NodeCandidate)
		if err == nil {
			// if node is candidate , need to backup asset
			s.AssetManager.CandidateDeactivate(nodeID)
		}
	}

	err := s.db.SaveForceOffline(nodeID, forceOffline)
	if err != nil {
		return xerrors.Errorf("SaveDeactivateNode %s err : %s", nodeID, err.Error())
	}

	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.ForceOffline = forceOffline
	}

	return nil
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

	// is candidate
	err := s.db.NodeExistsFromType(nodeID, types.NodeCandidate)
	if err != nil {
		return err
	}

	deactivateTime, err := s.db.LoadDeactivateNodeTime(nodeID)
	if err != nil {
		return xerrors.Errorf("LoadDeactivateNodeTime %s err : %s", nodeID, err.Error())
	}

	if deactivateTime > 0 {
		return xerrors.Errorf("node %s is waiting to deactivate", nodeID)
	}

	minute := hours * 60
	if minute <= 0 {
		minute = 30
	}

	penaltyPoint := 0.0

	info, err := s.db.LoadNodeInfo(nodeID)
	if err != nil {
		return err
	}
	// if node is candidate , need to backup asset
	s.AssetManager.CandidateDeactivate(nodeID)

	pe, _ := s.NodeManager.CalculateDowntimePenalty(info.Profit)
	penaltyPoint = info.Profit - pe

	deactivateTime = time.Now().Add(time.Duration(minute) * time.Minute).Unix()

	log.Warnf("DeactivateNode:[%s] Profit:[%.2f] - [%.2f] = [%.2f]", nodeID, info.Profit, penaltyPoint, pe)

	err = s.db.SaveDeactivateNode(nodeID, deactivateTime, penaltyPoint, 0)
	if err != nil {
		return xerrors.Errorf("SaveDeactivateNode %s err : %s", nodeID, err.Error())
	}

	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.DeactivateTime = deactivateTime
		node.Profit -= penaltyPoint
		s.NodeManager.RepayNodeWeight(node)
		// remove from validation
	}

	return nil
}

// MigrateNodeOut Migrate out the node
func (s *Scheduler) MigrateNodeOut(ctx context.Context, nodeID string) (*types.NodeMigrateInfo, error) {
	out := &types.NodeMigrateInfo{}

	rInfo, err := s.db.LoadNodeRegisterInfo(nodeID)
	if err != nil {
		return nil, err
	}

	// if rInfo.NodeType != types.NodeCandidate {
	// 	return nil, xerrors.Errorf("node :%s not candidate", nodeID)
	// }

	nInfo, err := s.db.LoadNodeInfo(nodeID)
	if err != nil {
		return nil, err
	}

	pInfo, err := s.db.LoadNodeProfits(nodeID, 200, 0, []int{0, 1, 2, 3, 4, 5, 6, 7, 8})
	if err != nil {
		return nil, err
	}

	now := time.Now()
	onlineCounts := make(map[time.Time]int)
	for i := 0; i < 8; i++ {
		date := now.AddDate(0, 0, -i)
		date = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())

		count, err := s.db.GetOnlineCount(nodeID, date)
		if err == nil {
			onlineCounts[date] = count
		}
	}

	var cInfo *types.CandidateCodeInfo
	if rInfo.NodeType == types.NodeCandidate {
		cInfo, err = s.db.GetCandidateCodeInfoForNodeID(nodeID)
		if err != nil {
			return nil, err
		}
	}

	out.ActivationInfo = rInfo
	out.NodeInfo = nInfo
	out.OnlineCounts = onlineCounts
	out.CodeInfo = cInfo
	out.Key = uuid.NewString()
	out.ProfitList = pInfo.Infos

	err = s.db.SaveMigrateKey(out.Key, nodeID)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// MigrateNodeIn Migrate in the node
func (s *Scheduler) MigrateNodeIn(ctx context.Context, info *types.NodeMigrateInfo) error {
	if info.ActivationInfo == nil || info.NodeInfo == nil {
		return xerrors.New("Parameter cannot be empty")
	}

	info.NodeInfo.SchedulerID = s.ServerID

	return s.db.MigrateNodeDetails(info)
}

// CleanupNode removes residual data from the source server after a node has been migrated.
func (s *Scheduler) CleanupNode(ctx context.Context, nodeID, key string) error {
	rInfo, err := s.db.LoadNodeRegisterInfo(nodeID)
	if err != nil {
		return err
	}

	if rInfo.MigrateKey != key {
		return xerrors.Errorf("Migrate key mismatch [%s] [%s]", key, rInfo.MigrateKey)
	}

	s.AssetManager.CandidateDeactivate(nodeID)

	err = s.db.CleanNodeInfo(nodeID)
	if err != nil {
		return err
	}

	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		s.NodeManager.SetNodeOffline(node)
	}

	return nil
}

// CalculateDowntimePenalty calculates the penalty points to be deducted for a node's requested downtime.
func (s *Scheduler) CalculateDowntimePenalty(ctx context.Context, nodeID string) (types.ExitProfitRsp, error) {
	nID := handler.GetNodeID(ctx)
	if len(nID) > 0 {
		nodeID = nID
	}

	err := s.db.NodeExistsFromType(nodeID, types.NodeCandidate)
	if err != nil {
		return types.ExitProfitRsp{}, err
	}

	info, err := s.db.LoadNodeInfo(nodeID)
	if err != nil {
		return types.ExitProfitRsp{}, err
	}

	pe, exitRate := s.NodeManager.CalculateDowntimePenalty(info.Profit)
	return types.ExitProfitRsp{
		CurrentPoint:   info.Profit,
		RemainingPoint: pe,
		PenaltyRate:    exitRate,
	}, err
}

// AssignTunserverURL assigns a Tunserver URL.
func (s *Scheduler) AssignTunserverURL(ctx context.Context) (*types.TunserverRsp, error) {
	return &types.TunserverRsp{}, nil
}

// CalculateExitProfit Deprecated
func (s *Scheduler) CalculateExitProfit(ctx context.Context, nodeID string) (types.ExitProfitRsp, error) {
	return s.CalculateDowntimePenalty(ctx, nodeID)
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

	err = s.db.SaveDeactivateNode(nodeID, 0, 0, deactivateTime)
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

// L3Connect l3 node login to the scheduler
func (s *Scheduler) L3Connect(ctx context.Context, opts *types.ConnectOptions) error {
	return s.lnNodeConnected(ctx, opts, types.NodeL3)
}

// L5Connect l5 node login to the scheduler
func (s *Scheduler) L5Connect(ctx context.Context, opts *types.ConnectOptions) error {
	return s.lnNodeConnected(ctx, opts, types.NodeL5)
}

func (s *Scheduler) lnNodeConnected(ctx context.Context, opts *types.ConnectOptions, nType types.NodeType) error {
	// return s.nodeConnect(ctx, opts, types.NodeEdge)
	remoteAddr := handler.GetRemoteAddr(ctx)
	nodeID := handler.GetNodeID(ctx)

	nNode := s.NodeManager.GetNode(nodeID)
	if nNode == nil {
		if err := s.db.NodeExistsFromType(nodeID, nType); err != nil {
			return xerrors.Errorf("node: %s, type: %d, error: %w", nodeID, nType, err)
		}
	}

	// TODO temporary codes
	err := s.db.UpdateNodeType(nodeID, nType)
	if err != nil {
		return xerrors.Errorf("UpdateNodeType %s err : %s", nodeID, err.Error())
	}

	externalIP, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return xerrors.Errorf("nodeConnect err SplitHostPort err:%s", err.Error())
	}

	pStr, err := s.db.LoadNodePublicKey(nodeID)
	if err != nil && err != sql.ErrNoRows {
		return xerrors.Errorf("load node port %s err : %s", nodeID, err.Error())
	}

	publicKey, err := titanrsa.Pem2PublicKey([]byte(pStr))
	if err != nil {
		return xerrors.Errorf("load node port %s err : %s", nodeID, err.Error())
	}

	if nNode == nil {
		nNode = node.New()
	}

	nNode.PublicKey = publicKey
	nNode.Token = opts.Token

	err = nNode.ConnectRPC(s.Transport, remoteAddr, nType)
	if err != nil {
		return err
	}

	// init node info
	nInfo, err := nNode.API.GetNodeInfo(context.Background())
	if err != nil {
		return xerrors.Errorf("%s nodeConnect err NodeInfo err:%s", nodeID, err.Error())
	}

	nodeInfo := s.nodeParametersApplyLimits(nInfo)

	ver := api.NewVerFromString(nodeInfo.SystemVersion)
	nodeInfo.Version = int64(ver)

	nodeInfo.NodeID = nodeID
	nodeInfo.Type = nType
	nodeInfo.RemoteAddr = remoteAddr
	nodeInfo.SchedulerID = s.ServerID
	nodeInfo.ExternalIP = externalIP
	nodeInfo.BandwidthUp = units.MiB
	nodeInfo.BandwidthDown = units.GiB
	nodeInfo.NATType = types.NatTypeUnknown.String()
	nodeInfo.LastSeen = time.Now()
	nodeInfo.FirstTime = time.Now()

	dbInfo, err := s.db.LoadNodeInfo(nodeID)
	if err != nil && err != sql.ErrNoRows {
		return xerrors.Errorf("nodeConnect err load node online duration %s err : %s", nodeID, err.Error())
	}

	if dbInfo != nil {
		// init node info
		nodeInfo.PortMapping = dbInfo.PortMapping
		nodeInfo.OnlineDuration = dbInfo.OnlineDuration
		nodeInfo.OfflineDuration = dbInfo.OfflineDuration
		nodeInfo.BandwidthDown = dbInfo.BandwidthDown
		nodeInfo.BandwidthUp = dbInfo.BandwidthUp
		nodeInfo.DownloadTraffic = dbInfo.DownloadTraffic
		nodeInfo.UploadTraffic = dbInfo.UploadTraffic
		nodeInfo.FirstTime = dbInfo.FirstTime
	}

	nNode.OnlineRate = s.NodeManager.ComputeNodeOnlineRate(nodeID, nodeInfo.FirstTime)

	nNode.InitInfo(nodeInfo)

	log.Infof("ln node %s connected, version %s remoteAddr %s", nodeID, ver, remoteAddr)

	err = s.saveNodeInfo(nodeInfo)
	if err != nil {
		return err
	}

	// node
	return s.NodeManager.NodeOnline(nNode, nodeInfo)
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

	if nType == types.NodeEdge || nType == types.NodeL3 {
		p.Allow = append(p.Allow, api.RoleEdge)
	} else if nType == types.NodeCandidate {
		p.Allow = append(p.Allow, api.RoleCandidate)
	} else if nType == types.NodeL5 {
		p.Allow = append(p.Allow, api.RoleL5)
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
func (s *Scheduler) GetNodeInfo(ctx context.Context, nodeID string) (*types.NodeInfo, error) {
	nodeInfo, err := s.NodeManager.LoadNodeInfo(nodeID)
	if err != nil {
		return nil, xerrors.Errorf("nodeID %s LoadNodeInfo err:%s", nodeID, err.Error())
	}

	today := time.Now()
	todayDate := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, today.Location())

	nodeInfo.Status = types.NodeOffline

	nodeInfo.ReplicaCount, err = s.NodeManager.LoadSucceedReplicaCountNodeID(nodeInfo.NodeID)

	n := s.NodeManager.GetNode(nodeID)
	if n != nil {
		nodeInfo.Status = types.NodeServicing
		nodeInfo.NATType = n.NATType
		nodeInfo.Type = n.Type
		nodeInfo.CPUUsage = n.CPUUsage
		nodeInfo.MemoryUsage = n.MemoryUsage
		nodeInfo.DiskUsage = n.DiskUsage
		nodeInfo.DiskSpace = n.DiskSpace
		nodeInfo.ExternalIP = n.ExternalIP
		nodeInfo.IncomeIncr = n.IncomeIncr
		nodeInfo.IsTestNode = n.IsTestNode
		nodeInfo.AreaID = n.AreaID
		nodeInfo.RemoteAddr = n.RemoteAddr
		nodeInfo.Mx = node.RateOfL2Mx(n.OnlineDuration)
		nodeInfo.TodayOnlineTimeWindow = s.loadNodeTodayOnlineTimeWindow(nodeID, todayDate)

		log.Debugf("%s node select codes:%v , url:%s", nodeID, n.SelectWeights(), n.ExternalURL)
	}

	return nodeInfo, nil
}

func (s *Scheduler) loadNodeTodayOnlineTimeWindow(nodeID string, todayDate time.Time) int {
	todayCount, _ := s.db.GetOnlineCount(nodeID, todayDate)

	return todayCount
}

// GetNodeList retrieves a list of nodes with pagination.
func (s *Scheduler) GetNodeList(ctx context.Context, offset int, limit int) (*types.ListNodesRsp, error) {
	info := &types.ListNodesRsp{Data: make([]types.NodeInfo, 0)}

	rows, total, err := s.NodeManager.LoadActiveNodeInfos(limit, offset)
	if err != nil {
		return nil, xerrors.Errorf("LoadNodeInfos err:%s", err.Error())
	}
	defer rows.Close()

	today := time.Now()
	todayDate := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, today.Location())

	nodeInfos := make([]types.NodeInfo, 0)
	for rows.Next() {
		nodeInfo := &types.NodeInfo{}
		err = rows.StructScan(nodeInfo)
		if err != nil {
			log.Errorf("NodeInfo StructScan err: %s", err.Error())
			continue
		}

		sInfo, err := s.NodeManager.LoadNodeStatisticsInfo(nodeInfo.NodeID)
		if err == nil {
			nodeInfo.NodeStatisticsInfo = sInfo
		}

		nodeInfo.ReplicaCount, err = s.NodeManager.LoadSucceedReplicaCountNodeID(nodeInfo.NodeID)

		n := s.NodeManager.GetNode(nodeInfo.NodeID)
		if n != nil {
			nodeInfo.Status = types.NodeServicing
			nodeInfo.NATType = n.NATType
			nodeInfo.Type = n.Type
			nodeInfo.CPUUsage = n.CPUUsage
			nodeInfo.MemoryUsage = n.MemoryUsage
			nodeInfo.DiskUsage = n.DiskUsage
			nodeInfo.DiskSpace = n.DiskSpace
			nodeInfo.ExternalIP = n.ExternalIP
			nodeInfo.IncomeIncr = n.IncomeIncr
			nodeInfo.IsTestNode = n.IsTestNode
			nodeInfo.AreaID = n.AreaID
			nodeInfo.RemoteAddr = n.RemoteAddr
			nodeInfo.Mx = node.RateOfL2Mx(n.OnlineDuration)
			nodeInfo.TodayOnlineTimeWindow = s.loadNodeTodayOnlineTimeWindow(nodeInfo.NodeID, todayDate)
		}

		nodeInfos = append(nodeInfos, *nodeInfo)
	}

	info.Data = nodeInfos
	info.Total = total

	return info, nil
}

// GetCandidateURLsForDetectNat returns candidate URLs for NAT detection.
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

		token, err := eNode.EncryptToken(cid, uuid.NewString(), titanRsa, s.NodeManager.PrivateKey)
		if err != nil {
			continue
		}

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

// GetNodeToken retrieves the token for the specified node.
func (s *Scheduler) GetNodeToken(ctx context.Context, nodeID string) (string, error) {
	node := s.NodeManager.GetNode(nodeID)
	if node == nil {
		return "", xerrors.Errorf("node %s not find ", nodeID)
	}

	return node.Token, nil
}

// GetNodeOfIP get nodes of ip
func (s *Scheduler) GetNodeOfIP(ctx context.Context, ip string) ([]string, error) {
	return s.NodeManager.IPMgr.GetNodeOfIP(ip), nil
}

// CheckIpUsage checks if a specific IP address is present on the server.
func (s *Scheduler) CheckIpUsage(ctx context.Context, ip string) (bool, error) {
	if s.NodeManager.IPMgr.CheckIPExist(ip) {
		return true, nil
	}

	count, err := s.db.RegisterCount(ip)
	return count > 0, err
}

func (s *Scheduler) getEdgeDownloadRatio() float64 {
	return s.SchedulerCfg.EdgeDownloadRatio
}

func (s *Scheduler) getSource(cNode *node.Node, cid string, titanRsa *titanrsa.Rsa) *types.SourceDownloadInfo {
	token, err := cNode.EncryptToken(cid, uuid.NewString(), titanRsa, s.NodeManager.PrivateKey)
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

func (s *Scheduler) getDownloadInfos(cid string, needCandidate bool) (*types.AssetSourceDownloadInfoRsp, int64, int, error) {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, 0, 0, xerrors.Errorf("GetAssetSourceDownloadInfo %s cid to hash err:%s", cid, err.Error())
	}

	aInfo, err := s.db.LoadAssetRecord(hash)
	if err != nil {
		return nil, 0, 0, err
	}

	out := &types.AssetSourceDownloadInfoRsp{}
	if aInfo.Source == int64(types.AssetSourceAWS) {
		out.AWSBucket = aInfo.Note
	}

	replicas, err := s.db.LoadReplicasByStatus(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		return out, 0, 0, err
	}

	count := len(replicas)
	if count == 0 {
		return out, 0, count, nil
	}

	type nodeBandwidthUp struct {
		NodeID      string
		BandwidthUp int64
	}

	list := []*nodeBandwidthUp{}
	for _, rInfo := range replicas {
		nodeID := rInfo.NodeID

		cNode := s.NodeManager.GetNode(nodeID)
		if cNode != nil {
			list = append(list, &nodeBandwidthUp{NodeID: nodeID, BandwidthUp: cNode.BandwidthUp})
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].BandwidthUp > list[j].BandwidthUp
	})

	// // Shuffle array
	// for i := len(replicas) - 1; i > 0; i-- {
	// 	j := rand.Intn(i + 1)
	// 	replicas[i], replicas[j] = replicas[j], replicas[i]
	// }

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sources := make([]*types.SourceDownloadInfo, 0)

	for i := 0; i < len(list); i++ {
		rInfo := list[i]
		nodeID := rInfo.NodeID
		// candidate
		cNode := s.NodeManager.GetCandidateNode(nodeID)
		if cNode != nil {
			source := s.getSource(cNode, cid, titanRsa)
			if source != nil {
				sources = append(sources, source)
			}

			continue
		}

		// edge
		if needCandidate {
			continue
		}

		if len(sources) > 10 {
			continue
		}

		eNode := s.NodeManager.GetEdgeNode(nodeID)
		if eNode == nil {
			continue
		}

		if eNode.NATType == types.NatTypeNo.String() || eNode.NATType == types.NatTypeFullCone.String() {
			source := s.getSource(cNode, cid, titanRsa)
			if source != nil {
				sources = append(sources, source)
			}
		}
	}

	if len(sources) == 0 {
		return out, 0, count, nil
	}

	rand.Shuffle(len(sources), func(i, j int) {
		sources[i], sources[j] = sources[j], sources[i]
	})

	out.SourceList = sources

	return out, aInfo.TotalSize, count, nil
}

// GetAssetSourceDownloadInfo retrieves the download details for a specified asset.
func (s *Scheduler) GetAssetSourceDownloadInfo(ctx context.Context, cid string) (*types.AssetSourceDownloadInfoRsp, error) {
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

	// TODO need good BandwidthUp
	// log.Infof("GetAssetSourceDownloadInfo clientID:%s, cid:%s , nodeID:%s", clientID, cid, nodeID)
	out, totalSize, _, err := s.getDownloadInfos(cid, false)
	if err != nil {
		return nil, err
	}

	// init workload
	// if event == types.WorkloadEventSync {
	out.WorkloadID = uuid.NewString()

	ws := make([]*types.Workload, 0)
	for _, info := range out.SourceList {
		ws = append(ws, &types.Workload{SourceID: info.NodeID})
	}

	buffer := &bytes.Buffer{}
	enc := gob.NewEncoder(buffer)
	err = enc.Encode(ws)
	if err != nil {
		log.Errorf("GetAssetSourceDownloadInfo encode error:%s", err.Error())
		return out, nil
	}

	record := &types.WorkloadRecord{
		WorkloadID: out.WorkloadID,
		AssetCID:   cid,
		ClientID:   clientID,
		AssetSize:  totalSize,
		Workloads:  buffer.Bytes(),
		Event:      event,
		Status:     types.WorkloadStatusCreate,
	}

	if err = s.NodeManager.SaveWorkloadRecord([]*types.WorkloadRecord{record}); err != nil {
		log.Errorf("GetAssetSourceDownloadInfo SaveWorkloadRecord error:%s", err.Error())
		return out, nil
	}
	// }

	return out, nil
}

// GetCandidateDownloadInfos finds candidate download info for the given CID.
func (s *Scheduler) GetCandidateDownloadInfos(ctx context.Context, cid string) ([]*types.CandidateDownloadInfo, error) {
	return nil, xerrors.New("The interface has been deprecated")
}

// NodeExists checks if the node with the specified ID exists.
func (s *Scheduler) NodeExists(ctx context.Context, nodeID string) error {
	return s.NodeManager.NodeExists(nodeID)
}

// NodeKeepaliveV3 candidate and edge keepalive
func (s *Scheduler) NodeKeepaliveV3(ctx context.Context, req *types.KeepaliveReq) (*types.KeepaliveRsp, error) {
	uuid, err := s.CommonAPI.Session(ctx)
	if err != nil {
		return nil, err
	}

	nodeID := handler.GetNodeID(ctx)
	if len(nodeID) == 0 {
		return nil, fmt.Errorf("Can not get nodeID from request")
	}

	node := s.NodeManager.GetNode(nodeID)
	if node == nil {
		return &types.KeepaliveRsp{ErrCode: int(terrors.NodeOffline), ErrMsg: fmt.Sprintf("node %s offline or not exist", nodeID)}, nil
	}

	if node.ForceOffline {
		return &types.KeepaliveRsp{ErrCode: int(terrors.ForceOffline), ErrMsg: fmt.Sprintf("The node %s has been forced offline", nodeID)}, nil
	}

	if node.Type == types.NodeCandidate {
		now := time.Now().Unix()
		log.Infof("NodeKeepaliveV3 node [%s] DeactivateTime:[%d] , [%d] \n", nodeID, node.DeactivateTime, now)

		if node.DeactivateTime > 0 && node.DeactivateTime < now {
			return &types.KeepaliveRsp{ErrCode: int(terrors.NodeDeactivate), ErrMsg: fmt.Sprintf("The node %s has been deactivate and cannot be logged in", nodeID)}, nil
		}

		if node.NATType == types.NatTypePortRestricted.String() || node.NATType == types.NatTypeRestricted.String() || node.NATType == types.NatTypeSymmetric.String() {
			return nil, xerrors.Errorf("The NAT type [%s] of the node [%s] does not conform to the rules", node.NATType, nodeID)
		}

		if node.NATType != types.NatTypeUnknown.String() && !node.IsStorageNode && !node.IsTestNode {
			return nil, xerrors.Errorf("%s checkDomain %s ", nodeID, node.ExternalURL)
		}

		if req != nil {
			node.SetBandwidths(req.Free, req.Peak)
		}
	}

	lastTime := time.Now()
	remoteAddr := handler.GetRemoteAddr(ctx)
	if remoteAddr != node.RemoteAddr {
		count, lastTime := node.GetNumberOfIPChanges()
		duration := time.Now().Sub(lastTime)
		seconds := duration.Seconds()

		if seconds > 10*6*20 {
			node.SetCountOfIPChanges(0)

			if count > 120 {
				log.Infof("NodeKeepaliveV3 Exceeded expectations %s , ip:%s : %s, count:%d ,resetSeconds:%.2f ", nodeID, remoteAddr, node.RemoteAddr, count, seconds)
			}
			return &types.KeepaliveRsp{ErrCode: int(terrors.NodeIPInconsistent), ErrMsg: fmt.Sprintf("node %s new ip %s, old ip %s, resetSeconds:%.2f , resetCount:%d", nodeID, remoteAddr, node.RemoteAddr, seconds, count)}, nil
		}

		count++
		node.SetCountOfIPChanges(count)
	}

	node.SetLastRequestTime(lastTime)
	return &types.KeepaliveRsp{SessionID: uuid.String(), SessionUUID: uuid}, nil
}

// NodeKeepalive candidate and edge keepalive
func (s *Scheduler) NodeKeepalive(ctx context.Context) (*types.KeepaliveRsp, error) {
	uuid, err := s.CommonAPI.Session(ctx)
	if err != nil {
		return nil, err
	}

	nodeID := handler.GetNodeID(ctx)
	if len(nodeID) == 0 {
		return nil, fmt.Errorf("Can not get nodeID from request")
	}

	node := s.NodeManager.GetNode(nodeID)
	if node == nil {
		return &types.KeepaliveRsp{ErrCode: int(terrors.NodeOffline), ErrMsg: fmt.Sprintf("node %s offline or not exist", nodeID)}, nil
	}

	if node.Type == types.NodeCandidate {
		now := time.Now().Unix()
		log.Infof("NodeKeepalive node [%s] DeactivateTime:[%d] , [%d] \n", nodeID, node.DeactivateTime, now)

		if node.DeactivateTime > 0 && node.DeactivateTime < now {
			return &types.KeepaliveRsp{ErrCode: int(terrors.NodeDeactivate), ErrMsg: fmt.Sprintf("The node %s has been deactivate and cannot be logged in", nodeID)}, nil
		}
	}

	if node.ForceOffline {
		return &types.KeepaliveRsp{ErrCode: int(terrors.ForceOffline), ErrMsg: fmt.Sprintf("The node %s has been forced offline", nodeID)}, nil
	}

	if node.Type == types.NodeCandidate {
		if node.NATType == types.NatTypePortRestricted.String() || node.NATType == types.NatTypeRestricted.String() || node.NATType == types.NatTypeSymmetric.String() {
			return nil, xerrors.Errorf("The NAT type [%s] of the node [%s] does not conform to the rules", node.NATType, nodeID)
		}

		if node.NATType != types.NatTypeUnknown.String() && !node.IsStorageNode && !node.IsTestNode {
			return nil, xerrors.Errorf("%s checkDomain %s ", nodeID, node.ExternalURL)
		}
	}

	lastTime := time.Now()
	remoteAddr := handler.GetRemoteAddr(ctx)
	if remoteAddr != node.RemoteAddr {
		count, lastTime := node.GetNumberOfIPChanges()
		duration := time.Now().Sub(lastTime)
		seconds := duration.Seconds()

		if seconds > 10*6*20 {
			node.SetCountOfIPChanges(0)

			if count > 120 {
				log.Infof("NodeKeepaliveV2 Exceeded expectations %s , ip:%s : %s, count:%d ,resetSeconds:%.2f ", nodeID, remoteAddr, node.RemoteAddr, count, seconds)
			}
			return &types.KeepaliveRsp{ErrCode: int(terrors.NodeIPInconsistent), ErrMsg: fmt.Sprintf("node %s new ip %s, old ip %s, resetSeconds:%.2f , resetCount:%d", nodeID, remoteAddr, node.RemoteAddr, seconds, count)}, nil
		}

		count++
		node.SetCountOfIPChanges(count)
	}

	node.SetLastRequestTime(lastTime)
	return &types.KeepaliveRsp{SessionID: uuid.String()}, nil
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
			if node.Type == types.NodeCandidate {
				now := time.Now().Unix()
				log.Infof("NodeKeepaliveV2 node [%s] DeactivateTime:[%d] , [%d] \n", nodeID, node.DeactivateTime, now)

				if node.DeactivateTime > 0 && node.DeactivateTime < now {
					return uuid, &api.ErrNode{Code: int(terrors.NodeDeactivate), Message: fmt.Sprintf("The node %s has been deactivate and cannot be logged in", nodeID)}
				}
			}

			if node.ForceOffline {
				return uuid, &api.ErrNode{Code: int(terrors.ForceOffline), Message: fmt.Sprintf("The node %s has been forced offline", nodeID)}
			}

			if node.Type == types.NodeCandidate {
				if node.NATType == types.NatTypePortRestricted.String() || node.NATType == types.NatTypeRestricted.String() || node.NATType == types.NatTypeSymmetric.String() {
					return uuid, xerrors.Errorf("The NAT type [%s] of the node [%s] does not conform to the rules", node.NATType, nodeID)
				}

				if node.NATType != types.NatTypeUnknown.String() && !node.IsStorageNode && !node.IsTestNode {
					return uuid, xerrors.Errorf("%s checkDomain %s ", nodeID, node.ExternalURL)
				}
			}

			if remoteAddr != node.RemoteAddr {
				count, lastTime := node.GetNumberOfIPChanges()
				duration := time.Now().Sub(lastTime)
				seconds := duration.Seconds()

				if seconds > 10*6*20 {
					node.SetCountOfIPChanges(0)

					if count > 120 {
						log.Infof("NodeKeepaliveV2 Exceeded expectations %s , ip:%s : %s, count:%d ,resetSeconds:%.2f ", nodeID, remoteAddr, node.RemoteAddr, count, seconds)
					}

					return uuid, &api.ErrNode{Code: int(terrors.NodeIPInconsistent), Message: fmt.Sprintf("node %s new ip %s, old ip %s, resetSeconds:%.2f , resetCount:%d", nodeID, remoteAddr, node.RemoteAddr, seconds, count)}
				}

				count++
				node.SetCountOfIPChanges(count)
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

// GetMinioConfigFromCandidate retrieves the MinIO configuration for the specified candidate node.
func (s *Scheduler) GetMinioConfigFromCandidate(ctx context.Context, nodeID string) (*types.MinioConfig, error) {
	node := s.NodeManager.GetCandidateNode(nodeID)
	if node == nil {
		return nil, fmt.Errorf("node %s does not exist", nodeID)
	}

	return node.API.GetMinioConfig(ctx)
}

// GetCandidateIPs returns a list of candidate node IP information.
func (s *Scheduler) GetCandidateIPs(ctx context.Context) ([]*types.NodeIPInfo, error) {
	list := make([]*types.NodeIPInfo, 0)

	_, cNodes := s.NodeManager.GetValidCandidateNodes()
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

// GetNodeOnlineState returns the online state of the node and any error encountered.
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

// GetAssetsInBucket get the assets of the bucket
func (s *Scheduler) GetAssetsInBucket(ctx context.Context, nodeID string, bucketID int, isFromNode bool) ([]string, error) {
	if isFromNode {
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

// PerformSyncData synchronizes data for the specified node.
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

// GetProfitDetailsForNode retrieves profit details for a specific node.
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
	fiveDaysAgo := now.AddDate(0, 0, -freeUpDayInterval)
	nextReleaseTime := t.AddDate(0, 0, +freeUpDayInterval)
	if !t.Before(fiveDaysAgo) {
		return &types.FreeUpDiskResp{NextTime: nextReleaseTime.Unix()}, xerrors.Errorf("Less than %d days have passed since the last release", freeUpDayInterval)
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
			return &types.FreeUpDiskResp{NextTime: now.AddDate(0, 0, +freeUpDayInterval).Unix()}, err
		}
	}

	err = s.db.SaveReplenishBackup(removeList)
	if err != nil {
		log.Errorf("FreeUpDiskSpace %s SaveReplenishBackup err:%s", nodeID, err.Error())
	}

	return &types.FreeUpDiskResp{Hashes: removeList, NextTime: now.AddDate(0, 0, +freeUpDayInterval).Unix()}, nil
}

// GetNextFreeTime returns the next available time slot for the given node.
func (s *Scheduler) GetNextFreeTime(ctx context.Context, nodeID string) (int64, error) {
	nID := handler.GetNodeID(ctx)
	if nID != "" {
		nodeID = nID
	}

	t, err := s.db.LoadFreeUpDiskTime(nodeID)
	if err != nil {
		return 0, err
	}

	return t.AddDate(0, 0, +freeUpDayInterval).Unix(), nil
}

// CandidateCodeExist checks if a candidate code exists in the database.
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

// SetTunserverURL sets the Tunnel server URL for the specified node.
func (s *Scheduler) SetTunserverURL(ctx context.Context, nodeID, wsNodeID string) error {
	return s.NodeManager.SetTunserverURL(nodeID, wsNodeID)
}

// GetTunserverURLFromUser retrieves the Tunnel server URL based on the provided AreaID.
func (s *Scheduler) GetTunserverURLFromUser(ctx context.Context, req *types.TunserverReq) (*types.TunserverRsp, error) {
	geoInfo, err := s.GetGeoInfoFromAreaID(req.AreaID)
	if geoInfo == nil {
		geoInfo, err = s.GetGeoInfo(req.IP)
	}

	nodeID := ""
	if err != nil {
		log.Warnf("GetTunserverURLFromUser user ip:[%s],area:[%s] err:%s", req.IP, req.AreaID, err.Error())

		list := s.NodeManager.GetRandomCandidates(1)
		for nID := range list {
			nodeID = nID
		}
	} else {
		log.Infof("GetTunserverURLFromUser %s get:%s", req.AreaID, geoInfo.Geo)

		list := s.NodeManager.GeoMgr.FindNodesFromGeo(geoInfo.Continent, geoInfo.Country, geoInfo.Province, geoInfo.City, types.NodeCandidate)
		for _, info := range list {
			nodeID = info.NodeID
			break
		}
	}

	node := s.NodeManager.GetNode(nodeID)
	if node == nil {
		return nil, xerrors.Errorf("node not found")
	}

	return &types.TunserverRsp{URL: node.WsURL(), NodeID: nodeID}, nil
}

// GetProjectReplicasForNode retrieves project replicas for a given node based on the provided criteria.
func (s *Scheduler) GetProjectReplicasForNode(ctx context.Context, req *types.NodeProjectReq) (*types.ListProjectReplicaRsp, error) {
	return s.db.LoadProjectReplicasForNode(req.NodeID, req.Limit, req.Offset, req.ProjectID, req.Statuses)
}

// GetProjectsForNode retrieves the project replicas for the given node ID.
func (s *Scheduler) GetProjectsForNode(ctx context.Context, nodeID string) ([]*types.ProjectReplicas, error) {
	nID := handler.GetNodeID(ctx)
	if nID != "" {
		nodeID = nID
	}

	list, err := s.db.LoadAllProjectReplicasForNode(nodeID)
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

// GetNodesFromRegion retrieves nodes from a specified region.
func (s *Scheduler) GetNodesFromRegion(ctx context.Context, areaID string) ([]*types.NodeInfo, error) {
	continent, country, province, city := region.DecodeAreaID(areaID)
	if continent != "" {
		return s.NodeManager.GeoMgr.FindNodesFromGeo(continent, country, province, city, types.NodeEdge), nil
	}

	return nil, xerrors.Errorf("continent is nil ; %s", areaID)
}

// GetCurrentRegionInfos returns the current region information for the given area ID.
func (s *Scheduler) GetCurrentRegionInfos(ctx context.Context, areaID string) (map[string]int, error) {
	continent, country, province, _ := region.DecodeAreaID(areaID)
	return s.NodeManager.GeoMgr.GetGeoKey(continent, country, province), nil
}

// RecompenseNodeProfit processes the recompense profit for a given node.
func (s *Scheduler) RecompenseNodeProfit(ctx context.Context, nodeID, note string, profit float64) error {
	data := s.NodeManager.GetRecompenseProfitDetails(nodeID, profit, note)
	if data != nil {
		err := s.db.AddNodeProfitDetails([]*types.ProfitDetails{data})
		if err != nil {
			return xerrors.Errorf("AddNodeProfit %s,%d, %.4f err:%s", data.NodeID, data.PType, data.Profit, err.Error())
		}
	}

	return nil
}

// CreateTunnel create tunnel for workerd communication
func (s *Scheduler) CreateTunnel(ctx context.Context, req *types.CreateTunnelReq) error {
	nodeID := handler.GetNodeID(ctx)
	if len(nodeID) == 0 {
		return fmt.Errorf("invalid request")
	}

	cNode := s.NodeManager.GetCandidateNode(nodeID)
	if cNode == nil {
		return fmt.Errorf("can not find candidate %s", nodeID)
	}

	req.WsURL = cNode.WsURL()

	eNode := s.NodeManager.GetEdgeNode(req.NodeID)
	if eNode == nil {
		return fmt.Errorf("can not find edge %s", req.NodeID)
	}

	return eNode.CreateTunnel(ctx, req)
}

func (s *Scheduler) LoadNodeBandwidthScores(ctx context.Context, nodeID string, start, end time.Time, limit int, offset int) (*types.ListBandwidthScoreRsp, error) {
	// return s.db.LoadBandwidthScoresOfNode(nodeID, limit, offset, start, end)
	return nil, nil
}

func (s *Scheduler) AddNodeServiceEvent(ctx context.Context, event *types.ServiceEvent) error {
	err := s.db.NodeExists(event.NodeID)
	if err != nil {
		return err
	}

	// node := s.NodeManager.GetNode(event.NodeID)
	// if node != nil {
	// 	node.AddServiceEvent(event)
	// }

	// TODO
	event.Score = 10

	return s.db.SaveServiceEvent(event)
}

func (s *Scheduler) LoadServiceEvents(ctx context.Context, startTime, endTime time.Time) (map[string]*types.ServiceStats, error) {
	// TODO cache
	events, err := s.db.LoadServiceEvents(startTime, endTime)
	if err != nil {
		return nil, err
	}

	nodeStats := make(map[string]*types.ServiceStats)

	for _, event := range events {
		nodeID := event.NodeID

		statsInfo, ok := nodeStats[nodeID]
		if !ok {
			statsInfo = &types.ServiceStats{NodeID: nodeID, DownloadSpeeds: []int64{}, UploadSpeeds: []int64{}, Scores: []int64{}}
			nodeStats[nodeID] = statsInfo
		}

		if event.Type == types.ServiceTypeUpload { // Upload
			statsInfo.UploadTotalCount++

			if event.Status == types.ServiceTypeSucceed {
				statsInfo.UploadSuccessCount++
				statsInfo.UploadSpeeds = append(statsInfo.UploadSpeeds, event.Speed)
				statsInfo.Scores = append(statsInfo.Scores, event.Score)
			} else if event.Status == types.ServiceTypeFailed {
				statsInfo.UploadFailCount++
			}
		} else if event.Type == types.ServiceTypeDownload { // Download
			statsInfo.DownloadTotalCount++

			if event.Status == types.ServiceTypeSucceed {
				statsInfo.DownloadSuccessCount++
				statsInfo.DownloadSpeeds = append(statsInfo.DownloadSpeeds, event.Speed)
				statsInfo.Scores = append(statsInfo.Scores, event.Score)
			} else if event.Status == types.ServiceTypeFailed {
				statsInfo.DownloadFailCount++
			}
		}
	}

	for _, stats := range nodeStats {
		if len(stats.DownloadSpeeds) > 0 {
			total := int64(0)
			for _, value := range stats.DownloadSpeeds {
				total += value
			}

			stats.DownloadAvgSpeed = total / int64(len(stats.DownloadSpeeds))
		}

		if len(stats.UploadSpeeds) > 0 {
			total := int64(0)
			for _, value := range stats.UploadSpeeds {
				total += value
			}

			stats.UploadAvgSpeed = total / int64(len(stats.UploadSpeeds))
		}

		if len(stats.Scores) > 0 {
			total := int64(0)
			for _, value := range stats.Scores {
				total += value
			}

			stats.AvgScore = total / int64(len(stats.Scores))
		}

		stats.DownloadSpeeds = nil
		stats.UploadSpeeds = nil
		stats.Scores = nil
	}

	return nodeStats, nil
}
