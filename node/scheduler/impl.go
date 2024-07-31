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
	"github.com/Filecoin-Titan/titan/node/scheduler/container"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/scheduler/nat"
	"github.com/Filecoin-Titan/titan/node/scheduler/projects"
	"github.com/Filecoin-Titan/titan/node/scheduler/validation"
	"github.com/Filecoin-Titan/titan/node/scheduler/workload"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/quic-go/quic-go"

	"go.uber.org/fx"

	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/scheduler/assets"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/common"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	logging "github.com/ipfs/go-log/v2"

	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	sSync "github.com/Filecoin-Titan/titan/node/scheduler/sync"
	"golang.org/x/xerrors"
)

var log = logging.Logger("scheduler")

const (
	cpuLimit           = 140
	memoryLimit        = 2250 * units.GiB
	diskSpaceLimit     = 500 * units.TiB
	bandwidthUpLimit   = 1500 * units.MiB
	availableDiskLimit = 2 * units.TiB

	l1CpuLimit       = 8
	l1MemoryLimit    = 16 * units.GB
	l1DiskSpaceLimit = 4 * units.TB
)

// Scheduler represents a scheduler node in a distributed system.
type Scheduler struct {
	fx.In

	region.Region
	*common.CommonAPI
	*EdgeUpdateManager
	dtypes.ServerID

	NodeManager            *node.Manager
	ValidationMgr          *validation.Manager
	AssetManager           *assets.Manager
	NatManager             *nat.Manager
	DataSync               *sSync.DataSync
	SchedulerCfg           *config.SchedulerCfg
	SetSchedulerConfigFunc dtypes.SetSchedulerConfigFunc
	GetSchedulerConfigFunc dtypes.GetSchedulerConfigFunc
	WorkloadManager        *workload.Manager
	ProjectManager         *projects.Manager
	ContainerManager       *container.Manager

	PrivateKey *rsa.PrivateKey
	Transport  *quic.Transport
}

var _ api.Scheduler = &Scheduler{}

// nodeConnect processes a node connect request with the given options and node type.
func (s *Scheduler) nodeConnect(ctx context.Context, opts *types.ConnectOptions, nodeType types.NodeType) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	nodeID := handler.GetNodeID(ctx)

	alreadyConnect := true

	cNode := s.NodeManager.GetNode(nodeID)
	if cNode == nil {
		if err := s.NodeManager.NodeExists(nodeID); err != nil {
			return xerrors.Errorf("node: %s, type: %d, error: %w", nodeID, nodeType, err)
		}
		cNode = node.New()
		alreadyConnect = false
	}

	// clean old info
	if cNode.ExternalIP != "" {
		s.NodeManager.IPMgr.RemoveNodeIP(nodeID, cNode.ExternalIP)
	}

	externalIP, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return xerrors.Errorf("SplitHostPort err:%s", err.Error())
	}

	if !s.NodeManager.IPMgr.StoreNodeIP(nodeID, externalIP) {
		return xerrors.Errorf("%s The number of IPs exceeds the limit", externalIP)
	}

	defer func() {
		if err != nil {
			s.NodeManager.IPMgr.RemoveNodeIP(nodeID, externalIP)
			s.NodeManager.GeoMgr.RemoveNodeGeo(nodeID, nodeType, cNode.AreaID)
		}
	}()

	cNode.Token = opts.Token
	cNode.ExternalURL = opts.ExternalURL
	cNode.TCPPort = opts.TcpServerPort
	cNode.IsPrivateMinioOnly = opts.IsPrivateMinioOnly

	log.Infof("node connected %s, address[%s] , %v, IsPrivateMinioOnly:%v , opts.ExternalURL:%s", nodeID, remoteAddr, alreadyConnect, cNode.IsPrivateMinioOnly, cNode.ExternalURL)

	err = cNode.ConnectRPC(s.Transport, remoteAddr, nodeType)
	if err != nil {
		return xerrors.Errorf("%s nodeConnect ConnectRPC err:%s", nodeID, err.Error())
	}

	// init node info
	nInfo, err := cNode.API.GetNodeInfo(context.Background())
	if err != nil {
		return xerrors.Errorf("%s nodeConnect NodeInfo err:%s", nodeID, err.Error())
	}

	if nodeID != nInfo.NodeID {
		return xerrors.Errorf("nodeID mismatch %s, %s", nodeID, nInfo.NodeID)
	}

	// for container
	{
		if nodeType == types.NodeCandidate {
			err = s.ContainerManager.AddNewProvider(ctx, &types.Provider{
				ID:         nodeID,
				IP:         externalIP,
				RemoteAddr: opts.ExternalURL,
				State:      types.ProviderStateOnline,
				CreatedAt:  time.Now(),
			})
			if err != nil {
				return xerrors.Errorf("add new container provider: %s %v", nodeID, err)
			}
		}
	}

	nodeInfo, err := s.checkNodeParameters(nInfo, nodeType)
	if err != nil {
		return xerrors.Errorf("Node %s does not meet the standard %s", nodeID, err.Error())
	}

	nodeInfo.NodeID = nodeID
	nodeInfo.RemoteAddr = remoteAddr
	nodeInfo.SchedulerID = s.ServerID
	nodeInfo.ExternalIP = externalIP
	nodeInfo.BandwidthUp = units.KiB
	nodeInfo.NATType = types.NatTypeUnknown.String()

	if opts.GeoInfo != nil {
		nodeInfo.AreaID = opts.GeoInfo.Geo
	} else {
		geoInfo, err := s.GetGeoInfo(externalIP)
		if err != nil {
			log.Warnf("%s getAreaID error %s", nodeID, err.Error())
		}
		nodeInfo.AreaID = geoInfo.Geo
	}

	oldInfo, err := s.NodeManager.LoadNodeInfo(nodeID)
	if err != nil && err != sql.ErrNoRows {
		return xerrors.Errorf("load node online duration %s err : %s", nodeID, err.Error())
	}

	nodeInfo.FirstTime = time.Now()
	if oldInfo != nil {
		// init node info
		nodeInfo.PortMapping = oldInfo.PortMapping
		nodeInfo.OnlineDuration = oldInfo.OnlineDuration
		nodeInfo.OfflineDuration = oldInfo.OfflineDuration
		nodeInfo.BandwidthDown = oldInfo.BandwidthDown
		nodeInfo.BandwidthUp = oldInfo.BandwidthUp
		nodeInfo.DeactivateTime = oldInfo.DeactivateTime
		nodeInfo.DownloadTraffic = oldInfo.DownloadTraffic
		nodeInfo.UploadTraffic = oldInfo.UploadTraffic
		nodeInfo.WSServerID = oldInfo.WSServerID
		nodeInfo.Profit = oldInfo.Profit
		nodeInfo.FirstTime = oldInfo.FirstTime

		if oldInfo.DeactivateTime > 0 && oldInfo.DeactivateTime < time.Now().Unix() {
			return xerrors.Errorf("The node %s has been deactivate and cannot be logged in", nodeID)
		}
	}

	cNode.InitInfo(nodeInfo)

	if !alreadyConnect {
		if nodeType == types.NodeEdge {
			incr, _ := s.NodeManager.GetEdgeBaseProfitDetails(cNode, 0)
			cNode.IncomeIncr = incr
		}
		s.NodeManager.GeoMgr.AddNodeGeo(nodeInfo, cNode.AreaID)
		cNode.OnlineRate = s.NodeManager.ComputeNodeOnlineRate(nodeID, nodeInfo.FirstTime)

		pStr, err := s.NodeManager.LoadNodePublicKey(nodeID)
		if err != nil && err != sql.ErrNoRows {
			return xerrors.Errorf("load node port %s err : %s", nodeID, err.Error())
		}

		publicKey, err := titanrsa.Pem2PublicKey([]byte(pStr))
		if err != nil {
			return xerrors.Errorf("load node port %s err : %s", nodeID, err.Error())
		}
		cNode.PublicKey = publicKey
		// init LastValidateTime
		cNode.LastValidateTime = s.getNodeLastValidateTime(nodeID)

		err = s.NodeManager.NodeOnline(cNode, nodeInfo)
		if err != nil {
			log.Errorf("nodeConnect err:%s,nodeID:%s", err.Error(), nodeID)
			return err
		}

		s.ProjectManager.CheckProjectReplicasFromNode(nodeID)
	}

	if nodeType == types.NodeEdge {
		go s.NatManager.DetermineEdgeNATType(context.Background(), nodeID)
	} else if nodeType == types.NodeCandidate {
		err := checkDomain(cNode.ExternalURL)
		log.Infof("%s checkDomain [%s] %v", nodeID, cNode.ExternalURL, err)
		cNode.IsStorageNode = err == nil

		go s.NatManager.DetermineCandidateNATType(context.Background(), nodeID)
	}

	s.DataSync.AddNodeToList(nodeID)
	return nil
}

func checkDomain(domain string) error {
	if domain == "" {
		return xerrors.New("domain is nil")
	}

	url := fmt.Sprintf("%s/abc", domain)
	_, err := http.Get(url)

	return err
}

func (s *Scheduler) getNodeLastValidateTime(nodeID string) int64 {
	rsp, err := s.NodeManager.LoadValidationResultInfos(nodeID, 1, 0)
	if err != nil || len(rsp.ValidationResultInfos) == 0 {
		return 0
	}

	info := rsp.ValidationResultInfos[0]
	return info.StartTime.Unix()
}

func checkNodeClientType(systemVersion, androidSymbol, iosSymbol, windowsSymbol, macosSymbol string) types.NodeClientType {
	if strings.Contains(systemVersion, androidSymbol) {
		return types.NodeAndroid
	}

	if strings.Contains(systemVersion, iosSymbol) {
		return types.NodeIOS
	}

	if strings.Contains(systemVersion, windowsSymbol) {
		return types.NodeWindows
	}

	if strings.Contains(systemVersion, macosSymbol) {
		return types.NodeMacos
	}

	return types.NodeOther
}

func roundUpToNextGB(bytes int64) int64 {
	const GB = 1 << 30
	if bytes%GB == 0 {
		return bytes
	}
	return ((bytes / GB) + 1) * GB
}

func (s *Scheduler) checkNodeParameters(nodeInfo types.NodeInfo, nodeType types.NodeType) (*types.NodeInfo, error) {
	if nodeInfo.AvailableDiskSpace <= 0 {
		nodeInfo.AvailableDiskSpace = 2 * units.GiB
	}

	if nodeInfo.AvailableDiskSpace > nodeInfo.DiskSpace {
		nodeInfo.AvailableDiskSpace = nodeInfo.DiskSpace
	}

	nodeInfo.Type = nodeType

	useSize, err := s.db.LoadReplicaSizeByNodeID(nodeInfo.NodeID)
	if err != nil {
		return nil, xerrors.Errorf("LoadReplicaSizeByNodeID %s err:%s", nodeInfo.NodeID, err.Error())
	}

	if nodeType == types.NodeEdge {
		if nodeInfo.AvailableDiskSpace > availableDiskLimit {
			nodeInfo.AvailableDiskSpace = availableDiskLimit
		}

		nodeInfo.ClientType = checkNodeClientType(nodeInfo.SystemVersion, s.SchedulerCfg.AndroidSymbol, s.SchedulerCfg.IOSSymbol, s.SchedulerCfg.WindowsSymbol, s.SchedulerCfg.MacosSymbol)
		// limit node availableDiskSpace to 5 GiB when using phone
		if nodeInfo.ClientType == types.NodeAndroid || nodeInfo.ClientType == types.NodeIOS {
			if nodeInfo.AvailableDiskSpace > float64(5*units.GiB) {
				nodeInfo.AvailableDiskSpace = float64(5 * units.GiB)
			}

			if useSize > 5*units.GiB {
				useSize = 5 * units.GiB
			}
		}

		if nodeInfo.DiskSpace > diskSpaceLimit || nodeInfo.DiskSpace < 0 {
			return nil, xerrors.Errorf("checkNodeParameters [%s] DiskSpace [%s]", nodeInfo.NodeID, units.BytesSize(nodeInfo.DiskSpace))
		}

		if nodeInfo.BandwidthDown < 0 {
			return nil, xerrors.Errorf("checkNodeParameters [%s] BandwidthDown [%s]", nodeInfo.NodeID, units.BytesSize(float64(nodeInfo.BandwidthDown)))
		}

		if nodeInfo.BandwidthUp > bandwidthUpLimit || nodeInfo.BandwidthUp < 0 {
			return nil, xerrors.Errorf("checkNodeParameters [%s] BandwidthUp [%s]", nodeInfo.NodeID, units.BytesSize(float64(nodeInfo.BandwidthUp)))
		}

		if nodeInfo.Memory > memoryLimit || nodeInfo.Memory < 0 {
			return nil, xerrors.Errorf("checkNodeParameters [%s] Memory [%s]", nodeInfo.NodeID, units.BytesSize(nodeInfo.Memory))
		}

		if nodeInfo.CPUCores > cpuLimit || nodeInfo.CPUCores < 0 {
			return nil, xerrors.Errorf("checkNodeParameters [%s] CPUCores [%d]", nodeInfo.NodeID, nodeInfo.CPUCores)
		}
	} else if nodeType == types.NodeCandidate {
		info, err := s.db.GetCandidateCodeInfoForNodeID(nodeInfo.NodeID)
		if err != nil {
			return nil, xerrors.Errorf("nodeID GetCandidateCodeInfoForNodeID %s, %s", nodeInfo.NodeID, err.Error())
		}
		nodeInfo.IsTestNode = info.IsTest

		if !nodeInfo.IsTestNode {
			if nodeInfo.Memory < l1MemoryLimit {
				return nil, xerrors.Errorf("Memory [%s]<[%s]", units.BytesSize(nodeInfo.Memory), units.BytesSize(l1MemoryLimit))
			}

			if nodeInfo.CPUCores < l1CpuLimit {
				return nil, xerrors.Errorf("CPUCores [%d]<[%d]", nodeInfo.CPUCores, l1CpuLimit)
			}

			if nodeInfo.DiskSpace < l1DiskSpaceLimit {
				return nil, xerrors.Errorf("DiskSpace [%s]<[%s]", units.BytesSize(nodeInfo.DiskSpace), units.BytesSize(l1DiskSpaceLimit))
			}
		}

		// isValidator, err := s.db.IsValidator(nodeInfo.NodeID)
		// if err != nil {
		// 	return nil, false, xerrors.Errorf("checkNodeParameters %s IsValidator err:%s", nodeInfo.NodeID, err.Error())
		// }

		// if isValidator {
		// nodeInfo.Type = types.NodeValidator
		// }
		nodeInfo.AvailableDiskSpace = nodeInfo.DiskSpace * 0.9
	}

	nodeInfo.TitanDiskUsage = float64(useSize)
	// if nodeInfo.AvailableDiskSpace < float64(useSize) {
	// 	nodeInfo.AvailableDiskSpace = float64(roundUpToNextGB(useSize))
	// }

	return &nodeInfo, nil
}

// NodeValidationResult processes the validation result for a node
func (s *Scheduler) NodeValidationResult(ctx context.Context, r io.Reader, sign string) error {
	validator := handler.GetNodeID(ctx)
	node := s.NodeManager.GetNode(validator)
	if node == nil {
		return xerrors.Errorf("node %s not online", validator)
	}

	signBuf, err := hex.DecodeString(sign)
	if err != nil {
		return err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	rsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	err = rsa.VerifySign(node.PublicKey, signBuf, data)
	if err != nil {
		return err
	}

	result := &api.ValidationResult{}
	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)
	err = dec.Decode(result)
	if err != nil {
		return err
	}

	result.Validator = validator
	s.ValidationMgr.PushResult(result)

	return nil
}

// TriggerElection triggers a single election for validators.
func (s *Scheduler) TriggerElection(ctx context.Context) error {
	// s.ValidationMgr.StartElection()
	return nil
}

// GetValidationResults retrieves a list of validation results.
func (s *Scheduler) GetValidationResults(ctx context.Context, nodeID string, limit, offset int) (*types.ListValidationResultRsp, error) {
	svm, err := s.NodeManager.LoadValidationResultInfos(nodeID, limit, offset)
	if err != nil {
		return nil, err
	}

	return svm, nil
}

// GetSchedulerPublicKey get server publicKey
func (s *Scheduler) GetSchedulerPublicKey(ctx context.Context) (string, error) {
	if s.PrivateKey == nil {
		return "", xerrors.Errorf("scheduler private key not exist")
	}

	publicKey := s.PrivateKey.PublicKey
	pem := titanrsa.PublicKey2Pem(&publicKey)
	return string(pem), nil
}

// GetNodePublicKey get node publicKey
func (s *Scheduler) GetNodePublicKey(ctx context.Context, nodeID string) (string, error) {
	pem, err := s.NodeManager.LoadNodePublicKey(nodeID)
	if err != nil {
		return "", xerrors.Errorf("%s load node public key failed: %w", nodeID, err)
	}

	return string(pem), nil
}

func (s *Scheduler) SubmitProjectReport(ctx context.Context, req *types.ProjectRecordReq) error {
	candidateID := handler.GetNodeID(ctx)
	if len(candidateID) == 0 {
		return xerrors.New("SubmitProjectReport invalid request")
	}

	if req.NodeID == "" {
		return xerrors.New("SubmitProjectReport node id is nil")
	}

	if req.ProjectID == "" {
		return xerrors.New("SubmitProjectReport project id is nil")
	}

	rInfo, err := s.db.LoadProjectReplicaInfo(req.ProjectID, req.NodeID)
	if err != nil {
		return xerrors.Errorf("SubmitProjectReport LoadProjectReplicaInfo err:%s", err.Error())
	}

	if rInfo.Status != types.ProjectReplicaStatusStarted {
		return xerrors.Errorf("SubmitProjectReport project status is %s", rInfo.Status.String())
	}

	wID, err := s.db.LoadWSServerID(req.NodeID)
	if err != nil {
		return xerrors.Errorf("SubmitProjectReport LoadWSServerID err:%s", err.Error())
	}

	if wID != candidateID {
		return xerrors.Errorf("SubmitProjectReport candidate id %s != %s", candidateID, wID)
	}

	node := s.NodeManager.GetEdgeNode(req.NodeID)
	if node == nil {
		return xerrors.Errorf("SubmitProjectReport node %s offline", req.NodeID)
	}

	if req.BandwidthDownSize > 0 {
		pInfo := s.NodeManager.GetDownloadProfitDetails(node, req.BandwidthDownSize, req.ProjectID)
		if pInfo != nil {
			pInfo.Profit = 0 // TODO test
			err := s.db.AddNodeProfit(pInfo)
			if err != nil {
				log.Errorf("SubmitProjectReport AddNodeProfit %s,%d, %.4f err:%s", pInfo.NodeID, pInfo.PType, pInfo.Profit, err.Error())
			}
		}
	}

	if req.BandwidthUpSize > 0 {
		pInfo := s.NodeManager.GetUploadProfitDetails(node, req.BandwidthUpSize, req.ProjectID)
		if pInfo != nil {
			pInfo.Profit = 0 // TODO test
			err := s.db.AddNodeProfit(pInfo)
			if err != nil {
				log.Errorf("SubmitProjectReport AddNodeProfit %s,%d, %.4f err:%s", pInfo.NodeID, pInfo.PType, pInfo.Profit, err.Error())
			}
		}
	}

	return nil
}

func (s *Scheduler) SubmitWorkloadReportV2(ctx context.Context, workload *types.WorkloadRecordReq) error {
	// from sdk or web or client
	return s.WorkloadManager.PushResult(workload, "")
}

// SubmitWorkloadReport
func (s *Scheduler) SubmitWorkloadReport(ctx context.Context, workload *types.WorkloadRecordReq) error {
	// from node
	nodeID := handler.GetNodeID(ctx)

	node := s.NodeManager.GetNode(nodeID)
	if node == nil {
		return xerrors.Errorf("node %s not exists", nodeID)
	}

	return s.WorkloadManager.PushResult(workload, nodeID)
}

// GetWorkloadRecords retrieves a list of workload results.
func (s *Scheduler) GetWorkloadRecords(ctx context.Context, nodeID string, limit, offset int) (*types.ListWorkloadRecordRsp, error) {
	return s.NodeManager.LoadWorkloadRecords(nodeID, limit, offset)
}

// GetWorkloadRecord retrieves a list of workload results.
func (s *Scheduler) GetWorkloadRecord(ctx context.Context, id string) (*types.WorkloadRecord, error) {
	return s.NodeManager.LoadWorkloadRecordOfID(id)
}

// GetRetrieveEventRecords retrieves a list of retrieve events
func (s *Scheduler) GetRetrieveEventRecords(ctx context.Context, nodeID string, limit, offset int) (*types.ListRetrieveEventRsp, error) {
	return s.NodeManager.LoadRetrieveEventRecords(nodeID, limit, offset)
}

// UpdateNetFlows update node net flow total,up,down usage
func (s *Scheduler) UpdateNetFlows(ctx context.Context, total, up, down int64) error {
	return nil
}

func (s *Scheduler) ReDetermineNodeNATType(ctx context.Context, nodeID string) error {
	node := s.NodeManager.GetCandidateNode(nodeID)
	if node != nil {
		go s.NatManager.DetermineCandidateNATType(ctx, nodeID)
		return nil
	}

	node = s.NodeManager.GetEdgeNode(nodeID)
	if node != nil {
		go s.NatManager.DetermineCandidateNATType(ctx, nodeID)
		return nil
	}

	return nil
}

func (s *Scheduler) GenerateCandidateCodes(ctx context.Context, count int, nodeType types.NodeType, isTest bool) ([]string, error) {
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

func (s *Scheduler) ResetCandidateCode(ctx context.Context, nodeID, code string) error {
	return s.db.ResetCandidateCodeInfo(code, nodeID)
}

func (s *Scheduler) RemoveCandidateCode(ctx context.Context, code string) error {
	return s.db.DeleteCandidateCodeInfo(code)
}
