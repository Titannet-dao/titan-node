package types

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

// NodeDynamicInfo contains the real-time status information of a node,
// such as the last online time, online duration, CPU usage rate, and score changes.
type NodeDynamicInfo struct {
	NodeID             string    `json:"node_id" form:"nodeId" gorm:"column:node_id;comment:;" db:"node_id"`
	OnlineDuration     int       `db:"online_duration"`  // unit:Minute
	OfflineDuration    int       `db:"offline_duration"` // unit:Minute
	DiskUsage          float64   `json:"disk_usage" form:"diskUsage" gorm:"column:disk_usage;comment:;" db:"disk_usage"`
	LastSeen           time.Time `db:"last_seen"`
	Profit             float64   `db:"profit"`
	PenaltyProfit      float64   `db:"penalty_profit"`
	TitanDiskUsage     float64   `db:"titan_disk_usage"`
	AvailableDiskSpace float64   `json:"available_disk_space" form:"availableDiskSpace" gorm:"column:available_disk_space;comment:;" db:"available_disk_space"`
	BandwidthUp        int64     `json:"bandwidth_up" db:"bandwidth_up"`
	BandwidthDown      int64     `json:"bandwidth_down" db:"bandwidth_down"`
	DownloadTraffic    int64     `db:"download_traffic"`
	UploadTraffic      int64     `db:"upload_traffic"`
	NATType            string    `db:"nat_type"`

	BandwidthUpScore   int64
	BandwidthDownScore int64

	TodayOnlineTimeWindow int // today online time window
}

type DeviceRunningStat struct {
	CPUCores      int
	CPUInfo       string
	CPUUsage      []float64
	CPUTotalUsage float64

	Memory      uint64
	MemoryUsed  uint64
	MemoryUsage float64

	// ProcessBandWidthUp   int64
	// ProcessBandWidthDown int64
	NicBandWidthUp    int64 // network interface card bandwidth
	NicBandWidthUpMax int64
	NicBandWidthUpAvg int64
	NicBandWidthUpMin int64

	NicBandWidthDown   int64
	NicBandWidthDowMax int64
	NicBandWidthDowAvg int64
	NicBandWidthDowMin int64

	LastSampleTime time.Time
}

// NodeStatisticsInfo holds statistics about node assets.
type NodeStatisticsInfo struct {
	AssetCount             int64 `db:"asset_count"`
	AssetSucceededCount    int64 `db:"asset_succeeded_count"`
	AssetFailedCount       int64 `db:"asset_failed_count"`
	RetrieveCount          int64 `db:"retrieve_count"`
	RetrieveSucceededCount int64 `db:"retrieve_succeeded_count"`
	RetrieveFailedCount    int64 `db:"retrieve_failed_count"`
	ProjectCount           int64 `db:"project_count"`
	ProjectSucceededCount  int64 `db:"project_succeeded_count"`
	ProjectFailedCount     int64 `db:"project_failed_count"`

	ReplicaCount int64
}

// NodeInfo contains information about a node.
type NodeInfo struct {
	IsTestNode  bool
	Type        NodeType
	ExternalIP  string
	InternalIP  string
	CPUUsage    float64
	MemoryUsage float64
	Status      NodeStatus
	// NATType         string
	ClientType      NodeClientType
	BackProjectTime int64
	RemoteAddr      string
	Level           int
	IncomeIncr      float64 // Base points increase every half hour (30 minute)
	AreaID          string
	Mx              float64 // Node online coefficient

	FirstTime      time.Time       `db:"first_login_time"`
	NetFlowUp      int64           `json:"netflow_up" db:"netflow_up" gorm:"column:netflow_up;"`
	NetFlowDown    int64           `json:"netflow_down" db:"netflow_down" gorm:"column:netflow_down;"`
	DiskSpace      float64         `json:"disk_space" form:"diskSpace" gorm:"column:disk_space;comment:;" db:"disk_space"`
	SystemVersion  string          `json:"system_version" form:"systemVersion" gorm:"column:system_version;comment:;" db:"system_version"`
	Version        int64           `db:"version"`
	DiskType       string          `json:"disk_type" form:"diskType" gorm:"column:disk_type;comment:;" db:"disk_type"`
	IoSystem       string          `json:"io_system" form:"ioSystem" gorm:"column:io_system;comment:;" db:"io_system"`
	NodeName       string          `json:"node_name" form:"nodeName" gorm:"column:node_name;comment:;" db:"node_name"`
	Memory         float64         `json:"memory" form:"memory" gorm:"column:memory;comment:;" db:"memory"`
	CPUCores       int             `json:"cpu_cores" form:"cpuCores" gorm:"column:cpu_cores;comment:;" db:"cpu_cores"`
	MacLocation    string          `json:"mac_location" form:"macLocation" gorm:"column:mac_location;comment:;" db:"mac_location"`
	PortMapping    string          `db:"port_mapping"`
	SchedulerID    dtypes.ServerID `db:"scheduler_sid"`
	DeactivateTime int64           `db:"deactivate_time"`
	CPUInfo        string          `json:"cpu_info" form:"cpuInfo" gorm:"column:cpu_info;comment:;" db:"cpu_info"`
	GPUInfo        string          `json:"gpu_info" form:"gpuInfo" gorm:"column:gpu_info;comment:;" db:"gpu_info"`
	FreeUpFiskTime time.Time       `db:"free_up_disk_time"`
	WSServerID     string          `db:"ws_server_id"`
	ForceOffline   bool            `db:"force_offline"`

	NodeDynamicInfo
	NodeStatisticsInfo
}

// NodeMigrateInfo holds information about node migration.
type NodeMigrateInfo struct {
	NodeInfo       *NodeInfo
	ActivationInfo *ActivationDetail
	CodeInfo       *CandidateCodeInfo
	OnlineCounts   map[time.Time]int
	ProfitList     []*ProfitDetails

	Key string
}

// NodeClientType node client type
type NodeClientType int

// NodeOther represents a node type that is not specifically defined.
const (
	NodeOther NodeClientType = iota
	NodeWindows
	NodeMacos
	NodeAndroid
	NodeIOS
)

// NodeStatus node status
type NodeStatus int

// NodeOffline indicates that the node is not currently online.
const (
	NodeOffline NodeStatus = iota

	NodeServicing
)

func (n NodeStatus) String() string {
	switch n {
	case NodeOffline:
		return "offline"
	case NodeServicing:
		return "servicing"
	}

	return ""
}

// NodeType node type
type NodeType int

// NodeUnknown represents an unknown node type.
const (
	NodeUnknown NodeType = iota

	NodeEdge
	NodeCandidate
	NodeValidator
	NodeScheduler
	NodeLocator
	NodeUpdater
	NodeL5
	NodeL3
)

func (n NodeType) String() string {
	switch n {
	case NodeEdge:
		return "edge"
	case NodeCandidate:
		return "candidate"
	case NodeScheduler:
		return "scheduler"
	// case NodeValidator:
	// 	return "validator"
	case NodeLocator:
		return "locator"
	case NodeL5:
		return "l5"
	case NodeL3:
		return "l3"
	}

	return ""
}

// RunningNodeType represents the type of the running node.
var RunningNodeType NodeType

// DownloadHistory represents the record of a node download
type DownloadHistory struct {
	ID           string    `json:"-"`
	NodeID       string    `json:"node_id" db:"node_id"`
	BlockCID     string    `json:"block_cid" db:"block_cid"`
	AssetCID     string    `json:"asset_cid" db:"asset_cid"`
	BlockSize    int       `json:"block_size" db:"block_size"`
	Speed        int64     `json:"speed" db:"speed"`
	Reward       int64     `json:"reward" db:"reward"`
	Status       int       `json:"status" db:"status"`
	FailedReason string    `json:"failed_reason" db:"failed_reason"`
	ClientIP     string    `json:"client_ip" db:"client_ip"`
	CreatedTime  time.Time `json:"created_time" db:"created_time"`
	CompleteTime time.Time `json:"complete_time" db:"complete_time"`
}

// EdgeDownloadInfo represents download information for an edge node
type EdgeDownloadInfo struct {
	Address string
	Tk      *Token
	NodeID  string
	NatType string
}

// ExitProfitRsp represents the response structure for exit profit calculations.
type ExitProfitRsp struct {
	CurrentPoint   float64
	RemainingPoint float64
	PenaltyRate    float64
}

// EdgeDownloadInfoList represents a list of EdgeDownloadInfo structures along with
// scheduler URL and key
type EdgeDownloadInfoList struct {
	Infos        []*EdgeDownloadInfo
	SchedulerURL string
	SchedulerKey string
}

// type DownloadSource int

// const (
// 	DownloadSourceIPFS DownloadSource = iota
// 	DownloadSourceAWS
// 	DownloadSourceSDK
// )

// func (n DownloadSource) String() string {
// 	switch n {
// 	case DownloadSourceIPFS:
// 		return "ipfs"
// 	case DownloadSourceAWS:
// 		return "aws"
// 	case DownloadSourceSDK:
// 		return "sdk"
// 	}

// 	return ""
// }

// CandidateDownloadInfo represents download information for a candidate
type CandidateDownloadInfo struct {
	NodeID  string
	Address string
	Tk      *Token
	// download from aws
	AWSBucket string
	// download from aws
	AWSKey string
}

// SourceDownloadInfo holds information about the source download.
type SourceDownloadInfo struct {
	NodeID  string
	Address string
	Tk      *Token
}

// AssetSourceDownloadInfoRsp contains information about the download source for an asset.
type AssetSourceDownloadInfoRsp struct {
	WorkloadID string

	// download from aws
	AWSBucket string
	// download from aws
	AWSKey string

	SchedulerURL string

	SourceList []*SourceDownloadInfo
}

// NodeIPInfo represents the IP information of a node.
type NodeIPInfo struct {
	NodeID      string
	IP          string
	ExternalURL string
}

// NodeReplicaStatus represents the status of a node cache
type NodeReplicaStatus struct {
	Hash   string        `db:"hash"`
	Status ReplicaStatus `db:"status"`
}

// NodeReplicaRsp represents the replicas of a node asset
type NodeReplicaRsp struct {
	Replica    []*NodeReplicaStatus
	TotalCount int
}

// NatType represents the type of NAT of a node
type NatType int

const (
	// NatTypeUnknown Unknown NAT type
	NatTypeUnknown NatType = iota
	// NatTypeNo not  nat
	NatTypeNo
	// NatTypeSymmetric Symmetric NAT
	NatTypeSymmetric // NAT4
	// NatTypeFullCone Full cone NAT
	NatTypeFullCone // NAT1
	// NatTypeRestricted Restricted NAT
	NatTypeRestricted // NAT2
	// NatTypePortRestricted Port-restricted NAT
	NatTypePortRestricted // NAT3
)

func (n NatType) String() string {
	switch n {
	case NatTypeNo:
		return "NoNAT"
	case NatTypeSymmetric:
		return "SymmetricNAT"
	case NatTypeFullCone:
		return "FullConeNAT"
	case NatTypeRestricted:
		return "RestrictedNAT"
	case NatTypePortRestricted:
		return "PortRestrictedNAT"
	}

	return "UnknowNAT"
}

// FromString converts a string representation of a NAT type to a NatType.
func (n NatType) FromString(natType string) NatType {
	switch natType {
	case "NoNat":
		return NatTypeNo
	case "SymmetricNAT":
		return NatTypeSymmetric
	case "FullConeNAT":
		return NatTypeFullCone
	case "RestrictedNAT":
		return NatTypeRestricted
	case "PortRestrictedNAT":
		return NatTypePortRestricted
	}
	return NatTypeUnknown
}

// ListNodesRsp list node rsp
type ListNodesRsp struct {
	Data  []NodeInfo `json:"data"`
	Total int64      `json:"total"`
}

// ListDownloadRecordRsp download record rsp
type ListDownloadRecordRsp struct {
	Data  []DownloadHistory `json:"data"`
	Total int64             `json:"total"`
}

// ListValidationResultRsp list validated result
type ListValidationResultRsp struct {
	Total                 int                    `json:"total"`
	ValidationResultInfos []ValidationResultInfo `json:"validation_result_infos"`
}

// ListWorkloadRecordRsp list workload result
type ListWorkloadRecordRsp struct {
	Total               int               `json:"total"`
	WorkloadRecordInfos []*WorkloadRecord `json:"workload_result_infos"`
}

// ValidationResultInfo validator result info
type ValidationResultInfo struct {
	ID               int              `db:"id"`
	RoundID          string           `db:"round_id"`
	NodeID           string           `db:"node_id"`
	Cid              string           `db:"cid"`
	ValidatorID      string           `db:"validator_id"`
	BlockNumber      int64            `db:"block_number"` // number of blocks verified
	Status           ValidationStatus `db:"status"`
	Duration         int64            `db:"duration"` // validator duration, microsecond
	Bandwidth        float64          `db:"bandwidth"`
	StartTime        time.Time        `db:"start_time"`
	EndTime          time.Time        `db:"end_time"`
	Profit           float64          `db:"profit"`
	CalculatedProfit bool             `db:"calculated_profit"`
	TokenID          string           `db:"token_id"`
	FileSaved        bool             `db:"file_saved"`
	NodeCount        int              `db:"node_count"`
}

// ValidationStatus Validation Status
type ValidationStatus int

const (
	// ValidationStatusCreate  is the initial validation status when the validation process starts.
	ValidationStatusCreate ValidationStatus = iota
	// ValidationStatusSuccess is the validation status when the validation is success.
	ValidationStatusSuccess
	// ValidationStatusCancel is the validation status when the validation is canceled.
	ValidationStatusCancel

	// Node error

	// ValidationStatusNodeTimeOut is the validation status when the node times out.
	ValidationStatusNodeTimeOut
	// ValidationStatusValidateFail is the validation status when the validation fail.
	ValidationStatusValidateFail

	// Validator error

	// ValidationStatusValidatorTimeOut is the validation status when the validator times out.
	ValidationStatusValidatorTimeOut
	// ValidationStatusGetValidatorBlockErr is the validation status when there is an error getting the blocks from validator.
	ValidationStatusGetValidatorBlockErr
	// ValidationStatusValidatorMismatch is the validation status when the validator mismatches.
	ValidationStatusValidatorMismatch

	// Server error

	// ValidationStatusLoadDBErr is the validation status when there is an error loading the database.
	ValidationStatusLoadDBErr
	// ValidationStatusCIDToHashErr is the validation status when there is an error converting a CID to a hash.
	ValidationStatusCIDToHashErr

	// ValidationStatusNodeOffline is the validation status when the node offline.
	ValidationStatusNodeOffline
)

// TokenPayload payload of token
type TokenPayload struct {
	ID          string    `db:"token_id"`
	NodeID      string    `db:"node_id"`
	AssetCID    string    `db:"asset_id"`
	ClientID    string    `db:"client_id"`
	LimitRate   int64     `db:"limit_rate"`
	CreatedTime time.Time `db:"created_time"`
	Expiration  time.Time `db:"expiration"`
}

// Token access download asset
type Token struct {
	ID string
	// CipherText encrypted TokenPayload by public key
	CipherText string
	// Sign signs CipherText by scheduler private key
	Sign string
}

// ProjectRecordReq represents a request for a project record.
type ProjectRecordReq struct {
	NodeID            string
	ProjectID         string
	BandwidthUpSize   float64
	BandwidthDownSize float64
	StartTime         time.Time
	EndTime           time.Time
	MaxDelay          int64
	MinDelay          int64
	AvgDelay          int64
}

// WorkloadEvent represents the different types of workload events.
type WorkloadEvent int

// WorkloadEventPull indicates a pull event for workloads.
const (
	WorkloadEventPull WorkloadEvent = iota
	WorkloadEventSync
	WorkloadEventRetrieve
)

// WorkloadStatus Workload Status
type WorkloadStatus int

const (
	// WorkloadStatusCreate is the initial workload status when the workload process starts.
	WorkloadStatusCreate WorkloadStatus = iota
	// WorkloadStatusSucceeded is the workload status when the workload is succeeded.
	WorkloadStatusSucceeded
	// WorkloadStatusFailed is the workload status when the workload is failed.
	WorkloadStatusFailed
	// WorkloadStatusInvalid is the workload status when the workload is invalid.
	WorkloadStatusInvalid
)

// WorkloadReqStatus Workload Status
type WorkloadReqStatus int

// WorkloadReqStatusSucceeded is the workload status.
const (
	WorkloadReqStatusSucceeded WorkloadReqStatus = iota
	WorkloadReqStatusFailed
)

// Workload represents a unit of work with a source ID and download size.
type Workload struct {
	SourceID     string
	DownloadSize int64
	CostTime     int64 // Millisecond

	Status WorkloadReqStatus
}

// WorkloadRecord use to store workloadReport
type WorkloadRecord struct {
	WorkloadID    string         `db:"workload_id"`
	AssetCID      string         `db:"asset_cid"`
	ClientID      string         `db:"client_id"`
	AssetSize     int64          `db:"asset_size"`
	CreatedTime   time.Time      `db:"created_time"`
	ClientEndTime time.Time      `db:"client_end_time"`
	Workloads     []byte         `db:"workloads"`
	Status        WorkloadStatus `db:"status"`
	Event         WorkloadEvent  `db:"event"`
}

// WorkloadRecordReq use to store workloadReport
type WorkloadRecordReq struct {
	WorkloadID string
	AssetCID   string
	Workloads  []Workload
}

// NodeWorkloadReport represents a report of workloads for a node.
type NodeWorkloadReport struct {
	// CipherText encrypted []*WorkloadReport by scheduler public key
	CipherText []byte
	// Sign signs CipherText by node private key
	Sign []byte
}

// NatPunchReq represents a request for NAT punching.
type NatPunchReq struct {
	Tk      *Token
	NodeID  string
	Timeout time.Duration
}

// ConnectOptions holds the configuration options for connecting to the server.
type ConnectOptions struct {
	Token         string
	TcpServerPort int
	// private minio storage only, not public storage
	IsPrivateMinioOnly bool
	ExternalURL        string

	GeoInfo             *region.GeoInfo
	ResourcesStatistics *ResourcesStatistics
}

// GeneratedCarInfo holds information about a generated CAR (Content Addressable Resource).
type GeneratedCarInfo struct {
	DataCid   string
	PieceCid  string
	PieceSize uint64
	Path      string
}

// NodeActivation represents the activation details for a node.
type NodeActivation struct {
	NodeID         string
	ActivationCode string
}

// NodeRegister represents a registration for a node with its ID and code.
type NodeRegister struct {
	NodeID    string
	Code      string
	PublicKey string
	AreaID    string
	NodeType  NodeType
}

// ActivationDetail holds the details of an activation for a node.
type ActivationDetail struct {
	NodeID        string   `json:"node_id" db:"node_id"`
	AreaID        string   `json:"area_id" `
	ActivationKey string   `json:"activation_key" db:"activation_key"`
	NodeType      NodeType `json:"node_type" db:"node_type"`
	IP            string   `json:"ip" db:"ip"`
	MigrateKey    string   `json:"migrate_key" db:"migrate_key"`
	PublicKey     string   `json:"public_key" db:"public_key"`
	CreatedTime   string   `db:"created_time"`
}

// Marshal converts ActivationDetail to a JSON string.
func (d *ActivationDetail) Marshal() (string, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(b), nil
}

// Unmarshal decodes a base64 encoded string into ActivationDetail.
func (d *ActivationDetail) Unmarshal(code string) error {
	sDec, err := base64.StdEncoding.DecodeString(code)
	if err != nil {
		return err
	}

	err = json.Unmarshal(sDec, d)
	if err != nil {
		return err
	}

	return nil
}

// ProfitType represents the type of profit
type ProfitType int

// ProfitTypeBase represents the base profit type.
const (
	ProfitTypeBase ProfitType = iota
	// ProfitTypePull
	ProfitTypePull
	// ProfitTypeBePull
	ProfitTypeBePull
	// ProfitTypeValidatable
	ProfitTypeValidatable
	// ProfitTypeValidator
	ProfitTypeValidator
	// ProfitTypeDownload
	ProfitTypeDownload
	// ProfitTypeUpload
	ProfitTypeUpload
	// ProfitTypeOfflinePenalty
	ProfitTypeOfflinePenalty
	// ProfitTypeRecompense
	ProfitTypeRecompense
)

// ProfitDetails holds the profit information for a specific node.
type ProfitDetails struct {
	ID          int64      `db:"id"`
	NodeID      string     `db:"node_id"`
	Profit      float64    `db:"profit"`
	CreatedTime time.Time  `db:"created_time"`
	PType       ProfitType `db:"profit_type"`
	Size        int64      `db:"size"`
	Note        string     `db:"note"`
	CID         string     `db:"cid"`
	Rate        float64    `db:"rate"`
	Penalty     float64
}

// ListNodeProfitDetailsRsp list node profit
type ListNodeProfitDetailsRsp struct {
	Total int              `json:"total"`
	Infos []*ProfitDetails `json:"infos"`
}

// RateLimiter controls the rate of bandwidth usage for uploading and downloading.
type RateLimiter struct {
	BandwidthUpLimiter   *rate.Limiter
	BandwidthDownLimiter *rate.Limiter
}

// CandidateCodeInfo holds information about a candidate's code.
type CandidateCodeInfo struct {
	Code       string    `db:"code"`
	NodeType   NodeType  `db:"node_type"`
	Expiration time.Time `db:"expiration"`
	NodeID     string    `db:"node_id"`
	IsTest     bool      `db:"is_test"`
}

// TunserverRsp represents the response from the Tunserver.
type TunserverRsp struct {
	URL    string
	NodeID string
}

// TunserverReq represents a request for a Tunserver with an IP and AreaID.
type TunserverReq struct {
	IP     string
	AreaID string
}

// CreateTunnelReq represents a request to create a tunnel.
type CreateTunnelReq struct {
	NodeID    string
	ProjectID string
	WsURL     string
	TunnelID  string
}

// AccessPointRsp represents the response structure for access points.
type AccessPointRsp struct {
	Schedulers []string
	GeoInfo    *region.GeoInfo
}

// KeepaliveRsp represents the response structure for keepalive requests.
type KeepaliveRsp struct {
	SessionUUID uuid.UUID
	SessionID   string
	ErrCode     int
	ErrMsg      string
}

// KeepaliveReq keepalive requests.
type KeepaliveReq struct {
	Free      FlowUnit
	Peak      FlowUnit
	TaskCount int16
}

type FlowUnit struct {
	U int64
	D int64
}

type ServiceType int

const (
	ServiceTypeUpload ServiceType = iota
	ServiceTypeDownload
)

type ServiceStatus int

const (
	ServiceTypeSucceed ServiceStatus = iota
	ServiceTypeFailed
)

type ServiceEvent struct {
	TraceID   string        `db:"trace_id"`
	NodeID    string        `db:"node_id"`
	Type      ServiceType   `db:"type"`
	Size      int64         `db:"size"`
	Status    ServiceStatus `db:"status"`
	Peak      int64         `db:"peak"`
	EndTime   time.Time     `db:"end_time"`
	StartTime time.Time     `db:"start_time"`
	Speed     int64         `db:"speed"`

	Score int64 `db:"score"`
}

type ServiceStats struct {
	NodeID               string
	UploadSuccessCount   int64
	UploadTotalCount     int64
	UploadFailCount      int64
	UploadAvgSpeed       int64
	DownloadSuccessCount int64
	DownloadTotalCount   int64
	DownloadFailCount    int64
	DownloadAvgSpeed     int64
	AvgScore             int64

	DownloadSpeeds []int64
	UploadSpeeds   []int64
	Scores         []int64
}

type BandwidthEvent struct {
	NodeID            string    `db:"node_id"`
	BandwidthUpPeak   int64     `db:"b_up_peak"`
	BandwidthDownPeak int64     `db:"b_down_peak"`
	BandwidthUpFree   int64     `db:"b_up_free"`
	BandwidthDownFree int64     `db:"b_down_free"`
	BandwidthUpLoad   int64     `db:"b_up_load"`
	BandwidthDownLoad int64     `db:"b_down_load"`
	TaskSuccess       int64     `db:"task_success"`
	TaskTotal         int64     `db:"task_total"`
	Size              int64     `db:"size"`
	Score             int64     `db:"score"`
	CreateTime        time.Time `db:"created_time"`
}
