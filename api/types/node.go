package types

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
)

// NodeSnapshot contains the real-time status information of a node,
// such as the last online time, online duration, CPU usage rate, and score changes.
type NodeSnapshot struct {
	NodeID             string    `db:"node_id"`
	OnlineDuration     int       `db:"online_duration"` // unit:Minute
	DiskUsage          float64   `db:"disk_usage"`
	LastSeen           time.Time `db:"last_seen"`
	BandwidthUp        int64     `db:"bandwidth_up"`
	BandwidthDown      int64     `db:"bandwidth_down"`
	Profit             float64   `db:"profit"`
	AvailableDiskSpace float64   `db:"available_disk_space"`
	TitanDiskUsage     float64   `db:"titan_disk_usage"`
}

// NodeDynamicInfo Dynamic information about the node
type NodeDynamicInfo struct {
	NodeID          string  `json:"node_id" form:"nodeId" gorm:"column:node_id;comment:;" db:"node_id"`
	DiskUsage       float64 `json:"disk_usage" form:"diskUsage" gorm:"column:disk_usage;comment:;" db:"disk_usage"`
	CPUUsage        float64
	MemoryUsage     float64
	Status          NodeStatus
	TitanDiskUsage  float64   `db:"titan_disk_usage"`
	IncomeIncr      float64   // Base points increase every half hour (30 minute)
	OnlineDuration  int       `db:"online_duration"` // unit:Minute
	Profit          float64   `db:"profit"`
	LastSeen        time.Time `db:"last_seen"`
	DownloadTraffic int64     `db:"download_traffic"`
	UploadTraffic   int64     `db:"upload_traffic"`
	AssetCount      int64     `db:"asset_count"`
	RetrieveCount   int64     `db:"retrieve_count"`
}

// NodeInfo contains information about a node.
type NodeInfo struct {
	Type       NodeType
	ExternalIP string
	InternalIP string

	FirstTime          time.Time       `db:"first_login_time"`
	BandwidthUp        int64           `json:"bandwidth_up" db:"bandwidth_up"`
	BandwidthDown      int64           `json:"bandwidth_down" db:"bandwidth_down"`
	DiskSpace          float64         `json:"disk_space" form:"diskSpace" gorm:"column:disk_space;comment:;" db:"disk_space"`
	AvailableDiskSpace float64         `json:"available_disk_space" form:"availableDiskSpace" gorm:"column:available_disk_space;comment:;" db:"available_disk_space"`
	SystemVersion      string          `json:"system_version" form:"systemVersion" gorm:"column:system_version;comment:;" db:"system_version"`
	DiskType           string          `json:"disk_type" form:"diskType" gorm:"column:disk_type;comment:;" db:"disk_type"`
	IoSystem           string          `json:"io_system" form:"ioSystem" gorm:"column:io_system;comment:;" db:"io_system"`
	NodeName           string          `json:"node_name" form:"nodeName" gorm:"column:node_name;comment:;" db:"node_name"`
	Memory             float64         `json:"memory" form:"memory" gorm:"column:memory;comment:;" db:"memory"`
	CPUCores           int             `json:"cpu_cores" form:"cpuCores" gorm:"column:cpu_cores;comment:;" db:"cpu_cores"`
	MacLocation        string          `json:"mac_location" form:"macLocation" gorm:"column:mac_location;comment:;" db:"mac_location"`
	NATType            string          `db:"nat_type"`
	PortMapping        string          `db:"port_mapping"`
	SchedulerID        dtypes.ServerID `db:"scheduler_sid"`
	DeactivateTime     int64           `db:"deactivate_time"`
	CPUInfo            string          `json:"cpu_info" form:"cpuInfo" gorm:"column:cpu_info;comment:;" db:"cpu_info"`

	NodeDynamicInfo
}

// NodeStatus node status
type NodeStatus int

const (
	NodeOffline NodeStatus = iota

	NodeServicing

	NodeUnregister
	// Exceptions
	NodeNatSymmetric
)

func (n NodeStatus) String() string {
	switch n {
	case NodeOffline:
		return "offline"
	case NodeServicing:
		return "servicing"
	case NodeUnregister:
		return "unregister"
	case NodeNatSymmetric:
		return "nat-symmetric"
	}

	return ""
}

// NodeType node type
type NodeType int

const (
	NodeUnknown NodeType = iota

	NodeEdge
	NodeCandidate
	NodeValidator
	NodeScheduler
	NodeLocator
	NodeUpdater
)

func (n NodeType) String() string {
	switch n {
	case NodeEdge:
		return "edge"
	case NodeCandidate:
		return "candidate"
	case NodeScheduler:
		return "scheduler"
	case NodeValidator:
		return "validator"
	case NodeLocator:
		return "locator"
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

// EdgeDownloadInfoList represents a list of EdgeDownloadInfo structures along with
// scheduler URL and key
type EdgeDownloadInfoList struct {
	Infos        []*EdgeDownloadInfo
	SchedulerURL string
	SchedulerKey string
}

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

// NodeIPInfo
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
	NatTypeSymmetric
	// NatTypeFullCone Full cone NAT
	NatTypeFullCone
	// NatTypeRestricted Restricted NAT
	NatTypeRestricted
	// NatTypePortRestricted Port-restricted NAT
	NatTypePortRestricted
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

	// ValidationStatusOther
	ValidationStatusOther
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

type Workload struct {
	DownloadSpeed int64
	DownloadSize  int64
	StartTime     time.Time
	EndTime       time.Time
	BlockCount    int64
}

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

type WorkloadReport struct {
	TokenID  string
	ClientID string
	NodeID   string
	Workload *Workload
}

// WorkloadReportRecord use to store workloadReport
type WorkloadRecord struct {
	TokenPayload
	Status         WorkloadStatus `db:"status"`
	ClientEndTime  int64          `db:"client_end_time"`
	ClientWorkload []byte         `db:"client_workload"`
	NodeWorkload   []byte         `db:"node_workload"`
}

type NodeWorkloadReport struct {
	// CipherText encrypted []*WorkloadReport by scheduler public key
	CipherText []byte
	// Sign signs CipherText by node private key
	Sign []byte
}

type NatPunchReq struct {
	Tk      *Token
	NodeID  string
	Timeout time.Duration
}

type ConnectOptions struct {
	Token         string
	TcpServerPort int
	// private minio storage only, not public storage
	IsPrivateMinioOnly bool
	ExternalURL        string
}

type GeneratedCarInfo struct {
	DataCid   string
	PieceCid  string
	PieceSize uint64
	Path      string
}

type NodeActivation struct {
	NodeID         string
	ActivationCode string
}

type ActivationDetail struct {
	NodeID        string   `json:"node_id" db:"node_id"`
	AreaID        string   `json:"area_id" `
	ActivationKey string   `json:"activation_key" db:"activation_key"`
	NodeType      NodeType `json:"node_type" db:"node_type"`
	IP            string   `json:"ip" db:"ip"`
}

func (d *ActivationDetail) Marshal() (string, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(b), nil
}

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

// RetrieveEvent retrieve event
type RetrieveEvent struct {
	TokenID     string  `db:"token_id"`
	NodeID      string  `db:"node_id"`
	ClientID    string  `db:"client_id"`
	CID         string  `db:"cid"`
	Size        int64   `db:"size"`
	CreatedTime int64   `db:"created_time"`
	EndTime     int64   `db:"end_time"`
	Profit      float64 `db:"profit"`
}

// ListRetrieveEventRsp list retrieve event
type ListRetrieveEventRsp struct {
	Total              int              `json:"total"`
	RetrieveEventInfos []*RetrieveEvent `json:"retrieve_event_infos"`
}
