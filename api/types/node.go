package types

import (
	"time"
)

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
	URL     string
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
	URL string
	Tk  *Token
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

// ValidationResultInfo validator result info
type ValidationResultInfo struct {
	ID          int              `db:"id"`
	RoundID     string           `db:"round_id"`
	NodeID      string           `db:"node_id"`
	Cid         string           `db:"cid"`
	ValidatorID string           `db:"validator_id"`
	BlockNumber int64            `db:"block_number"` // number of blocks verified
	Status      ValidationStatus `db:"status"`
	Duration    int64            `db:"duration"` // validator duration, microsecond
	Bandwidth   float64          `db:"bandwidth"`
	StartTime   time.Time        `db:"start_time"`
	EndTime     time.Time        `db:"end_time"`
	Profit      float64          `db:"profit"`
	Processed   bool             `db:"processed"`

	UploadTraffic float64 `db:"upload_traffic"`
}

// ValidationStatus Validation Status
type ValidationStatus int

const (
	// ValidationStatusCreate  is the initial validation status when the validation process starts.
	ValidationStatusCreate ValidationStatus = iota
	// ValidationStatusSuccess is the validation status when the validation is successful.
	ValidationStatusSuccess
	// ValidationStatusCancel is the validation status when the validation is canceled.
	ValidationStatusCancel

	// Node error

	// ValidationStatusNodeTimeOut is the validation status when the node times out.
	ValidationStatusNodeTimeOut
	// ValidationStatusValidateFail is the validation status when the validation fails.
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
)

// TokenPayload payload of token
type TokenPayload struct {
	ID         string    `db:"token_id"`
	NodeID     string    `db:"node_id"`
	AssetCID   string    `db:"asset_id"`
	ClientID   string    `db:"client_id"`
	LimitRate  int64     `db:"limit_rate"`
	CreateTime time.Time `db:"create_time"`
	Expiration time.Time `db:"expiration"`
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
	StartTime     int64
	EndTime       int64
}

type WorkloadReport struct {
	TokenID  string
	ClientID string
	NodeID   string
	Workload *Workload
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
}
