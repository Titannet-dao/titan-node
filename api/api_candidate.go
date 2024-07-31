package api

import (
	"context"

	"github.com/Filecoin-Titan/titan/api/types"
)

// Candidate is an interface for candidate node
type Candidate interface {
	Common
	Device
	Validation
	DataSync
	Asset
	ProviderAPI

	WaitQuiet(ctx context.Context) error                                                                             //perm:admin
	GetBlocksWithAssetCID(ctx context.Context, assetCID string, randomSeed int64, randomCount int) ([]string, error) //perm:admin
	// GetExternalAddress retrieves the external address of the caller.
	GetExternalAddress(ctx context.Context) (string, error) //perm:default
	// CheckNetworkConnectivity deprecated
	CheckNetworkConnectivity(ctx context.Context, network, targetURL string) error //perm:default
	// CheckNetworkConnectable check network if connectable
	CheckNetworkConnectable(ctx context.Context, network, targetURL string) (bool, error) //perm:admin
	GetMinioConfig(ctx context.Context) (*types.MinioConfig, error)                       //perm:admin

	DeactivateNode(ctx context.Context) error                             //perm:default
	CalculateExitProfit(ctx context.Context) (types.ExitProfitRsp, error) //perm:default
}

// ValidationResult node Validation result
type ValidationResult struct {
	Validator string
	CID       string
	// verification canceled due to download
	IsCancel  bool
	NodeID    string
	Bandwidth float64
	// seconds duration
	CostTime  int64
	IsTimeout bool

	// Msg string
	// if IsCancel is true, Token is valid
	// use for verify edge providing download
	Token string
	// key is random index
	// values is cid
	Cids []string
	// The number of random for validator
	RandomCount int
}
