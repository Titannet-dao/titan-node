package api

import "context"

// Candidate is an interface for candidate node
type Candidate interface {
	Common
	Device
	Validation
	DataSync
	Asset
	WaitQuiet(ctx context.Context) error                                                                                   //perm:read                                                        //perm:read
	GetBlocksWithAssetCID(ctx context.Context, assetCID string, randomSeed int64, randomCount int) (map[int]string, error) //perm:read
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

	// key is random index
	// values is cid
	Cids []string
	// The number of random for validator
	RandomCount int

	RoundID string
}
