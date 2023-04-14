package candidate

import (
	"context"

	"github.com/Filecoin-Titan/titan/node/asset"
	"github.com/Filecoin-Titan/titan/node/config"
	"go.uber.org/fx"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/node/common"
	"github.com/Filecoin-Titan/titan/node/device"
	datasync "github.com/Filecoin-Titan/titan/node/sync"

	vd "github.com/Filecoin-Titan/titan/node/validation"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("candidate")

const (
	schedulerAPITimeout = 3
	validateTimeout     = 5
	tcpPackMaxLength    = 52428800
)

// Candidate represents the candidate node.
type Candidate struct {
	fx.In

	*common.CommonAPI
	*asset.Asset
	*device.Device
	*vd.Validation
	*datasync.DataSync

	Scheduler api.Scheduler
	Config    *config.CandidateCfg
	TCPSrv    *TCPServer
}

// WaitQuiet does nothing and returns nil error.
func (candidate *Candidate) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}

// GetBlocksWithAssetCID returns a map of blocks with given asset CID, random seed, and random count.
func (candidate *Candidate) GetBlocksWithAssetCID(ctx context.Context, assetCID string, randomSeed int64, randomCount int) (map[int]string, error) {
	return candidate.Asset.GetBlocksOfAsset(assetCID, randomSeed, randomCount)
}
