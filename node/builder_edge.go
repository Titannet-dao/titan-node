package node

import (
	"errors"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/asset"
	"github.com/Filecoin-Titan/titan/node/asset/storage"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/device"
	"github.com/Filecoin-Titan/titan/node/edge"
	"github.com/Filecoin-Titan/titan/node/modules"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/repo"
	datasync "github.com/Filecoin-Titan/titan/node/sync"
	"github.com/Filecoin-Titan/titan/node/validation"
	"github.com/Filecoin-Titan/titan/node/workerd"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

func Edge(out *api.Edge) Option {
	return Options(
		ApplyIf(func(s *Settings) bool { return s.Config },
			Error(errors.New("the Edge option must be set before Config option")),
		),

		func(s *Settings) error {
			s.nodeType = repo.Edge
			return nil
		},

		func(s *Settings) error {
			resAPI := &edge.Edge{}
			s.invokes[ExtractAPIKey] = fx.Populate(resAPI)
			*out = resAPI
			return nil
		},
	)
}

func ConfigEdge(c interface{}) Option {
	cfg, ok := c.(*config.EdgeCfg)
	if !ok {
		return Error(xerrors.Errorf("invalid config from repo, got: %T", c))
	}
	log.Info("start to config edge")

	return Options(
		Override(new(*config.EdgeCfg), cfg),
		Override(new(*device.Device), modules.NewDevice(&cfg.CPU, &cfg.Memory, &cfg.Storage, &cfg.Bandwidth, &cfg.Netflow)),
		Override(new(*config.MinioConfig), &config.MinioConfig{}),
		Override(new(*storage.Manager), modules.NewNodeStorageManager),
		Override(new(*validation.Validation), modules.NewNodeValidation),
		Override(new(*types.RateLimiter), modules.NewRateLimiter),
		Override(new(*asset.Asset), asset.NewAsset),
		Override(new(*datasync.DataSync), modules.NewDataSync),
		Override(new(dtypes.WorkerdPath), modules.WorkerdPath),
		Override(new(*workerd.Workerd), modules.NewWorkerd),
		Override(new(*edge.TunManager), edge.NewTunManager()),
	)
}
