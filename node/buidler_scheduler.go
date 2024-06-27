package node

import (
	"crypto/rsa"
	"errors"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/modules"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/repo"
	"github.com/Filecoin-Titan/titan/node/scheduler"
	"github.com/Filecoin-Titan/titan/node/scheduler/assets"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/scheduler/filelogger"
	"github.com/Filecoin-Titan/titan/node/scheduler/leadership"
	"github.com/Filecoin-Titan/titan/node/scheduler/nat"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/Filecoin-Titan/titan/node/scheduler/projects"
	"github.com/Filecoin-Titan/titan/node/scheduler/sync"
	"github.com/Filecoin-Titan/titan/node/scheduler/validation"
	"github.com/Filecoin-Titan/titan/node/scheduler/workload"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/filecoin-project/pubsub"
	"github.com/jmoiron/sqlx"

	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

func Scheduler(out *api.Scheduler) Option {
	return Options(
		ApplyIf(func(s *Settings) bool { return s.Config },
			Error(errors.New("the Scheduler option must be set before Config option")),
		),

		func(s *Settings) error {
			s.nodeType = repo.Scheduler
			return nil
		},

		func(s *Settings) error {
			resAPI := &scheduler.Scheduler{}
			s.invokes[ExtractAPIKey] = fx.Populate(resAPI)
			*out = resAPI
			return nil
		},
	)
}

func ConfigScheduler(c interface{}) Option {
	cfg, ok := c.(*config.SchedulerCfg)
	if !ok {
		return Error(xerrors.Errorf("invalid config from repo, got: %T", c))
	}
	log.Info("start to config scheduler")

	return Options(
		Override(new(dtypes.ServerID), modules.NewServerID),
		Override(new(*config.SchedulerCfg), cfg),
		Override(new(*etcdcli.Client), modules.RegisterToEtcd),
		Override(new(*leadership.Manager), leadership.NewManager),
		Override(new(*filelogger.Manager), filelogger.NewManager),
		Override(new(*sqlx.DB), modules.NewDB),
		Override(new(*db.SQLDB), db.NewSQLDB),
		Override(new(*pubsub.PubSub), modules.NewPubSub),
		Override(InitDataTables, db.InitTables),
		Override(new(*node.Manager), node.NewManager),
		Override(new(*workload.Manager), workload.NewManager),
		Override(new(*projects.Manager), modules.NewProjectManager),
		Override(new(dtypes.AssetMetadataDS), modules.AssetDatastore),
		Override(new(dtypes.ProjectMetadataDS), modules.ProjectDatastore),
		Override(new(*assets.Manager), modules.NewAssetManager),
		Override(new(*sync.DataSync), sync.NewDataSync),
		Override(new(*validation.Manager), modules.NewValidation),
		Override(new(*nat.Manager), nat.NewManager),
		Override(new(*scheduler.EdgeUpdateManager), scheduler.NewEdgeUpdateManager),
		Override(new(dtypes.SetSchedulerConfigFunc), modules.NewSetSchedulerConfigFunc),
		Override(new(dtypes.GetSchedulerConfigFunc), modules.NewGetSchedulerConfigFunc),
		Override(new(*rsa.PrivateKey), modules.NewPrivateKey),
		Override(new(dtypes.GeoDBPath), func() dtypes.GeoDBPath {
			return dtypes.GeoDBPath(cfg.GeoDBPath)
		}),
		Override(new(region.Region), modules.NewRegion),
		// func() (*rsa.PrivateKey, error) {
		// return rsa.GenerateKey(rand.Reader, units.KiB) //nolint:gosec   // need smaller key
		// }),
	)
}
