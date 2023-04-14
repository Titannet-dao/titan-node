package node

import (
	"crypto/rand"
	"crypto/rsa"
	"errors"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/modules"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/repo"
	"github.com/Filecoin-Titan/titan/node/scheduler"
	"github.com/Filecoin-Titan/titan/node/scheduler/assets"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/Filecoin-Titan/titan/node/scheduler/sync"
	"github.com/Filecoin-Titan/titan/node/scheduler/validation"
	"github.com/docker/go-units"
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
		Override(RegisterEtcd, modules.RegisterToEtcd),
		Override(new(*pubsub.PubSub), modules.NewPubSub),
		Override(new(*sqlx.DB), modules.NewDB),
		Override(new(*db.SQLDB), db.NewSQLDB),
		Override(new(*node.Manager), node.NewManager),
		Override(new(dtypes.SessionCallbackFunc), node.KeepaliveCallBackFunc),
		Override(new(dtypes.MetadataDS), modules.Datastore),
		Override(new(*assets.Manager), modules.NewStorageManager),
		Override(new(*sync.DataSync), sync.NewDataSync),
		Override(new(*validation.Manager), modules.NewValidation),
		Override(new(*scheduler.EdgeUpdateManager), scheduler.NewEdgeUpdateManager),
		Override(new(dtypes.DatabaseAddress), func() dtypes.DatabaseAddress {
			return dtypes.DatabaseAddress(cfg.DatabaseAddress)
		}),
		Override(new(dtypes.SetSchedulerConfigFunc), modules.NewSetSchedulerConfigFunc),
		Override(new(dtypes.GetSchedulerConfigFunc), modules.NewGetSchedulerConfigFunc),
		Override(new(*rsa.PrivateKey), func() (*rsa.PrivateKey, error) {
			return rsa.GenerateKey(rand.Reader, units.KiB) //nolint:gosec   // need smaller key
		}),
	)
}
