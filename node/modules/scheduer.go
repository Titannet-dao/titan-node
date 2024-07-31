package modules

import (
	"context"
	"github.com/Filecoin-Titan/titan/node/scheduler/container"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/modules/helpers"
	"github.com/Filecoin-Titan/titan/node/repo"
	"github.com/Filecoin-Titan/titan/node/scheduler/assets"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/scheduler/leadership"
	"github.com/Filecoin-Titan/titan/node/scheduler/projects"
	"github.com/Filecoin-Titan/titan/node/scheduler/validation"
	"github.com/Filecoin-Titan/titan/node/scheduler/workload"
	"github.com/Filecoin-Titan/titan/node/sqldb"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/filecoin-project/pubsub"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/Filecoin-Titan/titan/node/common"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/jmoiron/sqlx"
)

var log = logging.Logger("modules")

// NewDB returns an *sqlx.DB instance
func NewDB(cfg *config.SchedulerCfg) (*sqlx.DB, error) {
	return sqldb.NewDB(cfg.DatabaseAddress)
}

// GenerateTokenWithWebPermission create a new token based on the given permissions
func GenerateTokenWithWebPermission(ca *common.CommonAPI) (dtypes.PermissionWebToken, error) {
	token, err := ca.AuthNew(context.Background(), &types.JWTPayload{Allow: []auth.Permission{api.RoleWeb}, ID: uuid.NewString()})
	if err != nil {
		return "", err
	}
	return dtypes.PermissionWebToken(token), nil
}

// AssetManagerParams Manager Params
type AssetManagerParams struct {
	fx.In

	Lifecycle      fx.Lifecycle
	MetricsCtx     helpers.MetricsCtx
	MetadataDS     dtypes.AssetMetadataDS
	NodeManger     *node.Manager
	WorkloadManger *workload.Manager
	dtypes.GetSchedulerConfigFunc
	*db.SQLDB
}

// NewAssetManager creates a new storage manager instance
func NewAssetManager(params AssetManagerParams) *assets.Manager {
	var (
		mctx    = params.MetricsCtx
		lc      = params.Lifecycle
		nodeMgr = params.NodeManger
		ds      = params.MetadataDS
		cfgFunc = params.GetSchedulerConfigFunc
		sdb     = params.SQLDB
		wMgr    = params.WorkloadManger
	)

	ctx := helpers.LifecycleCtx(mctx, lc)
	m := assets.NewManager(nodeMgr, ds, cfgFunc, sdb, wMgr)

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go m.Start(ctx)
			return nil
		},
		OnStop: m.Terminate,
	})

	return m
}

// ProjectManagerParams Manager Params
type ProjectManagerParams struct {
	fx.In

	Lifecycle      fx.Lifecycle
	MetricsCtx     helpers.MetricsCtx
	MetadataDS     dtypes.ProjectMetadataDS
	NodeManger     *node.Manager
	WorkloadManger *workload.Manager
	dtypes.GetSchedulerConfigFunc
	*db.SQLDB
}

// NewProjectManager creates a new project manager instance
func NewProjectManager(params ProjectManagerParams) *projects.Manager {
	var (
		mctx    = params.MetricsCtx
		lc      = params.Lifecycle
		nodeMgr = params.NodeManger
		ds      = params.MetadataDS
		sdb     = params.SQLDB
	)

	ctx := helpers.LifecycleCtx(mctx, lc)
	m := projects.NewManager(nodeMgr, sdb, ds)

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go m.StartTimer(ctx)
			return nil
		},
		OnStop: m.Terminate,
	})

	return m
}

// NewValidation creates a new validation manager instance
func NewValidation(mctx helpers.MetricsCtx, l fx.Lifecycle, nm *node.Manager, am *assets.Manager, configFunc dtypes.GetSchedulerConfigFunc, p *pubsub.PubSub, lmgr *leadership.Manager) *validation.Manager {
	v := validation.NewManager(nm, am, configFunc, p, lmgr)

	ctx := helpers.LifecycleCtx(mctx, l)
	l.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go v.Start(ctx)
			return nil
		},
		OnStop: v.Stop,
	})

	return v
}

// NewSetSchedulerConfigFunc creates a function to set the scheduler config
func NewSetSchedulerConfigFunc(r repo.LockedRepo) func(config.SchedulerCfg) error {
	return func(cfg config.SchedulerCfg) (err error) {
		return r.SetConfig(func(raw interface{}) {
			scfg, ok := raw.(*config.SchedulerCfg)
			if !ok {
				return
			}
			scfg.EnableValidation = cfg.EnableValidation
		})
	}
}

// NewGetSchedulerConfigFunc creates a function to get the scheduler config
func NewGetSchedulerConfigFunc(r repo.LockedRepo) func() (config.SchedulerCfg, error) {
	return func() (out config.SchedulerCfg, err error) {
		raw, err := r.Config()
		if err != nil {
			return
		}

		scfg, ok := raw.(*config.SchedulerCfg)
		if !ok {
			return
		}

		out = *scfg
		return
	}
}

// NewPubSub returns a new pubsub instance with a buffer of 50
func NewPubSub() *pubsub.PubSub {
	return pubsub.New(50)
}

// RegisterToEtcd registers the server to etcd
func RegisterToEtcd(mctx helpers.MetricsCtx, lc fx.Lifecycle, configFunc dtypes.GetSchedulerConfigFunc, serverID dtypes.ServerID, token dtypes.PermissionWebToken) (*etcdcli.Client, error) {
	cfg, err := configFunc()
	if err != nil {
		return nil, err
	}
	sCfg := &types.SchedulerCfg{
		AreaID:       cfg.AreaID,
		SchedulerURL: cfg.ExternalURL,
		AccessToken:  string(token),
		Weight:       cfg.Weight,
	}

	value, err := etcdcli.SCMarshal(sCfg)
	if err != nil {
		return nil, xerrors.Errorf("cfg SCMarshal err:%s", err.Error())
	}

	eCli, err := etcdcli.New(cfg.EtcdAddresses)
	if err != nil {
		return nil, err
	}

	ctx := helpers.LifecycleCtx(mctx, lc)
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			return eCli.ServerRegister(ctx, string(serverID), types.RunningNodeType.String(), string(value))
		},
		OnStop: func(context.Context) error {
			return eCli.ServerUnRegister(ctx, string(serverID), types.RunningNodeType.String())
		},
	})

	return eCli, nil
}

func NewContainerManager(mctx helpers.MetricsCtx, l fx.Lifecycle, nm *node.Manager, db *db.SQLDB, p *pubsub.PubSub) *container.Manager {
	m := container.NewManager(nm, db, p)

	ctx := helpers.LifecycleCtx(mctx, l)
	l.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go m.ListenNodeState(ctx)
			return nil
		},
	})

	return m
}
