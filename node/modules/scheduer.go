package modules

import (
	"context"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/modules/helpers"
	"github.com/Filecoin-Titan/titan/node/repo"
	"github.com/Filecoin-Titan/titan/node/scheduler/assets"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/scheduler/validation"
	"github.com/Filecoin-Titan/titan/node/sqldb"
	"github.com/filecoin-project/pubsub"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/Filecoin-Titan/titan/node/common"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/jmoiron/sqlx"
)

var log = logging.Logger("modules")

// NewDB returns an *sqlx.DB instance
func NewDB(dbPath dtypes.DatabaseAddress) (*sqlx.DB, error) {
	return sqldb.NewDB(string(dbPath))
}

// GenerateTokenWithWritePermission create a new token based on the given permissions
func GenerateTokenWithWritePermission(ca *common.CommonAPI) (dtypes.PermissionWriteToken, error) {
	token, err := ca.AuthNew(context.Background(), api.ReadWritePerms)
	if err != nil {
		return "", err
	}
	return dtypes.PermissionWriteToken(token), nil
}

// GenerateTokenWithAdminPermission create a new token based on the given permissions
func GenerateTokenWithAdminPermission(ca *common.CommonAPI) (dtypes.PermissionAdminToken, error) {
	token, err := ca.AuthNew(context.Background(), api.AllPermissions)
	if err != nil {
		return "", err
	}
	return dtypes.PermissionAdminToken(token), nil
}

// DefaultSessionCallback returns a default session callback function
func DefaultSessionCallback() dtypes.SessionCallbackFunc {
	return func(s string, s2 string) error { return nil }
}

// StorageManagerParams Manager Params
type StorageManagerParams struct {
	fx.In

	Lifecycle  fx.Lifecycle
	MetricsCtx helpers.MetricsCtx
	MetadataDS dtypes.MetadataDS
	NodeManger *node.Manager
	dtypes.GetSchedulerConfigFunc
	*db.SQLDB
}

// NewStorageManager creates a new storage manager instance
func NewStorageManager(params StorageManagerParams) *assets.Manager {
	var (
		mctx    = params.MetricsCtx
		lc      = params.Lifecycle
		nodeMgr = params.NodeManger
		ds      = params.MetadataDS
		cfgFunc = params.GetSchedulerConfigFunc
		sdb     = params.SQLDB
	)

	ctx := helpers.LifecycleCtx(mctx, lc)
	m := assets.NewManager(nodeMgr, ds, cfgFunc, sdb)

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go m.Start(ctx)
			return nil
		},
		OnStop: m.Terminate,
	})

	return m
}

// NewValidation creates a new validation manager instance
func NewValidation(mctx helpers.MetricsCtx, lc fx.Lifecycle, m *node.Manager, configFunc dtypes.GetSchedulerConfigFunc, p *pubsub.PubSub) *validation.Manager {
	v := validation.NewManager(m, configFunc, p)

	ctx := helpers.LifecycleCtx(mctx, lc)
	lc.Append(fx.Hook{
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
			scfg.SchedulerServer1 = cfg.SchedulerServer1
			scfg.SchedulerServer2 = cfg.SchedulerServer2
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
func RegisterToEtcd(mctx helpers.MetricsCtx, lc fx.Lifecycle, configFunc dtypes.GetSchedulerConfigFunc, serverID dtypes.ServerID, token dtypes.PermissionAdminToken) error {
	cfg, err := configFunc()
	if err != nil {
		return err
	}

	sCfg := &types.SchedulerCfg{
		AreaID:       cfg.AreaID,
		SchedulerURL: cfg.ExternalURL,
		AccessToken:  string(token),
	}

	value, err := etcdcli.SCMarshal(sCfg)
	if err != nil {
		return xerrors.Errorf("cfg SCMarshal err:%s", err.Error())
	}

	eCli, err := etcdcli.New(cfg.EtcdAddresses)
	if err != nil {
		return err
	}

	ctx := helpers.LifecycleCtx(mctx, lc)
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			return eCli.ServerRegister(ctx, serverID, string(value))
		},
		OnStop: func(context.Context) error {
			return eCli.ServerUnRegister(ctx, serverID)
		},
	})

	return nil
}
