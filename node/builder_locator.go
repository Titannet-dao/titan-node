package node

import (
	"errors"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/locator"
	"github.com/Filecoin-Titan/titan/node/modules"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/repo"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/jmoiron/sqlx"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

func Locator(out *api.Locator) Option {
	return Options(
		ApplyIf(func(s *Settings) bool { return s.Config },
			Error(errors.New("the Locator option must be set before Config option")),
		),

		func(s *Settings) error {
			s.nodeType = repo.Locator
			return nil
		},

		func(s *Settings) error {
			resAPI := &locator.Locator{}
			s.invokes[ExtractAPIKey] = fx.Populate(resAPI)
			*out = resAPI
			return nil
		},
	)
}

func ConfigLocator(c interface{}) Option {
	cfg, ok := c.(*config.LocatorCfg)
	if !ok {
		return Error(xerrors.Errorf("invalid config from repo, got: %T", c))
	}
	log.Info("start to config locator")

	return Options(
		Override(new(*config.LocatorCfg), cfg),
		Override(new(dtypes.ServerID), modules.NewServerID),
		Override(new(*sqlx.DB), modules.NewDB),
		Override(new(region.Region), modules.NewRegion),
		Override(new(locator.Storage), modules.NewLocatorStorage),
		Override(new(dtypes.SessionCallbackFunc), func() dtypes.SessionCallbackFunc {
			return func(s string, s2 string) error { return nil }
		}),
		Override(new(dtypes.EtcdAddresses), func() dtypes.EtcdAddresses {
			return dtypes.EtcdAddresses(cfg.EtcdAddresses)
		}),
		Override(new(dtypes.GeoDBPath), func() dtypes.GeoDBPath {
			return dtypes.GeoDBPath(cfg.GeoDBPath)
		}),
	)
}
