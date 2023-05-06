package modules

import (
	"context"
	"crypto/rsa"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/node/asset/fetcher"
	"github.com/Filecoin-Titan/titan/node/candidate"
	"github.com/Filecoin-Titan/titan/node/config"
	"go.uber.org/fx"
)

func NewBlockFetcherFromIPFS(cfg *config.CandidateCfg) fetcher.BlockFetcher {
	log.Info("ipfs-api " + cfg.IPFSAPIURL)
	return fetcher.NewIPFSClient(cfg.IPFSAPIURL)
}

// NewTCPServer returns a new TCP server instance.
func NewTCPServer(lc fx.Lifecycle, cfg *config.CandidateCfg, schedulerAPI api.Scheduler, key *rsa.PrivateKey) *candidate.TCPServer {
	srv := candidate.NewTCPServer(cfg, schedulerAPI, key)

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go srv.StartTCPServer()
			return nil
		},
		OnStop: srv.Stop,
	})

	return srv
}
