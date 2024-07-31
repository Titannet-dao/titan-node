package main

import (
	"context"
	"crypto/rsa"
	"fmt"
	"net"
	"path"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/metrics"
	"github.com/Filecoin-Titan/titan/node"
	"github.com/Filecoin-Titan/titan/node/asset"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/edge/clib"
	"github.com/Filecoin-Titan/titan/node/httpserver"
	"github.com/Filecoin-Titan/titan/node/modules"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/repo"
	tunclient "github.com/Filecoin-Titan/titan/node/tunnel/client"
	"github.com/Filecoin-Titan/titan/node/validation"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/quic-go/quic-go"
	"go.opencensus.io/stats/view"
	"golang.org/x/xerrors"
)

const serverInternalError = "software caused connection abort"

type daemon struct {
	ID           string
	httpServer   *httpserver.HttpServer
	transport    *quic.Transport
	edgeAPI      api.Edge
	edgeConfig   *config.EdgeCfg
	schedulerAPI api.Scheduler
	privateKey   *rsa.PrivateKey

	repoPath string

	stop           node.StopFunc
	closeScheduler jsonrpc.ClientCloser

	ctx       context.Context
	ctxCancel context.CancelFunc

	shutdownChan    chan struct{} // shutdown chan
	restartChan     chan struct{} // cli restart
	restartDoneChan chan struct{} // make sure all modules are ready to start

	geoInfo *region.GeoInfo
}

func newDaemon(ctx context.Context, repoPath string) (*daemon, error) {
	ctx, cancel := context.WithCancel(ctx)

	// Register all metric views
	if err := view.Register(
		metrics.DefaultViews...,
	); err != nil {
		log.Fatalf("Cannot register the view: %v", err)
	}

	// repoPath := cctx.String(FlagEdgeRepo)

	r, err := openRepoOrNew(repoPath)
	if err != nil {
		return nil, err
	}

	lr, err := r.Lock(repo.Edge)
	if err != nil {
		return nil, err
	}

	cfg, err := lr.Config()
	if err != nil {
		return nil, err
	}

	edgeCfg := cfg.(*config.EdgeCfg)

	err = lr.Close()
	if err != nil {
		return nil, err
	}

	privateKey, err := loadPrivateKey(r)
	if err != nil {
		return nil, fmt.Errorf(`please initialize edge, example: 
		titan-edge daemon start --init --url https://titan-network-url/rpc/v0`)
	}

	nodeIDBuf, err := r.NodeID()
	if err != nil {
		return nil, err
	}
	nodeID := string(nodeIDBuf)

	connectTimeout, err := time.ParseDuration(edgeCfg.Network.Timeout)
	if err != nil {
		return nil, err
	}

	udpPacketConn, err := net.ListenPacket("udp", edgeCfg.Network.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("ListenPacket %w", err)
	}

	transport := &quic.Transport{
		Conn: udpPacketConn,
	}

	accessPoint, err := getAccessPoint(edgeCfg.Network.LocatorURL, nodeID, edgeCfg.AreaID)
	if err != nil {
		return nil, err
	}

	if len(accessPoint.Schedulers) == 0 {
		return nil, fmt.Errorf("can not get access point, nodeID %s, areaID %s", nodeID, edgeCfg.AreaID)
	}

	schedulerURL := accessPoint.Schedulers[0]

	schedulerAPI, closeScheduler, err := newSchedulerAPI(transport, schedulerURL, nodeID, privateKey)
	if err != nil {
		return nil, xerrors.Errorf("new scheduler api: %w", err)
	}

	v, err := getSchedulerVersion(schedulerAPI, connectTimeout)
	if err != nil {
		return nil, err
	}

	if v.APIVersion != api.SchedulerAPIVersion0 {
		return nil, xerrors.Errorf("titan-scheduler API version doesn't match: expected: %s", api.APIVersion{APIVersion: api.SchedulerAPIVersion0})
	}
	log.Infof("Remote version %s", v)

	var (
		shutdownChan    = make(chan struct{}) // shutdown chan
		restartChan     = make(chan struct{}) // cli restart
		restartDoneChan = make(chan struct{}) // make sure all modules are ready to start
	)

	var httpServer *httpserver.HttpServer
	var edgeAPI api.Edge

	stop, err := node.New(ctx,
		node.Edge(&edgeAPI),
		node.Base(),
		node.RepoCtx(ctx, r),
		node.Override(new(dtypes.NodeID), dtypes.NodeID(nodeID)),
		node.Override(new(api.Scheduler), schedulerAPI),
		node.Override(new(dtypes.ShutdownChan), shutdownChan),
		node.Override(new(dtypes.RestartChan), restartChan),
		node.Override(new(dtypes.RestartDoneChan), restartDoneChan),
		node.Override(new(*quic.Transport), transport),
		node.Override(new(*asset.Manager), modules.NewAssetsManager(ctx, &edgeCfg.Puller, edgeCfg.IPFSAPIURL)),

		node.Override(new(dtypes.NodeMetadataPath), func() dtypes.NodeMetadataPath {
			metadataPath := edgeCfg.Storage.Path
			if len(metadataPath) == 0 {
				metadataPath = path.Join(lr.Path(), DefaultStorageDir)
			}

			log.Debugf("metadataPath:%s", metadataPath)
			return dtypes.NodeMetadataPath(metadataPath)
		}),
		node.Override(new(dtypes.AssetsPaths), func() dtypes.AssetsPaths {
			assetsPaths := []string{path.Join(lr.Path(), DefaultStorageDir)}
			if len(edgeCfg.Storage.Path) > 0 {
				assetsPaths = []string{edgeCfg.Storage.Path}
			}

			log.Debugf("storage path:%#v", assetsPaths)
			return dtypes.AssetsPaths(assetsPaths)
		}),
		node.Override(new(dtypes.InternalIP), func() (dtypes.InternalIP, error) {
			schedulerAddr := strings.Split(schedulerURL, "/")
			conn, err := net.DialTimeout("tcp", schedulerAddr[2], connectTimeout)
			if err != nil {
				return "", err
			}

			defer conn.Close() //nolint:errcheck
			localAddr := conn.LocalAddr().(*net.TCPAddr)

			return dtypes.InternalIP(strings.Split(localAddr.IP.String(), ":")[0]), nil
		}),

		node.Override(node.RunGateway, func(assetMgr *asset.Manager, validation *validation.Validation, apiSecret *jwt.HMACSHA, limiter *types.RateLimiter) error {
			opts := &httpserver.HttpServerOptions{
				Asset: assetMgr, Scheduler: schedulerAPI,
				PrivateKey:          privateKey,
				Validation:          validation,
				APISecret:           apiSecret,
				MaxSizeOfUploadFile: edgeCfg.MaxSizeOfUploadFile,
				RateLimiter:         limiter,
			}
			httpServer = httpserver.NewHttpServer(opts)

			return err
		}),

		node.Override(node.SetApiEndpointKey, func(lr repo.LockedRepo) error {
			return setEndpointAPI(lr, edgeCfg.Network.ListenAddress)
		}),
		node.Override(new(api.Scheduler), func() api.Scheduler { return schedulerAPI }),

		node.Override(new(*tunclient.Services), func(scheduler api.Scheduler, nid dtypes.NodeID) *tunclient.Services {
			return tunclient.NewServices(ctx, scheduler, string(nid))
		}),
	)
	if err != nil {
		return nil, xerrors.Errorf("creating node: %w", err)
	}

	log.Info("New titan daemon")

	d := &daemon{
		ID:           nodeID,
		httpServer:   httpServer,
		transport:    transport,
		edgeAPI:      edgeAPI,
		schedulerAPI: schedulerAPI,
		edgeConfig:   edgeCfg,
		privateKey:   privateKey,

		repoPath: repoPath,

		stop:           stop,
		closeScheduler: closeScheduler,

		ctx:       ctx,
		ctxCancel: cancel,

		shutdownChan:    shutdownChan,
		restartChan:     restartChan,
		restartDoneChan: restartDoneChan,

		geoInfo: accessPoint.GeoInfo,
	}

	return d, nil
}

func (d *daemon) startServer(daemonSwitch *clib.DaemonSwitch) error {
	registShutdownSignal(d.shutdownChan)

	handler, httpSrv := buildSrvHandler(d.httpServer, d.edgeAPI, d.edgeConfig, d.schedulerAPI, d.privateKey)

	go func() {
		err := startHTTP3Server(d.ctx, d.transport, handler, d.edgeConfig)
		if err != nil && strings.Contains(err.Error(), serverInternalError) {
			log.Warnf("http3 server was kill by system, daemon restart")
			d.restartChan <- struct{}{}
		}
	}()

	go startHTTPServer(d.ctx, httpSrv, d.edgeConfig.Network)

	// Wait for the server to start, if the server does not start, the scheduler will fail to connect back.
	waitServerStart(d.edgeConfig.Network.ListenAddress)

	hbeatParams := heartbeatParams{
		shutdownChan: d.shutdownChan,
		edgeAPI:      d.edgeAPI,
		schedulerAPI: d.schedulerAPI,
		nodeID:       d.ID,
		daemonSwitch: daemonSwitch,
		geoInfo:      d.geoInfo,
	}
	go heartbeat(d.ctx, hbeatParams)

	quitWg.Add(3)

	for {
		select {
		case <-d.shutdownChan:
			log.Warn("Shutting down...")
			d.ctxCancel()

			err := d.stop(context.TODO()) //nolint:errcheck
			if err != nil {
				log.Errorf("stop err: %v", err)
			}

			quitWg.Wait()

			log.Warn("Graceful shutdown successful")

			return nil

		case <-d.restartChan:
			log.Warn("Restarting ...")
			d.ctxCancel()

			err := d.stop(context.TODO())
			if err != nil {
				log.Errorf("stop err: %v", err)
			}

			quitWg.Wait()

			d.restartDoneChan <- struct{}{} // node/edge/impl.go

			d, err = newDaemon(context.Background(), d.repoPath)
			if err != nil {
				log.Errorf("newDaemon %s", err.Error())
				return err
			}
			return d.startServer(daemonSwitch)
			// return daemonStart(context.Background(), daemonSwitch, node.repoPath, node.locatorURL)
		}
	}

}
