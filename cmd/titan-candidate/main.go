package main

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/node"
	"github.com/Filecoin-Titan/titan/node/asset"
	"github.com/Filecoin-Titan/titan/node/httpserver"
	"github.com/Filecoin-Titan/titan/node/modules"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	tunserver "github.com/Filecoin-Titan/titan/node/tunnel/server"
	"github.com/Filecoin-Titan/titan/node/validation"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/terrors"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/build"
	lcli "github.com/Filecoin-Titan/titan/cli"
	"github.com/Filecoin-Titan/titan/lib/titanlog"
	"github.com/Filecoin-Titan/titan/metrics"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/repo"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"
)

var log = logging.Logger("main")

const (
	FlagCandidateRepo = "candidate-repo"

	// TODO remove after deprecation period
	FlagCandidateRepoDeprecation = "candidaterepo"
	DefaultStorageDir            = "storage"
	HeartbeatInterval            = 10 * time.Second
)

func main() {
	types.RunningNodeType = types.NodeCandidate
	titanlog.SetupLogLevels()

	local := []*cli.Command{
		daemonCmd,
	}

	local = append(local, lcli.CommonCommands...)

	// candidate cmds include edge cmds
	candidateCmds := append(local, lcli.CandidateCmds...)

	app := &cli.App{
		Name:                 "titan-candidate",
		Usage:                "Titan candidate node",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagCandidateRepo,
				Aliases: []string{FlagCandidateRepoDeprecation},
				EnvVars: []string{"TITAN_CANDIDATE_PATH", "CANDIDATE_PATH"},
				Value:   "~/.titancandidate", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify candidate repo path. flag %s and env TITAN_CANDIDATE_PATH are DEPRECATION, will REMOVE SOON", FlagCandidateRepoDeprecation),
			},
			&cli.StringFlag{
				Name:    "panic-reports",
				EnvVars: []string{"TITAN_PANIC_REPORT_PATH"},
				Hidden:  true,
				Value:   "~/.titancandidate", // should follow --repo default
			},
		},

		After: func(c *cli.Context) error {
			if r := recover(); r != nil {
				// Generate report in TITAN_PATH and re-raise panic
				build.GeneratePanicReport(c.String("panic-reports"), c.String(FlagCandidateRepo), c.App.Name)
				panic(r)
			}
			return nil
		},
		Commands: candidateCmds,
	}
	app.Setup()
	app.Metadata["repoType"] = repo.Candidate

	if err := app.Run(os.Args); err != nil {
		log.Errorf("%+v", err)
		return
	}
}

var daemonCmd = &cli.Command{
	Name:  "daemon",
	Usage: "daemon cmd",
	Subcommands: []*cli.Command{
		daemonStartCmd,
		daemonStopCmd,
	},
}

var daemonStopCmd = &cli.Command{
	Name:  "stop",
	Usage: "stop a running daemon",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		candidateAPI, close, err := lcli.GetCandidateAPI(cctx)
		if err != nil {
			return err
		}

		defer close()

		return candidateAPI.Shutdown(cctx.Context)
	},
}

var daemonStartCmd = &cli.Command{
	Name:  "start",
	Usage: "Start titan candidate node",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "init",
			Usage: "--init=true, initialize candidate at first run",
			Value: false,
		},
		&cli.StringFlag{
			Name:  "url",
			Usage: "--url=https://titan-server-domain/rpc/v0",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "code",
			Usage: "candidate register code",
			Value: "",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting titan edge node")

		// Register all metric views
		if err := view.Register(
			metrics.DefaultViews...,
		); err != nil {
			log.Fatalf("Cannot register the view: %v", err)
		}

		repoPath := cctx.String(FlagCandidateRepo)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			if err := r.Init(repo.Candidate); err != nil {
				return err
			}
		}

		lr, err := r.Lock(repo.Candidate)
		if err != nil {
			return err
		}

		_, nodeIDErr := r.NodeID()
		isNeedInit := cctx.Bool("init")
		if nodeIDErr == repo.ErrNodeIDNotExist && isNeedInit {
			locatorURL := cctx.String("url")
			if len(locatorURL) == 0 {
				return fmt.Errorf("Must set --url for --init")
			}

			code := cctx.String("code")
			if len(code) == 0 {
				return fmt.Errorf("--code can not empty")
			}

			if err := lcli.RegisterCandidateNode(lr, locatorURL, code); err != nil {
				return err
			}
		}

		cfg, err := lr.Config()
		if err != nil {
			return err
		}

		candidateCfg := cfg.(*config.CandidateCfg)

		nodeIDBuf, err := r.NodeID()
		if err != nil {
			return err
		}
		nodeID := string(nodeIDBuf)

		_, domainSuffix := fetchTlsConfigFromRemote(candidateCfg.AcmeUrl)
		if domainSuffix != "" {
			nodeVal := nodeID[2:]
			_, port, _ := net.SplitHostPort(candidateCfg.Network.ListenAddress)
			hostname := strings.Replace(domainSuffix, "*", nodeVal, 1)
			ingressHostname := strings.Replace(domainSuffix, "*.", "", 1)
			candidateCfg.ExternalURL = fmt.Sprintf("%s:%s", "https://"+hostname, port)
			if candidateCfg.IngressHostName == "" {
				candidateCfg.IngressHostName = ingressHostname
				candidateCfg.IngressCertificatePath = lr.GetCertificatePath()
				candidateCfg.IngressCertificateKeyPath = lr.GetCertificateKeyPath()
			}
		}

		err = lr.Close()
		if err != nil {
			return err
		}

		privateKey, err := loadPrivateKey(r)
		if err != nil {
			return fmt.Errorf(`please initialize candidate, example: 
			titan-candidate daemon start --init --url https://titan-network-url/rpc/v0`)
		}

		if len(candidateCfg.AreaID) == 0 {
			return fmt.Errorf(`please config node id and area id, example:
			titan-candidate config set --node-id=your_node_id --area-id=your_area_id`)
		}

		connectTimeout, err := time.ParseDuration(candidateCfg.Network.Timeout)
		if err != nil {
			return err
		}

		packetConn, err := net.ListenPacket("udp", candidateCfg.Network.ListenAddress)
		if err != nil {
			return err
		}
		defer packetConn.Close() //nolint:errcheck // ignore error

		transport := &quic.Transport{Conn: packetConn}

		schedulerURL, _, _ := lcli.GetRawAPI(cctx, repo.Scheduler, "v0")
		if len(schedulerURL) == 0 {
			schedulerURL, err = getAccessPoint(cctx, candidateCfg.Network.LocatorURL, nodeID, candidateCfg.AreaID)
			if err != nil {
				return err
			}
		}

		// Connect to scheduler
		schedulerAPI, closer, err := newSchedulerAPI(cctx, transport, schedulerURL, nodeID, privateKey)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		v, err := getSchedulerVersion(schedulerAPI, connectTimeout)
		if err != nil {
			return err
		}

		if v.APIVersion != api.SchedulerAPIVersion0 {
			return xerrors.Errorf("titan-scheduler API version doesn't match: expected: %s", api.APIVersion{APIVersion: api.SchedulerAPIVersion0})
		}
		log.Infof("Remote version %s", v)

		shutdownChan := make(chan struct{})
		var httpServer *httpserver.HttpServer
		var candidateAPI api.Candidate
		stop, err := node.New(cctx.Context,
			node.Candidate(&candidateAPI),
			node.Base(),
			node.Repo(r),
			node.Override(new(dtypes.NodeID), dtypes.NodeID(nodeID)),
			node.Override(new(api.Scheduler), schedulerAPI),
			node.Override(new(dtypes.ShutdownChan), shutdownChan),
			node.Override(new(*quic.Transport), transport),
			node.Override(new(*asset.Manager), modules.NewAssetsManager(ctx, &candidateCfg.Puller, candidateCfg.IPFSAPIURL)),
			node.Override(new(dtypes.NodeMetadataPath), func() dtypes.NodeMetadataPath {
				metadataPath := candidateCfg.MetadataPath
				if len(metadataPath) == 0 {
					metadataPath = path.Join(lr.Path(), DefaultStorageDir)
				}

				log.Infof("metadataPath:%s", metadataPath)
				return dtypes.NodeMetadataPath(metadataPath)
			}),
			node.Override(new(dtypes.AssetsPaths), func() dtypes.AssetsPaths {
				assetsPaths := candidateCfg.AssetsPaths
				if len(assetsPaths) == 0 {
					assetsPaths = []string{path.Join(lr.Path(), DefaultStorageDir)}
				}

				log.Debugf("assetsPaths:%#v", assetsPaths)
				return dtypes.AssetsPaths(assetsPaths)
			}),
			node.Override(new(dtypes.InternalIP), func() (dtypes.InternalIP, error) {
				ainfo, err := lcli.GetAPIInfo(cctx, repo.Scheduler)
				if err != nil {
					return "", xerrors.Errorf("could not get scheduler API info: %w", err)
				}

				schedulerAddr := strings.Split(ainfo.Addr, "/")
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
					MaxSizeOfUploadFile: candidateCfg.MaxSizeOfUploadFile,
					WebRedirect:         candidateCfg.WebRedirect,
					RateLimiter:         limiter,
				}
				httpServer = httpserver.NewHttpServer(opts)
				return nil
			}),
			node.Override(new(*rsa.PrivateKey), func() *rsa.PrivateKey {
				return privateKey
			}),
			node.Override(node.SetApiEndpointKey, func(lr repo.LockedRepo) error {
				return setEndpointAPI(lr, candidateCfg.Network.ListenAddress)
			}),
			node.Override(node.TlsConfigHandler, func(lr repo.LockedRepo) error {
				return NewTlsTickRefresher(candidateCfg.AcmeUrl, lr, candidateCfg)
			}),
		)
		if err != nil {
			return xerrors.Errorf("creating node: %w", err)
		}

		handler := CandidateHandler(candidateAPI.AuthVerify, candidateAPI, true)
		handler = httpServer.NewHandler(handler)
		handler = validation.AppendHandler(handler, schedulerAPI, privateKey, time.Duration(candidateCfg.ValidateDuration)*time.Second)
		handler = tunserver.NewTunserver(handler, schedulerAPI, nodeID)

		httpSrv := &http.Server{
			ReadHeaderTimeout: 30 * time.Second,
			Handler:           handler,
			BaseContext: func(listener net.Listener) context.Context {
				ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "titan-candidate"))
				return ctx
			},
			TLSConfig: &tls.Config{
				GetConfigForClient: func(chi *tls.ClientHelloInfo) (*tls.Config, error) {
					return tlsCfg, nil
				},
				GetCertificate: func(chi *tls.ClientHelloInfo) (*tls.Certificate, error) {
					return tlsCfg.GetCertificate(chi)
				},
			},
		}

		go startHTTP3Server(transport, handler, tlsCfg)

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")

			if err := transport.Close(); err != nil {
				log.Errorf("shutting down http3Srv failed: %s", err)
			}

			if err := httpSrv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down httpSrv failed: %s", err)
			}

			stop(ctx) //nolint:errcheck
			log.Warn("Graceful shutdown successful")
		}()

		nl, err := net.Listen("tcp", candidateCfg.Network.ListenAddress)
		if err != nil {
			return err
		}

		log.Infof("Candidate listen on %s", candidateCfg.Network.ListenAddress)

		schedulerSession, err := schedulerAPI.Session(ctx)
		if err != nil {
			return xerrors.Errorf("getting scheduler session: %w", err)
		}

		_, port, err := net.SplitHostPort(candidateCfg.TCPSrvAddr)
		if err != nil {
			return xerrors.Errorf("split tcp server addr: %w", err)
		}

		tcpServerPort, err := strconv.Atoi(port)
		if err != nil {
			return xerrors.Errorf("convert tcp server port from string: %w", err)
		}

		token, err := candidateAPI.AuthNew(cctx.Context, &types.JWTPayload{Allow: []auth.Permission{api.RoleAdmin}, ID: nodeID})
		if err != nil {
			return xerrors.Errorf("generate token for scheduler error: %w", err)
		}

		waitQuietCh := func() chan struct{} {
			out := make(chan struct{})
			go func() {
				ctx2 := context.Background()
				err = candidateAPI.WaitQuiet(ctx2)
				if err != nil {
					log.Errorf("wait quiet error: %s", err.Error())
				}
				close(out)
			}()
			return out
		}

		go func() {
			heartbeats := time.NewTicker(HeartbeatInterval)
			defer heartbeats.Stop()

			var readyCh chan struct{}
			for {
				// TODO: we could get rid of this, but that requires tracking resources for restarted tasks correctly
				if readyCh == nil {
					log.Info("Making sure no local tasks are running")
					readyCh = waitQuietCh()
				}

				for {
					select {
					case <-readyCh:
						// rs, rerr := candidateAPI.GetStatistics(ctx)
						// if rerr != nil {
						// 	log.Errorf("get resource statistics: %v", err)
						// }

						opts := &types.ConnectOptions{
							ExternalURL:        candidateCfg.ExternalURL,
							Token:              token,
							TcpServerPort:      tcpServerPort,
							IsPrivateMinioOnly: isPrivateMinioOnly(candidateCfg),
							// ResourcesStatistics: rs,
						}

						err := schedulerAPI.CandidateConnect(ctx, opts)
						if err != nil {
							log.Errorf("Registering candidate failed: %s", err.Error())
							cancel()
							return
						}

						log.Info("Candidate registered successfully, waiting for tasks")
						readyCh = nil
					case <-heartbeats.C:
					case <-ctx.Done():
						return // graceful shutdown
					case <-shutdownChan:
						cancel()
						return
					}

					curSession, err := keepalive(schedulerAPI, connectTimeout)
					if err != nil {
						log.Errorf("heartbeat: keepalive failed: %+v", err)
						errNode, ok := err.(*api.ErrNode)
						if ok {
							if errNode.Code == int(terrors.NodeDeactivate) {
								cancel()
								return
							} else if errNode.Code == int(terrors.NodeIPInconsistent) {
								break
							} else if errNode.Code == int(terrors.NodeOffline) && readyCh == nil {
								break
							}
						}
					} else if curSession != schedulerSession {
						log.Warn("change session id")
						schedulerSession = curSession
						break
					}
				}

				log.Errorf("TITAN-EDGE CONNECTION LOST")
			}
		}()

		return httpSrv.ServeTLS(nl, "", "")
	},
}

// private minio storage only, not public storage
func isPrivateMinioOnly(config *config.CandidateCfg) bool {
	return config.IsPrivate
}

func keepalive(api api.Scheduler, timeout time.Duration) (uuid.UUID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return api.NodeKeepaliveV2(ctx)
}

func getSchedulerVersion(api api.Scheduler, timeout time.Duration) (api.APIVersion, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return api.Version(ctx)
}

func newAuthTokenFromScheduler(schedulerURL, nodeID string, privateKey *rsa.PrivateKey) (string, error) {
	schedulerAPI, closer, err := client.NewScheduler(context.Background(), schedulerURL, nil, jsonrpc.WithHTTPClient(client.NewHTTP3Client()))
	if err != nil {
		return "", err
	}

	defer closer()

	rsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sign, err := rsa.Sign(privateKey, []byte(nodeID))
	if err != nil {
		return "", err
	}

	return schedulerAPI.NodeLogin(context.Background(), nodeID, hex.EncodeToString(sign))
}

func getAccessPoint(cctx *cli.Context, locatorURL, nodeID, areaID string) (string, error) {
	locator, close, err := client.NewLocator(cctx.Context, locatorURL, nil, jsonrpc.WithHTTPClient(client.NewHTTP3Client()))
	if err != nil {
		return "", err
	}
	defer close()

	schedulerURLs, err := locator.GetAccessPoints(context.Background(), nodeID, areaID)
	if err != nil {
		return "", err
	}

	if len(schedulerURLs) <= 0 {
		return "", fmt.Errorf("no access point in area %s for node %s", areaID, nodeID)
	}

	return schedulerURLs[0], nil
}

func newSchedulerAPI(cctx *cli.Context, tansport *quic.Transport, schedulerURL, nodeID string, privateKey *rsa.PrivateKey) (api.Scheduler, jsonrpc.ClientCloser, error) {
	token, err := newAuthTokenFromScheduler(schedulerURL, nodeID, privateKey)
	if err != nil {
		return nil, nil, err
	}

	httpClient, err := client.NewHTTP3ClientWithPacketConn(tansport)
	if err != nil {
		return nil, nil, err
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+token)
	headers.Add("Node-ID", nodeID)

	schedulerAPI, closer, err := client.NewScheduler(context.Background(), schedulerURL, headers, jsonrpc.WithHTTPClient(httpClient))
	if err != nil {
		return nil, nil, err
	}

	log.Debugf("scheduler url:%s, token:%s", schedulerURL, token)

	if err := os.Setenv("SCHEDULER_API_INFO", token+":"+schedulerURL); err != nil {
		log.Errorf("set env error:%s", err.Error())
	}

	return schedulerAPI, closer, nil
}

func loadPrivateKey(r *repo.FsRepo) (*rsa.PrivateKey, error) {
	pem, err := r.PrivateKey()
	if err != nil {
		return nil, err
	}
	return titanrsa.Pem2PrivateKey(pem)
}

func setEndpointAPI(lr repo.LockedRepo, address string) error {
	a, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return xerrors.Errorf("parsing address: %w", err)
	}

	ma, err := manet.FromNetAddr(a)
	if err != nil {
		return xerrors.Errorf("creating api multiaddress: %w", err)
	}

	if err := lr.SetAPIEndpoint(ma); err != nil {
		return xerrors.Errorf("setting api endpoint: %w", err)
	}

	return nil
}

func startHTTP3Server(transport *quic.Transport, handler http.Handler, config *tls.Config) error {
	var err error
	if config == nil {
		config, err = defaultTLSConfig()
		if err != nil {
			log.Errorf("startUDPServer, defaultTLSConfig error:%s", err.Error())
			return err
		}
	}
	ln, err := transport.ListenEarly(config, nil)
	if err != nil {
		log.Errorf("startUDPServer, ListenEarly error:%s", err.Error())
		return err
	}

	srv := http3.Server{
		TLSConfig: config,
		Handler:   handler,
	}
	return srv.ServeListener(ln)
}
