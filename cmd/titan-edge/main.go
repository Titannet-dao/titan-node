package main

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/node"
	"github.com/Filecoin-Titan/titan/node/asset"
	"github.com/Filecoin-Titan/titan/node/httpserver"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/validation"
	"github.com/gbrlsnchs/jwt/v3"

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
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

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
	FlagEdgeRepo            = "edge-repo"
	FlagEdgeRepoDeprecation = "edgerepo"
	DefaultStorageDir       = "storage"
	HeartbeatInterval       = 10 * time.Second
)

func main() {
	types.RunningNodeType = types.NodeEdge
	titanlog.SetupLogLevels()
	local := []*cli.Command{
		daemonCmd,
	}

	local = append(local, lcli.CommonCommands...)

	app := &cli.App{
		Name:                 "titan-edge",
		Usage:                "Titan edge node",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagEdgeRepo,
				Aliases: []string{FlagEdgeRepoDeprecation},
				EnvVars: []string{"TITAN_EDGE_PATH", "EDGE_PATH"},
				Value:   "~/.titanedge", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify edge repo path. flag %s and env TITAN_EDGE_PATH are DEPRECATION, will REMOVE SOON", FlagEdgeRepoDeprecation),
			},
			&cli.StringFlag{
				Name:    "panic-reports",
				EnvVars: []string{"TITAN_PANIC_REPORT_PATH"},
				Hidden:  true,
				Value:   "~/.titanedge", // should follow --repo default
			},
		},

		After: func(c *cli.Context) error {
			if r := recover(); r != nil {
				// Generate report in TITAN_EDGE_PATH and re-raise panic
				build.GeneratePanicReport(c.String("panic-reports"), c.String(FlagEdgeRepo), c.App.Name)
				panic(r)
			}
			return nil
		},
		Commands: append(local, lcli.EdgeCmds...),
	}
	app.Setup()
	app.Metadata["repoType"] = repo.Edge

	if err := app.Run(os.Args); err != nil {
		log.Errorf("%+v", err)
		os.Exit(1)
	}
}

var daemonCmd = &cli.Command{
	Name:  "daemon",
	Usage: "Daemon commands",
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
		edgeAPI, close, err := lcli.GetEdgeAPI(cctx)
		if err != nil {
			return err
		}

		defer close()

		return edgeAPI.Shutdown(cctx.Context)
	},
}

var daemonStartCmd = &cli.Command{
	Name:  "start",
	Usage: "Start titan edge node",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "init",
			Usage: "--init=true, initialize edge at first run",
			Value: false,
		},
		&cli.StringFlag{
			Name:  "url",
			Usage: "--url=https://titan-server-domain/rpc/v0",
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

		repoPath := cctx.String(FlagEdgeRepo)

		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			if err := r.Init(repo.Edge); err != nil {
				return err
			}
		}

		lr, err := r.Lock(repo.Edge)
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

			if err := lcli.RegitsterNode(cctx, lr, locatorURL, types.NodeEdge); err != nil {
				return err
			}
		}

		cfg, err := lr.Config()
		if err != nil {
			return err
		}

		edgeCfg := cfg.(*config.EdgeCfg)

		err = lr.Close()
		if err != nil {
			return err
		}

		privateKey, err := loadPrivateKey(r)
		if err != nil {
			return fmt.Errorf(`please initialize edge, example: 
			titan-edge daemon start --init --url https://titan-network-url/rpc/v0`)
		}

		if len(edgeCfg.AreaID) == 0 {
			return fmt.Errorf(`please config node id and area id, example:
			titan-edge config set --node-id=your_node_id --area-id=your_area_id `)
		}

		nodeIDBuf, err := r.NodeID()
		if err != nil {
			return err
		}
		nodeID := string(nodeIDBuf)

		connectTimeout, err := time.ParseDuration(edgeCfg.Network.Timeout)
		if err != nil {
			return err
		}

		udpPacketConn, err := net.ListenPacket("udp", edgeCfg.Network.ListenAddress)
		if err != nil {
			return err
		}
		defer udpPacketConn.Close() //nolint:errcheck  // ignore error

		transport := &quic.Transport{
			Conn: udpPacketConn,
		}

		schedulerURL, _, _ := lcli.GetRawAPI(cctx, repo.Scheduler, "v0")
		if len(schedulerURL) == 0 {
			schedulerURL, err = getAccessPoint(cctx, edgeCfg.Network.LocatorURL, nodeID, edgeCfg.AreaID)
			if err != nil {
				return err
			}
		}

		schedulerAPI, closer, err := newSchedulerAPI(cctx, transport, schedulerURL, nodeID, privateKey)
		if err != nil {
			return xerrors.Errorf("new scheduler api: %w", err)
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

		var shutdownChan = make(chan struct{})
		var httpServer *httpserver.HttpServer
		var edgeAPI api.Edge
		stop, err := node.New(cctx.Context,
			node.Edge(&edgeAPI),
			node.Base(),
			node.Repo(r),
			node.Override(new(dtypes.NodeID), dtypes.NodeID(nodeID)),
			node.Override(new(api.Scheduler), schedulerAPI),
			node.Override(new(dtypes.ShutdownChan), shutdownChan),
			node.Override(new(*quic.Transport), transport),
			node.Override(new(dtypes.NodeMetadataPath), func() dtypes.NodeMetadataPath {
				metadataPath := edgeCfg.Storage.Path
				if len(metadataPath) == 0 {
					metadataPath = path.Join(lr.Path(), DefaultStorageDir)
				}

				log.Debugf("metadataPath:%s", metadataPath)
				return dtypes.NodeMetadataPath(metadataPath)
			}),
			node.Override(new(dtypes.AssetsPaths), func() dtypes.AssetsPaths {
				var assetsPaths = []string{path.Join(lr.Path(), DefaultStorageDir)}
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

			node.Override(node.RunGateway, func(assetMgr *asset.Manager, validation *validation.Validation, apiSecret *jwt.HMACSHA) error {
				opts := &httpserver.HttpServerOptions{
					Asset: assetMgr, Scheduler: schedulerAPI,
					PrivateKey:          privateKey,
					Validation:          validation,
					APISecret:           apiSecret,
					MaxSizeOfUploadFile: edgeCfg.MaxSizeOfUploadFile,
				}
				httpServer = httpserver.NewHttpServer(opts)

				return err
			}),

			node.Override(node.SetApiEndpointKey, func(lr repo.LockedRepo) error {
				return setEndpointAPI(lr, edgeCfg.Network.ListenAddress)
			}),
		)
		if err != nil {
			return xerrors.Errorf("creating node: %w", err)
		}

		handler := EdgeHandler(edgeAPI.AuthVerify, edgeAPI, true)
		handler = httpServer.NewHandler(handler)

		httpSrv := &http.Server{
			ReadHeaderTimeout: 30 * time.Second,
			Handler:           handler,
			BaseContext: func(listener net.Listener) context.Context {
				ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "titan-edge"))
				return ctx
			},
		}

		go startHTTP3Server(transport, handler, edgeCfg)

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")

			if err := transport.Close(); err != nil {
				log.Errorf("shutting down http3Srv failed: %s", err)
			}

			if err := httpSrv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}

			stop(ctx) //nolint:errcheck
			log.Warn("Graceful shutdown successful")
		}()

		nl, err := net.Listen("tcp", edgeCfg.Network.ListenAddress)
		if err != nil {
			return err
		}

		log.Infof("Edge listen on tcp/udp %s", edgeCfg.Network.ListenAddress)

		schedulerSession, err := schedulerAPI.Session(ctx)
		if err != nil {
			return xerrors.Errorf("getting scheduler session: %w", err)
		}

		token, err := edgeAPI.AuthNew(cctx.Context, &types.JWTPayload{Allow: []auth.Permission{api.RoleAdmin}, ID: nodeID})
		if err != nil {
			return xerrors.Errorf("generate token for scheduler error: %w", err)
		}

		waitQuietCh := func() chan struct{} {
			out := make(chan struct{})
			go func() {
				ctx2 := context.Background()
				err = edgeAPI.WaitQuiet(ctx2)
				if err != nil {
					log.Errorf("wait quiet error %s", err.Error())
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
						opts := &types.ConnectOptions{Token: token}
						if err := schedulerAPI.EdgeConnect(ctx, opts); err != nil {
							log.Errorf("Registering edge failed: %s", err.Error())
							cancel()
							return
						}

						log.Info("Edge registered successfully, waiting for tasks")
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

		return httpSrv.Serve(nl)
	},
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

func newSchedulerAPI(cctx *cli.Context, transport *quic.Transport, schedulerURL, nodeID string, privateKey *rsa.PrivateKey) (api.Scheduler, jsonrpc.ClientCloser, error) {
	token, err := newAuthTokenFromScheduler(schedulerURL, nodeID, privateKey)
	if err != nil {
		return nil, nil, err
	}

	httpClient, err := client.NewHTTP3ClientWithPacketConn(transport)
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

	return schedulerAPI, closer, nil
}

func defaultTLSConfig() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		Certificates:       []tls.Certificate{tlsCert},
		NextProtos:         []string{"h2", "h3"},
		InsecureSkipVerify: true, //nolint:gosec // skip verify in default config
	}, nil
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

func startHTTP3Server(transport *quic.Transport, handler http.Handler, config *config.EdgeCfg) error {
	var tlsConfig *tls.Config
	if len(config.CertificatePath) == 0 && len(config.PrivateKeyPath) == 0 {
		config, err := defaultTLSConfig()
		if err != nil {
			log.Errorf("startUDPServer, defaultTLSConfig error:%s", err.Error())
			return err
		}
		tlsConfig = config
	} else {
		cert, err := tls.LoadX509KeyPair(config.CertificatePath, config.PrivateKeyPath)
		if err != nil {
			log.Errorf("startUDPServer, LoadX509KeyPair error:%s", err.Error())
			return err
		}

		tlsConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			Certificates:       []tls.Certificate{cert},
			NextProtos:         []string{"h2", "h3"},
			InsecureSkipVerify: false,
		}
	}

	ln, err := transport.ListenEarly(tlsConfig, nil)
	if err != nil {
		return err
	}

	srv := http3.Server{
		TLSConfig: tlsConfig,
		Handler:   handler,
	}

	if err = srv.ServeListener(ln); err != nil {
		log.Warn("http3 server ", err.Error())
	}
	return err
}
