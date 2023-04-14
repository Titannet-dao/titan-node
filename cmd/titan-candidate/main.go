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
	"strconv"
	"strings"
	"time"

	cliutil "github.com/Filecoin-Titan/titan/cli/util"
	"github.com/Filecoin-Titan/titan/node"
	"github.com/Filecoin-Titan/titan/node/asset"
	"github.com/Filecoin-Titan/titan/node/httpserver"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/build"
	lcli "github.com/Filecoin-Titan/titan/cli"
	"github.com/Filecoin-Titan/titan/lib/titanlog"
	"github.com/Filecoin-Titan/titan/metrics"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/repo"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/quic-go/quic-go/http3"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
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
		runCmd,
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

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start titan candidate node",
	Flags: []cli.Flag{},

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

		cfg, err := lr.Config()
		if err != nil {
			return err
		}

		candidateCfg := cfg.(*config.CandidateCfg)

		err = lr.Close()
		if err != nil {
			return err
		}

		if len(candidateCfg.NodeID) == 0 || len(candidateCfg.AreaID) == 0 {
			return xerrors.Errorf(`Please config node id and area id, example: \n
			titan-candidate config set --node-id=your_node_id --area-id=your_area_id`)
		}

		privateKey, err := loadPrivateKey(r)
		if err != nil {
			return xerrors.Errorf(`Please import private key, example: titan-candidate key import --path /path/to/private.key`)
		}

		connectTimeout, err := time.ParseDuration(candidateCfg.Timeout)
		if err != nil {
			return err
		}

		udpPacketConn, err := net.ListenPacket("udp", candidateCfg.ListenAddress)
		if err != nil {
			return err
		}
		defer udpPacketConn.Close() //nolint:errcheck // ignore error

		// all jsonrpc client use udp
		httpClient, err := cliutil.NewHTTP3Client(udpPacketConn, candidateCfg.InsecureSkipVerify, candidateCfg.CaCertificatePath)
		if err != nil {
			return xerrors.Errorf("new http3 client error %w", err)
		}
		jsonrpc.SetHttp3Client(httpClient)

		url, err := getSchedulerURL(cctx, candidateCfg.NodeID, candidateCfg.AreaID, candidateCfg.Locator)
		if err != nil {
			return err
		}

		// Connect to scheduler
		schedulerAPI, closer, err := newSchedulerAPI(cctx, url, candidateCfg.NodeID, privateKey)
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

		var httpServer *httpserver.HttpServer
		var candidateAPI api.Candidate
		stop, err := node.New(cctx.Context,
			node.Candidate(&candidateAPI),
			node.Base(),
			node.Repo(r),
			node.Override(new(dtypes.NodeID), dtypes.NodeID(candidateCfg.NodeID)),
			node.Override(new(api.Scheduler), schedulerAPI),
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
			node.Override(node.RunGateway, func(assetMgr *asset.Manager) error {
				httpServer = httpserver.NewHttpServer(assetMgr, schedulerAPI, privateKey)

				return nil
			}),
		)
		if err != nil {
			return xerrors.Errorf("creating node: %w", err)
		}

		handler := CandidateHandler(candidateAPI.AuthVerify, candidateAPI, true)
		handler = httpServer.NewHandler(handler)

		srv := &http.Server{
			ReadHeaderTimeout: 30 * time.Second,
			Handler:           handler,
			BaseContext: func(listener net.Listener) context.Context {
				ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "titan-candidate"))
				return ctx
			},
		}

		go startUDPServer(udpPacketConn, handler, candidateCfg) //nolint:errcheck

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}
			stop(ctx) //nolint:errcheck
			log.Warn("Graceful shutdown successful")
		}()

		nl, err := net.Listen("tcp", candidateCfg.ListenAddress)
		if err != nil {
			return err
		}

		log.Infof("Candidate listen on %s", candidateCfg.ListenAddress)

		schedulerSession, err := getSchedulerSession(schedulerAPI, connectTimeout)
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

				token, err := candidateAPI.AuthNew(cctx.Context, api.AllPermissions)
				if err != nil {
					log.Errorf("auth new error %s", err.Error())
					return
				}

				errCount := 0
				for {
					curSession, err := getSchedulerSession(schedulerAPI, connectTimeout)
					if err != nil {
						errCount++
						log.Errorf("heartbeat: checking remote session failed: %+v", err)
					} else {
						if curSession != schedulerSession {
							schedulerSession = curSession
							break
						}

						if errCount > 0 {
							break
						}
					}

					select {
					case <-readyCh:
						opts := &types.ConnectOptions{Token: token, TcpServerPort: tcpServerPort}
						err := schedulerAPI.CandidateConnect(ctx, opts)
						if err != nil {
							log.Errorf("Registering candidate failed: %+v", err)
							cancel()
							return
						}

						pk, err := getSchedulerPublicKey(schedulerAPI)
						if err != nil {
							log.Errorf("get scheduler public key failed: %s", err.Error())
							cancel()
							return
						}

						httpServer.SetSchedulerPublicKey(pk)

						log.Info("Candidate registered successfully, waiting for tasks")
						errCount = 0
						readyCh = nil
					case <-heartbeats.C:
					case <-ctx.Done():
						return // graceful shutdown
					}
				}

				log.Errorf("TITAN-EDGE CONNECTION LOST")
			}
		}()

		return srv.Serve(nl)
	},
}

func getSchedulerSession(api api.Scheduler, timeout time.Duration) (uuid.UUID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return api.Session(ctx)
}

func getSchedulerVersion(api api.Scheduler, timeout time.Duration) (api.APIVersion, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return api.Version(ctx)
}

func newAuthTokenFromScheduler(schedulerURL, nodeID string, privateKey *rsa.PrivateKey) (string, error) {
	schedulerAPI, closer, err := client.NewScheduler(context.Background(), schedulerURL, nil)
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

func getAccessPoint(cctx *cli.Context, nodeID, areaID string) (string, error) {
	locator, closer, err := lcli.GetLocatorAPI(cctx)
	if err != nil {
		return "", err
	}
	defer closer()

	schedulerURLs, err := locator.GetAccessPoints(context.Background(), nodeID, areaID)
	if err != nil {
		return "", err
	}

	if len(schedulerURLs) <= 0 {
		return "", fmt.Errorf("no access point in area %s for node %s", areaID, nodeID)
	}

	return schedulerURLs[0], nil
}

func getSchedulerURL(cctx *cli.Context, nodeID, areaID string, isPassLocator bool) (string, error) {
	if isPassLocator {
		schedulerURL, err := getAccessPoint(cctx, nodeID, areaID)
		if err != nil {
			return "", err
		}

		return schedulerURL, nil
	}

	schedulerURL, _, err := lcli.GetRawAPI(cctx, repo.Scheduler, "v0")
	if err != nil {
		return "", err
	}

	return schedulerURL, nil
}

func newSchedulerAPI(cctx *cli.Context, schedulerURL, nodeID string, privateKey *rsa.PrivateKey) (api.Scheduler, jsonrpc.ClientCloser, error) {
	token, err := newAuthTokenFromScheduler(schedulerURL, nodeID, privateKey)
	if err != nil {
		return nil, nil, err
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+token)
	headers.Add("Node-ID", nodeID)

	schedulerAPI, closer, err := client.NewScheduler(context.Background(), schedulerURL, headers)
	if err != nil {
		return nil, nil, err
	}
	log.Infof("scheduler url:%s, token:%s", schedulerURL, token)

	if err := os.Setenv("SCHEDULER_API_INFO", token+":"+schedulerURL); err != nil {
		log.Errorf("set env error:%s", err.Error())
	}

	return schedulerAPI, closer, nil
}

func startUDPServer(conn net.PacketConn, handler http.Handler, candidateCfg *config.CandidateCfg) error {
	var tlsConfig *tls.Config
	if candidateCfg.InsecureSkipVerify {
		config, err := defaultTLSConfig()
		if err != nil {
			log.Errorf("startUDPServer, defaultTLSConfig error:%s", err.Error())
			return err
		}
		tlsConfig = config
	} else {
		cert, err := tls.LoadX509KeyPair(candidateCfg.CaCertificatePath, candidateCfg.PrivateKeyPath)
		if err != nil {
			log.Errorf("startUDPServer, LoadX509KeyPair error:%s", err.Error())
			return err
		}

		tlsConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: false,
		}
	}

	srv := http3.Server{
		TLSConfig: tlsConfig,
		Handler:   handler,
	}

	return srv.Serve(conn)
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

func getSchedulerPublicKey(schedulerAPI api.Scheduler) (*rsa.PublicKey, error) {
	pem, err := schedulerAPI.GetSchedulerPublicKey(context.Background())
	if err != nil {
		return nil, err
	}

	return titanrsa.Pem2PublicKey([]byte(pem))
}
