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
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Filecoin-Titan/titan/node/edge/clib"
	"github.com/Filecoin-Titan/titan/node/httpserver"
	"github.com/Filecoin-Titan/titan/node/tunnel"
	"github.com/Filecoin-Titan/titan/node/validation"

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
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"
)

var log = logging.Logger("main")

const (
	FlagEdgeRepo            = "edge-repo"
	FlagEdgeRepoDeprecation = "edgerepo"
	DefaultStorageDir       = "storage"
	WorkerdDir              = "workerd"
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
		daemonRestartCmd,
		// daemonTestCmd,
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

var daemonRestartCmd = &cli.Command{
	Name:  "restart",
	Usage: "restart a running daemon from config",
	Action: func(cctx *cli.Context) error {
		edgeAPI, close, err := lcli.GetEdgeAPI(cctx)
		if err != nil {
			return err
		}

		defer close()

		return edgeAPI.Restart(context.Background())
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
		repoPath := cctx.String(FlagEdgeRepo)
		locatorURL := cctx.String("url")

		ok, err := registerNodeIfNotExist(repoPath, locatorURL)
		if err != nil {
			return err
		}

		if ok {
			log.Info("Register new node")
		}

		d, err := newDaemon(lcli.ReqContext(cctx), repoPath)
		if err != nil {
			return err
		}

		ds := clib.DaemonSwitch{StopChan: make(chan bool)}
		return d.startServer(&ds)
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

func getAccessPoint(locatorURL, nodeID, areaID string) (string, error) {
	locator, close, err := client.NewLocator(context.Background(), locatorURL, nil, jsonrpc.WithHTTPClient(client.NewHTTP3Client()))
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

func newSchedulerAPI(transport *quic.Transport, schedulerURL, nodeID string, privateKey *rsa.PrivateKey) (api.Scheduler, jsonrpc.ClientCloser, error) {
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

func loadPrivateKey(r repo.Repo) (*rsa.PrivateKey, error) {
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

func openRepoOrNew(repoPath string) (repo.Repo, error) {
	r, err := repo.NewFS(repoPath)
	if err != nil {
		return nil, err
	}

	ok, err := r.Exists()
	if err != nil {
		return nil, err
	}
	if !ok {
		if err := r.Init(repo.Edge); err != nil {
			return nil, err
		}
	}

	return r, nil
}

func waitQuietCh(edgeAPI api.Edge) chan struct{} {
	out := make(chan struct{})
	go func() {
		ctx2 := context.Background()
		err := edgeAPI.WaitQuiet(ctx2)
		if err != nil {
			log.Errorf("wait quiet error %s", err.Error())
		}
		close(out)
	}()
	return out
}

var quitWg = &sync.WaitGroup{}

func buildSrvHandler(httpServer *httpserver.HttpServer, edgeApi api.Edge, cfg *config.EdgeCfg, schedulerApi api.Scheduler, privateKey *rsa.PrivateKey) (http.Handler, *http.Server) {
	handler := EdgeHandler(edgeApi.AuthVerify, edgeApi, true)
	handler = httpServer.NewHandler(handler)
	handler = validation.AppendHandler(handler, schedulerApi, privateKey, time.Duration(cfg.ValidateDuration)*time.Second)
	handler = tunnel.NewTunserver(handler)

	httpSrv := &http.Server{
		ReadHeaderTimeout: 30 * time.Second,
		Handler:           handler,
		BaseContext: func(listener net.Listener) context.Context {
			ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "titan-edge"))
			return ctx
		},
	}

	return handler, httpSrv
}

func startHTTP3Server(ctx context.Context, transport *quic.Transport, handler http.Handler, config *config.EdgeCfg) error {
	defer transport.Conn.Close()

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

	go func() {
		<-ctx.Done()
		log.Warn("http3 server graceful shutting down...")
		if err := srv.Close(); err != nil {
			log.Warn("graceful shutting down http3 server with error: ", err.Error())
		}
	}()

	if err = srv.ServeListener(ln); err != nil {
		quitWg.Done()
		log.Warn("http3 server start with error: ", err.Error())
	}
	return err
}

func startHTTPServer(ctx context.Context, srv *http.Server, cfg config.Network) error {
	nl, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return err
	}

	log.Infof("Edge listen on tcp/udp %s", cfg.ListenAddress)

	go func() {
		<-ctx.Done()
		log.Warn("http server graceful shutting down...")
		if err := srv.Close(); err != nil {
			log.Warn("graceful shutting down http server with error: ", err.Error())
		}
	}()

	if err = srv.Serve(nl); err != nil {
		quitWg.Done()
		log.Warn("http server start with error: ", err.Error())
	}

	return err
}

func registShutdownSignal(shutdown chan struct{}) {
	sigChan := make(chan os.Signal, 2)
	go func() {
		<-sigChan
		shutdown <- struct{}{}
	}()
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
}

// check local server if start complete
func waitServerStart(listenAddress string) {
	_, port, err := net.SplitHostPort(listenAddress)
	if err != nil {
		log.Errorf("waitServerStart SplitHostPort %s", err.Error())
		return
	}

	url := fmt.Sprintf("https://127.0.0.1:%d/abc", port)

	httpclient := client.NewHTTP3Client()
	httpclient.Timeout = 3 * time.Second
	httpclient.Get(url)
}

func registerNodeIfNotExist(repoPath string, locatorURL string) (bool, error) {
	r, err := openRepoOrNew(repoPath)
	if err != nil {
		return false, err
	}

	_, nodeIDErr := r.NodeID()
	if nodeIDErr == repo.ErrNodeIDNotExist {
		if len(locatorURL) == 0 {
			return false, fmt.Errorf("Must set --url")
		}

		lr, err := r.Lock(repo.Edge)
		if err != nil {
			return false, err
		}
		defer lr.Close()

		if err := lcli.RegisterEdgeNode(lr, locatorURL); err != nil {
			return false, err
		}

		return true, nil
	}

	return false, nil

}
