package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"

	"github.com/Filecoin-Titan/titan/api/types"
	cliutil "github.com/Filecoin-Titan/titan/cli/util"
	"github.com/Filecoin-Titan/titan/node"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/build"
	lcli "github.com/Filecoin-Titan/titan/cli"
	"github.com/Filecoin-Titan/titan/lib/titanlog"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/repo"
	"github.com/Filecoin-Titan/titan/node/secret"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/xerrors"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("main")

const (
	// FlagSchedulerRepo Flag
	FlagSchedulerRepo = "scheduler-repo"

	// FlagSchedulerRepoDeprecation Flag
	FlagSchedulerRepoDeprecation = "schedulerrepo"
)

func main() {
	types.RunningNodeType = types.NodeScheduler

	titanlog.SetupLogLevels()

	local := []*cli.Command{
		initCmd,
		runCmd,
		getAPIKeyCmd,
	}

	local = append(local, lcli.SchedulerCMDs...)
	local = append(local, lcli.CommonCommands...)

	app := &cli.App{
		Name:                 "titan-scheduler",
		Usage:                "Titan scheduler node",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagSchedulerRepo,
				Aliases: []string{FlagSchedulerRepoDeprecation},
				EnvVars: []string{"TITAN_SCHEDULER_PATH", "SCHEDULER_PATH"},
				Value:   "~/.titanscheduler", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify scheduler repo path. flag %s and env TITAN_SCHEDULER_PATH are DEPRECATION, will REMOVE SOON", FlagSchedulerRepoDeprecation),
			},
			&cli.StringFlag{
				Name:    "panic-reports",
				EnvVars: []string{"TITAN_PANIC_REPORT_PATH"},
				Hidden:  true,
				Value:   "~/.titanscheduler", // should follow --repo default
			},
		},

		After: func(c *cli.Context) error {
			if r := recover(); r != nil {
				// Generate report in TITAN_SCHEDULER_PATH and re-raise panic
				build.GeneratePanicReport(c.String("panic-reports"), c.String(FlagSchedulerRepo), c.App.Name)
				log.Panic(r)
			}
			return nil
		},
		Commands: local,
	}
	app.Setup()
	app.Metadata["repoType"] = repo.Scheduler

	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}

type jwtPayload struct {
	Allow []auth.Permission
}

var initCmd = &cli.Command{
	Name:  "init",
	Usage: "Initialize a titan scheduler repo",
	Action: func(cctx *cli.Context) error {
		log.Info("Initializing titan scheduler")
		repoPath := cctx.String(FlagSchedulerRepo)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if ok {
			return xerrors.Errorf("repo at '%s' is already initialized", cctx.String(FlagSchedulerRepo))
		}

		log.Info("Initializing repo")

		if err := r.Init(repo.Scheduler); err != nil {
			return err
		}

		return nil
	},
}

var getAPIKeyCmd = &cli.Command{
	Name:  "get-api-key",
	Usage: "Generate API Key",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "perm",
			Usage: "permission to assign to the token, one of: read, write, sign, admin",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		lr, err := openRepo(cctx)
		if err != nil {
			return err
		}
		defer lr.Close() //nolint

		perm := cctx.String("perm")

		p := jwtPayload{}

		idx := 0
		for i, p := range api.AllPermissions {
			if auth.Permission(perm) == p {
				idx = i + 1
			}
		}

		if idx == 0 {
			return fmt.Errorf("--perm flag has to be one of: %s", api.AllPermissions)
		}

		p.Allow = api.AllPermissions[:idx]

		authKey, err := secret.APISecret(lr)
		if err != nil {
			return xerrors.Errorf("setting up api secret: %w", err)
		}

		k, err := jwt.Sign(&p, authKey)
		if err != nil {
			return xerrors.Errorf("jwt sign: %w", err)
		}

		fmt.Println(string(k))
		return nil
	},
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start titan scheduler node",
	Flags: []cli.Flag{},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting titan scheduler node")

		repoPath := cctx.String(FlagSchedulerRepo)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			if err := r.Init(repo.Scheduler); err != nil {
				return err
			}
		}

		lr, err := r.Lock(repo.Scheduler)
		if err != nil {
			return err
		}

		cfg, err := lr.Config()
		if err != nil {
			return err
		}

		schedulerCfg := cfg.(*config.SchedulerCfg)

		err = lr.Close()
		if err != nil {
			return err
		}

		shutdownChan := make(chan struct{})

		var schedulerAPI api.Scheduler
		stop, err := node.New(cctx.Context,
			node.Scheduler(&schedulerAPI),
			node.Base(),
			node.Repo(r),
		)
		if err != nil {
			return xerrors.Errorf("creating node: %w", err)
		}

		// Populate JSON-RPC options.
		serverOptions := []jsonrpc.ServerOption{jsonrpc.WithServerErrors(api.RPCErrors)}
		if maxRequestSize := cctx.Int("api-max-req-size"); maxRequestSize != 0 {
			serverOptions = append(serverOptions, jsonrpc.WithMaxRequestSize(int64(maxRequestSize)))
		}

		// Instantiate the scheduler handler.
		h, err := node.SchedulerHandler(schedulerAPI, true, serverOptions...)
		if err != nil {
			return fmt.Errorf("failed to instantiate rpc handler: %s", err.Error())
		}

		udpPacketConn, err := net.ListenPacket("udp", schedulerCfg.ListenAddress)
		if err != nil {
			return err
		}
		defer func() {
			err = udpPacketConn.Close()
			if err != nil {
				log.Errorf("udpPacketConn Close err:%s", err.Error())
			}
		}()

		httpClient, err := cliutil.NewHTTP3Client(udpPacketConn, schedulerCfg.InsecureSkipVerify, schedulerCfg.CaCertificatePath)
		if err != nil {
			return xerrors.Errorf("new http3 client error %w", err)
		}
		jsonrpc.SetHttp3Client(httpClient)

		go startUDPServer(udpPacketConn, h, schedulerCfg) //nolint:errcheck

		// Serve the RPC.
		rpcStopper, err := node.ServeRPC(h, "scheduler", schedulerCfg.ListenAddress)
		if err != nil {
			return fmt.Errorf("failed to start json-rpc endpoint: %s", err.Error())
		}

		log.Info("titan scheduler listen with:", schedulerCfg.ListenAddress)

		// Monitor for shutdown.
		finishCh := node.MonitorShutdown(shutdownChan,
			node.ShutdownHandler{Component: "rpc server", StopFunc: rpcStopper},
			node.ShutdownHandler{Component: "node", StopFunc: stop},
		)
		<-finishCh // fires when shutdown is complete.
		return nil
	},
}

func openRepo(cctx *cli.Context) (repo.LockedRepo, error) {
	repoPath := cctx.String(FlagSchedulerRepo)
	r, err := repo.NewFS(repoPath)
	if err != nil {
		return nil, err
	}

	ok, err := r.Exists()
	if err != nil {
		return nil, err
	}
	if !ok {
		if err := r.Init(repo.Scheduler); err != nil {
			return nil, err
		}
	}

	lr, err := r.Lock(repo.Scheduler)
	if err != nil {
		return nil, err
	}

	return lr, nil
}

func startUDPServer(conn net.PacketConn, handler http.Handler, schedulerCfg *config.SchedulerCfg) error {
	var tlsConfig *tls.Config
	if schedulerCfg.InsecureSkipVerify {
		config, err := defaultTLSConfig()
		if err != nil {
			log.Errorf("startUDPServer, defaultTLSConfig error:%s", err.Error())
			return err
		}
		tlsConfig = config
	} else {
		cert, err := tls.LoadX509KeyPair(schedulerCfg.CaCertificatePath, schedulerCfg.PrivateKeyPath)
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
