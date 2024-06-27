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

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/build"
	lcli "github.com/Filecoin-Titan/titan/cli"
	"github.com/Filecoin-Titan/titan/lib/titanlog"
	"github.com/Filecoin-Titan/titan/node"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/locator"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/repo"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("main")

const FlagLocatorRepo = "locator-repo"

// TODO remove after deprecation period
const FlagLocatorRepoDeprecation = "locatorrepo"

func main() {
	types.RunningNodeType = types.NodeLocator
	titanlog.SetupLogLevels()

	local := []*cli.Command{
		runCmd,
	}

	local = append(local, lcli.CommonCommands...)

	app := &cli.App{
		Name:                 "titan-locator",
		Usage:                "Titan locator node",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagLocatorRepo,
				Aliases: []string{FlagLocatorRepoDeprecation},
				EnvVars: []string{"TITAN_LOCATION_PATH", "LOCATION_PATH"},
				Value:   "~/.titanlocator", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify locator repo path. flag %s and env TITAN_EDGE_PATH are DEPRECATION, will REMOVE SOON", FlagLocatorRepoDeprecation),
			},
			&cli.StringFlag{
				Name:    "panic-reports",
				EnvVars: []string{"TITAN_PANIC_REPORT_PATH"},
				Hidden:  true,
				Value:   "~/.titanlocator", // should follow --repo default
			},
		},

		After: func(c *cli.Context) error {
			if r := recover(); r != nil {
				// Generate report in TITAN_LOCATOR_PATH and re-raise panic
				build.GeneratePanicReport(c.String("panic-reports"), c.String(FlagLocatorRepo), c.App.Name)
				panic(r)
			}
			return nil
		},
		Commands: local,
	}
	app.Setup()
	app.Metadata["repoType"] = repo.Locator

	if err := app.Run(os.Args); err != nil {
		log.Errorf("%+v", err)
		return
	}
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start titan locator node",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "geodb-path",
			Usage: "geodb path, example: --geodb-path=../../geoip/geolite2_city/city.mmdb",
			Value: "../../geoip/geolite2_city/city.mmdb",
		},
		&cli.StringSliceFlag{
			Name:  "etcd-addresses",
			Usage: "etcd, example: --etcd-addresses='etcd-server-ip:port'",
			Value: &cli.StringSlice{},
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting titan locator node")

		repoPath := cctx.String(FlagLocatorRepo)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			if err := r.Init(repo.Locator); err != nil {
				return err
			}
		}

		lr, err := r.Lock(repo.Locator)
		if err != nil {
			return err
		}

		cfg, err := lr.Config()
		if err != nil {
			return err
		}

		locatorCfg := cfg.(*config.LocatorCfg)

		err = lr.Close()
		if err != nil {
			return err
		}

		shutdownChan := make(chan struct{})

		var locatorAPI api.Locator
		stop, err := node.New(cctx.Context,
			node.Locator(&locatorAPI),
			node.Base(),
			node.Repo(r),
			node.Override(new(dtypes.ShutdownChan), shutdownChan),
			node.Override(new(*quic.Transport), &quic.Transport{}),
			node.ApplyIf(func(s *node.Settings) bool { return cctx.IsSet("etcd-addresses") },
				node.Override(new(dtypes.EtcdAddresses), func() dtypes.EtcdAddresses {
					return dtypes.EtcdAddresses(cctx.StringSlice("etcd-addresses"))
				})),
			node.ApplyIf(func(s *node.Settings) bool { return cctx.IsSet("geodb-path") },
				node.Override(new(dtypes.GeoDBPath), func() dtypes.GeoDBPath {
					return dtypes.GeoDBPath(cctx.String("geodb-path"))
				})),
			node.Override(new(*locator.DNSServer), func(config *config.LocatorCfg, reg region.Region) *locator.DNSServer {
				return locator.NewDNSServer(config, locatorAPI, reg)
			}),
		)
		if err != nil {
			return xerrors.Errorf("creating node: %w", err)
		}

		// Instantiate the locator node handler.
		handler, err := node.LocatorHandler(locatorAPI, true)
		if err != nil {
			return xerrors.Errorf("failed to instantiate rpc handler: %w", err)
		}

		udpPacketConn, err := net.ListenPacket("udp", locatorCfg.ListenAddress)
		if err != nil {
			return err
		}
		defer udpPacketConn.Close() //nolint:errcheck  // ignore error

		go startHTTP3Server(udpPacketConn, handler, locatorCfg) //nolint:errcheck

		// Serve the RPC.
		rpcStopper, err := node.ServeRPC(handler, "titan-locator", locatorCfg.ListenAddress)
		if err != nil {
			return fmt.Errorf("failed to start json-rpc endpoint: %s", err)
		}

		log.Infof("Titan locator server listen on %s", locatorCfg.ListenAddress)

		// Monitor for shutdown.
		finishCh := node.MonitorShutdown(shutdownChan,
			node.ShutdownHandler{Component: "rpc server", StopFunc: rpcStopper},
			node.ShutdownHandler{Component: "locator", StopFunc: stop},
		)

		<-finishCh
		return nil
	},
}

func startHTTP3Server(conn net.PacketConn, handler http.Handler, locatorCfg *config.LocatorCfg) error {
	var tlsConfig *tls.Config
	if len(locatorCfg.CertificatePath) == 0 && len(locatorCfg.PrivateKeyPath) == 0 {
		config, err := defaultTLSConfig()
		if err != nil {
			log.Errorf("startUDPServer, defaultTLSConfig error:%s", err.Error())
			return err
		}
		tlsConfig = config
	} else {
		cert, err := tls.LoadX509KeyPair(locatorCfg.CertificatePath, locatorCfg.PrivateKeyPath)
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
