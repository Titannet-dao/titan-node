package cli

import (
	"crypto/rsa"
	"fmt"
	"os"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/repo"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/urfave/cli/v2"
)

var EdgeCmds = []*cli.Command{
	nodeInfoCmd,
	cacheStatCmd,
	progressCmd,
	keyCmds,
	configCmds,
}

var nodeInfoCmd = &cli.Command{
	Name:  "info",
	Usage: "Print node info",
	Action: func(cctx *cli.Context) error {
		api, closer, err := getEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		v, err := api.GetNodeInfo(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("node id: %v \n", v.NodeID)
		fmt.Printf("node name: %v \n", v.NodeName)
		fmt.Printf("node external_ip: %v \n", v.ExternalIP)
		fmt.Printf("node internal_ip: %v \n", v.InternalIP)
		fmt.Printf("node systemVersion: %s \n", v.SystemVersion)
		fmt.Printf("node DiskUsage: %f \n", v.DiskUsage)
		fmt.Printf("node disk space: %f \n", v.DiskSpace)
		fmt.Printf("node fsType: %s \n", v.IoSystem)
		fmt.Printf("node mac: %v \n", v.MacLocation)
		fmt.Printf("node download bandwidth: %v \n", v.BandwidthDown)
		fmt.Printf("node upload bandwidth: %v \n", v.BandwidthUp)
		fmt.Printf("node cpu percent: %v \n", v.CPUUsage)

		return nil
	},
}

var cacheStatCmd = &cli.Command{
	Name:  "stat",
	Usage: "cache stat",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		api, closer, err := getEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		stat, err := api.GetAssetStats(ctx)
		if err != nil {
			fmt.Printf("Unlimit speed failed:%v", err)
			return err
		}

		fmt.Printf("Total asset count %d, block count %d, wait cache asset count %d", stat.TotalAssetCount, stat.TotalBlockCount, stat.WaitCacheAssetCount)
		return nil
	},
}

var progressCmd = &cli.Command{
	Name:  "progress",
	Usage: "get cache progress",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "cid",
			Usage: "asset cid",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		edgeAPI, closer, err := getEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		cid := cctx.String("cid")
		ctx := ReqContext(cctx)
		ret, err := edgeAPI.GetAssetProgresses(ctx, []string{cid})
		if err != nil {
			return err
		}

		for _, progress := range ret.Progresses {
			fmt.Printf("Cache asset %s %v\n", progress.CID, progress.Status)
			fmt.Printf("Total block count %d, done block count %d  \n", progress.BlocksCount, progress.DoneBlocksCount)
			fmt.Printf("Total block size %d, done block siez %d  \n", progress.Size, progress.DoneSize)
			fmt.Println()
		}
		return nil
	},
}

var keyCmds = &cli.Command{
	Name:  "key",
	Usage: "generate key, show key, import key, export key",
	Subcommands: []*cli.Command{
		generateRsaKey,
		showKey,
		importKey,
		exportKey,
	},
}

var generateRsaKey = &cli.Command{
	Name:  "generate",
	Usage: "generate rsa key",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "bits",
			Usage: "rsa bit: 1024,2048,4096",
			Value: 1024,
		},
	},
	Action: func(cctx *cli.Context) error {
		bits := cctx.Int("bits")

		if bits < 1024 {
			return fmt.Errorf("rsa bits is 1024,2048,4096")
		}

		lr, err := openRepo(cctx)
		if err != nil {
			return err
		}
		defer lr.Close() //nolint:errcheck  // ignore error

		privateKey, err := titanrsa.GeneratePrivateKey(bits)
		if err != nil {
			return err
		}

		privatePem := titanrsa.PrivateKey2Pem(privateKey)
		if err := lr.SetPrivateKey(privatePem); err != nil {
			return err
		}

		publicKey := privateKey.PublicKey
		publicPem := titanrsa.PublicKey2Pem(&publicKey)

		fmt.Println(string(publicPem))
		return nil
	},
}

var showKey = &cli.Command{
	Name:  "show",
	Usage: "show key",
	Action: func(cctx *cli.Context) error {
		repoPath := getRepoPath(cctx)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		pem, err := r.PrivateKey()
		if err != nil {
			return err
		}

		fmt.Println(string(pem))
		return nil
	},
}

var importKey = &cli.Command{
	Name:  "import",
	Usage: "import key",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Required: true,
			Name:     "path",
			Usage:    "example: --path=./private.key",
			Value:    "",
		},
	},
	Action: func(cctx *cli.Context) error {
		lr, err := openRepo(cctx)
		if err != nil {
			return err
		}
		defer lr.Close() //nolint:errcheck  // ignore error

		path := cctx.String("path")

		privateKey, err := readPrivateKey(path)
		if err != nil {
			return err
		}

		privatePem := titanrsa.PrivateKey2Pem(privateKey)
		if err := lr.SetPrivateKey(privatePem); err != nil {
			return err
		}

		publicKey := privateKey.PublicKey
		publicPem := titanrsa.PublicKey2Pem(&publicKey)

		fmt.Println(string(publicPem))
		return nil
	},
}

var exportKey = &cli.Command{
	Name:  "export",
	Usage: "export key",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "pk-path",
			Usage: "public key path, example: --pk-path=./public.pem",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "sk-path",
			Usage: "private key path, example: --sk-path=./private.key",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		pkPath := cctx.String("pk-path")
		skPath := cctx.String("sk-path")
		if len(pkPath) == 0 && len(skPath) == 0 {
			return fmt.Errorf("pk-path or sk-path can not empty")
		}

		repoPath := getRepoPath(cctx)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		privatePem, err := r.PrivateKey()
		if err != nil {
			return err
		}

		if len(skPath) > 0 {
			err := os.WriteFile(skPath, privatePem, 0o600)
			if err != nil {
				return err
			}
		}

		if len(pkPath) == 0 {
			return nil
		}

		privateKey, err := titanrsa.Pem2PrivateKey(privatePem)
		if err != nil {
			return err
		}

		publicPem := titanrsa.PublicKey2Pem(&privateKey.PublicKey)

		return os.WriteFile(pkPath, publicPem, 0o600)
	},
}

func openRepo(cctx *cli.Context) (repo.LockedRepo, error) {
	repoPath := getRepoPath(cctx)
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

	lr, err := r.Lock(repo.Edge)
	if err != nil {
		return nil, err
	}

	return lr, nil
}

func getRepoPath(cctx *cli.Context) string {
	ti, ok := cctx.App.Metadata["repoType"]
	if !ok {
		return ""
	}

	t, ok := ti.(repo.RepoType)
	if !ok {
		log.Errorf("repoType type does not match the type of repo.RepoType")
	}

	repoFlags := t.RepoFlags()
	if len(repoFlags) == 0 {
		return ""
	}

	return cctx.String(repoFlags[0])
}

func readPrivateKey(path string) (*rsa.PrivateKey, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return titanrsa.Pem2PrivateKey(pem)
}

func getEdgeAPI(ctx *cli.Context) (api.Edge, jsonrpc.ClientCloser, error) {
	ti, ok := ctx.App.Metadata["repoType"]
	if !ok {
		log.Errorf("unknown repo type, are you sure you want to use GetCommonAPI?")
		ti = repo.Scheduler
	}
	t, ok := ti.(repo.RepoType)
	if !ok {
		log.Errorf("repoType type does not match the type of repo.RepoType")
	}

	addr, headers, err := GetRawAPI(ctx, t, "v0")
	if err != nil {
		return nil, nil, err
	}

	return client.NewEdge(ctx.Context, addr, headers)
}

var configCmds = &cli.Command{
	Name:  "config",
	Usage: "set config",
	Subcommands: []*cli.Command{
		setConfigCmd,
	},
}

var setConfigCmd = &cli.Command{
	Name:  "set",
	Usage: "set config",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen-address",
			Usage: "local listen address, example: --listen-address=0.0.0.0:1234",
			Value: "0.0.0.0:1234",
		},
		&cli.StringFlag{
			Name:  "timeout",
			Usage: "network connect timeout. example: --timeout=30s",
			Value: "30s",
		},
		&cli.StringFlag{
			Name:  "node-id",
			Usage: "example: --node-id=your_node_id",
			Value: "your_node_id",
		},
		&cli.StringFlag{
			Name:  "area-id",
			Usage: "example: --area-id=your_area_id",
			Value: "your_area_id",
		},
		&cli.StringFlag{
			Name:  "metadata-path",
			Usage: "metadata path, example: --metadata-path=/path/to/metadata",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "assets-paths",
			Usage: "assets paths, example: --assets-paths=/path/to/assets1,/path/to/assets2",
			Value: "",
		},
		&cli.IntFlag{
			Name:  "bandwidth-up",
			Usage: "example: --bandwidth-up=104857600",
			Value: 104857600,
		},
		&cli.IntFlag{
			Name:  "bandwidth-down",
			Usage: "example: --bandwidth-down=1073741824",
			Value: 1073741824,
		},
		&cli.BoolFlag{
			Name:  "locator",
			Usage: "example: --locator=true",
			Value: true,
		},
		&cli.StringFlag{
			Name:  "certificate-path",
			Usage: "example: --certificate-path=/path/to/certificate",
			Value: "/path/to/certificate",
		},
		&cli.StringFlag{
			Name:  "private-key-path",
			Usage: "example: --private-key-path=/path/to/private.key",
			Value: "/path/to/private.key",
		},
		&cli.StringFlag{
			Name:  "ca-certificate-path",
			Usage: "example: --ca-certificate-path=/path/to/ca-certificate",
			Value: "/path/to/ca-certificate",
		},
		&cli.IntFlag{
			Name:  "fetch-block-timeout",
			Usage: "fetch block timeout, unit is seconds, example: --fetch-block-timeout=3",
			Value: 3,
		},
		&cli.IntFlag{
			Name:  "fetch-block-retry",
			Usage: "retry number when fetch block failed, example: --fetch-block-retry=2",
			Value: 2,
		},
		&cli.IntFlag{
			Name:  "fetch-batch",
			Usage: "example: --fetch-batch=5",
			Value: 5,
		},
	},

	Action: func(cctx *cli.Context) error {
		lr, err := openRepo(cctx)
		if err != nil {
			return err
		}
		defer lr.Close() //nolint:errcheck  // ignore error

		lr.SetConfig(func(raw interface{}) {
			cfg, ok := raw.(*config.EdgeCfg)
			if !ok {
				return
			}

			if cctx.IsSet("listen-address") {
				cfg.ListenAddress = cctx.String("listen-address")
			}
			if cctx.IsSet("timeout") {
				cfg.Timeout = cctx.String("timeout")
			}
			if cctx.IsSet("node-id") {
				cfg.NodeID = cctx.String("node-id")
			}
			if cctx.IsSet("area-id") {
				cfg.AreaID = cctx.String("area-id")
			}
			if cctx.IsSet("metadata-path") {
				cfg.MetadataPath = cctx.String("metadata-path")
			}
			if cctx.IsSet("assets-paths") {
				cfg.AssetsPaths = cctx.StringSlice("assets-paths")
			}
			if cctx.IsSet("bandwidth-up") {
				cfg.BandwidthUp = cctx.Int64("bandwidth-up")
			}
			if cctx.IsSet("bandwidth-down") {
				cfg.BandwidthDown = cctx.Int64("bandwidth-down")
			}
			if cctx.IsSet("locator") {
				cfg.Locator = cctx.Bool("locator")
			}
			if cctx.IsSet("certificate-path") {
				cfg.CertificatePath = cctx.String("certificate-path")
			}
			if cctx.IsSet("private-key-path") {
				cfg.PrivateKeyPath = cctx.String("private-key-path")
			}
			if cctx.IsSet("ca-certificate-path") {
				cfg.CaCertificatePath = cctx.String("ca-certificate-path")
			}
			if cctx.IsSet("fetch-block-timeout") {
				cfg.FetchBlockTimeout = cctx.Int("fetch-block-timeout")
			}
			if cctx.IsSet("fetch-block-retry") {
				cfg.FetchBlockRetry = cctx.Int("fetch-block-retry")
			}
			if cctx.IsSet("fetch-batch") {
				cfg.FetchBatch = cctx.Int("fetch-batch")
			}
		})

		return nil
	},
}
