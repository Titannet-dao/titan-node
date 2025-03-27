package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/repo"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/docker/go-units"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"github.com/mitchellh/go-homedir"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var EdgeCmds = []*cli.Command{
	nodeInfoCmd,
	showCmds,
	cacheStatCmd,
	progressCmd,
	keyCmds,
	configCmds,
	stateCmd,
	signCmd,
	bindCmd,
	syncDataCmd,
	deleteLocalAssetCmd,
	upgradeCmd,
	udpTestCmd,
	freeUpDiskCmd,
}

var showCmds = &cli.Command{
	Name:  "show",
	Usage: "Displays node information and binding information",
	Subcommands: []*cli.Command{
		bindingInfoCmd,
	},
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

		info, err := api.GetNodeInfo(ctx)
		if err != nil {
			return err
		}

		fmt.Printf("node id: %s \n", info.NodeID)
		fmt.Printf("name: %s \n", info.NodeName)
		fmt.Printf("internal_ip: %s \n", info.InternalIP)
		fmt.Printf("system version: %s \n", info.SystemVersion)
		fmt.Printf("disk usage: %.4f %s\n", info.DiskUsage, "%")
		fmt.Printf("disk space: %s \n", units.BytesSize(info.DiskSpace))
		fmt.Printf("titan disk usage: %s\n", units.BytesSize(info.TitanDiskUsage))
		fmt.Printf("titan disk space: %s\n", units.BytesSize(info.AvailableDiskSpace))
		fmt.Printf("fsType: %s \n", info.DiskType)
		fmt.Printf("mac: %s \n", info.MacLocation)
		fmt.Printf("download bandwidth: %s \n", units.BytesSize(float64(info.BandwidthDown)))
		fmt.Printf("upload bandwidth: %s \n", units.BytesSize(float64(info.BandwidthUp)))
		// fmt.Printf("netflow total: %s \n", units.BytesSize(float64(info.NetflowTotal)))
		fmt.Printf("netflow upload: %s \n", units.BytesSize(float64(info.NetFlowUp)))
		fmt.Printf("netflow download: %s \n", units.BytesSize(float64(info.NetFlowDown)))
		// fmt.Printf("upload bandwidth: %s \n", units.BytesSize(float64(info.BandwidthUp)))
		fmt.Printf("cpu percent: %.2f %s \n", info.CPUUsage, "%")
		return nil
	},
}

var bindingInfoCmd = &cli.Command{
	Name:      "binding-info",
	Usage:     "Print binding info",
	UsageText: "binding-info https://webserver-api-url",
	Action: func(cctx *cli.Context) error {
		webServer := cctx.Args().First()
		if len(webServer) == 0 {
			return fmt.Errorf("example: binding-info https://webserver-api-url")
		}

		r, err := openRepo(cctx)
		if err != nil {
			return err
		}

		nodeID, err := r.NodeID()
		if err != nil {
			return err
		}

		type ReqBindingInfo struct {
			NodeID string   `json:"node_id"`
			Keys   []string `json:"keys"`
			Since  int      `json:"since"`
		}

		req := ReqBindingInfo{
			NodeID: string(nodeID),
			Keys:   []string{"account"},
			Since:  0,
		}

		buf, err := json.Marshal(req)
		if err != nil {
			return err
		}

		resp, err := http.Post(webServer, "application/json", bytes.NewBuffer(buf))
		if err != nil {
			return err
		}

		if resp.StatusCode != http.StatusOK {
			buf, err := io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("status code %d, read body error %s", resp.StatusCode, err.Error())
			}
			return fmt.Errorf("status code %d, error message %s", resp.StatusCode, string(buf))
		}

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read body error %s", err.Error())
		}

		type Account struct {
			UserID        string `json:"user_id"`
			WalletAddress string `json:"wallet_address"`
			Code          string `json:"code"`
		}

		type Data struct {
			Account Account `json:"account"`
			Since   int64   `json:"since"`
		}

		type Resp struct {
			Code int    `json:"code"`
			Data Data   `json:"data"`
			Err  int    `json:"err"`
			Msg  string `json:"msg"`
		}

		rsp := Resp{}
		if err = json.Unmarshal(respBody, &rsp); err != nil {
			return err
		}

		if rsp.Code != 0 {
			fmt.Println(string(respBody))
			return nil
		}

		tm := time.Unix(rsp.Data.Since, 0)

		fmt.Println("Account:", rsp.Data.Account.UserID)
		fmt.Println("Code:", rsp.Data.Account.Code)
		fmt.Println("Wallet address:", rsp.Data.Account.WalletAddress)
		fmt.Println("Bound Since:", tm.Format("2006-01-02 15:04:05"))

		return nil
	},
}

var cacheStatCmd = &cli.Command{
	Name:  "cache",
	Usage: "Cache stat",
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

		fmt.Printf("Total asset count %d, wait cache asset count %d\n", stat.TotalAssetCount, stat.WaitCacheAssetCount)
		return nil
	},
}

var progressCmd = &cli.Command{
	Name:  "progress",
	Usage: "Get cache progress",
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
	Usage: "Generate key, show key, import key, export key",
	Subcommands: []*cli.Command{
		generateRsaKey,
		showKey,
		importKey,
		exportKey,
	},
}

var generateRsaKey = &cli.Command{
	Name:  "generate",
	Usage: "Generate rsa key",
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

		_, lr, err := openRepoAndLock(cctx)
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
		return nil
	},
}

var showKey = &cli.Command{
	Name:  "show",
	Usage: "Show key",
	Action: func(cctx *cli.Context) error {
		repoPath, err := getRepoPath(cctx)
		if err != nil {
			return err
		}

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
	Usage: "Import key",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Required: true,
			Name:     "path",
			Usage:    "example: --path=./private.key",
			Value:    "",
		},
	},
	Action: func(cctx *cli.Context) error {
		_, lr, err := openRepoAndLock(cctx)
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
		return nil
	},
}

var exportKey = &cli.Command{
	Name:  "export",
	Usage: "Export key",
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

		repoPath, err := getRepoPath(cctx)
		if err != nil {
			return err
		}

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

func openRepo(cctx *cli.Context) (repo.Repo, error) {
	repoPath, err := getRepoPath(cctx)
	if err != nil {
		return nil, err
	}

	r, err := repo.NewFS(repoPath)
	if err != nil {
		return nil, err
	}

	repoType, err := getRepoType(cctx)
	if err != nil {
		return nil, err
	}

	ok, err := r.Exists()
	if err != nil {
		return nil, err
	}
	if !ok {
		if err := r.Init(repoType); err != nil {
			return nil, err
		}
	}

	return r, nil
}

func openRepoAndLock(cctx *cli.Context) (repo.Repo, repo.LockedRepo, error) {
	r, err := openRepo(cctx)
	if err != nil {
		return nil, nil, err
	}

	repoType, err := getRepoType(cctx)
	if err != nil {
		return nil, nil, err
	}

	lr, err := r.Lock(repoType)
	if err != nil {
		return nil, nil, err
	}

	return r, lr, nil
}

func getRepoPath(cctx *cli.Context) (string, error) {
	t, err := getRepoType(cctx)
	if err != nil {
		return "", err
	}

	repoFlags := t.RepoFlags()
	if len(repoFlags) == 0 {
		return "", fmt.Errorf("not setting repo flags")
	}

	return cctx.String(repoFlags[0]), nil
}

func getRepoType(cctx *cli.Context) (repo.RepoType, error) {
	ti, ok := cctx.App.Metadata["repoType"]
	if !ok {
		return nil, fmt.Errorf("not setting repo type in app metadata")
	}

	t, ok := ti.(repo.RepoType)
	if !ok {
		return nil, fmt.Errorf("type does not match the type of repo.RepoType")
	}

	return t, nil
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

	addr = strings.Replace(addr, "ws", "https", 1)
	addr = strings.Replace(addr, "0.0.0.0", "localhost", 1)

	return client.NewEdge(ctx.Context, addr, headers, jsonrpc.WithHTTPClient(client.NewHTTP3Client()))
}

var configCmds = &cli.Command{
	Name:  "config",
	Usage: "Config commands",
	Subcommands: []*cli.Command{
		setConfigCmd,
		showConfigCmd,
		mergeConfigCmd,
	},
}

var showConfigCmd = &cli.Command{
	Name:  "show",
	Usage: "show config",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		configPath, err := getConfigPath(cctx)
		if err != nil {
			return err
		}
		cfg, err := config.FromFile(configPath, config.DefaultEdgeCfg())
		if err != nil {
			return xerrors.Errorf("load local config file error %w", err)
		}

		// buf, err := json.Marshal(cfg)
		// if err != nil {
		// 	return err
		// }
		buf, err := json.MarshalIndent(cfg, "", "    ")
		if err != nil {
			return err
		}
		fmt.Println(string(buf))
		return nil
	},
}

var setConfigCmd = &cli.Command{
	Name:  "set",
	Usage: "set config, after modifying the configuration, the node must be restarted",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen-address",
			Usage: "set local listen address, example: --listen-address=local_server_ip:port",
			Value: "0.0.0.0:1234",
		},
		&cli.StringFlag{
			Name:  "storage-path",
			Usage: "set storage path, example: --storage-path=/data/your-storage-path-here",
		},
		&cli.StringFlag{
			Name:  "storage-size",
			Usage: "set storage size, the unit is GB, example: --storage-size=32GB",
		},
	},

	Action: func(cctx *cli.Context) error {
		configPath, err := getConfigPath(cctx)
		if err != nil {
			return err
		}
		cfg, err := config.FromFile(configPath, config.DefaultEdgeCfg())
		if err != nil {
			return xerrors.Errorf("load local config file error %w", err)
		}

		edgeConfig := cfg.(*config.EdgeCfg)
		if cctx.IsSet("listen-address") {
			edgeConfig.Network.ListenAddress = cctx.String("listen-address")
		}

		if cctx.IsSet("storage-path") {
			storagePath := cctx.String("storage-path")
			if _, err := os.Stat(storagePath); os.IsNotExist(err) {
				return fmt.Errorf("storage path %s not exist", storagePath)
			}

			oldStoragePath := edgeConfig.Storage.Path
			if len(oldStoragePath) == 0 {
				repoPath, err := getRepoPath(cctx)
				if err != nil {
					return err
				}

				repoPath, err = homedir.Expand(repoPath)
				if err != nil {
					return err
				}
				oldStoragePath = path.Join(repoPath, "storage")
			}

			if oldStoragePath != storagePath {
				if err = copyDir(oldStoragePath, storagePath); err != nil {
					return err
				}
			}

			edgeConfig.Storage.Path = storagePath
		}

		if cctx.IsSet("storage-size") {
			storageSize := cctx.String("storage-size")
			if !strings.Contains(storageSize, "GB") {
				return fmt.Errorf("storage size %s is an invalid value", storageSize)
			}
			sizeString := strings.TrimSuffix(storageSize, "GB")
			size, err := strconv.Atoi(sizeString)
			if err != nil {
				return fmt.Errorf("storage size %s is an invalid value", sizeString)
			}

			if size <= 0 {
				return fmt.Errorf("storage size %d is an invalid value", size)
			}
			edgeConfig.Storage.StorageGB = int64(size)
		}

		configBytes, err := config.GenerateConfigUpdate(edgeConfig, config.DefaultEdgeCfg(), true)
		if err != nil {
			return xerrors.Errorf("update config error %w", err)
		}

		return os.WriteFile(configPath, configBytes, 0o644)
	},
}

func getConfigPath(cctx *cli.Context) (string, error) {
	repoPath, err := getRepoPath(cctx)
	if err != nil {
		return "", err
	}

	repoPath, err = homedir.Expand(repoPath)
	if err != nil {
		return "", err
	}

	return filepath.Join(repoPath, "config.toml"), nil
}

var mergeConfigCmd = &cli.Command{
	Name:  "merge",
	Usage: "merge config",
	Flags: []cli.Flag{},

	Action: func(cctx *cli.Context) error {
		configString := cctx.Args().Get(0)
		reader := bytes.NewReader([]byte(configString))

		newEdgeConfig := config.EdgeCfg{}
		if _, err := toml.NewDecoder(reader).Decode(&newEdgeConfig); err != nil {
			return xerrors.Errorf("decode config error %w", err)
		}

		// check storage path
		if len(newEdgeConfig.Storage.Path) > 0 {
			if err := checkPath(newEdgeConfig.Storage.Path); err != nil {
				return err
			}
		}
		// check quota

		configPath, err := getConfigPath(cctx)
		if err != nil {
			return err
		}
		cfg, err := config.FromFile(configPath, config.DefaultEdgeCfg())
		if err != nil {
			return xerrors.Errorf("load local config file error %w", err)
		}

		edgeConfig := cfg.(*config.EdgeCfg)

		edgeConfig.Storage = newEdgeConfig.Storage
		edgeConfig.Memory = newEdgeConfig.Memory
		edgeConfig.CPU = newEdgeConfig.CPU
		edgeConfig.Bandwidth.BandwidthMB = newEdgeConfig.Bandwidth.BandwidthMB
		edgeConfig.Bandwidth.BandwidthUp = newEdgeConfig.Bandwidth.BandwidthUp
		edgeConfig.Bandwidth.BandwidthDown = newEdgeConfig.Bandwidth.BandwidthDown

		configBytes, err := config.GenerateConfigUpdate(edgeConfig, config.DefaultEdgeCfg(), true)
		if err != nil {
			return xerrors.Errorf("update config error %w", err)
		}

		return os.WriteFile(configPath, configBytes, 0o644)
	},
}

func checkPath(path string) error {
	if stat, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("check path %s error %s", path, err.Error())
		}

		parentDir := filepath.Dir(path)
		if stat, err := os.Stat(parentDir); err != nil {
			return fmt.Errorf("check path %s error %s", parentDir, err.Error())
		} else if !stat.IsDir() {
			return fmt.Errorf("%s is not dir", parentDir)
		}
	} else if !stat.IsDir() {
		return fmt.Errorf("%s is not dir", path)
	}

	return nil
}

func saveConfigAfterRegister(lr repo.LockedRepo, info *types.ActivationDetail, locatorURL string) error {
	tokenBytes, err := json.Marshal(info)
	if err != nil {
		return err
	}

	return lr.SetConfig(func(raw interface{}) {
		cfg, ok := raw.(*config.EdgeCfg)
		if !ok {
			candidateCfg, ok := raw.(*config.CandidateCfg)
			if !ok {
				log.Errorf("can not convert interface to CandidateCfg")
				return
			}
			cfg = &candidateCfg.EdgeCfg
		}
		cfg.Network.LocatorURL = locatorURL
		cfg.Basic.Token = base64.StdEncoding.EncodeToString(tokenBytes)
		cfg.AreaID = info.AreaID
	})
}

func RegisterEdgeNode(lr repo.LockedRepo, locatorURL string) error {
	schedulerURL, err := getUserAccessPoint(locatorURL)
	if err != nil {
		return err
	}

	schedulerAPI, closer, err := client.NewScheduler(context.Background(), schedulerURL, nil, jsonrpc.WithHTTPClient(client.NewHTTP3Client()))
	if err != nil {
		return err
	}
	defer closer()

	bits := 1024

	privateKey, err := titanrsa.GeneratePrivateKey(bits)
	if err != nil {
		return err
	}

	pem := titanrsa.PublicKey2Pem(&privateKey.PublicKey)
	nodeID := fmt.Sprintf("e_%s", uuid.NewString())

	info, err := schedulerAPI.RegisterNode(context.Background(), nodeID, string(pem), types.NodeEdge)
	if err != nil {
		return err
	}

	privatePem := titanrsa.PrivateKey2Pem(privateKey)
	if err := lr.SetPrivateKey(privatePem); err != nil {
		return err
	}

	if err := lr.SetNodeID([]byte(nodeID)); err != nil {
		return err
	}

	return saveConfigAfterRegister(lr, info, locatorURL)
}

var stateCmd = &cli.Command{
	Name:  "state",
	Usage: "Check daemon state",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		api, close, err := getEdgeAPI(cctx)
		if err != nil {
			return err
		}

		defer close()

		type NodeState struct {
			Runing bool `json:"running"`
			Online bool `json:"online"`
		}

		nodeState := NodeState{}
		online, err := api.GetEdgeOnlineStateFromScheduler(cctx.Context)
		if err == nil {
			nodeState.Runing = true
			nodeState.Online = online
		} else {
			fmt.Println("err ", err.Error())
		}

		buf, err := json.Marshal(nodeState)
		fmt.Println(string(buf))
		return nil
	},
}

var signCmd = &cli.Command{
	Name:      "sign",
	Usage:     "Sign with the hash",
	UsageText: "sign your-hash-here",
	Flags:     []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		hash := cctx.Args().Get(0)
		if len(hash) == 0 {
			return fmt.Errorf("hash can not empty")
		}

		r, err := openRepo(cctx)
		if err != nil {
			return err
		}

		privateKeyPem, err := r.PrivateKey()
		if err != nil {
			return err
		}

		privateKey, err := titanrsa.Pem2PrivateKey(privateKeyPem)
		if err != nil {
			return err
		}

		titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
		sign, err := titanRsa.Sign(privateKey, []byte(hash))
		if err != nil {
			return err
		}

		fmt.Println(hex.EncodeToString(sign))
		return nil
	},
}

var bindCmd = &cli.Command{
	Name:      "bind",
	Usage:     "Bind with the hash",
	UsageText: "bind your-hash-here https://your-web-server-api",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "hash",
			Usage: "--hash=your-hash",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		webServer := cctx.Args().Get(0)
		if len(webServer) == 0 {
			return fmt.Errorf("web server can not empty, example: bind --hash your-hash https://your-web-server-api")
		}

		hash := cctx.String("hash")
		if len(hash) == 0 {
			return fmt.Errorf("hash can not empty")
		}

		r, err := openRepo(cctx)
		if err != nil {
			return err
		}

		privateKeyPem, err := r.PrivateKey()
		if err != nil {
			return err
		}

		privateKey, err := titanrsa.Pem2PrivateKey(privateKeyPem)
		if err != nil {
			return err
		}

		titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
		sign, err := titanRsa.Sign(privateKey, []byte(hash))
		if err != nil {
			return err
		}

		nodeID, err := r.NodeID()
		if err != nil {
			return err
		}

		if len(nodeID) == 0 {
			return fmt.Errorf("edge not init, can not get nodeID")
		}

		configPath, err := getConfigPath(cctx)
		if err != nil {
			return err
		}

		cfg, err := config.FromFile(configPath, config.DefaultEdgeCfg())
		if err != nil {
			return xerrors.Errorf("load local config file error %w", err)
		}

		edgeConfig := cfg.(*config.EdgeCfg)

		type ReqBind struct {
			Hash      string `json:"hash"`
			NodeID    string `json:"node_id"`
			Signature string `json:"signature"`
			AreaID    string `json:"area_id"`
		}

		reqBind := ReqBind{
			Hash:      hash,
			NodeID:    string(nodeID),
			Signature: hex.EncodeToString(sign),
			AreaID:    edgeConfig.AreaID,
		}

		buf, err := json.Marshal(reqBind)
		if err != nil {
			return err
		}

		resp, err := http.Post(webServer, "application/json", bytes.NewBuffer(buf))
		if err != nil {
			return err
		}

		if resp.StatusCode != http.StatusOK {
			buf, err := io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("status code %d, read body error %s", resp.StatusCode, err.Error())
			}
			return fmt.Errorf("status code %d, error message %s", resp.StatusCode, string(buf))
		}

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read body error %s", err.Error())
		}

		type Resp struct {
			Code int    `json:"code"`
			Err  int    `json:"err"`
			Msg  string `json:"msg"`
		}

		rsp := Resp{}
		json.Unmarshal(respBody, &rsp)
		if rsp.Code != 0 {
			fmt.Println(rsp.Msg)
		}

		return nil
	},
}

// getNodeAccessPoint get scheduler url by user ip
func getUserAccessPoint(locatorURL string) (string, error) {
	locator, close, err := client.NewLocator(context.Background(), locatorURL, nil, jsonrpc.WithHTTPClient(client.NewHTTP3Client()))
	if err != nil {
		return "", err
	}
	defer close()

	accessPoint, err := locator.GetUserAccessPoint(context.Background(), "")
	if err != nil {
		return "", err
	}

	if accessPoint != nil && len(accessPoint.SchedulerURLs) > 0 {
		return accessPoint.SchedulerURLs[0], nil
	}

	return "", fmt.Errorf("not scheduler exit")
}

var syncDataCmd = &cli.Command{
	Name:  "sync-data",
	Usage: "sync local asset view and car",
	Action: func(cctx *cli.Context) error {
		api, closer, err := getEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		err = api.SyncAssetViewAndData(ctx)
		if err != nil {
			return err
		}

		return nil
	},
}

var deleteLocalAssetCmd = &cli.Command{
	Name:  "delete",
	Usage: "delete local asset",
	Action: func(cctx *cli.Context) error {
		api, closer, err := getEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		assetCID := cctx.Args().Get(0)
		if len(assetCID) == 0 {
			return fmt.Errorf("Asset cid can not empty")
		}

		ctx := ReqContext(cctx)

		err = api.DeleteAsset(ctx, assetCID)
		if err != nil {
			return err
		}

		return nil
	},
}

var upgradeCmd = &cli.Command{
	Name:  "upgrade",
	Usage: "Upgrade titan egde",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		client := github.NewClient(nil)

		owner := "Titannet-dao"
		repo := "titan-node"

		release, _, err := client.Repositories.GetLatestRelease(cctx.Context, owner, repo)
		if err != nil {
			return fmt.Errorf("cannot access release repo: %s", err.Error())
		}

		latestVersion, err := newVersion(pickVersionInString(release.GetTagName()))
		if err != nil {
			return fmt.Errorf("cannot parse latest version: %s", err.Error())
		}

		curEdgeVersion, err := currentEdgeVersion()
		if err != nil {
			return err
		}

		if latestVersion == curEdgeVersion {
			log.Info("Titan-Edge is already update to date")
			return nil
		}

		if latestVersion > curEdgeVersion {
			// fetch download url
			downloadUrl := releaseUrlFromGithub(latestVersion, release.Assets)
			if downloadUrl == "" {
				return errors.New("cannot find download url for the release")
			}

			// download
			tar, err := downloadApp(downloadUrl)
			if err != nil {
				return fmt.Errorf("cannot download application: %s", err.Error())
			}

			// unpack and replace current binary
			err = unpackAndReplaceBinary(tar)
			if err != nil {
				return fmt.Errorf("cannot unpack and replace binary: %s", err.Error())
			}

			log.Info("Titan-Edge has been updated to latest version, please restart titan-edge!")
		}

		return nil
	},
}

func currentEdgeVersion() (api.Version, error) {
	currentEdge, err := os.Executable()
	if err != nil {
		return api.Version(0), fmt.Errorf("failed to get current executable path: %s", err.Error())
	}

	cmd := exec.Command(currentEdge, "-v")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return api.Version(0), fmt.Errorf("failed to get stdout pipe: %s", err.Error())
	}

	err = cmd.Start()
	if err != nil {
		return api.Version(0), fmt.Errorf("failed to get current edge version: %s", err.Error())
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, stdout) // titan-edge version 0.1.18
	if err != nil {
		return api.Version(0), fmt.Errorf("failed to read stdout: %s", err.Error())
	}
	v := pickVersionInString(buf.String())
	if v == "" {
		return api.Version(0), errors.New("invalid version format")
	}

	return newVersion(v)
}

func pickVersionInString(from string) string {
	pattern := `\d+.\d+.\d+`
	re := regexp.MustCompile(pattern)
	return re.FindString(from)
}

func releaseUrlFromGithub(version api.Version, assests []*github.ReleaseAsset) string {
	for _, v := range assests {
		if strings.Contains(v.GetName(), runtime.GOARCH) &&
			strings.Contains(v.GetName(), runtime.GOOS) &&
			strings.Contains(v.GetName(), version.String()) &&
			strings.HasSuffix(v.GetName(), ".tar.gz") {
			return v.GetBrowserDownloadURL()
		}
	}

	return ""
}

func downloadApp(downloadURL string) ([]byte, error) {
	client := &http.Client{Timeout: 60 * time.Second}
	req, err := http.NewRequest("GET", downloadURL, nil)
	if err != nil {
		return nil, err
	}

	log.Infof("Downloading titan-edge from %s", downloadURL)
	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("Failed to download titan-edge: %v", err)
		return nil, err
	}

	defer resp.Body.Close() //nolint:errcheck // ignore error

	log.Info("Download complete")
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func unpackAndReplaceBinary(f []byte) error {
	buf := bytes.NewBuffer(f)

	gzipReader, err := gzip.NewReader(buf)
	if err != nil {
		return err
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	var found bool
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if header.Typeflag == tar.TypeReg && strings.Contains(header.Name, "titan-edge") {
			header.Name = header.Name[strings.LastIndex(header.Name, "/")+1:]
			found = true

			// check whether exsist same tmp file
			if _, err := os.Stat(header.Name + "_tmp"); err == nil {
				if err := os.Remove(header.Name + "_tmp"); err != nil {
					return err
				}
			}

			// write new binary to tmp
			outFile, err := os.Create(header.Name + "_tmp")
			if err != nil {
				return err
			}
			defer outFile.Close()

			if _, err := io.Copy(outFile, tarReader); err != nil {
				return err
			}

			// remove old binary
			oldFileName, err := os.Executable()
			if err != nil {
				return err
			}
			if err := os.Remove(oldFileName); err != nil {
				return err
			}

			// set tmp binary to final name
			if err := os.Rename(header.Name+"_tmp", header.Name); err != nil {
				return err
			}

			// set permission
			if err := os.Chmod(header.Name, 0o755); err != nil {
				return err
			}

			return nil
		}
	}

	if !found {
		return errors.New("binary not found in archive")
	}

	return nil
}

var udpTestCmd = &cli.Command{
	Name:  "udp-test",
	Usage: "Test client quic network through udp is available", // 测试以udp为传输层的quic网络是否通畅
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "test-url",
			Usage: "network test access url, example: --test-url=https://www.google.com",
			Value: "https://www.google.com",
		},
	},
	Action: func(cctx *cli.Context) error {
		addr := cctx.String("test-url")
		if addr == "" {
			return errors.New("please input a specified url to run test")
		}
		_, err := url.Parse(addr)
		if err != nil {
			return errors.New("please input a valid url to run test")
		}

		hclient := &http.Client{
			Transport: &http3.RoundTripper{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				QUICConfig: &quic.Config{
					MaxIncomingStreams:    100,
					MaxIncomingUniStreams: 100,
				},
			},
		}

		_, err = hclient.Get(addr)
		if err != nil {
			return fmt.Errorf("network test failed: %s", err.Error())
		}

		log.Info("network test success")
		return nil
	},
}

var freeUpDiskCmd = &cli.Command{
	Name:  "fuds",
	Usage: "Free up disk space",
	Subcommands: []*cli.Command{
		{
			Name:  "state",
			Usage: "Check the last request to free up disk space is done. Example: --state",
			Action: func(cctx *cli.Context) error {
				api, edgeClose, err := getEdgeAPI(cctx)
				if err != nil {
					log.Errorf("get egde api failed: %s", err.Error())
					return err
				}
				defer edgeClose()

				ret, err := api.StateFreeUpDisk(cctx.Context)
				if err != nil {
					log.Errorf("fetch state of fuds task error: %s, next release time is %s", err.Error(), time.Unix(ret.NextTime, 0).Format("2006-01-02 15:04:05"))
					return err
				}

				if len(ret.Hashes) == 0 {
					log.Info("task is done!")
					return nil
				}
				log.Info("task in in-progress!")
				return nil
			},
		},
		{
			Name:  "clear",
			Usage: "Clear the previous failed free-up-disk task",
			Action: func(cctx *cli.Context) error {
				api, edgeClose, err := getEdgeAPI(cctx)
				if err != nil {
					log.Errorf("get egde api failed: %s", err.Error())
					return err
				}
				defer edgeClose()

				err = api.ClearFreeUpDisk(cctx.Context)
				if err != nil {
					log.Errorf("clear fuds task error: %s", err.Error())
					return err
				}
				return nil
			},
		},
	},
	Flags: []cli.Flag{
		&cli.Float64Flag{
			Name:  "size",
			Value: 0,
			Usage: "Size of request free up disk space, units is GB, example: --size 1.1",
			Action: func(cctx *cli.Context, f float64) error {
				if f <= 0 {
					return nil
				}

				api, edgeClose, err := getEdgeAPI(cctx)
				if err != nil {
					log.Errorf("get egde api failed: %s", err.Error())
					return err
				}
				defer edgeClose()

				if err := api.RequestFreeUpDisk(cctx.Context, f); err != nil {
					log.Warnf("request to free up disk failed: %s, if previous task exists, please call --clear to remove", err.Error())
					return err
				}

				return nil
			},
		},

		// &cli.ActionFunc{
		// 	Name:  "state",
		// 	Usage: "Check the last request to free up disk space is done. Example: --state",
		// 	Action: func(ctx *cli.Context, i int) error {
		// 		return nil
		// 	},
		// },
	},
}
