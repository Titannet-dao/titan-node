package clib

import (
	"bytes"
	"context"
	"crypto"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	lcli "github.com/Filecoin-Titan/titan/cli"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/repo"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/filecoin-project/go-jsonrpc"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

var log = logging.Logger("jsoncall")

const (
	methodStartDaemon      = "startDaemon"
	methodStopDaemon       = "stopDaemon"
	methodSign             = "sign"
	methodState            = "state"
	methodMergeConfig      = "mergeConfig"
	methodReadConfig       = "readConfig"
	methodDownloadFile     = "downloadFile"
	methodDownloadProgress = "downloadProgress"
	methodDownloadCancel   = "downloadCancel"
)

type DaemonSwitch struct {
	StopChan chan bool
	IsStop   bool
	IsOnline bool
}

type StartDaemonReq struct {
	RepoPath   string `json:"repoPath"`
	LogPath    string `json:"logPath"`
	LocatorURL string `json:"locatorURL"`
}

type ReadConfigReq struct {
	RepoPath string `json:"repoPath"`
}

type MergeConfigReq struct {
	RepoPath string `json:"repoPath"`
	Config   string `json:"config"`
}

type SignReq struct {
	RepoPath string `json:"repoPath"`
	Hash     string `json:"hash"`
}

type SignResult struct {
	Signature string `json:"signature"`
}

type StateResult struct {
	Runing bool `json:"running"`
	Online bool `json:"online"`
}

type DownloadFileReq struct {
	CID          string `json:"cid"`
	DownloadPath string `json:"download_path"`
	LocatorURL   string `json:"locator_url"`
}

type DownloadProgressReq struct {
	FilePath string `json:"file_path"`
}

type DownloadProgressResult struct {
	FilePath  string `json:"file_path"`
	Status    string `json:"status"`
	TotalSize int64  `json:"total_size"`
	DoneSize  int64  `json:"done_size"`
}

type DownloadCancelReq struct {
	FilePath string `json:"file_path"`
}

type JSONCallArgs struct {
	Method     string `json:"method"`
	JSONParams string `json:"jsonParams"`
}

type JSONCallResult struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data string `json:"data"`
}

func errorResult(err error) *JSONCallResult {
	result := JSONCallResult{Code: 0, Msg: "ok"}
	if err != nil {
		result.Code = -1
		result.Msg = err.Error()
	}

	return &result
}

type daemonStartFunc func(ctx context.Context, daemonSwitch *DaemonSwitch, repoPath, locatorURL string) error
type CLib struct {
	daemonStart daemonStartFunc
	repoPath    string
	isInit      bool
	dSwitch     DaemonSwitch

	downloader *Downloader
}

func NewCLib(daemonStart daemonStartFunc) *CLib {
	return &CLib{daemonStart: daemonStart, downloader: newDownloader()}
}

func (clib *CLib) JSONCall(jsonStr string) *JSONCallResult {
	args := JSONCallArgs{}
	err := json.Unmarshal([]byte(jsonStr), &args)
	if err != nil {
		return errorResult(fmt.Errorf("marshal input args failed:%s", err.Error()))
	}

	result := &JSONCallResult{}
	switch args.Method {
	case methodStartDaemon:
		return errorResult(clib.startDaemon(args.JSONParams))
	case methodStopDaemon:
		return errorResult(clib.stopDaemon())
	case methodSign:
		return clib.sign(args.JSONParams)
	case methodState:
		return clib.state()
	case methodMergeConfig:
		return clib.mergeConfig(args.JSONParams)
	case methodReadConfig:
		return clib.readConfig(args.JSONParams)
	case methodDownloadFile:
		return clib.downloadFile(args.JSONParams)
	case methodDownloadProgress:
		return clib.downloadProgress(args.JSONParams)
	case methodDownloadCancel:
		return clib.downloadCancel(args.JSONParams)
	default:
		result.Code = -1
		result.Msg = fmt.Sprintf("Method %s not found", args.Method)

	}

	return result
}

func setLogFile(logPath string) {
	config := logging.GetConfig()
	config.Stderr = false
	config.Stdout = false
	config.File = logPath
	config.Format = logging.JSONOutput
	logging.SetupLogging(config)
	logging.SetAllLoggers(logging.LevelInfo)
}

func (clib *CLib) startDaemon(jsonStr string) error {
	req := StartDaemonReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return fmt.Errorf("marshal input args failed:%s", err.Error())
	}

	if len(req.RepoPath) == 0 {
		return fmt.Errorf("Args RepoPath can not emtpy")
	}

	if clib.isInit {
		if clib.dSwitch.IsStop {
			clib.dSwitch.StopChan <- false
		} else {
			log.Infof("daemon already start")
		}
		return nil
	}
	clib.isInit = true
	clib.dSwitch = DaemonSwitch{StopChan: make(chan bool)}

	types.RunningNodeType = types.NodeEdge

	if len(req.LogPath) > 0 {
		setLogFile(req.LogPath)
	}

	clib.repoPath = req.RepoPath

	go func() {
		if err = clib.daemonStart(context.Background(), &clib.dSwitch, req.RepoPath, req.LocatorURL); err != nil {
			log.Warnf("daemonStart %s", err.Error())
		}

		clib.isInit = false
		clib.dSwitch.IsStop = true

		log.Info("deamon stop")
	}()
	return nil
}

func (clib *CLib) stopDaemon() error {
	if !clib.isInit {
		return fmt.Errorf("Daemon not running")
	}

	if clib.dSwitch.IsStop {
		return fmt.Errorf("Daemon already stop")
	}

	clib.dSwitch.StopChan <- true

	return nil
}

func (clib *CLib) state() *JSONCallResult {
	if len(clib.repoPath) == 0 {
		return &JSONCallResult{Code: -1, Msg: "daemon not start"}
	}

	result := StateResult{Runing: !clib.dSwitch.IsStop, Online: clib.dSwitch.IsOnline}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: err.Error()}
	}

	return &JSONCallResult{Code: 0, Msg: "ok", Data: string(resultJSON)}
}

func (clib *CLib) sign(jsonStr string) *JSONCallResult {
	req := SignReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	if len(req.Hash) == 0 {
		return &JSONCallResult{Code: -1, Msg: "Args Hash can not empty"}
	}

	if len(req.RepoPath) == 0 {
		return &JSONCallResult{Code: -1, Msg: "Args RepoPath can not empty"}
	}

	r, err := openRepoOrNew(req.RepoPath)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: err.Error()}
	}

	privateKeyPem, err := r.PrivateKey()
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: err.Error()}
	}

	privateKey, err := titanrsa.Pem2PrivateKey(privateKeyPem)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: err.Error()}
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sign, err := titanRsa.Sign(privateKey, []byte(req.Hash))
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: err.Error()}
	}

	result := SignResult{Signature: hex.EncodeToString(sign)}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: err.Error()}
	}

	return &JSONCallResult{Code: 0, Msg: "ok", Data: string(resultJSON)}
}

func (clib *CLib) mergeConfig(jsonStr string) *JSONCallResult {
	req := MergeConfigReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	if len(req.RepoPath) == 0 {
		return &JSONCallResult{Code: -1, Msg: "Args RepoPath can not empty"}
	}

	if len(req.Config) == 0 {
		return &JSONCallResult{Code: -1, Msg: "Args Config can not empty"}
	}

	newEdgeConfig := config.EdgeCfg{}
	reader := bytes.NewReader([]byte(req.Config))
	if _, err := toml.NewDecoder(reader).Decode(&newEdgeConfig); err != nil {
		return &JSONCallResult{Code: -1, Msg: fmt.Sprintf("decode config error %s, config string %s", err.Error(), req.Config)}
	}

	// check storage path
	if len(newEdgeConfig.Storage.Path) > 0 {
		if err := checkPath(newEdgeConfig.Storage.Path); err != nil {
			return &JSONCallResult{Code: -1, Msg: err.Error()}
		}
	}
	// check quota
	configPath := path.Join(req.RepoPath, "config.toml")
	cfg, err := config.FromFile(configPath, config.DefaultEdgeCfg())
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: fmt.Sprintf("load local config file error %s", err.Error())}
	}

	edgeConfig := cfg.(*config.EdgeCfg)

	if len(edgeConfig.Basic.Token) == 0 {
		r, err := openRepoOrNew(req.RepoPath)
		if err != nil {
			return &JSONCallResult{Code: -1, Msg: err.Error()}
		}

		lr, err := r.Lock(repo.Edge)
		if err != nil {
			return &JSONCallResult{Code: -1, Msg: err.Error()}
		}

		defer lr.Close()

		if err := lcli.RegitsterNode(lr, newEdgeConfig.Network.LocatorURL, types.NodeEdge); err != nil {
			return &JSONCallResult{Code: -1, Msg: fmt.Sprintf("import private key error %s", err.Error())}
		}

		// reload config
		cfg, err := config.FromFile(configPath, config.DefaultEdgeCfg())
		if err != nil {
			return &JSONCallResult{Code: -1, Msg: fmt.Sprintf("load local config file error %s", err.Error())}
		}

		edgeConfig = cfg.(*config.EdgeCfg)
	}

	edgeConfig.Storage = newEdgeConfig.Storage
	edgeConfig.Memory = newEdgeConfig.Memory
	edgeConfig.CPU = newEdgeConfig.CPU
	edgeConfig.Bandwidth.BandwidthMB = newEdgeConfig.Bandwidth.BandwidthMB

	configBytes, err := config.GenerateConfigUpdate(edgeConfig, config.DefaultEdgeCfg(), true)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: fmt.Sprintf("update config error %s", err.Error())}
	}

	err = os.WriteFile(configPath, configBytes, 0o644)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: err.Error()}
	}

	edgeConfigJSON, err := json.Marshal(edgeConfig)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: err.Error()}
	}
	return &JSONCallResult{Code: 0, Msg: "ok", Data: string(edgeConfigJSON)}
}

func (clib *CLib) readConfig(jsonStr string) *JSONCallResult {
	req := ReadConfigReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	if len(req.RepoPath) == 0 {
		return &JSONCallResult{Code: -1, Msg: "Args RepoPath can not empty"}
	}

	// check quota
	configPath := path.Join(req.RepoPath, "config.toml")
	cfg, err := config.FromFile(configPath, config.DefaultEdgeCfg())
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: fmt.Sprintf("load local config file error %s", err.Error())}
	}

	edgeConfigJSON, err := json.Marshal(cfg)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: err.Error()}
	}
	return &JSONCallResult{Code: 0, Msg: "ok", Data: string(edgeConfigJSON)}
}

func newEdgeAPI(repoPath string) (api.Edge, jsonrpc.ClientCloser, error) {
	r, err := openRepoOrNew(repoPath)
	if err != nil {
		return nil, nil, err
	}

	token, err := r.APIToken()
	if err != nil {
		return nil, nil, err
	}

	mAddr, err := r.APIEndpoint()
	if err != nil {
		return nil, nil, err
	}

	ma, err := multiaddr.NewMultiaddr(mAddr.String())
	if err != nil {
		return nil, nil, err
	}

	_, addr, err := manet.DialArgs(ma)
	if err != nil {
		return nil, nil, err
	}

	url := fmt.Sprintf("ws://%s/rpc/v0", addr)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(token))

	edgeAPI, closer, err := client.NewEdge(context.Background(), url, headers)
	if err != nil {
		return nil, nil, err
	}

	return edgeAPI, closer, nil
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

func (clib *CLib) downloadFile(jsonStr string) *JSONCallResult {
	req := DownloadFileReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	if err = clib.downloader.downloadFile(&req); err != nil {
		return &JSONCallResult{Code: -1, Msg: err.Error()}
	}

	return &JSONCallResult{Code: 0, Msg: "ok"}
}

func (clib *CLib) downloadProgress(jsonStr string) *JSONCallResult {
	req := DownloadProgressReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	result, err := clib.downloader.queryProgress(req.FilePath)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: err.Error()}
	}

	buf, err := json.Marshal(result)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: err.Error()}
	}

	return &JSONCallResult{Code: 0, Msg: "ok", Data: string(buf)}
}

func (clib *CLib) downloadCancel(jsonStr string) *JSONCallResult {
	req := DownloadCancelReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	if err := clib.downloader.cancelDownloadTask(req.FilePath); err != nil {
		return &JSONCallResult{Code: 0, Msg: err.Error()}
	}

	return &JSONCallResult{Code: 0, Msg: "ok"}
}
