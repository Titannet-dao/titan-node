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
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/repo"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/filecoin-project/go-jsonrpc"
	logging "github.com/ipfs/go-log/v2"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	methodReqFreeUpDisk    = "reqFreeUpDisk"
	methodStateFreeUpDisk  = "stateFreeUpDisk"
	methodRestart          = "restart"
	methodCheckLocators    = "checkLocators"
)

type DaemonSwitch struct {
	StopChan chan bool
	IsStop   bool
	IsOnline bool
	ErrMsg   string // client immdiate error notice
}

type StartDaemonReq struct {
	RepoPath   string `json:"repoPath"`
	LogPath    string `json:"logPath"`
	LocatorURL string `json:"locatorURL"`
	LogRotate  bool   `json:"logRotate"`
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
	Runing bool   `json:"running"`
	Online bool   `json:"online"`
	ErrMsg string `json:"errMsg"`
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

type FreeUpDiskReq struct {
	Size float64 `json:"size"`
}

type FreeUpDiskResp struct{}

type JSONCallArgs struct {
	Method     string `json:"method"`
	JSONParams string `json:"jsonParams"`
}

type JSONCallResult struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data string `json:"data"`
}

func errorResult(code int, err error) *JSONCallResult {
	result := JSONCallResult{Code: 0, Msg: "ok"}
	if err != nil {
		result.Code = code
		result.Msg = err.Error()
	}

	return &result
}

type (
	daemonStartFunc func(ctx context.Context, daemonSwitch *DaemonSwitch, repoPath, locatorURL string) error
	CLib            struct {
		daemonStart daemonStartFunc
		repoPath    string
		isStaring   bool
		dSwitch     DaemonSwitch
		downloader  *Downloader
		quicClient  *http.Client
	}
)

func NewCLib(daemonStart daemonStartFunc) *CLib {
	return &CLib{daemonStart: daemonStart, downloader: newDownloader(), dSwitch: DaemonSwitch{IsStop: true}, quicClient: client.NewHTTP3Client()}
}

func (clib *CLib) JSONCall(jsonStr string) *JSONCallResult {
	args := JSONCallArgs{}
	err := json.Unmarshal([]byte(jsonStr), &args)
	if err != nil {
		return errorResult(CodeClibRequestInvalid, fmt.Errorf("marshal input args failed:%s", err.Error()))
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
	case methodReqFreeUpDisk:
		return clib.reqFreeUpDisk(args.JSONParams)
	case methodStateFreeUpDisk:
		return clib.stateFreeUpDisk()
	case methodRestart:
		return clib.restart()
	case methodCheckLocators:
		return clib.checkLocators(args.JSONParams)
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

func setLogRotate(logPath string) error {
	config := logging.GetConfig()
	config.Stderr = false
	config.Stdout = false
	logging.SetupLogging(config)

	logDir := filepath.Dir(logPath)
	ext := filepath.Ext(logPath)
	logbaseName := strings.TrimRight(filepath.Base(logPath), ext)
	rotator, err := rotatelogs.New(
		path.Join(logDir, logbaseName+"-%Y-%m-%d"+ext),
		rotatelogs.WithLinkName(logPath),
		rotatelogs.WithRotationTime(24*time.Hour),
		rotatelogs.WithMaxAge(7*24*time.Hour),
	)
	if err != nil {
		return err
	}

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderCfg.EncodeLevel = zapcore.CapitalLevelEncoder
	encoder := zapcore.NewJSONEncoder(encoderCfg)
	writer := zapcore.AddSync(rotator)
	core := zapcore.NewCore(encoder, writer, zapcore.InfoLevel)

	logging.SetPrimaryCore(core)
	logging.SetAllLoggers(logging.LevelInfo)

	// set log and natural day align
	now := time.Now()
	nextMidnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	delay := nextMidnight.Sub(now)
	time.AfterFunc(delay, func() {
		rotator.Rotate()
	})

	return nil
}

func (clib *CLib) startDaemon(jsonStr string) (int, error) {
	req := StartDaemonReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return CodeClibRequestInvalid, fmt.Errorf("marshal input args failed:%s", err.Error())
	}

	if len(req.RepoPath) == 0 {
		return CodeOpenPathError, fmt.Errorf("Args RepoPath can not emtpy")
	}

	// if clib.isInit {
	// 	if clib.dSwitch.IsStop {
	// 		// clib.dSwitch.StopChan <- false
	// 	} else {
	// 		log.Infof("daemon already start")
	// 	}
	// 	return nil
	// }
	if !clib.dSwitch.IsStop {
		log.Infof("daemon already start")
		return CodeDaemonAlreadyStart, fmt.Errorf("Daemon already start")
	}

	if clib.isStaring {
		return CodeDaemonIsStarting, fmt.Errorf("Is starting, please wait")
	}
	clib.isStaring = true
	defer func() {
		clib.isStaring = false
	}()

	// clib.isInit = true
	// clib.dSwitch = DaemonSwitch{StopChan: make(chan bool)}

	types.RunningNodeType = types.NodeEdge

	if len(req.LogPath) > 0 {
		if req.LogRotate {
			if err := setLogRotate(req.LogPath); err != nil {
				return CodeDaemonLogSetError, fmt.Errorf("set log rotate failed %s", err.Error())
			}
		} else {
			setLogFile(req.LogPath)
		}
	}

	clib.repoPath = req.RepoPath

	if err = clib.daemonStart(context.Background(), &clib.dSwitch, req.RepoPath, req.LocatorURL); err != nil {
		// clib.isInit = false
		return CodeDaemonStartFailed, err
	}

	// clib.isInit = true
	clib.dSwitch.IsStop = false

	log.Info("deamon start")
	return CodeSuccess, nil
}

func (clib *CLib) stopDaemon() (int, error) {
	// if !clib.isInit {
	// 	return fmt.Errorf("Daemon not init")
	// }

	if clib.dSwitch.IsStop {
		return CodeDaemonAlreadyStoppped, fmt.Errorf("Daemon already stop")
	}

	// clib.dSwitch.StopChan <- true
	edgeApi, closer, err := newEdgeAPI(clib.repoPath)
	if err != nil {
		return CodeNewEdgeAPIErr, fmt.Errorf("get edge api error %s", err.Error())
	}
	defer closer()

	if err := edgeApi.Shutdown(context.Background()); err != nil {
		return CodeDaemonStopFailed, fmt.Errorf("restart occurs with error: %s", err.Error())
	}

	return CodeSuccess, nil
}

func (clib *CLib) state() *JSONCallResult {
	if len(clib.repoPath) == 0 {
		return &JSONCallResult{Code: CodeRepoPathInvalid, Msg: "daemon not start"}
	}

	result := StateResult{Runing: !clib.dSwitch.IsStop, Online: clib.dSwitch.IsOnline, ErrMsg: clib.dSwitch.ErrMsg}

	clib.dSwitch.ErrMsg = ""

	resultJSON, err := json.Marshal(result)
	if err != nil {
		return &JSONCallResult{Code: CodeClibResponseInvalid, Msg: err.Error()}
	}

	return &JSONCallResult{Code: 0, Msg: "ok", Data: string(resultJSON)}
}

func (clib *CLib) sign(jsonStr string) *JSONCallResult {
	req := SignReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return &JSONCallResult{Code: CodeClibRequestInvalid, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	if len(req.Hash) == 0 {
		return &JSONCallResult{Code: CodeSignHashEmpty, Msg: "Args Hash can not empty"}
	}

	if len(req.RepoPath) == 0 {
		return &JSONCallResult{Code: CodeRepoPathInvalid, Msg: "Args RepoPath can not empty"}
	}

	r, err := openRepoOrNew(req.RepoPath)
	if err != nil {
		return &JSONCallResult{Code: CodeOpenPathError, Msg: err.Error()}
	}

	privateKeyPem, err := r.PrivateKey()
	if err != nil {
		return &JSONCallResult{Code: CodePrivateKeyError, Msg: err.Error()}
	}

	privateKey, err := titanrsa.Pem2PrivateKey(privateKeyPem)
	if err != nil {
		return &JSONCallResult{Code: CodePrivateKeyError, Msg: err.Error()}
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sign, err := titanRsa.Sign(privateKey, []byte(req.Hash))
	if err != nil {
		return &JSONCallResult{Code: CodePrivateKeyError, Msg: err.Error()}
	}

	result := SignResult{Signature: hex.EncodeToString(sign)}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return &JSONCallResult{Code: CodeClibResponseInvalid, Msg: err.Error()}
	}

	return &JSONCallResult{Code: 0, Msg: "ok", Data: string(resultJSON)}
}

func (clib *CLib) mergeConfig(jsonStr string) *JSONCallResult {
	req := MergeConfigReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return &JSONCallResult{Code: CodeClibRequestInvalid, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	if len(req.RepoPath) == 0 {
		return &JSONCallResult{Code: CodeClibRequestInvalid, Msg: "Args RepoPath can not empty"}
	}

	if len(req.Config) == 0 {
		return &JSONCallResult{Code: CodeClibRequestInvalid, Msg: "Args Config can not empty"}
	}

	newEdgeConfig := config.EdgeCfg{}
	reader := bytes.NewReader([]byte(req.Config))
	if _, err := toml.NewDecoder(reader).Decode(&newEdgeConfig); err != nil {
		return &JSONCallResult{Code: CodeConfigInvalid, Msg: fmt.Sprintf("decode config error %s, config string %s", err.Error(), req.Config)}
	}

	// check storage path
	if len(newEdgeConfig.Storage.Path) > 0 {
		if err := checkPath(newEdgeConfig.Storage.Path); err != nil {
			return &JSONCallResult{Code: CodeConfigStoragePathInvalid, Msg: err.Error()}
		}
	}
	// check quota
	configPath := path.Join(req.RepoPath, "config.toml")
	cfg, err := config.FromFile(configPath, config.DefaultEdgeCfg())
	if err != nil {
		return &JSONCallResult{Code: CodeConfigLocalDiskLoadError, Msg: fmt.Sprintf("load local config file error %s", err.Error())}
	}

	edgeConfig := cfg.(*config.EdgeCfg)

	edgeConfig.Storage = newEdgeConfig.Storage
	edgeConfig.Memory = newEdgeConfig.Memory
	edgeConfig.CPU = newEdgeConfig.CPU
	edgeConfig.Bandwidth = newEdgeConfig.Bandwidth
	edgeConfig.Netflow = newEdgeConfig.Netflow
	// as the request of front-end
	edgeConfig.Network.LocatorURL = newEdgeConfig.Network.LocatorURL

	configBytes, err := config.GenerateConfigUpdate(edgeConfig, config.DefaultEdgeCfg(), true)
	if err != nil {
		return &JSONCallResult{Code: CodeUntracked, Msg: fmt.Sprintf("update config error %s", err.Error())}
	}

	err = os.WriteFile(configPath, configBytes, 0o644)
	if err != nil {
		return &JSONCallResult{Code: CodeConfigFilePermissionError, Msg: err.Error()}
	}

	edgeConfigJSON, err := json.Marshal(edgeConfig)
	if err != nil {
		return &JSONCallResult{Code: CodeClibResponseInvalid, Msg: err.Error()}
	}
	return &JSONCallResult{Code: 0, Msg: "ok", Data: string(edgeConfigJSON)}
}

func (clib *CLib) readConfig(jsonStr string) *JSONCallResult {
	req := ReadConfigReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return &JSONCallResult{Code: CodeClibRequestInvalid, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	if len(req.RepoPath) == 0 {
		return &JSONCallResult{Code: CodeClibRequestInvalid, Msg: "Args RepoPath can not empty"}
	}

	// check quota
	configPath := path.Join(req.RepoPath, "config.toml")
	cfg, err := config.FromFile(configPath, config.DefaultEdgeCfg())
	if err != nil {
		return &JSONCallResult{Code: CodeConfigLocalDiskLoadError, Msg: fmt.Sprintf("load local config file error %s", err.Error())}
	}

	edgeConfigJSON, err := json.Marshal(cfg)
	if err != nil {
		return &JSONCallResult{Code: CodeClibResponseInvalid, Msg: err.Error()}
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
		return &JSONCallResult{Code: CodeClibRequestInvalid, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	if err = clib.downloader.downloadFile(&req); err != nil {
		return &JSONCallResult{Code: CodeDownloadFileFailed, Msg: err.Error()}
	}

	return &JSONCallResult{Code: 0, Msg: "ok"}
}

func (clib *CLib) downloadProgress(jsonStr string) *JSONCallResult {
	req := DownloadProgressReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return &JSONCallResult{Code: CodeClibRequestInvalid, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	result, err := clib.downloader.queryProgress(req.FilePath)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: err.Error()}
	}

	buf, err := json.Marshal(result)
	if err != nil {
		return &JSONCallResult{Code: CodeClibResponseInvalid, Msg: err.Error()}
	}

	return &JSONCallResult{Code: 0, Msg: "ok", Data: string(buf)}
}

func (clib *CLib) downloadCancel(jsonStr string) *JSONCallResult {
	req := DownloadCancelReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return &JSONCallResult{Code: CodeClibRequestInvalid, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	if err := clib.downloader.cancelDownloadTask(req.FilePath); err != nil {
		return &JSONCallResult{Code: 0, Msg: err.Error()}
	}

	return &JSONCallResult{Code: 0, Msg: "ok"}
}

func (clib *CLib) reqFreeUpDisk(jsonStr string) *JSONCallResult {
	req := FreeUpDiskReq{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return &JSONCallResult{Code: CodeClibRequestInvalid, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	edgeApi, closer, err := newEdgeAPI(clib.repoPath)
	if err != nil {
		return &JSONCallResult{Code: CodeNewEdgeAPIErr, Msg: fmt.Sprintf("get edge api error %s", err.Error())}
	}
	defer closer()

	if err := edgeApi.RequestFreeUpDisk(context.Background(), req.Size); err != nil {
		return &JSONCallResult{Code: CodeReqDiskFreeErr, Msg: fmt.Sprintf("request free up disk error %s", err.Error())}
	}

	return &JSONCallResult{Code: 0, Msg: "ok"}
}

type stateFreeUpDiskResp struct {
	WaitingList []*types.FreeUpDiskState `json:"waitingList"`
	NextTime    int64                    `json:"nextTime"`
}

func (clib *CLib) stateFreeUpDisk() *JSONCallResult {
	edgeApi, closer, err := newEdgeAPI(clib.repoPath)
	if err != nil {
		return &JSONCallResult{Code: CodeNewEdgeAPIErr, Msg: fmt.Sprintf("get edge api error %s", err.Error())}
	}
	defer closer()

	ret, err := edgeApi.StateFreeUpDisk(context.Background())
	if err != nil {
		return &JSONCallResult{Code: CodeStateFreeUpDiskErr, Msg: fmt.Sprintf("state free up disk error %s", err.Error())}
	}

	respStr := stateFreeUpDiskResp{
		WaitingList: ret.Hashes, NextTime: ret.NextTime,
	}
	if len(ret.Hashes) == 0 {
		return &JSONCallResult{Code: 0, Msg: "ok", Data: jsonStr(respStr)}
	}
	return &JSONCallResult{Code: CodeFreeUpDiskInProgress, Msg: "free up task is still in progress", Data: jsonStr(respStr)}
}

func (clib *CLib) restart() *JSONCallResult {
	edgeApi, closer, err := newEdgeAPI(clib.repoPath)
	if err != nil {
		return &JSONCallResult{Code: CodeNewEdgeAPIErr, Msg: fmt.Sprintf("get edge api error %s", err.Error())}
	}
	defer closer()

	if err := edgeApi.Restart(context.Background()); err != nil {
		return &JSONCallResult{Code: CodeEdgeRestartFailed, Msg: fmt.Sprintf("restart occurs with error: %s", err.Error())}
	}

	return &JSONCallResult{Msg: "ok"}
}

type checkLocatorsReq struct {
	Endpoint string `json:"endpoint"`
}

type checkLocatorItem struct {
	Locator string `json:"locator"`
	Timeout int64  `json:"timeout"`
}

func (clib *CLib) checkLocators(str string) *JSONCallResult {
	req := checkLocatorsReq{}
	err := json.Unmarshal([]byte(str), &req)
	if err != nil {
		return &JSONCallResult{Code: CodeClibRequestInvalid, Msg: fmt.Sprintf("marshal input args failed:%s", err.Error())}
	}

	err = os.Setenv("QUIC_GO_DISABLE_ECN", "true")
	if err != nil {
		log.Errorf("Error setting environment QUIC_GO_DISABLE_ECN:", err)
	}

	r, err := http.NewRequest("GET", req.Endpoint, nil)
	if err != nil {
		return &JSONCallResult{Code: -1, Msg: fmt.Sprintf("new request error %s", err.Error())}
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return &JSONCallResult{Code: CodeEdgeNetworkErr, Msg: fmt.Sprintf("do request error %s", err.Error())}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return &JSONCallResult{Code: CodeWebServerErr, Msg: fmt.Sprintf("response status code %d", resp.StatusCode)}
	}

	type WebResp struct {
		Code int `json:"code"`
		Data struct {
			List []string `json:"list"`
		} `json:"data"`
	}

	var webResp WebResp
	if err := json.NewDecoder(resp.Body).Decode(&webResp); err != nil {
		return &JSONCallResult{Code: CodeWebServerErr, Msg: fmt.Sprintf("decode response error %s", err.Error())}
	}

	if webResp.Code != 0 {
		return &JSONCallResult{Code: CodeWebServerErr, Msg: fmt.Sprintf("response code %d", webResp.Code)}
	}

	var ret = make([]checkLocatorItem, 0)

	for _, v := range webResp.Data.List {
		ret = append(ret, checkLocatorItem{Locator: v})
	}

	var wg sync.WaitGroup
	wg.Add(len(ret))

	clib.quicClient.Timeout = 2 * time.Second

	for i := range ret {
		go func(idx int) {
			defer wg.Done()
			ret[i].Timeout = 9999
			api, closer, err := client.NewLocator(context.Background(), ret[i].Locator, nil, jsonrpc.WithHTTPClient(clib.quicClient))
			if err != nil {
				return
			}
			defer closer()
			ts := time.Now()
			_, err = api.Version(context.Background())
			if err != nil {
				return
			}
			ret[i].Timeout = int64(time.Since(ts).Milliseconds())
		}(i)
	}
	wg.Wait()

	return &JSONCallResult{Code: 0, Msg: "ok", Data: jsonStr(ret)}
}

func jsonStr(v any) string {
	str, _ := json.Marshal(v)
	return string(str)
}
