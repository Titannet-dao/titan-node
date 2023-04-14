package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/build"
	"github.com/Filecoin-Titan/titan/lib/titanlog"
	"github.com/filecoin-project/go-jsonrpc"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("main")

func main() {
	types.RunningNodeType = types.NodeUpdater
	titanlog.SetupLogLevels()

	app := &cli.App{
		Name:                 "titan-update",
		Usage:                "Titan update",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Commands:             []*cli.Command{runCmd},
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		log.Errorf("%+v", err)
		return
	}
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start titan edge node",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "update-server",
			Usage: "update server url",
			Value: "http://192.168.0.132:3456/rpc/v0",
		},
		&cli.StringFlag{
			Name:  "install-path",
			Usage: "install path",
			Value: "/root/edge",
		},
	},
	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting titan update app")
		schedulerURL := cctx.String("update-server")
		schedulerAPI, close, err := newSchedulerAPI(cctx, schedulerURL)
		if err != nil {
			return err
		}
		defer close()

		for {
			checkUpdate(cctx, schedulerAPI)

			time.Sleep(60 * time.Second)
		}
	},
}

func newSchedulerAPI(cctx *cli.Context, schedulerURL string) (api.Scheduler, jsonrpc.ClientCloser, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 15*time.Second)
	defer cancel()

	schedulerAPI, closer, err := client.NewScheduler(ctx, schedulerURL, nil)
	if err != nil {
		return nil, nil, err
	}
	return schedulerAPI, closer, nil
}

func checkUpdate(cctx *cli.Context, schedulerAPI api.Scheduler) {
	updateInfos, err := edgeUpdateInfos(schedulerAPI)
	if err != nil {
		log.Errorf("EdgeUpdateInfo error:%s", err.Error())
	}

	// log.Infof("checkUpdate, updateInfos:%v", updateInfos)
	mySelfUpdateInfo, ok := updateInfos[int(types.NodeUpdater)]
	if ok && isNeedToUpdateMySelf(cctx, mySelfUpdateInfo) {
		updateApp(cctx, mySelfUpdateInfo)
	}

	edgeUpdateInfo, ok := updateInfos[int(types.NodeEdge)]
	if ok && isNeedToUpdateEdge(cctx, edgeUpdateInfo) {
		updateApp(cctx, edgeUpdateInfo)
	}
}

func edgeUpdateInfos(schedulerAPI api.Scheduler) (map[int]*api.EdgeUpdateConfig, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 15*time.Second)
	defer cancel()

	return schedulerAPI.GetEdgeUpdateConfigs(ctx)
}

func getLocalEdgeVersion(appPath string) (api.Version, error) {
	cmd := exec.Command(appPath, "-v")
	stdout, err := cmd.Output()
	if err != nil {
		return api.Version(0), err
	}

	output := strings.TrimSpace(string(stdout))

	// stdout="titan-edge version 0.1.0"
	stringSplit := strings.Split(output, " ")
	if len(stringSplit) != 3 {
		return api.Version(0), fmt.Errorf("parse cmd output error")
	}

	stringSplit = strings.Split(stringSplit[2], "+")

	return newVersion(stringSplit[0])
}

func isNeedToUpdateMySelf(cctx *cli.Context, updateInfo *api.EdgeUpdateConfig) bool {
	version, err := newVersion(cctx.App.Version)
	if err != nil {
		log.Errorf("newVersion error:%s", err.Error())
		return false
	}

	if updateInfo.Version > version {
		return true
	}
	return false
}

func isNeedToUpdateEdge(cctx *cli.Context, updateInfo *api.EdgeUpdateConfig) bool {
	installPath := cctx.String("install-path")
	appPath := path.Join(installPath, updateInfo.AppName)

	version, err := getLocalEdgeVersion(appPath)
	if err != nil {
		log.Errorf("isNeedToUpdateEdge error:%s", err)
		return false
	}

	if updateInfo.Version > version {
		return true
	}
	return false
}

func updateApp(cctx *cli.Context, updateInfo *api.EdgeUpdateConfig) {
	log.Debugf("updateApp, AppName:%s, Hash:%s, newVersion:%s, downloadURL:%s", updateInfo.AppName, updateInfo.Hash, updateInfo.Version.String(), updateInfo.DownloadURL)
	installPath := cctx.String("install-path")
	data, err := downloadApp(updateInfo.DownloadURL)
	if err != nil {
		log.Errorf("download app error:%s", err)
		return
	}

	// check file hash
	h := sha256.New()
	h.Write(data)
	hash := hex.EncodeToString(h.Sum(nil))

	if updateInfo.Hash != hash {
		log.Errorf("download file incomplete, file hash %s != %s", hash, updateInfo.Hash)
		return
	}

	fileName := updateInfo.AppName + "_tmp"
	tmpFilePath := filepath.Join(installPath, fileName)

	err = os.WriteFile(tmpFilePath, data, 0o644)
	if err != nil {
		log.Errorf("save app error:%s", err)
		return
	}

	filePath := filepath.Join(installPath, updateInfo.AppName)
	err = os.Remove(filePath)
	if err != nil {
		log.Errorf("remove old app error:%s", err)
		return
	}

	err = os.Rename(tmpFilePath, filePath)
	if err != nil {
		log.Errorf("remove old app error:%s", err)
		return
	}

	// change permission
	err = changePermission(filePath)
	if err != nil {
		log.Errorf("changePermission failed:%s", err.Error())
		return
	}

	// pkill app will restart
	err = killApp(updateInfo.AppName)
	if err != nil {
		log.Errorf("killApp failed:%s", err.Error())
		return
	}
}

func changePermission(appPath string) error {
	cmd := exec.Command("chmod", "+x", appPath)
	_, err := cmd.Output()
	if err != nil {
		return err
	}

	return nil
}

func downloadApp(downloadURL string) ([]byte, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest("GET", downloadURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close() //nolint:errcheck // ignore error

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func newVersion(version string) (api.Version, error) {
	stringSplit := strings.Split(version, ".")
	if len(stringSplit) != 3 {
		return api.Version(0), fmt.Errorf("parse version error")
	}

	// major, minor, patch
	major, err := strconv.Atoi(stringSplit[0])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version major %s error:%s", stringSplit[0], err)
	}

	minor, err := strconv.Atoi(stringSplit[1])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version minor %s error:%s", stringSplit[1], err)
	}

	patch, err := strconv.Atoi(stringSplit[2])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version patch %s error:%s", stringSplit[2], err)
	}

	return api.Version(uint32(major)<<16 | uint32(minor)<<8 | uint32(patch)), nil
}
