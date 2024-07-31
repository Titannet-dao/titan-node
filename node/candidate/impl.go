package candidate

import (
	"context"
	"fmt"
	"github.com/Filecoin-Titan/titan/node/container"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/asset"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/handler"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/node/common"
	"github.com/Filecoin-Titan/titan/node/device"
	datasync "github.com/Filecoin-Titan/titan/node/sync"

	vd "github.com/Filecoin-Titan/titan/node/validation"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("candidate")

const (
	validateTimeout          = 3
	tcpPackMaxLength         = 52428800
	connectivityCheckTimeout = 3
)

var http3Client *http.Client

// Candidate represents the c node.
type Candidate struct {
	fx.In

	*common.CommonAPI
	*asset.Asset
	*device.Device
	*vd.Validation
	*datasync.DataSync
	*container.Client
	Scheduler api.Scheduler
	Config    *config.CandidateCfg
	TCPSrv    *TCPServer
}

// WaitQuiet does nothing and returns nil error.
func (c *Candidate) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}

// GetBlocksWithAssetCID returns a map of blocks with given asset CID, random seed, and random count.
func (c *Candidate) GetBlocksWithAssetCID(ctx context.Context, assetCID string, randomSeed int64, randomCount int) ([]string, error) {
	return c.Asset.GetBlocksOfAsset(assetCID, randomSeed, randomCount)
}

// GetExternalAddress retrieves the external address of the caller.
func (c *Candidate) GetExternalAddress(ctx context.Context) (string, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	return remoteAddr, nil
}

// checkNetworkConnectivity check tcp or udp network connectivity
// network is "tcp" or "udp"
func (c *Candidate) CheckNetworkConnectivity(ctx context.Context, network, targetURL string) error {
	switch network {
	case "tcp":
		ok, err := c.verifyTCPConnectivity(targetURL)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("tcp can not connect to %s", targetURL)
		}
		return nil
	case "udp":
		ok, err := c.verifyUDPConnectivity(targetURL)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("tcp can not connect to %s", targetURL)
		}
		return nil
	}

	return fmt.Errorf("unknow network %s type", network)
}

func (c *Candidate) CheckNetworkConnectable(ctx context.Context, network, targetURL string) (bool, error) {
	switch network {
	case "tcp":
		return c.verifyTCPConnectivity(targetURL)
	case "udp":
		return c.verifyUDPConnectivity(targetURL)
	}

	return false, fmt.Errorf("unknow network %s type", network)
}

func (c *Candidate) GetMinioConfig(ctx context.Context) (*types.MinioConfig, error) {
	return &types.MinioConfig{
		Endpoint:        c.Config.MinioConfig.Endpoint,
		AccessKeyID:     c.Config.MinioConfig.AccessKeyID,
		SecretAccessKey: c.Config.MinioConfig.SecretAccessKey,
	}, nil
}

func (c *Candidate) verifyTCPConnectivity(targetURL string) (bool, error) {
	url, err := url.ParseRequestURI(targetURL)
	if err != nil {
		return false, xerrors.Errorf("parse uri error: %s, url: %s", err.Error(), targetURL)
	}

	conn, err := net.DialTimeout("tcp", url.Host, connectivityCheckTimeout*time.Second)
	if err != nil {
		log.Infof("verifyTCPConnectivity, dial tcp %s, addr %s", err.Error(), url.Host)
		return false, nil
	}
	defer conn.Close()

	return true, nil
}

func (c *Candidate) verifyUDPConnectivity(targetURL string) (bool, error) {
	if http3Client == nil {
		http3Client = client.NewHTTP3Client()
		http3Client.Timeout = connectivityCheckTimeout * time.Second
	}

	resp, err := http3Client.Get(targetURL)
	if err != nil {
		log.Infof("verifyUDPConnectivity, http3 client get error: %s, url: %s", err.Error(), targetURL)
		return false, nil
	}
	defer resp.Body.Close()

	return true, nil
}

func (c *Candidate) DeactivateNode(ctx context.Context) error {
	return c.Scheduler.DeactivateNode(ctx, "", 1)
}

func (c *Candidate) CalculateExitProfit(ctx context.Context) (types.ExitProfitRsp, error) {
	return c.Scheduler.CalculateExitProfit(ctx, "")
}
