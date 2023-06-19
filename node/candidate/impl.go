package candidate

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/Filecoin-Titan/titan/node/asset"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/handler"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/node/common"
	"github.com/Filecoin-Titan/titan/node/device"
	datasync "github.com/Filecoin-Titan/titan/node/sync"

	cliutil "github.com/Filecoin-Titan/titan/cli/util"
	vd "github.com/Filecoin-Titan/titan/node/validation"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("candidate")

const (
	validateTimeout          = 3
	tcpPackMaxLength         = 52428800
	connectivityCheckTimeout = 3
)

// Candidate represents the c node.
type Candidate struct {
	fx.In

	*common.CommonAPI
	*asset.Asset
	*device.Device
	*vd.Validation
	*datasync.DataSync

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
		return c.verifyTCPConnectivity(targetURL)
	case "udp":
		return c.verifyUDPConnectivity(targetURL)
	}

	return fmt.Errorf("unknow network %s type", network)
}

func (c *Candidate) verifyTCPConnectivity(targetURL string) error {
	url, err := url.ParseRequestURI(targetURL)
	if err != nil {
		return xerrors.Errorf("parse uri error: %w, url: %s", err, targetURL)
	}

	conn, err := net.DialTimeout("tcp", url.Host, connectivityCheckTimeout*time.Second)
	if err != nil {
		return xerrors.Errorf("dial tcp %w, addr %s", err, url.Host)
	}
	defer conn.Close()

	return nil
}

func (c *Candidate) verifyUDPConnectivity(targetURL string) error {
	udpPacketConn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return xerrors.Errorf("list udp %w, url %s", err, targetURL)
	}
	defer func() {
		err = udpPacketConn.Close()
		if err != nil {
			log.Errorf("udpPacketConn Close err:%s", err.Error())
		}
	}()

	httpClient, err := cliutil.NewHTTP3Client(udpPacketConn, true, "")
	if err != nil {
		return xerrors.Errorf("new http3 client %w", err)
	}
	httpClient.Timeout = connectivityCheckTimeout * time.Second

	resp, err := httpClient.Get(targetURL)
	if err != nil {
		return xerrors.Errorf("http3 client get error: %w, url: %s", err, targetURL)
	}
	defer resp.Body.Close()

	return nil
}
