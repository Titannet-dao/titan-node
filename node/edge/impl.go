package edge

import (
	"context"
	"net"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	cliutil "github.com/Filecoin-Titan/titan/cli/util"
	"github.com/Filecoin-Titan/titan/node/asset"
	"github.com/Filecoin-Titan/titan/node/common"
	"github.com/Filecoin-Titan/titan/node/device"
	datasync "github.com/Filecoin-Titan/titan/node/sync"
	validate "github.com/Filecoin-Titan/titan/node/validation"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

var log = logging.Logger("edge")

// Edge edge node
type Edge struct {
	fx.In

	*common.CommonAPI
	*device.Device
	*asset.Asset
	*validate.Validation
	*datasync.DataSync

	PConn        net.PacketConn
	SchedulerAPI api.Scheduler
}

// WaitQuiet waits for the edge device to become idle.
func (edge *Edge) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}

// ExternalServiceAddress returns the external service address of the scheduler.
func (edge *Edge) ExternalServiceAddress(ctx context.Context, schedulerURL string) (string, error) {
	schedulerAPI, closer, err := client.NewScheduler(ctx, schedulerURL, nil)
	if err != nil {
		return "", err
	}
	defer closer()

	return schedulerAPI.GetExternalAddress(ctx)
}

// UserNATPunch checks network connectivity from the edge device to the specified URL.
func (edge *Edge) UserNATPunch(ctx context.Context, sourceURL string, req *types.NatPunchReq) error {
	return edge.checkNetworkConnectivity(sourceURL, req.Timeout)
}

// checkNetworkConnectivity uses HTTP/3 to check network connectivity to a target URL.
func (edge *Edge) checkNetworkConnectivity(targetURL string, timeout int) error {
	udpPacketConn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return xerrors.Errorf("list udp %w", err)
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
	httpClient.Timeout = time.Duration(timeout) * time.Second

	resp, err := httpClient.Get(targetURL)
	if err != nil {
		return xerrors.Errorf("http3 client get error: %w, url: %s", err, targetURL)
	}
	defer resp.Body.Close()

	return nil
}
