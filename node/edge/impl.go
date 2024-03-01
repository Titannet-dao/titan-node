package edge

import (
	"context"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/asset"
	"github.com/Filecoin-Titan/titan/node/common"
	"github.com/Filecoin-Titan/titan/node/device"
	datasync "github.com/Filecoin-Titan/titan/node/sync"
	validate "github.com/Filecoin-Titan/titan/node/validation"
	"github.com/filecoin-project/go-jsonrpc"
	logging "github.com/ipfs/go-log/v2"
	"github.com/quic-go/quic-go"
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

	Transport    *quic.Transport
	SchedulerAPI api.Scheduler
}

// WaitQuiet waits for the edge device to become idle.
func (edge *Edge) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}

// ExternalServiceAddress returns the external service address of the scheduler.
func (edge *Edge) ExternalServiceAddress(ctx context.Context, candidateURL string) (string, error) {
	httpClient, err := client.NewHTTP3ClientWithPacketConn(edge.Transport)
	if err != nil {
		return "", err
	}

	candidateAPI, closer, err := client.NewCandidate(ctx, candidateURL, nil, jsonrpc.WithHTTPClient(httpClient))
	if err != nil {
		return "", err
	}
	defer closer()

	return candidateAPI.GetExternalAddress(ctx)
}

// UserNATPunch checks network connectivity from the edge device to the specified URL.
func (edge *Edge) UserNATPunch(ctx context.Context, sourceURL string, req *types.NatPunchReq) error {
	return edge.checkNetworkConnectivity(sourceURL, req.Timeout)
}

// checkNetworkConnectivity uses HTTP/3 to check network connectivity to a target URL.
func (edge *Edge) checkNetworkConnectivity(targetURL string, timeout time.Duration) error {
	httpClient, err := client.NewHTTP3ClientWithPacketConn(edge.Transport)
	if err != nil {
		return xerrors.Errorf("new http3 client %w", err)
	}
	if timeout != 0 {
		httpClient.Timeout = timeout
	}

	resp, err := httpClient.Get(targetURL)
	if err != nil {
		return xerrors.Errorf("http3 client get error: %w, url: %s", err, targetURL)
	}
	defer resp.Body.Close()

	return nil
}
