package common

import (
	"context"
	"fmt"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/build"
	"github.com/Filecoin-Titan/titan/journal/alerting"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/repo"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/quic-go/quic-go"

	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var session = uuid.New()

// CommonAPI api o
type CommonAPI struct {
	Alerting     *alerting.Alerting
	APISecret    *jwt.HMACSHA
	ShutdownChan dtypes.ShutdownChan
	Transport    *quic.Transport
}

// SessionCallbackFunc will be called after node connection
type SessionCallbackFunc func(string, string)

// MethodGroup: Auth

// NewCommonAPI initializes a new CommonAPI
func NewCommonAPI(lr repo.LockedRepo, secret *jwt.HMACSHA, shutdownChan dtypes.ShutdownChan, Transport *quic.Transport) (CommonAPI, error) {
	commAPI := CommonAPI{
		APISecret:    secret,
		ShutdownChan: shutdownChan,
		Transport:    Transport,	
	}

	return commAPI, nil
}

// AuthVerify verifies a JWT token and returns the permissions associated with it
func (a *CommonAPI) AuthVerify(ctx context.Context, token string) (*types.JWTPayload, error) {
	var payload types.JWTPayload
	if _, err := jwt.Verify([]byte(token), a.APISecret, &payload); err != nil {
		return nil, xerrors.Errorf("JWT Verification failed: %w", err)
	}

	// replace ID to NodeID
	if len(payload.NodeID) > 0 {
		payload.ID = payload.NodeID
	}
	return &payload, nil
}

// AuthNew generates a new JWT token with the provided permissions
func (a *CommonAPI) AuthNew(ctx context.Context, payload *types.JWTPayload) (string, error) {
	tk, err := jwt.Sign(&payload, a.APISecret)
	if err != nil {
		return "", err
	}

	return string(tk), nil
}

// LogList returns a list of available logging subsystems
func (a *CommonAPI) LogList(context.Context) ([]string, error) {
	return logging.GetSubsystems(), nil
}

// LogSetLevel sets the log level for a given subsystem
func (a *CommonAPI) LogSetLevel(ctx context.Context, subsystem, level string) error {
	return logging.SetLogLevel(subsystem, level)
}

// LogAlerts returns an empty list of alerts
func (a *CommonAPI) LogAlerts(ctx context.Context) ([]alerting.Alert, error) {
	return []alerting.Alert{}, nil
}

// Version provides information about API provider
func (a *CommonAPI) Version(context.Context) (api.APIVersion, error) {
	v, err := api.VersionForType(types.RunningNodeType)
	if err != nil {
		return api.APIVersion{}, err
	}

	return api.APIVersion{
		Version:    build.UserVersion(),
		APIVersion: v,
	}, nil
}

// Discover returns an OpenRPC document describing an RPC API.
func (a *CommonAPI) Discover(ctx context.Context) (types.OpenRPCDocument, error) {
	return nil, fmt.Errorf("not implement")
}

// Shutdown trigger graceful shutdown
func (a *CommonAPI) Shutdown(context.Context) error {
	a.ShutdownChan <- struct{}{}
	return nil
}

// // Restart trigger graceful restart
// func (a *CommonAPI) Restart(ctx context.Context) error {
// 	a.RestartChan <- struct{}{}
// 	return nil
// }

// Session returns a UUID of api provider session
func (a *CommonAPI) Session(ctx context.Context) (uuid.UUID, error) {
	return session, nil
}

// Closing jsonrpc closing
func (a *CommonAPI) Closing(context.Context) (<-chan struct{}, error) {
	return make(chan struct{}), nil // relies on jsonrpc closing
}

// ExternalServiceAddress returns the external service address of the scheduler.
func (a *CommonAPI) ExternalServiceAddress(ctx context.Context, candidateURL string) (string, error) {
	if types.RunningNodeType == types.NodeScheduler || types.RunningNodeType == types.NodeLocator {
		return "", fmt.Errorf("not implement")
	}

	httpClient, err := client.NewHTTP3ClientWithPacketConn(a.Transport)
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
