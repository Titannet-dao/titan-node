package common

import (
	"context"
	"fmt"
	"os"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/build"
	"github.com/Filecoin-Titan/titan/journal/alerting"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/repo"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var session = uuid.New()

// CommonAPI api o
type CommonAPI struct {
	Alerting        *alerting.Alerting
	APISecret       *jwt.HMACSHA
	ShutdownChan    chan struct{}
	SessionCallBack dtypes.SessionCallbackFunc
}

type jwtPayload struct {
	Allow []auth.Permission
}

type (
	// PermissionWriteToken A token with write permission
	PermissionWriteToken []byte
	// PermissionAdminToken A token admin write permission
	PermissionAdminToken []byte
)

// SessionCallbackFunc will be called after node connection
type SessionCallbackFunc func(string, string)

// MethodGroup: Auth

// NewCommonAPI initializes a new CommonAPI
func NewCommonAPI(lr repo.LockedRepo, secret *jwt.HMACSHA, callback dtypes.SessionCallbackFunc) (CommonAPI, error) {
	commAPI := CommonAPI{
		APISecret:       secret,
		SessionCallBack: callback,
	}

	return commAPI, nil
}

// AuthVerify verifies a JWT token and returns the permissions associated with it
func (a *CommonAPI) AuthVerify(ctx context.Context, token string) ([]auth.Permission, error) {
	var payload jwtPayload
	if _, err := jwt.Verify([]byte(token), a.APISecret, &payload); err != nil {
		return nil, xerrors.Errorf("JWT Verification failed: %w", err)
	}

	return payload.Allow, nil
}

// AuthNew generates a new JWT token with the provided permissions
func (a *CommonAPI) AuthNew(ctx context.Context, perms []auth.Permission) (string, error) {
	p := jwtPayload{
		Allow: perms, // TODO: consider checking validity
	}

	tk, err := jwt.Sign(&p, a.APISecret)
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

// Session returns a UUID of api provider session
func (a *CommonAPI) Session(ctx context.Context) (uuid.UUID, error) {
	if a.SessionCallBack != nil {
		remoteAddr := handler.GetRemoteAddr(ctx)
		nodeID := handler.GetNodeID(ctx)
		if nodeID != "" && remoteAddr != "" {
			err := a.SessionCallBack(nodeID, remoteAddr)
			if err != nil {
				return session, err
			}
		}
	}

	return session, nil
}

// Closing jsonrpc closing
func (a *CommonAPI) Closing(context.Context) (<-chan struct{}, error) {
	return make(chan struct{}), nil // relies on jsonrpc closing
}

// ShowLogFile returns the summary information of the log file
func (a *CommonAPI) ShowLogFile(ctx context.Context) (*api.LogFile, error) {
	logFilePath := os.Getenv("GOLOG_FILE")
	if logFilePath == "" {
		return nil, fmt.Errorf("GOLOG_FILE not config, example: export GOLOG_FILE=/path/log")
	}
	info, err := os.Stat(logFilePath)
	if err != nil {
		return nil, err
	}

	return &api.LogFile{Name: info.Name(), Size: info.Size()}, nil
}

// DownloadLogFile return log file
func (a *CommonAPI) DownloadLogFile(ctx context.Context) ([]byte, error) {
	logFilePath := os.Getenv("GOLOG_FILE")
	if logFilePath == "" {
		return nil, fmt.Errorf("GOLOG_FILE not config, example: export GOLOG_FILE=/path/log")
	}
	return os.ReadFile(logFilePath)
}

// DeleteLogFile delete log file
func (a *CommonAPI) DeleteLogFile(ctx context.Context) error {
	logFilePath := os.Getenv("GOLOG_FILE")
	if logFilePath == "" {
		return nil
	}

	return os.WriteFile(logFilePath, []byte(""), 0o755)
}
