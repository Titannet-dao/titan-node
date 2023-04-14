package api

import (
	"context"
	"fmt"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/journal/alerting"

	"github.com/google/uuid"

	"github.com/filecoin-project/go-jsonrpc/auth"
)

//                       MODIFYING THE API INTERFACE
//
// When adding / changing methods in this file:
// * Do the change here
// * Adjust implementation in `node/impl/`
// * Run `make gen` - this will:
//  * Generate proxy structs
//  * Generate mocks
//  * Generate markdown docs
//  * Generate openrpc blobs

// Common is an interface for titan network
type Common interface {
	// MethodGroup: Auth

	// AuthVerify checks whether the specified token is valid and returns the list of permissions associated with it.
	AuthVerify(ctx context.Context, token string) ([]auth.Permission, error) //perm:read
	// AuthNew creates a new token with the specified list of permissions.
	AuthNew(ctx context.Context, perms []auth.Permission) (string, error) //perm:admin

	// MethodGroup: Log

	// LogList returns a list of all logs in the system.
	LogList(context.Context) ([]string, error) //perm:write
	// LogSetLevel sets the log level of the specified logger.
	LogSetLevel(context.Context, string, string) error //perm:write
	// LogAlerts returns list of all, active and inactive alerts tracked by the
	LogAlerts(ctx context.Context) ([]alerting.Alert, error) //perm:admin

	// MethodGroup: Common

	// Version provides information about API provider
	Version(context.Context) (APIVersion, error) //perm:read
	// Discover returns an OpenRPC document describing an RPC API.
	Discover(ctx context.Context) (types.OpenRPCDocument, error) //perm:read
	// Shutdown trigger graceful shutdown
	Shutdown(context.Context) error //perm:admin
	// Session returns a UUID of api provider session
	Session(ctx context.Context) (uuid.UUID, error) //perm:write

	// Closing jsonrpc closing
	Closing(context.Context) (<-chan struct{}, error) //perm:read
}

// APIVersion provides various build-time information
type APIVersion struct {
	Version string

	// APIVersion is a binary encoded semver version of the remote implementing
	// this api
	//
	// See APIVersion in build/version.go
	APIVersion Version

	// TODO: git commit / os / genesis cid?

	// Seconds
	BlockDelay uint64
}

func (v APIVersion) String() string {
	return fmt.Sprintf("%s+api%s", v.Version, v.APIVersion.String())
}

type LogFile struct {
	Name string
	Size int64
}
