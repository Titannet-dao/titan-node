package api

import (
	"context"

	"github.com/Filecoin-Titan/titan/api/types"
)

// Edge is an interface for edge node
type Edge interface {
	Common
	Device
	Validation
	DataSync
	Asset
	Workerd
	WaitQuiet(ctx context.Context) error //perm:admin
	// UserNATTravel build connection for user
	UserNATPunch(ctx context.Context, userServiceAddress string, req *types.NatPunchReq) error //perm:admin
	// GetEdgeOnlineStateFromScheduler this online state is get from scheduler
	GetEdgeOnlineStateFromScheduler(ctx context.Context) (bool, error) //perm:default
	// Restart trigger graceful restart of edge node
	Restart(ctx context.Context) error // perm:admin

	// CreateTunnel create a tunnel for workerd communication
	CreateTunnel(ctx context.Context, req *types.CreateTunnelReq) error // perm:admin
}
