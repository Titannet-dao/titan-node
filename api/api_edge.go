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
	WaitQuiet(ctx context.Context) error //perm:admin
	// ExternalServiceAddress check service address with different candidate
	// if behind nat, service address maybe different
	ExternalServiceAddress(ctx context.Context, candidateURL string) (string, error) //perm:admin
	// UserNATTravel build connection for user
	UserNATPunch(ctx context.Context, userServiceAddress string, req *types.NatPunchReq) error //perm:admin
	// GetEdgeOnlineStateFromScheduler this online state is get from scheduler
	GetEdgeOnlineStateFromScheduler(ctx context.Context) (bool, error) //perm:default
}
