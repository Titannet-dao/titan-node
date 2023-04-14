package api

import (
	"context"

	"github.com/Filecoin-Titan/titan/api/types"
)

// Device is an interface for node
type Device interface {
	GetNodeInfo(ctx context.Context) (types.NodeInfo, error) //perm:read
	GetNodeID(ctx context.Context) (string, error)           //perm:read
}
