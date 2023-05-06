package api

import (
	"context"

	"github.com/Filecoin-Titan/titan/api/types"
)

// Locator is an interface for locator services
type Locator interface {
	Common
	// GetAccessPoints retrieves all access points associated with a node.
	GetAccessPoints(ctx context.Context, nodeID, areaID string) ([]string, error) //perm:default
	// user api
	// EdgeDownloadInfos retrieves download information for a content identifier (CID).
	EdgeDownloadInfos(ctx context.Context, cid string) ([]*types.EdgeDownloadInfoList, error) //perm:default
	// GetUserAccessPoint retrieves an access point for a user with a specified IP address.
	GetUserAccessPoint(ctx context.Context, userIP string) (*AccessPoint, error) //perm:default
}

// AccessPoint represents an access point within an area, containing scheduler information.
type AccessPoint struct {
	AreaID        string
	SchedulerURLs []string
}
