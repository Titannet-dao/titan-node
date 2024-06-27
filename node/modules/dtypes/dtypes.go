package dtypes

import (
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/ipfs/go-datastore"
)

// AssetMetadataDS stores metadata.
type AssetMetadataDS datastore.Batching

// ProjectMetadataDS stores metadata.
type ProjectMetadataDS datastore.Batching

// GeoDBPath the location of a geo database
type GeoDBPath string

// EtcdAddresses the locator to connect to the database
type EtcdAddresses []string

// PermissionWebToken token with admin permission
type PermissionWebToken string

// LocatorUUID the locator unique identifier
type LocatorUUID string

// NodeID candidate or edge unique identifier
type NodeID string

// InternalIP local network address
type InternalIP string

type (
	NodeMetadataPath string
	AssetsPaths      []string
	WorkerdPath      string
)

// ServerID server id
type ServerID string

// SetSchedulerConfigFunc is a function which is used to
// sets the scheduler config.
type SetSchedulerConfigFunc func(cfg config.SchedulerCfg) error

// GetSchedulerConfigFunc is a function which is used to
// get the sealing config.
type GetSchedulerConfigFunc func() (config.SchedulerCfg, error)

type ShutdownChan chan struct{}

type RestartChan chan struct{}

type RestartDoneChan chan struct{}
