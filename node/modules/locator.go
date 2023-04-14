package modules

import (
	"github.com/Filecoin-Titan/titan/node/locator"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/region"
)

// NewRegion creates a new region instance using the given database path.
func NewRegion(dbPath dtypes.GeoDBPath) (region.Region, error) {
	return region.NewGeoLiteRegion(string(dbPath))
}

// NewLocatorStorage creates a locator storage using the give addresses
func NewLocatorStorage(addresses dtypes.EtcdAddresses) (locator.Storage, error) {
	return locator.NewEtcdClient(addresses)
}
