package region

import (
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("region")

const (
	unknown  = "unknown"
	separate = "-"
)

var defaultArea = ""

// Region represents a geo region interface
type Region interface {
	GetGeoInfo(ip string) (*GeoInfo, error)
}

// GeoInfo represents the geo information
type GeoInfo struct {
	Latitude  float64
	Longitude float64
	IP        string
	Geo       string
}

var region Region

// NewRegion initializes the geo region using the specified database path, geo type, and default area
func NewRegion(dbPath, geoType, area string) error {
	var err error

	defaultArea = area

	switch geoType {
	case TypeGeoLite():
		region, err = InitGeoLite(dbPath)
	default:
		// panic("unknown Region type")
		err = xerrors.New("unknown Region type")
	}

	return err
}

// GetRegion returns the current geo region
func GetRegion() Region {
	return region
}

// DefaultGeoInfo creates a default GeoInfo object with the specified IP and default area
func DefaultGeoInfo(ip string) *GeoInfo {
	return &GeoInfo{
		Latitude:  0,
		Longitude: 0,
		IP:        ip,
		Geo:       defaultArea,
	}
}
