package region

import (
	"fmt"
	"strings"

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
	GetGeoInfoFromAreaID(areaID string) (*GeoInfo, error)
}

// GeoInfo represents the geo information
type GeoInfo struct {
	Latitude  float64
	Longitude float64
	IP        string
	Geo       string

	Continent string
	Country   string
	Province  string
	City      string
}

var region Region

// NewRegion initializes the geo region using the specified database path, geo type, and default area
func NewRegion(dbPath, geoType, area string) error {
	var err error

	// defaultArea = area

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
		Geo:       fmt.Sprintf("%s%s%s%s%s%s%s", unknown, separate, unknown, separate, unknown, separate, unknown),

		Continent: unknown,
		Country:   unknown,
		Province:  unknown,
		City:      unknown,
	}
}

func DecodeAreaID(areaID string) (continent, country, province, city string) {
	parts := strings.Split(areaID, separate)

	size := len(parts)
	switch size {
	case 1:
		continent = parts[0]
	case 2:
		continent = parts[0]
		country = parts[1]
	case 3:
		continent = parts[0]
		country = parts[1]
		province = parts[2]
	case 4:
		continent = parts[0]
		country = parts[1]
		province = parts[2]
		city = parts[3]
	}

	return
}
