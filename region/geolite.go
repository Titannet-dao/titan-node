package region

import (
	"fmt"
	"net"
	"strings"

	"github.com/oschwald/geoip2-golang"
	"golang.org/x/xerrors"
)

// var reader *geoip2.Reader

// TypeGeoLite returns the name of the GeoLite type
func TypeGeoLite() string {
	return "GeoLite"
}

// InitGeoLite initializes a new GeoLiteRegion using the given database path
func InitGeoLite(dbPath string) (Region, error) {
	gl := &geoLite{dbPath}

	db, err := geoip2.Open(gl.dbPath)
	if err != nil {
		return gl, err
	}
	defer func() {
		err = db.Close()
		if err != nil {
			log.Errorf("geo close db err:%s", err.Error())
		}
	}()
	// reader = db

	return gl, nil
}

type geoLite struct {
	dbPath string
}

func (g geoLite) GetGeoInfoFromAreaID(areaID string) (*GeoInfo, error) {
	if areaID == "" {
		return nil, xerrors.New("areaID is nil")
	}

	areaID = strings.Replace(areaID, " ", "", -1)
	geoInfo := &GeoInfo{
		Latitude:  0,
		Longitude: 0,
		Geo:       areaID,

		Continent: unknown,
		Country:   unknown,
		Province:  unknown,
		City:      unknown,
	}

	continent, country, province, city := DecodeAreaID(areaID)

	geoInfo.Continent = strings.ToLower(strings.Replace(continent, " ", "", -1))
	geoInfo.Country = strings.ToLower(strings.Replace(country, " ", "", -1))
	geoInfo.Province = strings.ToLower(strings.Replace(province, " ", "", -1))
	geoInfo.City = strings.ToLower(strings.Replace(city, " ", "", -1))

	return geoInfo, nil
}

// GetGeoInfo retrieves the geographic information of the given IP address using the GeoLite database
func (g geoLite) GetGeoInfo(ip string) (*GeoInfo, error) {
	geoInfo := DefaultGeoInfo(ip)
	if ip == "" {
		return geoInfo, xerrors.New("ip is nil")
	}

	db, err := geoip2.Open(g.dbPath)
	if err != nil {
		return geoInfo, err
	}
	defer func() {
		err = db.Close()
		if err != nil {
			log.Errorf("geo close db err:%s", err.Error())
		}
	}()

	// If you are using strings that may be invalid, check that ip is not nil
	ipA := net.ParseIP(ip)
	record, err := db.City(ipA)
	if err != nil {
		return geoInfo, err
	}

	if record.Country.IsoCode == "" {
		return geoInfo, err
	}

	continent := unknown
	country := unknown
	city := unknown
	province := unknown

	// geoInfo.IsoCode = record.Country.IsoCode
	if record.Continent.Names["en"] != "" {
		continent = record.Continent.Names["en"]
	}

	if record.Country.Names["en"] != "" {
		country = record.Country.Names["en"]
	}

	if record.City.Names["en"] != "" {
		city = record.City.Names["en"]
	}

	if len(record.Subdivisions) > 0 {
		province = record.Subdivisions[0].Names["en"]
	}

	geoInfo.Latitude = record.Location.Latitude
	geoInfo.Longitude = record.Location.Longitude

	// geoInfo.Geo = strings.Replace(geoInfo.Geo, " ", "", -1)

	geoInfo.Continent = strings.ToLower(strings.Replace(continent, " ", "", -1))
	geoInfo.Country = strings.ToLower(strings.Replace(country, " ", "", -1))
	geoInfo.Province = strings.ToLower(strings.Replace(province, " ", "", -1))
	geoInfo.City = strings.ToLower(strings.Replace(city, " ", "", -1))

	geoInfo.Geo = fmt.Sprintf("%s%s%s%s%s%s%s", geoInfo.Continent, separate, geoInfo.Country, separate, geoInfo.Province, separate, geoInfo.City)

	return geoInfo, nil
}
