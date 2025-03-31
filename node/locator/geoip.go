package locator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/golang/geo/s2"
)

const unknown = "unknown"

func getLatLngOfIP(ip string, rg region.Region) (float64, float64, error) {
	geoInfo, err := rg.GetGeoInfo(ip)
	if err != nil {
		return 0, 0, err
	}
	return geoInfo.Latitude, geoInfo.Longitude, nil
}

func calculateDistance(lat1, lon1, lat2, lon2 float64) float64 {
	p1 := s2.PointFromLatLng(s2.LatLngFromDegrees(lat1, lon1))
	p2 := s2.PointFromLatLng(s2.LatLngFromDegrees(lat2, lon2))

	distance := s2.ChordAngleBetweenPoints(p1, p2).Angle().Radians()

	distanceKm := distance * 6371.0

	return distanceKm
}

// ipList can not emtpy
func getUserNearestIP(userIP string, ipList []string, reg region.Region) string {
	if len(ipList) == 0 {
		panic("ipList can not empty")
	}

	if len(ipList) == 1 {
		return ipList[0]
	}

	userGeoInfo, err := reg.GetGeoInfo(userIP)
	if err != nil {
		return ipList[0]
	}

	geoInfos := getGeoInfos(ipList, reg)
	if len(geoInfos) == 0 {
		return ipList[0]
	}

	geoInfos = getNearestGeoInfos(userGeoInfo, geoInfos)

	ipDistanceMap := make(map[string]float64)
	for _, geoInfo := range geoInfos {
		distance := calculateDistance(userGeoInfo.Latitude, userGeoInfo.Longitude, geoInfo.Latitude, geoInfo.Longitude)
		ipDistanceMap[geoInfo.IP] = distance
		fmt.Printf("distance %f ip %#v\n", distance, *geoInfo)
	}

	minDistance := math.MaxFloat64
	var nearestIP string
	for ip, distance := range ipDistanceMap {
		if distance < minDistance {
			minDistance = distance
			nearestIP = ip
		}
	}

	return nearestIP
}

func getGeoInfos(ipList []string, reg region.Region) []*region.GeoInfo {
	geoInfos := make([]*region.GeoInfo, 0, len(ipList))
	for _, ip := range ipList {
		geoInfo, err := reg.GetGeoInfo(ip)
		if err != nil {
			log.Errorf("GetGeoInfo %s, ip %s", err.Error(), ip)
			continue
		}

		if geoInfo.Latitude == 0 && geoInfo.Longitude == 0 {
			log.Errorf("Invalid GeoInfo %#v, ip %s", *geoInfo, ip)
			continue
		}

		geoInfos = append(geoInfos, geoInfo)
	}
	return geoInfos
}

func getNearestGeoInfos(userGeoInfo *region.GeoInfo, geoInfos []*region.GeoInfo) []*region.GeoInfo {
	sameCity := make([]*region.GeoInfo, 0)
	sameProvince := make([]*region.GeoInfo, 0)
	sameCountry := make([]*region.GeoInfo, 0)

	for _, geoInfo := range geoInfos {
		if strings.ToLower(geoInfo.Country) != strings.ToLower(userGeoInfo.Country) {
			continue
		}

		// Distinguish between Mainland China and Hong Kong
		if userGeoInfo.Country == "china" && userGeoInfo.City != "hongkong" && geoInfo.City == "hongkong" {
			continue
		}

		if strings.ToLower(userGeoInfo.Province) == strings.ToLower(geoInfo.Province) &&
			strings.ToLower(userGeoInfo.City) == strings.ToLower(geoInfo.City) {
			sameCity = append(sameCity, geoInfo)
			continue
		}

		if strings.ToLower(userGeoInfo.Province) == strings.ToLower(geoInfo.Province) {
			sameProvince = append(sameProvince, geoInfo)
			continue
		}

		sameCountry = append(sameCountry, geoInfo)
	}

	if len(sameCity) > 0 {
		return sameCity
	}

	if len(sameProvince) > 0 {
		return sameProvince
	}

	if len(sameCountry) > 0 {
		return sameCountry
	}

	return geoInfos
}

type titanRegion struct {
	webAPI string
}

func NewRegion(config *config.LocatorCfg) region.Region {
	return &titanRegion{webAPI: config.WebGeoAPI}
}

func (tr *titanRegion) GetGeoInfo(ip string) (*region.GeoInfo, error) {
	if len(ip) == 0 {
		return nil, fmt.Errorf("ip can not empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s?ip=%s", tr.webAPI, ip)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()

	if rsp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(rsp.Body)
		return nil, fmt.Errorf("status code %d, error %s", rsp.StatusCode, string(buf))
	}

	type Data struct {
		Latitude  string `json:"latitude"`
		Longitude string `json:"longitude"`
		IP        string `json:"ip"`
		Geo       string `json:""`

		Continent string `json:"continent"`
		Country   string `json:"country"`
		Province  string `json:"province"`
		City      string `json:"city"`
	}

	result := struct {
		Code int    `json:"code"`
		Err  int    `json:"err"`
		Msg  string `json:"msg"`
		Data *Data  `json:"data"`
	}{}

	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	if result.Code != 0 {
		return nil, fmt.Errorf("code: %d err: %d msg: %s", result.Code, result.Err, result.Msg)
	}

	if result.Data == nil {
		return nil, fmt.Errorf("can not get geo info for %s", ip)
	}

	latitude, _ := strconv.ParseFloat(result.Data.Latitude, 64)
	longitude, _ := strconv.ParseFloat(result.Data.Longitude, 64)
	geoInfo := &region.GeoInfo{
		Latitude:  latitude,
		Longitude: longitude,
		IP:        result.Data.IP,

		Continent: unknown,
		Country:   unknown,
		Province:  unknown,
		City:      unknown,
	}

	if len(result.Data.Continent) != 0 {
		geoInfo.Continent = strings.ToLower(strings.Replace(result.Data.Continent, " ", "", -1))
	}

	if len(result.Data.Country) != 0 {
		geoInfo.Country = strings.ToLower(strings.Replace(result.Data.Country, " ", "", -1))
	}

	if len(result.Data.Province) != 0 {
		geoInfo.Province = strings.ToLower(strings.Replace(result.Data.Province, " ", "", -1))
	}

	if len(result.Data.City) != 0 {
		geoInfo.City = strings.ToLower(strings.Replace(result.Data.City, " ", "", -1))
	}

	geoInfo.Geo = fmt.Sprintf("%s-%s-%s-%s", geoInfo.Continent, geoInfo.Country, geoInfo.Province, geoInfo.City)
	return geoInfo, nil
}

func (tr *titanRegion) GetGeoInfoFromAreaID(areaID string) (*region.GeoInfo, error) {
	if areaID == "" {
		return nil, fmt.Errorf("areaID is nil")
	}

	areaID = strings.Replace(areaID, " ", "", -1)
	geoInfo := &region.GeoInfo{
		Latitude:  0,
		Longitude: 0,
		Geo:       areaID,

		Continent: unknown,
		Country:   unknown,
		Province:  unknown,
		City:      unknown,
	}

	continent, country, province, city := region.DecodeAreaID(areaID)

	geoInfo.Continent = strings.ToLower(strings.Replace(continent, " ", "", -1))
	geoInfo.Country = strings.ToLower(strings.Replace(country, " ", "", -1))
	geoInfo.Province = strings.ToLower(strings.Replace(province, " ", "", -1))
	geoInfo.City = strings.ToLower(strings.Replace(city, " ", "", -1))

	return geoInfo, nil
}
