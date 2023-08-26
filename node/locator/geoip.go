package locator

import (
	"math"

	"github.com/Filecoin-Titan/titan/region"
	"github.com/golang/geo/s2"
)

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

func calculateTwoIPDistance(ip1, ip2 string, rg region.Region) (float64, error) {
	lat1, lon1, err := getLatLngOfIP(ip1, rg)
	if err != nil {
		return 0, err
	}

	lat2, lon2, err := getLatLngOfIP(ip2, rg)
	if err != nil {
		return 0, err
	}

	distance := calculateDistance(lat1, lon1, lat2, lon2)
	return distance, nil
}

func getUserNearestIP(userIP string, ipList []string, reg region.Region) string {
	ipDistanceMap := make(map[string]float64)
	for _, ip := range ipList {
		distance, err := calculateTwoIPDistance(userIP, ip, reg)
		if err != nil {
			log.Errorf("calculate tow ip distance error %s", err.Error())
			continue
		}
		ipDistanceMap[ip] = distance
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
