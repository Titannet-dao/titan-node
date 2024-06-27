package region

import (
	"testing"
)

func TestXxx(t *testing.T) {
	region, err := InitGeoLite("../cmd/titan-scheduler/city.mmdb")
	if err != nil {
		t.Errorf(" InitGeoLite: %s", err.Error())
		return
	}

	info, err := region.GetGeoInfo("142.2.87.2")
	if err != nil {
		t.Errorf(" GetGeoInfo: %s", err.Error())
		return
	}

	t.Logf("info: %s", info.Geo)
}
