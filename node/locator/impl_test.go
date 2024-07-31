package locator

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
)

func TestRandomSchedulerConfigWithWeight(t *testing.T) {
	t.Logf("TestRandomSchedulerConfigWithWeight")

	configs := make([]*types.SchedulerCfg, 0)
	configs = append(configs,
		&types.SchedulerCfg{
			SchedulerURL: "https://192.168.0.1:3456/rpc/v0",
			AreaID:       "Asia-China-Guangdong-Shenzhen",
			Weight:       100,
			AccessToken:  "11213213213213213213213132132",
		},
	)

	configs = append(configs,
		&types.SchedulerCfg{
			SchedulerURL: "https://192.168.0.2:3456/rpc/v0",
			AreaID:       "Asia-China-Guangdong-Shenzhen",
			Weight:       100,
			AccessToken:  "11213213213213213213213132132",
		},
	)

	configs = append(configs,
		&types.SchedulerCfg{
			SchedulerURL: "https://192.168.0.3:3456/rpc/v0",
			AreaID:       "Asia-China-Guangdong-Shenzhen",
			Weight:       10,
			AccessToken:  "11213213213213213213213132132",
		},
	)

	configs = append(configs,
		&types.SchedulerCfg{
			SchedulerURL: "https://192.168.0.4:3456/rpc/v0",
			AreaID:       "Asia-China-Guangdong-Shenzhen",
			Weight:       100,
			AccessToken:  "11213213213213213213213132132",
		},
	)

	locator := &Locator{Rand: rand.New(rand.NewSource(time.Now().Unix()))}
	for i := 0; i < 100; i++ {
		urls := locator.randomSchedulerConfigWithWeight(configs)
		t.Logf("urls:%#v", urls)
	}

}

func TestGetUserNearestIP(t *testing.T) {
	userIP := "219.128.79.150"
	ipList := []string{"49.213.6.70", "207.90.194.123", "183.221.214.108", "162.250.189.26"}
	reg := &titanRegion{webAPI: "https://api-test1.container1.titannet.io/api/v2/location"}
	targetIP := getUserNearestIP(userIP, ipList, reg)

	fmt.Println("targetIP ", targetIP)
}
