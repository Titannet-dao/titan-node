package locator

import (
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
