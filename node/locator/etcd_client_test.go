package locator

import (
	"testing"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
)

func TestEtcd(t *testing.T) {
	t.Logf("TestDB")
	addresses := []string{"192.168.0.51:2379"}
	etcd, err := etcdcli.New(addresses)
	if err != nil {
		t.Errorf("new etcd client error:%s", err.Error())
		return
	}

	resp, err := etcd.GetServers(types.NodeScheduler)
	if err != nil {
		t.Errorf("get server error:%s", err.Error())
		return
	}

	for _, kv := range resp.Kvs {
		t.Logf("k:%s v:%s", kv.Key, kv.Value)
	}

}
