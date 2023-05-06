package locator

import (
	"testing"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
)

const (
	testEtcdAddress = "192.168.0.51:2379"
)

func TestEtcd(t *testing.T) {
	t.Logf("TestEtcd")
	etcd, err := etcdcli.New([]string{testEtcdAddress})
	if err != nil {
		t.Errorf("new etcd client error:%s", err.Error())
		return
	}

	resp, err := etcd.GetServers(types.NodeScheduler.String())
	if err != nil {
		t.Errorf("get server error:%s", err.Error())
		return
	}

	for _, kv := range resp.Kvs {
		t.Logf("k:%s v:%s", kv.Key, kv.Value)
	}
}
