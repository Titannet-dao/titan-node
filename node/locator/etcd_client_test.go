package locator

import (
	"os"
	"testing"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
)

const (
	testEtcdAddress = "39.108.143.56:2379"
)

func TestEtcd(t *testing.T) {
	t.Logf("TestEtcd")
	if err := os.Setenv("ETCD_USERNAME", "web"); err != nil {
		t.Errorf("Setenv ETCD_USERNAME error %s", err.Error())
	}
	if err := os.Setenv("ETCD_PASSWORD", "web_123"); err != nil {
		t.Errorf("Setenv ETCD_PASSWORD error %s", err.Error())
	}
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
