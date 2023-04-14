package storage

import (
	"testing"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

func init() {
	_ = logging.SetLogLevel("asset/store", "DEBUG")
}

func TestManager(t *testing.T) {
	baseDir := "C:/Users/aaa/.titanedge-1/storage"
	Manager, err := NewManager(&ManagerOptions{MetaDataPath: baseDir, AssetsPaths: []string{baseDir}})
	if err != nil {
		t.Errorf("new Manager error:%s", err.Error())
		return
	}

	cidStr := "QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG"
	c, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("decode error:%s", err.Error())
		return
	}

	_, err = Manager.GetAsset(c)
	if err != nil {
		t.Errorf("get asset error:%s", err.Error())
		return
	}
}
