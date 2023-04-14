package asset

import (
	"testing"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/asset/fetcher"
	"github.com/Filecoin-Titan/titan/node/asset/storage"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

type TestCachedResultImpl struct {
	t *testing.T
}

func (t *TestCachedResultImpl) CacheResult(result *types.PullResult) error {
	t.t.Logf("result:%#v", *result)
	return nil
}

func TestManager(t *testing.T) {
	t.Log("TestManager")
	_ = logging.SetLogLevel("/asset/cache", "DEBUG")

	cidStr := "QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG"
	c, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode err:%s", err)
		return
	}

	metaDataPath := "./test"
	assetsPaths := []string{metaDataPath}
	storageMgr, err := storage.NewManager(&storage.ManagerOptions{MetaDataPath: metaDataPath, AssetsPaths: assetsPaths})
	if err != nil {
		t.Errorf("NewManager err:%s", err)
		return
	}

	bFetcher := fetcher.NewIPFSClient("http://192.168.0.132:5001", 15, 1)
	opts := &ManagerOptions{Storage: storageMgr, BFetcher: bFetcher, PullParallel: 5}

	mgr, err := NewManager(opts)
	if err != nil {
		t.Errorf("new manager err:%s", err)
		return
	}

	mgr.addToWaitList(c, nil)

	time.Sleep(1 * time.Minute)
}
