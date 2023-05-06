package asset

import (
	"context"
	"testing"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/asset/fetcher"
	"github.com/Filecoin-Titan/titan/node/asset/storage"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

const (
	testIPFSAddress = "http://192.168.0.132:5001"
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

	bFetcher := fetcher.NewIPFSClient(testIPFSAddress)
	opts := &ManagerOptions{Storage: storageMgr, BFetcher: bFetcher, PullParallel: 5, PullTimeout: 3, PullRetry: 2}

	mgr, err := NewManager(opts)
	if err != nil {
		t.Errorf("new manager err:%s", err)
		return
	}

	mgr.addToWaitList(c, nil)

	time.Sleep(1 * time.Minute)
}

func TestScanBlocks(t *testing.T) {
	t.Log("TestManager")
	_ = logging.SetLogLevel("asset", "DEBUG")

	cidStr := "QmSUs7pPXL9jqcLSXNgZ92QXB36oXurgRCYzFrzAkYdT5d"
	root, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode err:%s", err)
		return
	}

	metaDataPath := "C:/Users/aaa/.titancandidate-1/storage/"
	assetsPaths := []string{metaDataPath}
	storageMgr, err := storage.NewManager(&storage.ManagerOptions{MetaDataPath: metaDataPath, AssetsPaths: assetsPaths})
	if err != nil {
		t.Errorf("new storage manager error:%s", err)
		return
	}

	bFetcher := fetcher.NewIPFSClient(testIPFSAddress)
	opts := &ManagerOptions{Storage: storageMgr, BFetcher: bFetcher, PullParallel: 5, PullTimeout: 3, PullRetry: 2}

	mgr, err := NewManager(opts)
	if err != nil {
		t.Errorf("new asset manager error:%s", err)
		return
	}

	err = mgr.ScanBlocks(context.Background(), root)
	if err != nil {
		t.Errorf("scan blocks err:%s", err)
		return
	}
}

func TestGetChecker(t *testing.T) {
	metaDataPath := "C:/Users/aaa/.titancandidate-1/storage/"
	assetsPaths := []string{metaDataPath}
	storageMgr, err := storage.NewManager(&storage.ManagerOptions{MetaDataPath: metaDataPath, AssetsPaths: assetsPaths})
	if err != nil {
		t.Errorf("new storage manager error:%s", err)
		return
	}

	bFetcher := fetcher.NewIPFSClient(testIPFSAddress)
	opts := &ManagerOptions{Storage: storageMgr, BFetcher: bFetcher, PullParallel: 5, PullTimeout: 3, PullRetry: 2}

	mgr, err := NewManager(opts)
	if err != nil {
		t.Errorf("new asset manager error:%s", err)
		return
	}

	nowTime := time.Now().Unix()
	t.Logf("randomSeed:%d", nowTime)
	asset, err := mgr.GetAssetForValidation(context.Background(), 1681803392)
	if err != nil {
		t.Errorf("get checker error:%s", err)
		return
	}

	for i := 0; i < 100; i++ {
		block, err := asset.GetBlock(context.Background())
		if err != nil {
			t.Errorf("get checker error:%s", err)
			return
		}
		t.Logf("random block %s", block.Cid().String())
	}

	check, ok := asset.(*randomCheck)
	if !ok {
		t.Errorf("can not convert asset interface to randomCheck")
		return
	}
	t.Logf("root:%s", check.root.String())
}
