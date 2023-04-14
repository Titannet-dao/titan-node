package asset

import (
	"testing"

	"github.com/Filecoin-Titan/titan/node/asset/fetcher"
	"github.com/Filecoin-Titan/titan/node/asset/storage"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

func TestCache(t *testing.T) {
	t.Log("TestCache")

	_ = logging.SetLogLevel("/asset/cache", "DEBUG")
	_ = logging.SetLogLevel("datastore", "DEBUG")

	cidStr := "QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG"
	c, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode err:%s", err)
		return
	}

	metaDataPath := "./test"
	assetsPaths := []string{metaDataPath}
	manger, err := storage.NewManager(&storage.ManagerOptions{MetaDataPath: metaDataPath, AssetsPaths: assetsPaths})
	if err != nil {
		t.Errorf("NewManager err:%s", err)
		return
	}

	assetPuller := newAssetPuller(&pullerOptions{root: c, dss: nil, storage: manger, bFetcher: fetcher.NewIPFSClient("http://192.168.0.132:5001", 15, 1), parallel: 5})
	err = assetPuller.pullAsset()
	if err != nil {
		t.Errorf("pull asset error:%s", err)
		return
	}
}
