package httpserver

import (
	"context"
	"testing"

	"github.com/Filecoin-Titan/titan/node/asset"
	"github.com/Filecoin-Titan/titan/node/asset/storage"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

const (
	testIPFSAddress = "http://192.168.0.132:5001"
)

func TestHttpServer(t *testing.T) {
	t.Log("TestGateway")
}

func TestResolvePath(t *testing.T) {
	t.Log("TestResolvePath")

	p := "/ipfs/QmSUs7pPXL9jqcLSXNgZ92QXB36oXurgRCYzFrzAkYdT5d/log"
	metaDataPath := "C:/Users/aaa/.titancandidate-1/storage"
	assetsPaths := []string{metaDataPath}
	storageMgr, err := storage.NewManager(&storage.ManagerOptions{MetaDataPath: metaDataPath, AssetsPaths: assetsPaths})
	if err != nil {
		t.Errorf("NewManager err:%s", err)
		return
	}

	assetCID, err := cid.Decode(p)
	if err != nil {
		t.Errorf("Decode err:%s", err)
		return
	}

	opts := &asset.ManagerOptions{Storage: storageMgr, IPFSAPIURL: testIPFSAddress, PullerConfig: &config.Puller{PullBlockTimeout: 3, PullBlockRetry: 2, PullBlockParallel: 5}}

	mgr, err := asset.NewManager(context.Background(), opts)
	if err != nil {
		t.Errorf("TestResolvePath error:%s", err.Error())
		return
	}

	hs := &HttpServer{asset: mgr}

	resolvePath, err := hs.resolvePath(context.Background(), path.New(p), assetCID)
	if err != nil {
		t.Errorf("TestResolvePath error:%s", err.Error())
		return
	}

	t.Logf("root: %s, cid: %s, rest:%v", resolvePath.Root().String(), resolvePath.Cid().String(), resolvePath.Remainder())
}

func TestGetBlock(t *testing.T) {
	t.Log("TestGetBlock")

	metaDataPath := "C:/Users/aaa/.titancandidate-1/storage"
	assetsPaths := []string{metaDataPath}
	storageMgr, err := storage.NewManager(&storage.ManagerOptions{MetaDataPath: metaDataPath, AssetsPaths: assetsPaths})
	if err != nil {
		t.Errorf("NewManager err:%s", err)
		return
	}

	root := "QmSUs7pPXL9jqcLSXNgZ92QXB36oXurgRCYzFrzAkYdT5d"
	assetCID, err := cid.Decode(root)
	if err != nil {
		t.Errorf("Decode err:%s", err)
		return
	}

	opts := &asset.ManagerOptions{Storage: storageMgr, IPFSAPIURL: testIPFSAddress, PullerConfig: &config.Puller{PullBlockTimeout: 3, PullBlockRetry: 2, PullBlockParallel: 5}}

	mgr, err := asset.NewManager(context.Background(), opts)
	if err != nil {
		t.Errorf("TestResolvePath error:%s", err.Error())
		return
	}

	hs := &HttpServer{asset: mgr}
	blk, err := hs.asset.GetBlock(context.Background(), assetCID, assetCID)
	if err != nil {
		t.Errorf("GetBlock error:%s", err.Error())
		return
	}

	t.Logf("block size:%d", len(blk.RawData()))

}
