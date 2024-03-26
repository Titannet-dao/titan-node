package asset

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/asset/storage"
	"github.com/Filecoin-Titan/titan/node/ipld"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
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

	opts := &ManagerOptions{Storage: storageMgr, IPFSAPIURL: testIPFSAddress, PullParallel: 5, PullTimeout: 3, PullRetry: 2}

	mgr, err := NewManager(opts)
	if err != nil {
		t.Errorf("new manager err:%s", err)
		return
	}

	mgr.addToWaitList(c, nil, false)

	time.Sleep(1 * time.Minute)
}

func TestGetAssetSize(t *testing.T) {
	t.Log("TestManager")
	_ = logging.SetLogLevel("asset", "DEBUG")

	metaDataPath := "C:/Users/aaa/.titancandidate-1/storage/"
	assetsPaths := []string{metaDataPath}
	storageMgr, err := storage.NewManager(&storage.ManagerOptions{MetaDataPath: metaDataPath, AssetsPaths: assetsPaths})
	if err != nil {
		t.Errorf("new storage manager error:%s", err)
		return
	}

	opts := &ManagerOptions{Storage: storageMgr, IPFSAPIURL: testIPFSAddress, PullParallel: 5, PullTimeout: 3, PullRetry: 2}

	mgr, err := NewManager(opts)
	if err != nil {
		t.Errorf("new asset manager error:%s", err)
		return
	}

	cidStr := "bafkreie5dcw6r7ggjj6t2plaa3g7ajvl72xzibrlbcqupomhgnxwzyjk44"
	root, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode err:%s", err)
		return
	}

	err = testGetAssetSize(mgr, root)
	if err != nil {
		t.Errorf("scan blocks err:%s", err)
		return
	}
}

func testGetAssetSize(m *Manager, root cid.Cid) error {
	block, err := m.GetBlock(context.Background(), root, root)
	if err != nil {
		return xerrors.Errorf("get block %s error %w", root.String(), err)
	}

	blk := blocks.NewBlock(block.RawData())
	fmt.Printf("cid %s prefix code %d, block prefix code %d\n", blk.String(), root.Prefix().Codec, blk.Cid().Prefix().Codec)

	node, err := ipld.DecodeNode(context.Background(), blk)
	if err != nil {
		return err
	}

	totalSize := int64(len(block.RawData()))
	for _, link := range node.Links() {
		totalSize += int64(link.Size)
	}

	fmt.Printf("totalSize;%d, linksize:%d", totalSize, len(node.Links()))
	return nil
}

func TestGetChecker(t *testing.T) {
	var count = 1500
	var randomSeed = int64(1686298500725909400)
	metaDataPath := "C:/Users/aaa/.titancandidate-2/storage/"
	assetsPaths := []string{metaDataPath}
	storageMgr, err := storage.NewManager(&storage.ManagerOptions{MetaDataPath: metaDataPath, AssetsPaths: assetsPaths})
	if err != nil {
		t.Errorf("new storage manager error:%s", err)
		return
	}

	opts := &ManagerOptions{Storage: storageMgr, IPFSAPIURL: testIPFSAddress, PullParallel: 5, PullTimeout: 3, PullRetry: 2}

	mgr, err := NewManager(opts)
	if err != nil {
		t.Errorf("new asset manager error:%s", err)
		return
	}

	t.Logf("randomSeed:%d", randomSeed)
	asset, err := mgr.GetAssetForValidation(context.Background(), randomSeed)
	if err != nil {
		t.Errorf("get checker error:%s", err)
		return
	}

	cids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		block, err := asset.GetBlock(context.Background())
		if err != nil {
			t.Errorf("get checker error:%s", err)
			return
		}

		if len(block.RawData()) == 0 {
			continue
		}
		// t.Logf("random %d block %s", i, block.Cid().String())
		cids = append(cids, block.Cid().String())
	}

	check, ok := asset.(*randomCheck)
	if !ok {
		t.Errorf("can not convert asset interface to randomCheck")
	}
	t.Logf("root %s", check.root.String())

	if err := checkBlocksFromCandidate(cids, *check.root, randomSeed); err != nil {
		t.Errorf("checkBlocksFromCandidate error %s", err.Error())
	}
}

func checkBlocksFromCandidate(blockCIDs []string, root cid.Cid, randomSeed int64) error {
	metaDataPath := "C:/Users/aaa/.titancandidate-1/storage/"
	assetsPaths := []string{metaDataPath}
	storageMgr, err := storage.NewManager(&storage.ManagerOptions{MetaDataPath: metaDataPath, AssetsPaths: assetsPaths})
	if err != nil {
		return err
	}

	opts := &ManagerOptions{Storage: storageMgr, IPFSAPIURL: testIPFSAddress, PullParallel: 5, PullTimeout: 3, PullRetry: 2}
	mgr, err := NewManager(opts)
	if err != nil {
		return err
	}

	cids, err := mgr.GetBlocksOfAsset(root, randomSeed, len(blockCIDs))
	if err != nil {
		return err
	}

	if len(cids) != len(blockCIDs) {
		return fmt.Errorf("block cids len %d, but get %d from candidate", len(blockCIDs), len(cids))
	}

	for i := 0; i < len(cids); i++ {
		c1, err := cid.Decode(cids[i])
		if err != nil {
			return err
		}

		c2, err := cid.Decode(blockCIDs[i])
		if err != nil {
			return err
		}

		if !bytes.Equal(c1.Hash(), c2.Hash()) {
			return fmt.Errorf("index %d, validated cid %s, candidate cid %s", i, blockCIDs[i], cids[i])
		}

		// fmt.Printf("%d check block %s ok\n", i, c1.String())
	}
	return nil
}

func TestBlocksFromCandidate(t *testing.T) {
	count := 900
	var randomSeed = int64(1686298500725909400)
	cidStr := "QmU61MNaAS7yNL8kfVPqzBwB5QkGwr1wasKh3awTTe4452"
	root, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("decode error %s", err.Error())
		return
	}

	metaDataPath := "C:/Users/aaa/.titancandidate-2/storage/"
	assetsPaths := []string{metaDataPath}
	storageMgr, err := storage.NewManager(&storage.ManagerOptions{MetaDataPath: metaDataPath, AssetsPaths: assetsPaths})
	if err != nil {
		t.Errorf("new storage manager error %s", err.Error())
		return
	}

	opts := &ManagerOptions{Storage: storageMgr, IPFSAPIURL: testIPFSAddress, PullParallel: 5, PullTimeout: 3, PullRetry: 2}
	mgr, err := NewManager(opts)
	if err != nil {
		t.Errorf("decode error %s", err.Error())
		return
	}

	cids, err := mgr.GetBlocksOfAsset(root, randomSeed, count)
	if err != nil {
		t.Errorf("decode error %s", err.Error())
		return
	}

	for index, c := range cids {
		t.Logf("index %d, %s", index, c)
	}
}
