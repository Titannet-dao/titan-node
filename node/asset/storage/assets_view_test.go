package storage

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multihash"
)

func init() {
	_ = logging.SetLogLevel("asset/store", "DEBUG")
}
func TestBucket(t *testing.T) {
	ds, err := createDatastore("./test")
	if err != nil {
		t.Errorf("new kv store error:%s", err.Error())
		return
	}

	bucket := &bucket{ds: ds, size: 100}

	cidStr := "QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG"
	c1, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode cid error:%s", err.Error())
		return
	}

	bucketID := bucket.bucketID(c1)
	bucket.setAssetHashes(context.Background(), bucketID, []multihash.Multihash{c1.Hash()})

	cidStr = "QmUuNfFwuRrxbRFt5ze3EhuQgkGnutwZtsYMbAcYbtb6j3"
	c2, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode cid error:%s", err.Error())
		return
	}

	bucketID = bucket.bucketID(c2)
	err = bucket.setAssetHashes(context.Background(), bucketID, []multihash.Multihash{c2.Hash()})
	if err != nil {
		t.Errorf("put error:%s", err.Error())
		return
	}

	bucketID = bucket.bucketID(c1)
	assets, err := bucket.getAssetHashes(context.Background(), bucketID)
	if err != nil {
		t.Errorf("put error:%s", err.Error())
		return
	}

	t.Logf("bucketID:%d", bucketID)

	for _, asset := range assets {
		t.Logf("mh:%s", asset.String())
	}

	bucketID = bucket.bucketID(c2)
	assets, err = bucket.getAssetHashes(context.Background(), bucketID)
	if err != nil {
		t.Errorf("put error:%s", err.Error())
		return
	}

	t.Logf("index:%d", bucketID)

	for _, asset := range assets {
		t.Logf("mh:%s", asset.String())
	}
}

func TestAssetView(t *testing.T) {
	assetsView, err := newAssetsView("C:/Users/aaa/.titancandidate-1/storage/assets-view", 128)
	if err != nil {
		t.Errorf("new assets view error:%s", err.Error())
		return
	}

	cidStr := "QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG"
	root, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode cid error:%s", err.Error())
		return
	}
	if err := assetsView.addAsset(context.Background(), root); err != nil {
		t.Errorf("add asset error:%s", err.Error())
		return
	}

	if topHash, err := assetsView.getTopHash(context.Background()); err != nil {
		t.Errorf("get top Hash error:%s", err.Error())
		return
	} else {
		t.Logf("topHash: %s", topHash)
	}

	if bucketHashes, err := assetsView.getBucketHashes(context.Background()); err != nil {
		t.Errorf("get bucketHashes error:%s", err.Error())
		return
	} else {
		t.Logf("bucketHashes: %#v", bucketHashes)
	}
}
