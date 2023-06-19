package storage

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/sqldb"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
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
	if err = bucket.setAssetHashes(context.Background(), bucketID, []string{c1.Hash().String()}); err != nil {
		t.Errorf("setAssetHashes error %s", err.Error())
		return
	}

	cidStr = "QmUuNfFwuRrxbRFt5ze3EhuQgkGnutwZtsYMbAcYbtb6j3"
	c2, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode cid error:%s", err.Error())
		return
	}

	bucketID = bucket.bucketID(c2)
	err = bucket.setAssetHashes(context.Background(), bucketID, []string{c2.Hash().String()})
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
		t.Logf("mh:%s", asset)
	}

	bucketID = bucket.bucketID(c2)
	assets, err = bucket.getAssetHashes(context.Background(), bucketID)
	if err != nil {
		t.Errorf("put error:%s", err.Error())
		return
	}

	t.Logf("index:%d", bucketID)

	for _, asset := range assets {
		t.Logf("mh:%s", asset)
	}
}

func TestAssetView(t *testing.T) {
	bucketSize := uint32(128)
	assetsView, err := newAssetsView("C:/Users/aaa/.titanedge-1/storage/assets-view", bucketSize)
	if err != nil {
		t.Errorf("new assets view error:%s", err.Error())
		return
	}

	var topHash string
	if topHash, err = assetsView.getTopHash(context.Background()); err != nil {
		t.Errorf("get top Hash error:%s", err.Error())
		return
	}

	t.Logf("topHash: %s", topHash)

	bucketHashes, err := assetsView.getBucketHashes(context.Background())
	if err != nil {
		t.Errorf("get bucketHashes error:%s", err.Error())
		return
	}

	t.Logf("bucketHashes: %#v", bucketHashes)
	recalculateTopHash, err := assetsView.calculateTopHash(bucketHashes)
	if err != nil {
		t.Errorf("get bucketHashes error:%s", err.Error())
		return
	}
	t.Logf("recalculateTopHash: %s", recalculateTopHash)

	bucketIDs := make([]int, 0, len(bucketHashes))
	for k := range bucketHashes {
		bucketIDs = append(bucketIDs, int(k))
	}

	sort.Ints(bucketIDs)

	for _, bucketID := range bucketIDs {
		if assetHashes, err := assetsView.getAssetHashes(context.Background(), uint32(bucketID)); err != nil {
			t.Errorf("get getAssetHashes error:%s", err.Error())
			continue
		} else {
			t.Logf("bucket %d, assets %#v", bucketID, assetHashes)
		}
	}
}

func TestLoadAssetViewFromDB(t *testing.T) {
	sqldb, err := sqldb.NewDB("user01:sql001@tcp(127.0.0.1:3306)/test")
	if err != nil {
		t.Errorf("NewDB error:%s", err.Error())
		return
	}

	db, err := db.NewSQLDB(sqldb)
	if err != nil {
		t.Errorf("NewSQLDB error:%s", err.Error())
		return
	}

	nodeID := "c_461705e502b945df97420312f5f3c2c1"
	topHash, err := db.LoadTopHash(nodeID)
	if err != nil {
		t.Errorf("LoadTopHash error:%s", err.Error())
		return
	}
	t.Logf("topHash:%s", topHash)

	bucketHashBytes, err := db.LoadBucketHashes(nodeID)
	if err != nil {
		t.Errorf("LoadBucketHashes error:%s", err.Error())
		return
	}

	// t.Logf("bucketHashes: %#v", bucketHashes)
	buffer := bytes.NewBuffer(bucketHashBytes)
	dec := gob.NewDecoder(buffer)

	out := make(map[uint32]string)
	if err = dec.Decode(&out); err != nil {
		t.Errorf("Decode error:%s", err.Error())
		return
	}

	bucketIDs := make([]int, 0, len(out))
	for k := range out {
		bucketIDs = append(bucketIDs, int(k))
	}

	sort.Ints(bucketIDs)

	t.Logf("bucketIDs: %#v", out)

	for _, bucketID := range bucketIDs {
		id := fmt.Sprintf("%s:%d", nodeID, bucketID)
		assetHashes, err := db.LoadBucket(id)
		if err != nil {
			t.Errorf("LoadBucket error:%s", err.Error())
			continue
		}

		if len(assetHashes) > 0 {
			hashes := make([]string, 0)
			buffer := bytes.NewBuffer(assetHashes)
			dec := gob.NewDecoder(buffer)
			err = dec.Decode(&hashes)
			if err != nil {
				t.Errorf("decode error:%s", err.Error())
				continue
			}
			t.Logf("bucket %d, assets %#v", bucketID, hashes)
		}
	}

}

func TestRandAssetFromScheduler(t *testing.T) {
	sqldb, err := sqldb.NewDB("user01:sql001@tcp(127.0.0.1:3306)/test")
	if err != nil {
		t.Errorf("NewDB error:%s", err.Error())
		return
	}

	db, err := db.NewSQLDB(sqldb)
	if err != nil {
		t.Errorf("NewSQLDB error:%s", err.Error())
		return
	}

	seed := time.Now().Unix()
	nodeID := "e_676e52451ccd4c4eb0693daa7a06d1f3"

	r := rand.New(rand.NewSource(seed))
	bytes, err := db.LoadBucketHashes(nodeID)
	if err != nil {
		t.Errorf("LoadBucketHashes error:%s", err.Error())
		return
	}

	hashes := make(map[uint32]string)
	if err := decode(bytes, &hashes); err != nil {
		t.Errorf("decode error:%s", err.Error())
		return
	}

	if len(hashes) == 0 {
		t.Errorf("no bucket exist")
		return
	}
	t.Logf("hashes:%#v", hashes)

	// TODO　save bucket hashes as array
	bucketIDs := make([]int, 0, len(hashes))
	for k := range hashes {
		bucketIDs = append(bucketIDs, int(k))
	}

	sort.Ints(bucketIDs)

	index := r.Intn(len(bucketIDs))
	bucketID := bucketIDs[index]

	id := fmt.Sprintf("%s:%d", nodeID, bucketID)
	t.Logf("id:%s", id)
	bytes, err = db.LoadBucket(id)
	if err != nil {
		t.Errorf("LoadBucket error %s", err.Error())
		return
	}

	assetHashes := make([]string, 0)
	if err = decode(bytes, &assetHashes); err != nil {
		t.Errorf("decode error %s", err.Error())
		return
	}

	if len(assetHashes) == 0 {
		t.Errorf(" len(assetHashes) == 0 ")
		return
	}

	index = r.Intn(len(assetHashes))
	hash := assetHashes[index]

	bytes, err = hex.DecodeString(hash)
	if err != nil {
		t.Errorf("DecodeString error %s", err.Error())
		return
	}

	c := cid.NewCidV0(bytes)

	t.Logf("random asset %s", c.String())
}

func decode(data []byte, out interface{}) error {
	if len(data) == 0 {
		return nil
	}

	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)
	return dec.Decode(out)
}
