package storage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
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

// calculateBucketHash calculates the hash of all asset hashes within a bucket.
func calculateBucketHash(hashes []string) (string, error) {
	hash := sha256.New()
	for _, h := range hashes {
		multiHash, err := hex.DecodeString(h)
		if err != nil {
			return "", err
		}
		if _, err := hash.Write(multiHash); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func calculateTopHash(bucketHashes map[uint32]string) (string, error) {
	keys := make([]int, 0, len(bucketHashes))
	for k := range bucketHashes {
		keys = append(keys, int(k))
	}

	sort.Ints(keys)

	hash := sha256.New()
	for _, key := range keys {
		if cs, err := hex.DecodeString(bucketHashes[uint32(key)]); err != nil {
			return "", err
		} else if _, err := hash.Write(cs); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
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
			oldHash := bucketHashes[uint32(bucketID)]
			newHash, err := calculateBucketHash(assetHashes)
			if err != nil {
				t.Errorf("calculateBucketHash error:%s", err.Error())
			}
			if oldHash != newHash {
				t.Errorf("recalculateBucketHash %d %s != %s", bucketID, oldHash, newHash)
			}
			t.Logf("bucket %d, %s, assets %#v", bucketID, oldHash, assetHashes)
		}
	}
}

func TestLoadAssetViewFromDB(t *testing.T) {
	sqldb, err := sqldb.NewDB("scheduler:scheduler_password@tcp(127.0.0.1:3305)/titan_scheduler")
	if err != nil {
		t.Errorf("NewDB error:%s", err.Error())
		return
	}

	db, err := db.NewSQLDB(sqldb)
	if err != nil {
		t.Errorf("NewSQLDB error:%s", err.Error())
		return
	}

	nodeID := "c_6b7e28d16ae5494f8007645c34bc4fc4"
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

	// t.Logf("bucketHashes: %#v", out)

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

			oldHash := out[uint32(bucketID)]
			newHash, err := calculateBucketHash(hashes)
			if err != nil {
				t.Errorf("calculateBucketHash error:%s", err.Error())
			}
			if oldHash != newHash {
				t.Errorf("recalculateBucketHash %d %s != %s", bucketID, oldHash, newHash)
			}

			t.Logf("bucket %d, %s, assets %#v", bucketID, oldHash, hashes)
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

func TestRepairAssetView(t *testing.T) {
	sqldb, err := sqldb.NewDB("scheduler:scheduler_password@tcp(127.0.0.1:3305)/titan_scheduler")
	if err != nil {
		t.Fatalf("NewDB error:%s", err.Error())
	}

	db, err := db.NewSQLDB(sqldb)
	if err != nil {
		t.Fatalf("NewSQLDB error:%s", err.Error())
	}

	// file, err := os.Open("../../../dev-id")
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// defer file.Close()

	// scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	// for scanner.Scan() {
	// 	fmt.Println(scanner.Text())
	// }

	// if err := scanner.Err(); err != nil {
	// 	log.Fatal(err)
	// }
	nodeIDs := []string{"c_6b7e28d16ae5494f8007645c34bc4fc4"}

	for _, nodeID := range nodeIDs {
		// nodeID := scanner.Text()

		log.Debugf("check %s assetView", nodeID)

		topHash, err := db.LoadTopHash(nodeID)
		if err != nil {
			t.Fatalf("LoadTopHash error:%s", err.Error())
		}
		t.Logf("topHash:%s", topHash)

		bucketHashBytes, err := db.LoadBucketHashes(nodeID)
		if err != nil {
			t.Fatalf("LoadBucketHashes error:%s", err.Error())
		}

		if len(bucketHashBytes) == 0 {
			t.Logf("%s no assetview exist", nodeID)
			continue
		}

		buffer := bytes.NewBuffer(bucketHashBytes)
		dec := gob.NewDecoder(buffer)

		bucketHashes := make(map[uint32]string)
		if err = dec.Decode(&bucketHashes); err != nil {
			t.Fatalf("Decode error:%s", err.Error())
		}

		bucketIDs := make([]int, 0, len(bucketHashes))
		for k := range bucketHashes {
			bucketIDs = append(bucketIDs, int(k))
		}

		sort.Ints(bucketIDs)

		// t.Logf("bucketIDs: %#v", bucketIDs)

		for _, bucketID := range bucketIDs {
			id := fmt.Sprintf("%s:%d", nodeID, bucketID)
			assetHashes, err := db.LoadBucket(id)
			if err != nil {
				t.Fatalf("LoadBucket error:%s", err.Error())
			}

			if len(assetHashes) > 0 {
				hashes := make([]string, 0)
				buffer := bytes.NewBuffer(assetHashes)
				dec := gob.NewDecoder(buffer)
				err = dec.Decode(&hashes)
				if err != nil {
					t.Fatalf("decode error:%s", err.Error())
				}

				oldHash := bucketHashes[uint32(bucketID)]
				newHash, err := calculateBucketHash(hashes)
				if err != nil {
					t.Fatalf("calculateBucketHash error:%s", err.Error())
				}

				if oldHash != newHash {
					t.Logf("repair bucket %d", bucketID)
					bucketHashes[uint32(bucketID)] = newHash
				}
			}
		}

		newTopHash, err := calculateTopHash(bucketHashes)
		if err != nil {
			t.Fatalf("calculateTopHash %s", err.Error())
		}

		if newTopHash == topHash {
			t.Logf("%s top hash no change", nodeID)
			continue
		}

		var bucketHashesBuffer bytes.Buffer
		enc := gob.NewEncoder(&bucketHashesBuffer)
		err = enc.Encode(bucketHashes)
		if err != nil {
			t.Fatalf("encode %s", err.Error())
		}

		db.SaveAssetsView(nodeID, newTopHash, bucketHashesBuffer.Bytes())
		t.Logf("new top hash %s", newTopHash)
	}

	// if err := scanner.Err(); err != nil {
	// 	log.Fatal(err)
	// }

}

func TestScanLineByLine(t *testing.T) {
	file, err := os.Open("../../../dev-id")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		nodeID := scanner.Text()
		t.Logf("node id %s", nodeID)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func TestAssetBucket(t *testing.T) {
	cidString := "bafkreic5tw5dyiifake3bz5w7kkle7krd77pugdjy4jk3lyxv6p4igwsyq"

	c, err := cid.Decode(cidString)
	if err != nil {
		t.Fatal("decode error ", err)
	}

	h := fnv.New32a()
	h.Write(c.Hash())
	bucketID := h.Sum32() % 128
	t.Logf("bucketID %d", bucketID)
}
