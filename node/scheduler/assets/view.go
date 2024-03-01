package assets

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sort"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

// removeAssetFromView removes an asset from the node's asset view
func (m *Manager) removeAssetFromView(nodeID string, assetCID string) error {
	c, err := cid.Decode(assetCID)
	if err != nil {
		return err
	}

	bucketNumber := determineBucketNumber(c)
	bucketID := fmt.Sprintf("%s:%d", nodeID, bucketNumber)
	bytes, err := m.LoadBucket(bucketID)
	if err != nil {
		return xerrors.Errorf("load bucket error %w", err)
	}

	assetHashes := make([]string, 0)
	if err = decode(bytes, &assetHashes); err != nil {
		return err
	}

	assetHashes = removeTargetHash(assetHashes, c.Hash().String())

	bytes, err = m.LoadBucketHashes(nodeID)
	if err != nil {
		return err
	}

	bucketHashes := make(map[uint32]string)
	if err = decode(bytes, &bucketHashes); err != nil {
		return err
	}

	if len(assetHashes) == 0 {
		if err := m.DeleteBucket(bucketID); err != nil {
			return err
		}
		delete(bucketHashes, bucketNumber)
	} else {
		hash, err := computeBucketHash(assetHashes)
		if err != nil {
			return err
		}

		bucketHashes[bucketNumber] = hash

		bytes, err = encode(assetHashes)
		if err != nil {
			return err
		}
		if err = m.SaveBucket(bucketID, bytes); err != nil {
			return err
		}
	}

	if len(bucketHashes) == 0 {
		return m.DeleteAssetsView(nodeID)
	}

	topHash, err := calculateOverallHash(bucketHashes)
	if err != nil {
		return err
	}

	bytes, err = encode(bucketHashes)
	if err != nil {
		return err
	}

	return m.SaveAssetsView(nodeID, topHash, bytes)
}

// addAssetToView adds an asset to the node's asset view
func (m *Manager) addAssetToView(nodeID string, assetCID string) error {
	c, err := cid.Decode(assetCID)
	if err != nil {
		return err
	}
	bucketNumber := determineBucketNumber(c)
	bucketID := fmt.Sprintf("%s:%d", nodeID, bucketNumber)
	bytes, err := m.LoadBucket(bucketID)
	if err != nil {
		return xerrors.Errorf("load bucket error %w", err)
	}

	assetHashes := make([]string, 0)
	if err = decode(bytes, &assetHashes); err != nil {
		return err
	}

	if contains(assetHashes, c.Hash().String()) {
		return nil
	}

	assetHashes = append(assetHashes, c.Hash().String())
	sort.Strings(assetHashes)

	hash, err := computeBucketHash(assetHashes)
	if err != nil {
		return err
	}

	bytes, err = m.LoadBucketHashes(nodeID)
	if err != nil {
		return err
	}

	bucketHashes := make(map[uint32]string)
	if err = decode(bytes, &bucketHashes); err != nil {
		return err
	}

	bucketHashes[bucketNumber] = hash

	topHash, err := calculateOverallHash(bucketHashes)
	if err != nil {
		return err
	}

	bytes, err = encode(bucketHashes)
	if err != nil {
		return err
	}

	if err := m.SaveAssetsView(nodeID, topHash, bytes); err != nil {
		return err
	}

	bytes, err = encode(assetHashes)
	if err != nil {
		return err
	}
	return m.SaveBucket(bucketID, bytes)
}

// determineBucketNumber calculates the bucket number for a given CID
func determineBucketNumber(c cid.Cid) uint32 {
	h := fnv.New32a()
	if _, err := h.Write(c.Hash()); err != nil {
		log.Panicf("hash write buffer error %s", err.Error())
	}
	return h.Sum32() % numAssetBuckets
}

// removeTargetHash removes a target hash from a list of hashes
func removeTargetHash(hashes []string, target string) []string {
	for i, hash := range hashes {
		if hash == target {
			return append(hashes[:i], hashes[i+1:]...)
		}
	}

	return hashes
}

// computeBucketHash calculates the hash for a given list of asset hashes
func computeBucketHash(hashes []string) (string, error) {
	hash := sha256.New()
	for _, h := range hashes {
		if cs, err := hex.DecodeString(h); err != nil {
			return "", err
		} else if _, err := hash.Write(cs); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// calculateOverallHash computes the top hash for a given map of bucket hashes
func calculateOverallHash(hashes map[uint32]string) (string, error) {
	keys := make([]int, 0, len(hashes))
	for k := range hashes {
		keys = append(keys, int(k))
	}

	sort.Ints(keys)

	hash := sha256.New()
	for _, key := range keys {
		if cs, err := hex.DecodeString(hashes[uint32(key)]); err != nil {
			return "", err
		} else if _, err := hash.Write(cs); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// contains checks if a target hash exists in a list of hashes
func contains(hashes []string, target string) bool {
	for _, hash := range hashes {
		if hash == target {
			return true
		}
	}
	return false
}

func decode(data []byte, out interface{}) error {
	if len(data) == 0 {
		return nil
	}

	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)
	return dec.Decode(out)
}

func encode(in interface{}) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(in)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// RandomAsset get node asset with random seed
func (m *Manager) RandomAsset(nodeID string, seed int64) (*cid.Cid, error) {
	r := rand.New(rand.NewSource(seed))
	bytes, err := m.LoadBucketHashes(nodeID)
	if err != nil {
		return nil, err
	}

	hashes := make(map[uint32]string)
	if err := decode(bytes, &hashes); err != nil {
		return nil, err
	}

	if len(hashes) == 0 {
		return nil, sql.ErrNoRows
	}

	// TODO　save bucket hashes as array
	bucketIDs := make([]int, 0, len(hashes))
	for k := range hashes {
		bucketIDs = append(bucketIDs, int(k))
	}

	sort.Ints(bucketIDs)

	index := r.Intn(len(bucketIDs))
	bucketID := bucketIDs[index]

	id := fmt.Sprintf("%s:%d", nodeID, bucketID)
	bytes, err = m.LoadBucket(id)
	if err != nil {
		return nil, err
	}

	assetHashes := make([]string, 0)
	if err = decode(bytes, &assetHashes); err != nil {
		return nil, err
	}

	if len(assetHashes) == 0 {
		return nil, sql.ErrNoRows
	}

	index = r.Intn(len(assetHashes))
	hash := assetHashes[index]

	bytes, err = hex.DecodeString(hash)
	if err != nil {
		return nil, err
	}

	c := cid.NewCidV0(bytes)
	return &c, nil
}
