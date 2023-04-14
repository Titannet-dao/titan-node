package assets

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
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
	assetHashes, err := m.LoadBucket(bucketID)
	if err != nil {
		return xerrors.Errorf("load bucket error %w", err)
	}
	assetHashes = removeTargetHash(assetHashes, c.Hash().String())

	bucketHashes, err := m.LoadBucketHashes(nodeID)
	if err != nil {
		return err
	}

	if len(assetHashes) == 0 {
		if err := m.DeleteBucket(bucketID); err != nil {
			return err
		}
		delete(bucketHashes, bucketNumber)
	}

	if len(bucketHashes) == 0 {
		return m.DeleteAssetsView(nodeID)
	}

	topHash, err := calculateOverallHash(bucketHashes)
	if err != nil {
		return err
	}

	if err := m.SaveAssetsView(nodeID, topHash, bucketHashes); err != nil {
		return err
	}

	if len(assetHashes) > 0 {
		return m.SaveBucket(bucketID, assetHashes)
	}
	return nil
}

// addAssetToView adds an asset to the node's asset view
func (m *Manager) addAssetToView(nodeID string, assetCID string) error {
	c, err := cid.Decode(assetCID)
	if err != nil {
		return err
	}
	bucketNumber := determineBucketNumber(c)
	bucketID := fmt.Sprintf("%s:%d", nodeID, bucketNumber)
	assetHashes, err := m.LoadBucket(bucketID)
	if err != nil {
		return xerrors.Errorf("load bucket error %w", err)
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

	bucketHashes, err := m.LoadBucketHashes(nodeID)
	if err != nil {
		return err
	}
	bucketHashes[bucketNumber] = hash

	topHash, err := calculateOverallHash(bucketHashes)
	if err != nil {
		return err
	}

	if err := m.SaveAssetsView(nodeID, topHash, bucketHashes); err != nil {
		return err
	}

	return m.SaveBucket(bucketID, assetHashes)
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

// contains checks if a target hash exists in a list of hashes
func contains(hashes []string, target string) bool {
	for _, hash := range hashes {
		if hash == target {
			return true
		}
	}
	return false
}
