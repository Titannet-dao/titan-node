package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"golang.org/x/xerrors"
)

const (
	keyOfTopHash      = "top"
	keyOfBucketHashes = "checksums"
)

// TODO　assetsView should put in asset package, not storage package
// assetsView manages and stores the assets cid in a bucket-based hash.
type assetsView struct {
	*bucket

	lock *sync.Mutex
}

// newAssetsView creates a new AssetView instance.
func newAssetsView(baseDir string, bucketSize uint32) (*assetsView, error) {
	ds, err := createDatastore(baseDir)
	if err != nil {
		return nil, err
	}

	return &assetsView{bucket: &bucket{ds: ds, size: bucketSize}, lock: &sync.Mutex{}}, nil
}

// setTopHash sets the top hash values of the buckets
func (av *assetsView) setTopHash(ctx context.Context, topHash string) error {
	key := ds.NewKey(keyOfTopHash)
	return av.ds.Put(ctx, key, []byte(topHash))
}

// getTopHash gets the top hash values of the buckets
func (av *assetsView) getTopHash(ctx context.Context) (string, error) {
	key := ds.NewKey(keyOfTopHash)
	val, err := av.ds.Get(ctx, key)
	if err != nil {
		if err == ds.ErrNotFound {
			return "", nil
		}
		return "", err
	}

	return string(val), nil
}

// removeTopHash removes the top hash values of the buckets
func (av *assetsView) removeTopHash(ctx context.Context) error {
	key := ds.NewKey(keyOfTopHash)
	return av.ds.Delete(ctx, key)
}

// TODO save hashes as array
func (av *assetsView) setBucketHashes(ctx context.Context, hashes map[uint32]string) error {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(hashes)
	if err != nil {
		return err
	}

	key := ds.NewKey(keyOfBucketHashes)
	return av.ds.Put(ctx, key, buffer.Bytes())
}

// getBucketHashes gets the hash values for each bucket.
func (av *assetsView) getBucketHashes(ctx context.Context) (map[uint32]string, error) {
	key := ds.NewKey(keyOfBucketHashes)
	val, err := av.ds.Get(ctx, key)
	if err != nil {
		if err == ds.ErrNotFound {
			return make(map[uint32]string), nil
		}
		return nil, err
	}

	out := make(map[uint32]string)

	buffer := bytes.NewBuffer(val)
	dec := gob.NewDecoder(buffer)
	err = dec.Decode(&out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// removeBucketHashes removes the hash values for each bucket.
func (av *assetsView) removeBucketHashes(ctx context.Context) error {
	key := ds.NewKey(keyOfBucketHashes)
	return av.ds.Delete(ctx, key)
}

// addAsset adds an asset to the AssetView.
func (av *assetsView) addAsset(ctx context.Context, root cid.Cid) error {
	av.lock.Lock()
	defer av.lock.Unlock()

	bucketID := av.bucketID(root)
	assetHashes, err := av.bucket.getAssetHashes(ctx, bucketID)
	if err != nil {
		return err
	}

	if contains(assetHashes, root.Hash().String()) {
		return nil
	}

	assetHashes = append(assetHashes, root.Hash().String())
	sort.Strings(assetHashes)

	return av.update(ctx, bucketID, assetHashes)
}

// removeAsset removes an asset from the AssetView.
func (av *assetsView) removeAsset(ctx context.Context, root cid.Cid) error {
	av.lock.Lock()
	defer av.lock.Unlock()

	bucketID := av.bucketID(root)
	assetHashes, err := av.bucket.getAssetHashes(ctx, bucketID)
	if err != nil {
		return err
	}

	if !contains(assetHashes, root.Hash().String()) {
		return nil
	}

	assetHashes = removeHash(assetHashes, root.Hash().String())
	return av.update(ctx, bucketID, assetHashes)
}

// update updates the hash values in the AssetView after adding or removing an asset.
func (av *assetsView) update(ctx context.Context, bucketID uint32, assetHashes []string) error {
	bucketHashes, err := av.getBucketHashes(ctx)
	if err != nil {
		return err
	}

	if len(assetHashes) == 0 {
		if err := av.remove(ctx, bucketID); err != nil {
			return err
		}
		delete(bucketHashes, bucketID)
	} else {
		if err := av.setAssetHashes(ctx, bucketID, assetHashes); err != nil {
			return err
		}

		bucketHash, err := av.calculateBucketHash(assetHashes)
		if err != nil {
			return err
		}

		bucketHashes[bucketID] = bucketHash
	}

	if len(bucketHashes) == 0 {
		if err := av.removeTopHash(ctx); err != nil {
			return err
		}
		return av.removeBucketHashes(ctx)
	}

	topHash, err := av.calculateTopHash(bucketHashes)
	if err != nil {
		return err
	}

	if err := av.setBucketHashes(ctx, bucketHashes); err != nil {
		return err
	}

	if err := av.setTopHash(ctx, topHash); err != nil {
		return err
	}

	return nil
}

// calculateBucketHash calculates the hash of all asset hashes within a bucket.
func (av *assetsView) calculateBucketHash(hashes []string) (string, error) {
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

// calculateTopHash calculates the top hash value from the bucket hash values.
func (av *assetsView) calculateTopHash(bucketHashes map[uint32]string) (string, error) {
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

// bucket sort multi hash by hash code
type bucket struct {
	ds   ds.Batching
	size uint32
}

func (b *bucket) getAssetHashes(ctx context.Context, bucketID uint32) ([]string, error) {
	if int(bucketID) > int(b.size) {
		return nil, fmt.Errorf("bucket id %d is out of %d", bucketID, b.size)
	}

	key := ds.NewKey(fmt.Sprintf("%d", bucketID))
	val, err := b.ds.Get(ctx, key)
	if err != nil && err != ds.ErrNotFound {
		return nil, xerrors.Errorf("failed to get value for bucket %d, err: %w", bucketID, err)
	}

	if errors.Is(err, ds.ErrNotFound) {
		return nil, nil
	}

	hashes := make([]string, 0)
	buffer := bytes.NewBuffer(val)
	dec := gob.NewDecoder(buffer)
	err = dec.Decode(&hashes)
	if err != nil {
		return nil, err
	}

	return hashes, nil
}

// hashes must be sorted before saving
func (b *bucket) setAssetHashes(ctx context.Context, bucketID uint32, hashes []string) error {
	key := ds.NewKey(fmt.Sprintf("%d", bucketID))

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(hashes)
	if err != nil {
		return err
	}

	return b.ds.Put(ctx, key, buffer.Bytes())
}

func (b *bucket) remove(ctx context.Context, bucketID uint32) error {
	key := ds.NewKey(fmt.Sprintf("%d", bucketID))
	return b.ds.Delete(ctx, key)
}

func (b *bucket) bucketID(c cid.Cid) uint32 {
	h := fnv.New32a()
	h.Write(c.Hash())
	return h.Sum32() % b.size
}

func removeHash(hashes []string, target string) []string {
	// remove mhs
	for i, hash := range hashes {
		if hash == target {
			return append(hashes[:i], hashes[i+1:]...)
		}
	}
	return hashes
}

func contains(hashes []string, target string) bool {
	for _, hash := range hashes {
		if hash == target {
			return true
		}
	}

	return false
}
