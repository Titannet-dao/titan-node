package api

import (
	"context"
)

// DataSync sync scheduler asset to node
type DataSync interface {
	// CompareTopHash check asset if same as scheduler.
	// topHash is hash of all buckets
	CompareTopHash(ctx context.Context, topHash string) (bool, error) //perm:write
	// CompareBucketHashes group asset in bucket, and compare single bucket hash
	// hashes are map of bucket, key is number of bucket, value is hash
	// return mismatch bucket number
	CompareBucketHashes(ctx context.Context, hashes map[uint32]string) ([]uint32, error) //perm:write
}
