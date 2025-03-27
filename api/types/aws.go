package types

import "time"

// AWSDataInfo aws data
type AWSDataInfo struct {
	Bucket         string    `db:"bucket"`
	Cid            string    `db:"cid"`
	Replicas       int       `db:"replicas"`
	IsDistribute   bool      `db:"is_distribute"`
	DistributeTime time.Time `db:"distribute_time"`
	Size           float64   `db:"size"`
}

// AssetDataInfo ipfs data
type AssetDataInfo struct {
	Cid            string    `db:"cid"`
	Hash           string    `db:"hash"`
	Replicas       int64     `db:"replicas"`
	Status         int       `db:"status"`
	DistributeTime time.Time `db:"distribute_time"`
	Owner          string    `db:"owner"`
	Expiration     time.Time `db:"expiration"`
}
