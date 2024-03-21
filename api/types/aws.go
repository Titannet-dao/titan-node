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
