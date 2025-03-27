package node

import (
	"math"
	"sync"

	"github.com/docker/go-units"
)

// BandwidthTracker represents the bandwidth tracker for a node
type BandwidthTracker struct {
	maxSize int

	bandwidthDowns     []int64
	bandwidthDownIndex int
	bandwidthDownMutex sync.RWMutex

	bandwidthUps     []int64
	bandwidthUpIndex int
	bandwidthUpMutex sync.RWMutex
}

// NewBandwidthTracker creates a new NodeBandwidth instance
func NewBandwidthTracker(size int) *BandwidthTracker {
	nb := &BandwidthTracker{
		maxSize: size,

		bandwidthDowns:     make([]int64, 0, size),
		bandwidthDownIndex: 0,
		bandwidthDownMutex: sync.RWMutex{},

		bandwidthUps:     make([]int64, 0, size),
		bandwidthUpIndex: 0,
		bandwidthUpMutex: sync.RWMutex{},
	}

	nb.PutBandwidthDown(units.GiB)
	nb.PutBandwidthUp(units.MiB)

	return nb
}

// PutBandwidthDown adds a new download speed to the tracker
func (nst *BandwidthTracker) PutBandwidthDown(speed int64) int64 {
	if speed <= 0 {
		return nst.getAverageBandwidthDown()
	}

	nst.bandwidthDownMutex.Lock()
	defer nst.bandwidthDownMutex.Unlock()

	if len(nst.bandwidthDowns) < nst.maxSize {
		nst.bandwidthDowns = append(nst.bandwidthDowns, speed)
	} else {
		nst.bandwidthDowns[nst.bandwidthDownIndex] = speed
		nst.bandwidthDownIndex = (nst.bandwidthDownIndex + 1) % nst.maxSize
	}

	// log.Infof("UpdateNodeBandwidths speed:[%d]  %v", speed, nst.bandwidthDowns)

	return nst.getAverageBandwidthDown()
}

// getAverageBandwidthDown calculates and returns the average download speed
func (nst *BandwidthTracker) getAverageBandwidthDown() int64 {
	if len(nst.bandwidthDowns) == 0 {
		return 0
	}

	if len(nst.bandwidthDowns) == 1 {
		return nst.bandwidthDowns[0]
	}

	sum := int64(0)
	minSpeed := int64(math.MaxInt64)
	for _, speed := range nst.bandwidthDowns {
		if speed < minSpeed {
			minSpeed = speed
		}

		sum += speed
	}

	len := int64(len(nst.bandwidthDowns))

	adjustedSum := sum - minSpeed
	adjustedCount := len - 1

	return adjustedSum / adjustedCount
}

// PutBandwidthUp adds a new upload speed to the tracker
func (nst *BandwidthTracker) PutBandwidthUp(speed int64) int64 {
	if speed <= 0 {
		return nst.getAverageBandwidthUp()
	}

	nst.bandwidthUpMutex.Lock()
	defer nst.bandwidthUpMutex.Unlock()

	if len(nst.bandwidthUps) < nst.maxSize {
		nst.bandwidthUps = append(nst.bandwidthUps, speed)
	} else {
		nst.bandwidthUps[nst.bandwidthUpIndex] = speed
		nst.bandwidthUpIndex = (nst.bandwidthUpIndex + 1) % nst.maxSize
	}

	// log.Infof("UpdateNodeBandwidths speed:[%d]  %v", speed, nst.bandwidthUps)

	return nst.getAverageBandwidthUp()
}

// getAverageBandwidthUp calculates and returns the average upload speed
func (nst *BandwidthTracker) getAverageBandwidthUp() int64 {
	if len(nst.bandwidthUps) == 0 {
		return 0
	}

	if len(nst.bandwidthUps) == 1 {
		return nst.bandwidthUps[0]
	}

	sum := int64(0)
	minSpeed := int64(math.MaxInt64)
	for _, speed := range nst.bandwidthUps {
		if speed < minSpeed {
			minSpeed = speed
		}

		sum += speed
	}

	len := int64(len(nst.bandwidthUps))

	adjustedSum := sum - minSpeed
	adjustedCount := len - 1

	return adjustedSum / adjustedCount
}
