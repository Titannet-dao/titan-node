package tunserver

import (
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
)

const trafficStatIntervel = 10 * time.Minute

type TrafficStat struct {
	lock       sync.Mutex
	startTime  time.Time
	upstream   int64
	downstream int64
}

func (ts *TrafficStat) upstreamCount(count int) {
	ts.lock.Lock()
	defer ts.lock.Unlock()

	ts.downstream += int64(count)
}

func (ts *TrafficStat) downstreamCount(count int) {
	ts.lock.Lock()
	defer ts.lock.Unlock()

	ts.upstream += int64(count)
}

func (ts *TrafficStat) setTimeToNow() {
	ts.lock.Lock()
	defer ts.lock.Unlock()

	ts.startTime = time.Now()
}

func (ts *TrafficStat) clean() {
	ts.lock.Lock()
	ts.lock.Unlock()

	ts.startTime = time.Time{}
	ts.downstream = 0
	ts.upstream = 0
}

func (ts *TrafficStat) total() int64 {
	ts.lock.Lock()
	defer ts.lock.Unlock()

	return ts.downstream + ts.upstream
}

func (ts *TrafficStat) isTimeToSubmitProjectReport() bool {
	if !ts.startTime.IsZero() && time.Since(ts.startTime) > trafficStatIntervel && ts.total() > 0 {
		return true
	}

	return false
}

func (ts *TrafficStat) toProjectRecord() *types.ProjectRecordReq {
	ts.lock.Lock()
	record := &types.ProjectRecordReq{
		BandwidthUpSize:   float64(ts.upstream),
		BandwidthDownSize: float64(ts.downstream),
		StartTime:         ts.startTime,
		EndTime:           time.Now(),
	}

	ts.upstream = 0
	ts.downstream = 0
	ts.startTime = time.Now()

	ts.lock.Unlock()
	return record
}
