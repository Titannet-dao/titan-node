package httpserver

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed index.html
var monitorIndexHTML embed.FS

const (
	RequiredBandDown = 500 * 1 << 20 / 8 // 500Mbps 62.5MB/s
	RequiredBandUp   = 200 * 1 << 20 / 8 // 200Mbps 25MB/s

	LowStress    = 0.2 // low stress of peak0. will degrade to peak1
	Peak0LowTask = 1
	Peak1LowTask = 5

	Peak0WindowSize = 60
	Peak1WindowSize = 600
)

type Monitor struct {
	Routes *RouteLoads

	Loads [Peak1WindowSize]FlowUnit
	_p    int16 // index of current slide window

	Peak0 FlowUnit // peak in window
	Peak1 FlowUnit // peak in last 10 rinutes
	Peak2 FlowUnit // peak since last run

	muPeak sync.Mutex //
	fs     http.Handler
}

type FlowUnit struct {
	U int64 // upstrear rate
	D int64 // downstrear rate
}

type RouteLoads struct {
	list []*RouteInstance
	sync.RWMutex
}

// RouteInstance should include start and end times, writer and reader,
// number of bytes read and written, and the route name.
// Requests that lasted more than 10 minutes and have already ended should be discarded.
type RouteInstance struct {
	name string // route nare

	w     http.ResponseWriter
	r     *http.Request
	rBody io.ReadCloser

	rc int64 // read bytes count
	wc int64 // write bytes count

	rc_last int64
	wc_last int64

	start int64
	end   int64
}

func NewRouterInstance(name string, w http.ResponseWriter, r *http.Request) *RouteInstance {
	ri := &RouteInstance{
		name:  name,
		w:     w,
		r:     r,
		rBody: r.Body,
		start: time.Now().UnixMilli(),
	}
	if r.Body != nil {
		r.Body = ri
	}
	return ri
}

// impl ResponseWriter
func (ri *RouteInstance) Header() http.Header {
	return ri.w.Header()
}

// impl ResponseWriter
func (ri *RouteInstance) Write(b []byte) (int, error) {
	n, err := ri.w.Write(b)
	atomic.AddInt64(&ri.wc, int64(n))
	return n, err
}

// impl ResponseWriter
func (ri *RouteInstance) WriteHeader(statusCode int) {
	ri.w.WriteHeader(statusCode)
}

// impl io.ReadCloser
func (ri *RouteInstance) Read(p []byte) (int, error) {
	if ri.rBody == nil {
		return 0, io.EOF
	}
	n, err := ri.rBody.Read(p)
	atomic.AddInt64(&ri.rc, int64(n))
	return n, err
}

// impl io.ReadCloser
func (ri *RouteInstance) Close() error {
	if ri.rBody != nil {
		return ri.rBody.Close()
	}
	return nil
}

func (ri *RouteInstance) IsRunning() bool {
	// running and util next window
	return ri.end == 0 || time.Now().UnixMilli()-ri.end < 1*1000
}

func NewMonitor() *Monitor {

	fs := http.FileServer(http.FS(monitorIndexHTML))
	monitorFs := http.StripPrefix(reqMonitor+"/", fs)
	return &Monitor{
		Routes: &RouteLoads{list: make([]*RouteInstance, 0)},
		_p:     0,
		fs:     monitorFs,
	}
}

func (rl *RouteLoads) AddRoute(ri *RouteInstance) {
	rl.Lock()
	defer rl.Unlock()
	rl.list = append(rl.list, ri)
}

func (rl *RouteLoads) Cleanup() {
	rl.Lock()
	defer rl.Unlock()
	cutoff := time.Now().UnixMilli() - 600*1000 // 10 minutes
	var updated []*RouteInstance
	for _, ri := range rl.list {
		if ri.end == 0 || ri.end >= cutoff {
			updated = append(updated, ri)
		}
	}
	rl.list = updated
}

func (rl *RouteLoads) TaskRunningCount() int16 {
	rl.RLock()
	defer rl.RUnlock()
	tc := 0
	for _, v := range rl.list {
		if v.IsRunning() {
			fmt.Printf("running %s\n", v.name)
			tc += 1
		}
	}
	return int16(tc)
}

func (rl *RouteLoads) TaskCount() int16 {
	rl.RLock()
	defer rl.RUnlock()
	return int16(len(rl.list))
}

func (m *Monitor) Middleware(next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		router := NewRouterInstance(r.URL.Path, w, r)

		m.Routes.AddRoute(router)

		next.ServeHTTP(router, r)

		router.end = time.Now().UnixMilli()
	})
}

// setWindow reset index of slide window
func (m *Monitor) setWindow() {

	// reset _p
	m._p = (m._p + 1) % int16(len(m.Loads))

	// remove old data
	m.Loads[m._p].U = 0
	m.Loads[m._p].D = 0

	m.updatePeaks()
}

func (m *Monitor) updatePeaks() {
	m.muPeak.Lock()
	defer m.muPeak.Unlock()

	var currentUp, currentDown int64

	m.Routes.RLock()
	for _, r := range m.Routes.list {
		if !r.IsRunning() {
			continue
		}
		deltaW := atomic.LoadInt64(&r.wc) - r.wc_last
		deltaR := atomic.LoadInt64(&r.rc) - r.rc_last

		currentUp += deltaW
		currentDown += deltaR

		r.wc_last = atomic.LoadInt64(&r.wc)
		r.rc_last = atomic.LoadInt64(&r.rc)
	}
	m.Routes.RUnlock()

	m.Loads[m._p].U = currentUp
	m.Loads[m._p].D = currentDown

	m.Peak0.U, m.Peak0.D = m.maxFlowDual(Peak0WindowSize)
	m.Peak1.U, m.Peak1.D = m.maxFlowDual(Peak1WindowSize)

	m.Peak2.U = max(m.Peak2.U, currentUp)
	m.Peak2.D = max(m.Peak2.D, currentDown)
}

func (m *Monitor) maxFlowDual(duration int) (int64, int64) {
	var maxU, maxD int64
	for i := 0; i < duration; i++ {
		idx := (m._p - int16(i) + int16(len(m.Loads))) % int16(len(m.Loads))
		valU := m.Loads[idx].U
		valD := m.Loads[idx].D
		if valU > maxU {
			maxU = valU
		}
		if valD > maxD {
			maxD = valD
		}
	}
	return maxU, maxD
}

func (m *Monitor) loadCurrent() (int64, int64) {
	var currentUp, currentDown int64

	m.Routes.RLock()
	for _, r := range m.Routes.list {
		if !r.IsRunning() {
			continue
		}
		deltaW := atomic.LoadInt64(&r.wc) - r.wc_last
		deltaR := atomic.LoadInt64(&r.rc) - r.rc_last

		currentUp += deltaW
		currentDown += deltaR
	}
	m.Routes.RUnlock()

	return currentUp, currentDown
}

func (m *Monitor) Start() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			m.setWindow()
			m.Routes.Cleanup()
		}
	}()
}

type Stats struct {
	Peak             FlowUnit
	Free             FlowUnit
	TaskRunningCount int16
	TaskCount        int16

	Raw *StatsRaw
}

type StatsRaw struct {
	Peak0   FlowUnit
	Peak1   FlowUnit
	Peak2   FlowUnit
	Current FlowUnit

	Loads  [Peak1WindowSize]FlowUnit
	Routes *RouteLoads
}

func (m *Monitor) GetStats() *Stats {
	loadU, loadD := m.loadCurrent()

	var pu, pd int64 = m.Peak0.U, m.Peak0.D

	// degrade to peak1
	if pu < 0.2*RequiredBandUp && pu < m.Peak1.U {
		pu = m.Peak1.U
	}

	if pd < 0.2*RequiredBandDown && pd < m.Peak1.D {
		pd = m.Peak1.D
	}

	// degrade to peak2
	if pu < 0.2*RequiredBandUp && m.Routes.TaskRunningCount() < Peak1LowTask {
		pu = m.Peak2.U
	}

	if pd < 0.2*RequiredBandDown && m.Routes.TaskRunningCount() < Peak1LowTask {
		pd = m.Peak2.D
	}

	return &Stats{
		Peak:             FlowUnit{U: pu, D: pd},
		Free:             FlowUnit{U: max(pu-loadU, 0), D: max(pd-loadD, 0)},
		TaskRunningCount: m.Routes.TaskRunningCount(),
		TaskCount:        m.Routes.TaskCount(),
		Raw: &StatsRaw{
			Peak0:   m.Peak0,
			Peak1:   m.Peak1,
			Peak2:   m.Peak2,
			Current: FlowUnit{U: loadU, D: loadD},
			Loads:   m.Loads,
			Routes:  m.Routes,
		},
	}

}

func (m *Monitor) statsHandler(w http.ResponseWriter, r *http.Request) {
	resp := m.GetStats()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Errorf("json.NewEncoder(w).Encode(resp) error(%v)", err)
	}
}

func (m *Monitor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, reqMonitor) {
		m.fs.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, reqStats) {
		// Handle /stats API endpoint
		m.statsHandler(w, r)
		return
	}

	http.NotFound(w, r)
}

/*
Negative Feedback:
When the SDK encounters upload/download failures, it reports failure information to the scheduler via the browser, reducing the node's weight. L1 will raise an alert based on the specific situation. The errors include:
1. Network error – L1 raises an alert and notifies the scheduler.
2. Client actively cancels – if the accumulated cancelled data exceeds 10 MB (10 * 1024^3 bytes), L1 raises an alert and notifies the scheduler.
3. 4xx errors – no action taken.
4. 5xx errors – L1 raises an alert.

Positive Feedback:
L1 provides peak0 (within 1 minute), peak1 (within 10 minutes), and peak2 (maximum value since last run).
1. The scheduler fetches peak values from all L1s (default is peak0; if peak0 is less than 20% of the minimum requirement and there are no current tasks, it falls back to peak1. If peak1 is also less than 20% and there have been no tasks for 10 minutes, it falls back to peak2).
2. The scheduler updates node peak information every 10 minutes.
3. Active Measurement: When cases 1 or 2 from the negative feedback occur, they can trigger an active measurement.
*/
