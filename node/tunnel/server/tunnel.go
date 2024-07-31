package tunserver

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/gorilla/websocket"
)

type CMD int

const (
	cMDPing = 1
	cMDPong = 2
)

type DataCount struct {
	FromReq int64
	FromTun int64
}

type waitConn struct {
	create   chan bool
	complete chan bool
}

type TunnelOptions struct {
	scheduler    api.Scheduler
	projectID    string
	targetNodeID string
	nodeID       string
	tunID        string
	req          Req
}

type Tunnel struct {
	ctx          context.Context
	scheduler    api.Scheduler
	projectID    string
	targetNodeID string
	nodeID       string
	id           string
	req          Req

	conn *websocket.Conn

	lastActivitTime time.Time

	dataCount   DataCount
	trafficStat *TrafficStat

	waitConnCh chan bool
	writeLock  sync.Mutex
}

func checkOptions(opts *TunnelOptions) error {
	if opts.scheduler == nil {
		return fmt.Errorf("opts.scheduler == nil")
	}

	if len(opts.projectID) == 0 {
		return fmt.Errorf("opts.projectID is emtpy")
	}

	if len(opts.nodeID) == 0 {
		return fmt.Errorf("opts.nodeID is emtpy")
	}

	if len(opts.tunID) == 0 {
		return fmt.Errorf("opts.tunID is emtpy")
	}

	if opts.req == nil {
		return fmt.Errorf("opts.req == nil")
	}

	return nil
}

func newTunnel(ctx context.Context, ts *Tunserver, relays []string, opts *TunnelOptions) (*Tunnel, error) {
	if err := checkOptions(opts); err != nil {
		return nil, err
	}

	if len(relays) > 0 {
		return newTunnelWithRelays(context.Background(), relays, opts)
	}
	return newTunnelWithTargetNode(context.Background(), ts, opts)
}

func newTunnelWithRelays(ctx context.Context, relays []string, opts *TunnelOptions) (*Tunnel, error) {
	if len(relays) == 0 {
		return nil, fmt.Errorf("relays == nil")
	}

	wsURL := relays[0]
	header := make(http.Header)
	for _, relay := range relays[1:] {
		header.Add("Relay", relay)
	}

	wsURL = fmt.Sprintf("%s/relay/%s/%s", wsURL, opts.targetNodeID, opts.projectID)
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		return nil, fmt.Errorf("dial %s failed %s", wsURL, err.Error())
	}

	log.Debugf("newTunnelWithRelays %s, projectID %s", wsURL, opts.tunID)
	t := &Tunnel{
		ctx:          ctx,
		scheduler:    opts.scheduler,
		projectID:    opts.projectID,
		targetNodeID: opts.targetNodeID,
		nodeID:       opts.nodeID,
		id:           opts.tunID,
		req:          opts.req,
		conn:         conn,

		writeLock:   sync.Mutex{},
		dataCount:   DataCount{},
		waitConnCh:  nil,
		trafficStat: &TrafficStat{lock: sync.Mutex{}, startTime: time.Now()},
	}

	go t.handleTrafficStat()
	go t.startService()

	return t, nil
}

func newTunnelWithTargetNode(ctx context.Context, ts *Tunserver, opts *TunnelOptions) (*Tunnel, error) {
	t := &Tunnel{
		ctx:          ctx,
		scheduler:    opts.scheduler,
		projectID:    opts.projectID,
		targetNodeID: opts.targetNodeID,
		nodeID:       opts.nodeID,
		id:           opts.tunID,
		req:          opts.req,

		writeLock:   sync.Mutex{},
		dataCount:   DataCount{},
		waitConnCh:  make(chan bool),
		trafficStat: &TrafficStat{lock: sync.Mutex{}, startTime: time.Now()},
	}

	ts.tunMgr.addTunnel(t)

	createTunnelReq := &types.CreateTunnelReq{
		NodeID:    t.targetNodeID,
		ProjectID: t.projectID,
		TunnelID:  t.id,
	}
	err := ts.scheduler.CreateTunnel(ctx, createTunnelReq)
	if err != nil {
		ts.tunMgr.removeTunnel(t)
		return nil, err
	}

	// wait connect
	<-t.waitConnCh
	close(t.waitConnCh)
	t.waitConnCh = nil

	go t.handleTrafficStat()

	log.Debugf("newTunnelWithNode id %s", t.id)
	return t, nil
}

func (t *Tunnel) startService() {
	defer t.onClose()

	conn := t.conn
	conn.SetPingHandler(t.onPing)

	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			log.Info("Error reading message:", err)
			if !IsNetErrUseOfCloseNetworkConnection(err) {
				if err = t.req.Close(); err != nil {
					log.Errorf("close req failed %d", err.Error())
				}
			}
			return
		}

		if messageType != websocket.BinaryMessage {
			log.Errorf("unsupport message type %d", messageType)
			continue
		}

		// log.Debugf("on tunnel message %d, id %s", len(p), t.id)
		if err = t.onTunMessage(p); err != nil {
			log.Info("write to req ", err)
		}
	}
}

func (t *Tunnel) onTunMessage(buf []byte) error {
	if t.dataCount.FromTun == 0 {
		buf = AppendTraceInfoToResponseHeader(buf, t.nodeID)
		// log.Infof("onTunMessage header: %s", string(buf))
	}

	t.dataCount.FromTun++
	t.trafficStat.upstreamCount(len(buf))
	return t.req.Write(buf)
}

func (t *Tunnel) onReqMessage(buf []byte) error {
	if t.dataCount.FromReq == 0 {
		buf = AppendTraceInfoToRequestHeader(buf, t.nodeID)
		// log.Infof("onReqMessage header: %s", string(buf))
	}

	t.dataCount.FromReq++
	t.trafficStat.downstreamCount(len(buf))
	return t.write(buf)
}

func (t *Tunnel) onClose() error {
	if t.conn != nil {
		t.conn.Close()
		t.conn = nil
	}
	return nil
}

func (t *Tunnel) close() error {
	if t.conn != nil {
		return t.conn.Close()
	}
	return fmt.Errorf("t.conn == nil ")
}

func (t *Tunnel) onPing(data string) error {
	t.lastActivitTime = time.Now()
	return t.writePong([]byte(data))
}

func (t *Tunnel) writePong(data []byte) error {
	t.writeLock.Lock()
	defer t.writeLock.Unlock()

	return t.conn.WriteMessage(websocket.PongMessage, data)
}

func (t *Tunnel) write(data []byte) error {
	t.writeLock.Lock()
	defer t.writeLock.Unlock()
	return t.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (t *Tunnel) handleTrafficStat() {
	timer := time.NewTicker(trafficStatIntervel / 2)
	defer timer.Stop()

	for {
		<-timer.C
		if err := t.submitProjectReport(); err != nil {
			log.Errorf("submitProjectReport failed: %s", err.Error())
		}

	}
}

func (t *Tunnel) submitProjectReport() error {
	if t.scheduler == nil {
		return fmt.Errorf("t.scheduler == nil")
	}

	if !t.trafficStat.isTimeToSubmitProjectReport() {
		return nil
	}

	log.Debugf("submitProjectReport %s %s", t.targetNodeID, t.projectID)
	record := t.trafficStat.toProjectRecord()
	record.NodeID = t.targetNodeID
	record.ProjectID = t.projectID
	return t.scheduler.SubmitProjectReport(context.Background(), record)
}
