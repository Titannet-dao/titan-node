package tunclient

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	tunserver "github.com/Filecoin-Titan/titan/node/tunnel/server"
	"github.com/gorilla/websocket"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("tunclient")

const (
	sendIntervel      = 3
	keepaliveIntervel = 10 * time.Second
	keepaliveTimeout  = 3 * keepaliveIntervel
)

type DataCount struct {
	FromServer int64
	FromTun    int64
}

type Tunclient struct {
	ID          string
	nodeID      string
	wsConn      *websocket.Conn
	workerdConn net.Conn
	// reqq            *Reqq
	writeLock       sync.Mutex
	lastActivitTime time.Time

	closeFun  func(id string)
	dataCount DataCount
}

func NewTunclient(req *types.CreateTunnelReq, service *Service, onClose func(id string)) (*Tunclient, error) {
	if len(req.WsURL) == 0 || len(req.NodeID) == 0 || len(req.TunnelID) == 0 || len(req.ProjectID) == 0 {
		return nil, fmt.Errorf("create tunclient failed, params %#v", *req)
	}

	addr := fmt.Sprintf("%s:%d", service.Address, service.Port)
	workerdConn, err := newWorkerdConn(addr)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/tunnel/%s/%s?tunid=%s", req.WsURL, req.NodeID, req.ProjectID, req.TunnelID)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, fmt.Errorf("dial %s failed %s", url, err.Error())
	}

	log.Debugf("New tunclient %s", url)

	// reqq := newReqq(maxCap)
	tunclient := &Tunclient{
		ID:          req.TunnelID,
		nodeID:      req.NodeID,
		wsConn:      conn,
		workerdConn: workerdConn,
		writeLock:   sync.Mutex{},
		closeFun:    onClose,
		dataCount:   DataCount{},
	}
	return tunclient, nil
}

func (tc *Tunclient) StartService() error {
	go tc.handleWorkerdMessage(tc.workerdConn)

	wsConn := tc.wsConn
	defer tc.onClose()

	wsConn.SetPongHandler(tc.onPone)
	go tc.keepalive()

	for {
		messageType, p, err := wsConn.ReadMessage()
		if err != nil {
			log.Info("Error reading message:", err)
			if !tunserver.IsNetErrUseOfCloseNetworkConnection(err) {
				if err = tc.workerdConn.Close(); err != nil {
					log.Errorf("close tunnel failed: %s", err.Error())
				}
			}
			return err
		}

		if messageType != websocket.BinaryMessage {
			log.Errorf("unsupport message type %d", messageType)
			continue
		}

		if err = tc.onTunnelMessage(p); err != nil {
			log.Errorf("onTunnelMessage: %s", err.Error())
		}

		// log.Debugf("Received message len: %d, type: %d\n", len(p), messageType)
	}
}

func (tc *Tunclient) keepalive() error {
	ticker := time.NewTicker(keepaliveIntervel)
	tc.lastActivitTime = time.Now()

	for {
		<-ticker.C
		if tc.wsConn == nil {
			log.Infof("keepalive wsConn == nil")
			return nil
		}

		if time.Since(tc.lastActivitTime) > keepaliveTimeout {
			log.Warnf("keepalive timeout, id %s", tc.ID)
			return tc.wsConn.Close()
		}

		tc.writePing()
	}
}

func (tc *Tunclient) writePing() error {
	tc.writeLock.Lock()
	defer tc.writeLock.Unlock()

	if tc.wsConn == nil {
		return fmt.Errorf("writePing wsConn == nil")
	}

	now := time.Now().Unix()
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(now))
	return tc.wsConn.WriteMessage(websocket.PingMessage, b)
}

func (tc *Tunclient) onPone(data string) error {
	tc.lastActivitTime = time.Now()
	return nil
}

func (tc *Tunclient) onClose() {
	if tc.wsConn != nil {
		if err := tc.wsConn.Close(); err != nil {
			log.Errorf("close wsConn %s", err.Error())
		}
		tc.wsConn = nil
	}

	if tc.closeFun != nil {
		tc.closeFun(tc.ID)
	}
}

func (tc *Tunclient) onTunnelMessage(message []byte) error {
	if tc.dataCount.FromTun == 0 {
		// log.Infof("onTunnelMessage %s", string(message))
		message = tunserver.AppendTraceInfoToRequestHeader(message, tc.nodeID)
	}
	tc.dataCount.FromTun++
	_, err := tc.workerdConn.Write(message)
	return err
}

func (tc *Tunclient) write(msg []byte) error {
	tc.writeLock.Lock()
	defer tc.writeLock.Unlock()

	if tc.wsConn == nil {
		return fmt.Errorf("write wsConn == nil")
	}
	return tc.wsConn.WriteMessage(websocket.BinaryMessage, msg)
}

func (tc *Tunclient) onWorkerdData(data []byte) error {
	if tc.dataCount.FromServer == 0 {
		// log.Infof("onWorkerdData %s", string(data))
		data = tunserver.AppendTraceInfoToResponseHeader(data, tc.nodeID)
	}

	tc.dataCount.FromServer++
	return tc.write(data)
}

func newWorkerdConn(addr string) (net.Conn, error) {
	ts := time.Second * 2
	conn, err := net.DialTimeout("tcp", addr, ts)
	if err != nil {
		return nil, fmt.Errorf("connect to %s failed: %s", addr, err)
	}

	return conn, nil
}

func (tc *Tunclient) handleWorkerdMessage(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Debugf("read server message failed: %s", err.Error())
			if !tunserver.IsNetErrUseOfCloseNetworkConnection(err) {
				if err = tc.wsConn.Close(); err != nil {
					log.Errorf("close tunnel failed: %s", err.Error())
				}
			}
			return
		}
		tc.onWorkerdData(buf[:n])
	}

}
