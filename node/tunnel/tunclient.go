package tunnel

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	sendIntervel = 3
)

type Tunclient struct {
	id              string
	conn            *websocket.Conn
	reqq            *Reqq
	writeLock       sync.Mutex
	services        *Services
	lastActivitTime time.Time
}

func newTunclient(tunServerURL string, nodeID string, services *Services) (*Tunclient, error) {
	if len(tunServerURL) == 0 || len(nodeID) == 0 {
		return nil, fmt.Errorf("tun server url %s and nodeID %s must not be null", tunServerURL, nodeID)
	}

	url := fmt.Sprintf("%s/tunnel/%s", tunServerURL, nodeID)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, fmt.Errorf("dial %s failed %s", url, err.Error())
	}

	log.Infof("New tunclient %s", url)

	reqq := newReqq(maxCap)
	tunclient := &Tunclient{id: nodeID, conn: conn, reqq: reqq, writeLock: sync.Mutex{}, services: services}

	return tunclient, nil
}

func (tc *Tunclient) startService() error {
	conn := tc.conn
	defer tc.onClose()

	conn.SetPongHandler(tc.onPone)
	go tc.keepalive()

	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			log.Info("Error reading message:", err)
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
		if tc.conn == nil {
			log.Infof("keepalive conn == nil")
			return nil
		}

		if time.Since(tc.lastActivitTime) > keepaliveTimeout {
			log.Warnf("keepalive timeout, id %s", tc.id)
			return tc.conn.Close()
		}

		tc.writePing()
	}
}

func (tc *Tunclient) writePing() error {
	tc.writeLock.Lock()
	defer tc.writeLock.Unlock()

	if tc.conn == nil {
		return fmt.Errorf("writePing conn == nil")
	}

	now := time.Now().Unix()
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(now))
	return tc.conn.WriteMessage(websocket.PingMessage, b)
}

func (tc *Tunclient) onPone(data string) error {
	tc.lastActivitTime = time.Now()
	return nil
}

func (tc *Tunclient) onClose() {
	if tc.conn != nil {
		if err := tc.conn.Close(); err != nil {
			log.Errorf("close conn %s", err.Error())
		}
		tc.conn = nil
	}

	tc.reqq.cleanup()
}

func (tc *Tunclient) onTunnelMessage(message []byte) error {
	cmd := message[0]
	idx := binary.LittleEndian.Uint16(message[1:])
	tag := binary.LittleEndian.Uint16(message[3:])

	serviceID := string(message[5:])

	switch cmd {
	case uint8(cMDReqCreated):
		return tc.handlRequestCreated(idx, tag, serviceID)
	case uint8(cMDReqData):
		data := message[5:]
		return tc.handlRequestData(idx, tag, data)
	case uint8(cMDReqClientClosed):
		return tc.handlRequestClosed(idx, tag)

	default:
		log.Errorf("onTunnelMessage, unknown tunnel cmd:%d", cmd)
	}
	return nil
}

func (tc *Tunclient) handlRequestCreated(idx, tag uint16, projectID string) error {
	log.Debugf("handlRequestCreated idx:%d tag:%d, projectID:%s", idx, tag, projectID)
	service := tc.services.get(projectID)
	if service == nil {
		err := tc.sendClose2Client(idx, tag)
		return fmt.Errorf("service %s not exist, sendClose2Client %#v", projectID, err)
	}

	req := tc.reqq.allocReq()
	if req == nil {
		return fmt.Errorf("can not alloc, idx %d tag %d", idx, tag)
	}
	req.idx = idx
	req.tag = tag

	addr := fmt.Sprintf("%s:%d", service.Address, service.Port)

	log.Debugf("handlRequestCreated connet to %s", addr)

	ts := time.Second * 2
	conn, err := net.DialTimeout("tcp", addr, ts)
	if err != nil {
		log.Errorf("connet to %s failed: ", addr, err)
		err2 := tc.sendClose2Client(idx, tag)
		err3 := tc.onRequestTerminate(idx, tag)
		return fmt.Errorf("connet to %s failed: %s, sendClose2Client %#v, onRequestTerminate %#v", addr, err, err2, err3)
	}
	req.conn = conn

	go req.proxy(tc)

	return nil
}

func (tc *Tunclient) handlRequestData(idx, tag uint16, data []byte) error {
	log.Debugf("handlRequestData idx:%d tag:%d, data len:%d", idx, tag, len(data))

	req := tc.reqq.getReq(idx, tag)
	if req == nil {
		return fmt.Errorf("get req idx:%d tag:%d failed", idx, tag)
	}
	return req.write(data)
}

func (tc *Tunclient) handlRequestClosed(idx, tag uint16) error {
	log.Debugf("handlRequestClosed idx:%d tag:%d", idx, tag)
	req := tc.reqq.getReq(idx, tag)
	if req == nil {
		return fmt.Errorf("get req idx:%d tag:%d failed", idx, tag)
	}
	req.dofree()
	return nil
}

func (tc *Tunclient) onRequestTerminate(idx, tag uint16) error {
	log.Debugf("onRequestTerminate idx:%d tag:%d", idx, tag)
	return tc.reqq.free(idx, tag)
}

func (tc *Tunclient) sendClose2Client(idx, tag uint16) error {
	log.Debugf("sendClose2Client idx:%d tag:%d", idx, tag)

	buf := make([]byte, 5)
	buf[0] = uint8(cMDReqServerClosed)
	binary.LittleEndian.PutUint16(buf[1:], idx)
	binary.LittleEndian.PutUint16(buf[3:], tag)

	return tc.write(buf)
}

func (tc *Tunclient) write(msg []byte) error {
	tc.writeLock.Lock()
	defer tc.writeLock.Unlock()

	if tc.conn == nil {
		return fmt.Errorf("write conn == nil")
	}
	return tc.conn.WriteMessage(websocket.BinaryMessage, msg)
}

func (tc *Tunclient) onRequestData(req *Request, data []byte) error {
	log.Debugf("onRequestData idx:%d tag:%d, data len:%d", req.idx, req.tag, len(data))

	buf := make([]byte, 5+len(data))
	buf[0] = uint8(cMDReqData)
	binary.LittleEndian.PutUint16(buf[1:], req.idx)
	binary.LittleEndian.PutUint16(buf[3:], req.tag)
	copy(buf[5:], data)

	return tc.write(buf)
}
