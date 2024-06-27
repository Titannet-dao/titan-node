package tunnel

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type CMD int

const (
	cMDNone CMD = iota
	cMDReqData
	cMDReqCreated
	cMDReqClientClosed
	cMDReqClientFinished
	cMDReqServerFinished
	cMDReqServerClosed
)

const maxCap = 100

type Tunnel struct {
	ID              string
	conn            *websocket.Conn
	lastActivitTime time.Time
	reqq            *Reqq
	writeLock       sync.Mutex
}

func newTunnel(id string, conn *websocket.Conn) *Tunnel {
	return &Tunnel{conn: conn, ID: id, reqq: newReqq(maxCap), writeLock: sync.Mutex{}, lastActivitTime: time.Now()}
}

func (t *Tunnel) onPing(data string) error {
	t.lastActivitTime = time.Now()
	return t.writePong([]byte(data))
}

func (t *Tunnel) onClose() {
	log.Debugf("tunnel close %s", t.ID)
	t.reqq.cleanup()
}

func (t *Tunnel) onTunnelMessage(message []byte) error {
	cmd := message[0]
	idx := binary.LittleEndian.Uint16(message[1:])
	tag := binary.LittleEndian.Uint16(message[3:])

	switch cmd {
	case uint8(cMDReqData):
		data := message[5:]
		t.onServerRequestData(idx, tag, data)
	case uint8(cMDReqServerClosed):
		t.onServerRequestClose(idx, tag)
	default:
		log.Errorf("onTunnelMessage, unknown tunnel cmd:%d", cmd)
	}

	return nil
}

func (t *Tunnel) onServerRequestData(idx, tag uint16, data []byte) error {
	log.Infof("onServerRequestData, idx:%d tag:%d, data len:%d", idx, tag, len(data))

	req := t.reqq.getReq(idx, tag)
	if req == nil {
		return fmt.Errorf("can not find request, idx %d, tag %d", idx, tag)
	}
	return req.write(data)
}

func (t *Tunnel) onServerRequestClose(idx, tag uint16) error {
	log.Infof("onServerRequestClose, idx:%d tag:%d", idx, tag)

	req := t.reqq.getReq(idx, tag)
	if req == nil {
		return fmt.Errorf("can not find request, idx %d, tag %d", idx, tag)
	}
	req.dofree()
	return nil
}

func (t *Tunnel) onAcceptRequest(w http.ResponseWriter, r *http.Request) error {
	serviceID := t.getServiceID(r)
	if len(serviceID) == 0 {
		http.Error(w, fmt.Sprintf("%s not include servicd id", r.URL.Path), http.StatusBadRequest)
		return fmt.Errorf("request service id can not empty")
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
		return fmt.Errorf("Hijacking not supported")
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return err
	}
	defer conn.Close()

	req := t.reqq.allocReq()
	if req == nil {
		http.Error(w, "can not allocate request", http.StatusInternalServerError)
		return fmt.Errorf("request is max %d", maxCap)
	}

	req.tag = req.tag + 1
	req.conn = conn
	// send req create
	t.sendCreate2Server(req, serviceID)

	headerString := t.getHeaderString(r, serviceID)
	t.onClientRecvData(req.idx, req.tag, []byte(headerString))

	return t.serveConn(conn, req.idx, req.tag)
}

func (t *Tunnel) serveConn(conn net.Conn, idx uint16, tag uint16) error {
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Infof("serveConnection, read message failed: %s", err.Error())
			if !isNetErrUseOfCloseNetworkConnection(err) {
				t.sendClose2Server(idx, tag)
			}
			break
		}
		t.onClientRecvData(idx, tag, buf[:n])
	}

	return t.onClientClose(idx, tag)
}

func (t *Tunnel) onClientClose(idx, tag uint16) error {
	log.Infof("onClientClose idx:%d tag:%d", idx, tag)
	return t.reqq.free(idx, tag)
}

func (t *Tunnel) onClientRecvData(idx, tag uint16, data []byte) error {
	buf := make([]byte, 5+len(data))
	buf[0] = byte(cMDReqData)
	binary.LittleEndian.PutUint16(buf[1:], idx)
	binary.LittleEndian.PutUint16(buf[3:], tag)
	copy(buf[5:], data)

	return t.write(buf)
}

func (t *Tunnel) sendCreate2Server(req *Request, serviceID string) error {
	buf := make([]byte, 5+len(serviceID))
	buf[0] = uint8(cMDReqCreated)
	binary.LittleEndian.PutUint16(buf[1:], uint16(req.idx))
	binary.LittleEndian.PutUint16(buf[3:], uint16(req.tag))
	copy(buf[5:], []byte(serviceID))

	return t.write(buf)
}

func (t *Tunnel) sendClose2Server(idx, tag uint16) error {
	buf := make([]byte, 5)
	buf[0] = byte(cMDReqClientClosed)
	binary.LittleEndian.PutUint16(buf[1:], idx)
	binary.LittleEndian.PutUint16(buf[3:], tag)
	return t.write(buf)
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

func (t *Tunnel) getServiceID(r *http.Request) string {
	// path = /project/nodeID/project/{custom}
	reqPath := r.URL.Path
	parts := strings.Split(reqPath, "/")
	if len(parts) >= 4 {
		return parts[3]
	}
	return ""
}

func (t *Tunnel) getHeaderString(r *http.Request, serviceID string) string {
	// path = /project/{nodeID}/{projectID}/{customPath}
	urlString := r.URL.String()
	prefix := fmt.Sprintf("%s%s/%s", projectPathPrefix, t.ID, serviceID)
	if strings.HasPrefix(urlString, prefix) {
		urlString = strings.TrimPrefix(urlString, prefix)
	}

	if !strings.HasPrefix(urlString, "/") {
		urlString = "/" + urlString
	}

	log.Infof("url %s", urlString)

	headerString := fmt.Sprintf("%s %s %s\r\n", r.Method, urlString, r.Proto)
	headerString += fmt.Sprintf("Host: %s\r\n", r.RemoteAddr)
	for name, values := range r.Header {
		for _, value := range values {
			headerString += fmt.Sprintf("%s: %s\r\n", name, value)
		}
	}
	headerString += "\r\n"
	return headerString
}
