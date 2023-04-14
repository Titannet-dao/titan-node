package candidate

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/node/config"
)

type tcpMsg struct {
	msgType api.TCPMsgType
	msg     []byte
	length  int
}

// TCPServer handles incoming TCP connections from devices.
type TCPServer struct {
	schedulerAPI   api.Scheduler
	config         *config.CandidateCfg
	blockWaiterMap *sync.Map
	listen         *net.TCPListener
}

// NewTCPServer initializes a new instance of TCPServer.
func NewTCPServer(cfg *config.CandidateCfg, schedulerAPI api.Scheduler) *TCPServer {
	return &TCPServer{
		config:         cfg,
		blockWaiterMap: &sync.Map{},
		schedulerAPI:   schedulerAPI,
	}
}

// StartTCPServer starts listening for incoming TCP connections.
func (t *TCPServer) StartTCPServer() {
	tcpAddr, err := net.ResolveTCPAddr("tcp", t.config.TCPSrvAddr)
	if err != nil {
		log.Fatal(err)
	}

	listen, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal(err)
	}
	t.listen = listen
	// close listener
	defer listen.Close() //nolint:errcheck // ignore error

	log.Infof("tcp server listen on %s", t.config.TCPSrvAddr)

	for {
		conn, err := listen.AcceptTCP()
		if err != nil {
			log.Errorf("tcp server close %s", err.Error())
			return
		}

		go t.handleMessage(conn)
	}
}

// Stop stops the TCP server.
func (t *TCPServer) Stop(ctx context.Context) error {
	return t.listen.Close()
}

// handleMessage handles incoming TCP messages from devices.
func (t *TCPServer) handleMessage(conn *net.TCPConn) {
	defer func() {
		if r := recover(); r != nil {
			log.Infof("handleMessage recovered. Error:\n", r)
			return
		}
	}()

	// first item is device id
	msg, err := readTCPMsg(conn)
	if err != nil {
		log.Errorf("read nodeID error:%v", err)
		return
	}

	if msg.msgType != api.TCPMsgTypeNodeID {
		log.Errorf("read tcp msg error, msg type not TCPMsgTypeNodeID")
		return
	}
	nodeID := string(msg.msg)
	if len(nodeID) == 0 {
		log.Errorf("nodeID is empty")
		return
	}

	_, ok := t.blockWaiterMap.Load(nodeID)
	if ok {
		log.Errorf("Validator already wait for device %s", nodeID)
		return
	}

	ch := make(chan tcpMsg, 1)
	bw := newBlockWaiter(nodeID, ch, t.config.ValidateDuration, t.schedulerAPI)
	t.blockWaiterMap.Store(nodeID, bw)

	defer func() {
		close(ch)
		t.blockWaiterMap.Delete(nodeID)
	}()

	log.Debugf("node %s connect to Validator", nodeID)

	timer := time.NewTimer(time.Duration(t.config.ValidateDuration) * time.Second)
	for {
		select {
		case <-timer.C:
			conn.Close()
			return
		default:
		}
		// next item is file content
		msg, err = readTCPMsg(conn)
		if err != nil {
			log.Infof("read tcp message error:%v, nodeID:%s", err, nodeID)
			return
		}
		bw.ch <- *msg

		if msg.msgType == api.TCPMsgTypeCancel {
			conn.Close()
			return
		}
	}
}

// readTCPMsg reads a TCP message from a connection.
func readTCPMsg(conn net.Conn) (*tcpMsg, error) {
	contentLen, err := readContentLen(conn)
	if err != nil {
		return nil, fmt.Errorf("read tcp msg error %v", err)
	}

	if contentLen <= 0 {
		return nil, fmt.Errorf("pack len %d is invalid", contentLen)
	}

	if contentLen > tcpPackMaxLength {
		return nil, fmt.Errorf("pack len %d is invalid", contentLen)
	}

	buf, err := readBuffer(conn, contentLen)
	if err != nil {
		return nil, fmt.Errorf("read content error %v", err)
	}

	if len(buf) <= 0 {
		return nil, fmt.Errorf("invalid tcp msg, content len == 0")
	}

	msgType, err := extractMsgType(buf[0:1])
	if err != nil {
		return nil, err
	}

	msg := &tcpMsg{msgType: msgType, length: contentLen + 4, msg: buf[1:]}
	// log.Infof("read tcp msg, type:%d, buf len:%d", msg.msgType, len(msg.msg))
	return msg, nil
}

// extractMsgType extracts the TCP message type from the given byte slice
func extractMsgType(buf []byte) (api.TCPMsgType, error) {
	var msgType uint8
	err := binary.Read(bytes.NewReader(buf), binary.LittleEndian, &msgType)
	if err != nil {
		return 0, err
	}

	return api.TCPMsgType(msgType), nil
}

// readContentLen reads the length of a TCP message from a connection
func readContentLen(conn net.Conn) (int, error) {
	buffer, err := readBuffer(conn, 4)
	if err != nil {
		return 0, err
	}

	var contentLen int32
	err = binary.Read(bytes.NewReader(buffer), binary.LittleEndian, &contentLen)
	if err != nil {
		return 0, err
	}

	return int(contentLen), nil
}

// readBuffer reads a byte buffer of the given length from a connection
func readBuffer(conn net.Conn, bufferLen int) ([]byte, error) {
	buffer := make([]byte, bufferLen)
	readLen := 0
	for {
		n, err := conn.Read(buffer[readLen:])
		if err != nil {
			return nil, err
		}

		if n == 0 {
			return nil, fmt.Errorf("buffer len not match, buffer len:%d, current read len:%d", bufferLen, readLen+n)
		}

		readLen += n
		if readLen > len(buffer) {
			return nil, fmt.Errorf("buffer len not match, buffer len:%d, current read len:%d", bufferLen, readLen)
		}

		if len(buffer) == readLen {
			return buffer, nil
		}
	}
}
