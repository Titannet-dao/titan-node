package tunserver

import (
	"fmt"
	"net"
	"sync"

	"github.com/gorilla/websocket"
)

type Req interface {
	Write(buf []byte) error
	Close() error
}

type TcpReq struct {
	conn net.Conn
}

func (req *TcpReq) Write(buf []byte) error {
	if req.conn == nil {
		return fmt.Errorf("req.conn == nil")
	}

	wrote := 0
	l := len(buf)
	for {
		n, err := req.conn.Write(buf[wrote:])
		if err != nil {
			return err
		}
		wrote = wrote + n
		if wrote == l {
			break
		}
	}
	return nil
}

func (req *TcpReq) Close() error {
	if req.conn != nil {
		return req.conn.Close()
	}
	return fmt.Errorf("req.conn[tcp] already close")
}

type WsReq struct {
	conn      *websocket.Conn
	writeLock sync.Mutex
}

func (req *WsReq) Write(buf []byte) error {
	req.writeLock.Lock()
	defer req.writeLock.Unlock()

	if req.conn == nil {
		return fmt.Errorf("req.conn == nil")
	}
	return req.conn.WriteMessage(websocket.BinaryMessage, buf)
}

func (req *WsReq) Close() error {
	if req.conn != nil {
		return req.conn.Close()
	}
	return fmt.Errorf("req.conn[websocket] already close")
}
