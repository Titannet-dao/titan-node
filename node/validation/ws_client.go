package validation

import (
	"fmt"

	"github.com/gorilla/websocket"
)

type WebSocket struct {
	conn *websocket.Conn
}

// newWSClient creates a new websocket client with a given address
func newWSClient(addr string, nodeID string) (*WebSocket, error) {
	url := fmt.Sprintf("%s/validation/%s", addr, nodeID)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	log.Infof("new ws connect to %s", url)
	return &WebSocket{conn: conn}, nil
}

// transmitData sends data along with its message type over a TCP connection with rate limiting
func (ws *WebSocket) sendData(data []byte) error {
	return ws.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (ws *WebSocket) cancelValidate(data []byte) error {
	return ws.conn.WriteMessage(websocket.CloseMessage, data)
}
