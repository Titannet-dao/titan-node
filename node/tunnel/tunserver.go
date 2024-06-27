package tunnel

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("tunnel")

const (
	// tunnel path /tunnel/{tunnel-id}, tunnel-id is same as node-id
	tunnelPathPrefix = "/tunnel/"
	// project path /project/{node-id}/{project-id}
	projectPathPrefix = "/project/"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Tunserver struct {
	handler http.Handler
	tunMgr  *TunManager
}

// ServeHTTP checks if the request path starts with the IPFS path prefix and delegates to the appropriate handler
func (o *Tunserver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, tunnelPathPrefix):
		o.handleTunnel(w, r)
	case strings.HasPrefix(r.URL.Path, projectPathPrefix):
		o.handleProject(w, r)
	default:
		o.handler.ServeHTTP(w, r)
	}
}

// NewTunserver creates a tunserver with the given HTTP handler
func NewTunserver(handler http.Handler) http.Handler {
	return &Tunserver{
		handler: handler,
		tunMgr:  &TunManager{tunnels: &sync.Map{}},
	}
}

func (ts *Tunserver) handleTunnel(w http.ResponseWriter, r *http.Request) {
	log.Infof("handleTunnel %s", r.URL.Path)
	// TODO: verify the auth of tunnel
	nodeID := filepath.Base(r.URL.Path)
	if isInvalidNodeID(nodeID) {
		log.Errorf("invalid node id %s", nodeID)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Errorf("Error upgrading to WebSocket:", err)
		return
	}
	defer conn.Close()

	tunnel := newTunnel(nodeID, conn)
	defer tunnel.onClose()

	ts.tunMgr.addTunnel(tunnel)
	defer ts.tunMgr.removeTunnel(tunnel)

	conn.SetPingHandler(tunnel.onPing)

	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			log.Info("Error reading message:", err)
			return
		}

		if messageType != websocket.BinaryMessage {
			log.Errorf("unsupport message type %d", messageType)
			continue
		}

		if err = tunnel.onTunnelMessage(p); err != nil {
			log.Errorf("onTunnelMessage %s", err.Error())
		}
	}
}

// handleProject accept user connect to project
func (ts *Tunserver) handleProject(w http.ResponseWriter, r *http.Request) {
	log.Infof("handleProject %s", r.URL.Path)
	// TODO: verify the auth of client
	nodeID := ts.getNodeID(r)
	if isInvalidNodeID(nodeID) {
		http.Error(w, fmt.Sprintf("invalid node id %s", nodeID), http.StatusBadRequest)
		return
	}

	tunnel := ts.tunMgr.getTunnel(nodeID)
	if tunnel == nil {
		http.Error(w, fmt.Sprintf("can not allocate tunnel for request %s", r.URL.Path), http.StatusBadRequest)
		return
	}

	if err := tunnel.onAcceptRequest(w, r); err != nil {
		log.Errorf("onAcceptWebsocketRequest %s", err.Error())
	}

}

func (ts *Tunserver) getNodeID(r *http.Request) string {
	path := r.URL.Path
	parts := strings.Split(path, "/")
	if len(parts) < 3 {
		return ""
	}

	return parts[2]
}

func isInvalidNodeID(nodeID string) bool {
	if len(nodeID) == 0 {
		return true
	}
	return false
}
