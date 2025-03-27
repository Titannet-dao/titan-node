package tunserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("tunserver")

const (
	// tunnel path /tunnel/{tunnel-id}, tunnel-id is same as node-id
	tunnelPathPrefix = "/tunnel/"
	// project path /project/{node-id}/{project-id}
	projectPathPrefix = "/project/"
	relayPathPrefix   = "/relay/"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Tunserver struct {
	handler   http.Handler
	tunMgr    *TunManager
	scheduler api.Scheduler
	nodeID    string
}

// ServeHTTP checks if the request path starts with the IPFS path prefix and delegates to the appropriate handler
func (ts *Tunserver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, tunnelPathPrefix):
		ts.handleTunclient(w, r)
	case strings.HasPrefix(r.URL.Path, projectPathPrefix):
		ts.handleProject(w, r)
	case strings.HasPrefix(r.URL.Path, relayPathPrefix):
		ts.handleRelay(w, r)
	default:
		ts.handler.ServeHTTP(w, r)
	}
}

// NewTunserver creates a tunserver with the given HTTP handler
func NewTunserver(handler http.Handler, scheduler api.Scheduler, nodeID string) http.Handler {
	return &Tunserver{
		handler:   handler,
		tunMgr:    &TunManager{tunnels: &sync.Map{}},
		scheduler: scheduler,
		nodeID:    nodeID,
	}
}

func (ts *Tunserver) handleTunclient(w http.ResponseWriter, r *http.Request) {
	log.Debugf("handleTunnel %s", r.URL.Path)
	tunID := r.URL.Query().Get("tunid")
	if len(tunID) == 0 {
		http.Error(w, "tunid can not empty", http.StatusInternalServerError)
		return
	}

	// waitConn, ok := ts.waitConns[tunID]
	// if !ok {
	// 	http.Error(w, "no waitConn exist", http.StatusInternalServerError)
	// 	return
	// }
	t := ts.tunMgr.getTunnel(tunID)
	if t == nil {
		http.Error(w, fmt.Sprintf("no tun %s exist", tunID), http.StatusInternalServerError)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Errorf("Error upgrading to WebSocket:", err)
		return
	}
	defer conn.Close()

	t.conn = conn
	t.waitConnCh <- true

	t.startService()
}

// handleProject accept user connect to project
func (ts *Tunserver) handleProject(w http.ResponseWriter, r *http.Request) {
	log.Debugf("handleProject %s", r.URL.Path)
	// TODO: verify the auth of client
	targetNodeID := ts.getNodeID(r)
	if isInvalidNodeID(targetNodeID) {
		log.Errorf("invalid node id %s", targetNodeID)
		http.Error(w, fmt.Sprintf("invalid node id %s", targetNodeID), http.StatusBadRequest)
		return
	}

	projectID := ts.getProjectID(r)
	if len(projectID) == 0 {
		log.Errorf("request %s not include project id", r.URL.Path)
		http.Error(w, fmt.Sprintf("%s not include project id", r.URL.Path), http.StatusBadRequest)
		return
	}

	relays := r.Header["Relay"]

	opts := TunnelOptions{
		scheduler:    ts.scheduler,
		projectID:    projectID,
		targetNodeID: targetNodeID,
		nodeID:       ts.nodeID,
		tunID:        uuid.NewString(),
		// req:          &TcpReq{conn: conn},
	}

	tunnel, err := newTunnel(context.Background(), ts, relays, &opts)
	if err != nil {
		log.Errorf("new tunnel failed: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		log.Errorf("request %s Hijacking not supported", r.URL.Path)
		http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
		return
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		log.Errorf("Hijack %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	tunnel.req = &TcpReq{conn: conn}

	headerString := ts.getHeaderString(r, targetNodeID, projectID)
	tunnel.onReqMessage([]byte(headerString))

	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Debugf("handleProject, read message failed: %s", err.Error())
			if !IsNetErrUseOfCloseNetworkConnection(err) {
				if err = tunnel.close(); err != nil {
					log.Errorf("close tunnel failed: %s", err.Error())
				}
			}
			return
		}

		// log.Debugf("handleProject recive message %d", n)
		if err = tunnel.onReqMessage(buf[:n]); err != nil {
			log.Errorf("tunnel write %s", err.Error())
		}
	}
}

// handleProject accept user connect to project
func (ts *Tunserver) handleRelay(w http.ResponseWriter, r *http.Request) {
	log.Debugf("handleRelay %s", r.URL.Path)
	// TODO: verify the auth of client
	targetNodeID := ts.getNodeID(r)
	if isInvalidNodeID(targetNodeID) {
		http.Error(w, fmt.Sprintf("invalid node id %s", targetNodeID), http.StatusBadRequest)
		return
	}

	projectID := ts.getProjectID(r)
	if len(projectID) == 0 {
		log.Errorf("request %s not include service id", r.URL.Path)
		http.Error(w, fmt.Sprintf("%s not include project id", r.URL.Path), http.StatusBadRequest)
		return
	}

	relays := r.Header["Relay"]

	opts := TunnelOptions{
		scheduler:    ts.scheduler,
		projectID:    projectID,
		targetNodeID: targetNodeID,
		nodeID:       ts.nodeID,
		tunID:        uuid.NewString(),
		// req:          &WsReq{conn: conn, writeLock: sync.Mutex{}},
	}

	tunnel, err := newTunnel(context.Background(), ts, relays, &opts)
	if err != nil {
		log.Errorf("new tunnel failed: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Errorf("Error upgrading to WebSocket:", err)
		return
	}
	defer conn.Close()

	tunnel.req = &WsReq{conn: conn, writeLock: sync.Mutex{}}

	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			log.Info("handleRelay error, reading message:", err)
			if !IsNetErrUseOfCloseNetworkConnection(err) {
				if err = tunnel.close(); err != nil {
					log.Errorf("close tunnel failed: %s", err.Error())
				}
			}
			return
		}

		if messageType != websocket.BinaryMessage {
			log.Errorf("unsupport message type %d", messageType)
			continue
		}

		// log.Debugf("handleRelay recive message %d", len(p))
		if err = tunnel.onReqMessage(p); err != nil {
			log.Errorf("tunnel write %s", err.Error())
		}
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

func (ts *Tunserver) getProjectID(r *http.Request) string {
	// path = /project/nodeID/project/{custom}
	reqPath := r.URL.Path
	parts := strings.Split(reqPath, "/")
	if len(parts) >= 4 {
		return parts[3]
	}
	return ""
}

func (ts *Tunserver) getHeaderString(r *http.Request, nodeID, projectID string) string {
	// path = /project/{nodeID}/{projectID}/{customPath}
	urlString := r.URL.String()
	prefix := fmt.Sprintf("%s%s/%s", projectPathPrefix, nodeID, projectID)
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

// func (ts *Tunserver) getRelays(r *http.Request) []string {
// 	return r.Header["Relay"]
// }
