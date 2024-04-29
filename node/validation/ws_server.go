package validation

import (
	"crypto/rsa"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/gorilla/websocket"
)

const (
	validationPathPrefix = "/validation/"
	validateDuration     = 10
	nodeIDLengthV0       = 34
	nodeIDLengthV1       = 38
	edgeNodeIDPrefix     = "e_"
	candidteNodeIDPrefix = "c_"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Handler struct {
	handler        http.Handler
	scheduler      api.Scheduler
	privateKey     *rsa.PrivateKey
	blockWaiterMap *sync.Map
}

// ServeHTTP checks if the request path starts with the IPFS path prefix and delegates to the appropriate handler
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, validationPathPrefix):
		h.handleValidation(w, r)
	default:
		h.handler.ServeHTTP(w, r)
	}
}

// NewHandler creates a new Handler with the given HTTP handler
func AppendHandler(handler http.Handler, scheduler api.Scheduler, privateKey *rsa.PrivateKey) http.Handler {
	return &Handler{handler: handler, scheduler: scheduler, privateKey: privateKey, blockWaiterMap: &sync.Map{}}
}

func (h *Handler) handleValidation(w http.ResponseWriter, r *http.Request) {
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

	conn.SetReadDeadline(time.Now().Add((validateDuration + 3) * time.Second))

	_, ok := h.blockWaiterMap.Load(nodeID)
	if ok {
		log.Errorf("Validator already wait for device %s", nodeID)
		return
	}

	log.Infof("node %s connect to Validator", nodeID)

	ch := make(chan []byte, 10)
	opts := blockWaiterOptions{nodeID: nodeID, ch: ch, duration: validateDuration, scheduler: h.scheduler, privateKey: h.privateKey}
	bw := newBlockWaiter(&opts)
	h.blockWaiterMap.Store(nodeID, bw)

	defer func() {
		h.blockWaiterMap.Delete(nodeID)
	}()

	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			log.Info("Error reading message:", err)
			close(ch)
			return
		}

		if messageType == websocket.CloseMessage {
			bw.result.Token = string(p)
			return
		}

		ch <- p
		// log.Debugf("Received message len: %d, type: %d\n", len(p), messageType)
	}
}

func isInvalidNodeID(nodeID string) bool {
	if !strings.HasPrefix(nodeID, edgeNodeIDPrefix) && !strings.HasPrefix(nodeID, candidteNodeIDPrefix) {
		log.Errorf("%s not start with e_ or c_", nodeID)
		return true
	}
	if len(nodeID) == nodeIDLengthV0 || len(nodeID) == nodeIDLengthV1 {
		return false
	}

	log.Errorf("%s not match len %d and %d", nodeID, nodeIDLengthV0, nodeIDLengthV1)
	return true
}
