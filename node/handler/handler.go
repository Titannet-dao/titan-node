package handler

import (
	"context"
	"net/http"
	"strings"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/filecoin-project/go-jsonrpc/auth"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("handler")

type (
	// RemoteAddr client address
	RemoteAddr struct{}
	// RemoteAddr node ID
	NodeID struct{}
)

// Handler represents an HTTP handler that also adds remote client address and node ID to the request context
type Handler struct {
	handler *auth.Handler
}

// GetRemoteAddr returns the remote address of the client
func GetRemoteAddr(ctx context.Context) string {
	v, ok := ctx.Value(RemoteAddr{}).(string)
	if !ok {
		return ""
	}
	return v
}

// GetNodeID returns the node ID of the client
func GetNodeID(ctx context.Context) string {
	v, ok := ctx.Value(NodeID{}).(string)
	if !ok {
		return ""
	}
	return v
}

// New returns a new HTTP handler with the given auth handler and additional request context fields
func New(ah *auth.Handler) http.Handler {
	return &Handler{ah}
}

// ServeHTTP serves an HTTP request with the added client remote address and node ID in the request context
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	remoteAddr := r.Header.Get("X-Remote-Addr")
	if remoteAddr == "" {
		remoteAddr = r.RemoteAddr
	}

	nodeID := r.Header.Get("Node-ID")

	ctx := r.Context()
	ctx = context.WithValue(ctx, RemoteAddr{}, remoteAddr)
	ctx = context.WithValue(ctx, NodeID{}, nodeID)

	token := r.Header.Get("Authorization")
	if token == "" {
		token = r.FormValue("token")
		if token != "" {
			token = "Bearer " + token
		}
	}

	if token != "" {
		if !strings.HasPrefix(token, "Bearer ") {
			log.Warn("missing Bearer prefix in auth header")
			w.WriteHeader(401)
			return
		}
		token = strings.TrimPrefix(token, "Bearer ")

		allow, err := h.handler.Verify(ctx, token)
		if err != nil {
			log.Warnf("JWT Verification failed (originating from %s): %s", r.RemoteAddr, err)
			w.WriteHeader(401)
			return
		}

		ctx = api.WithPerm(ctx, allow)
	}

	h.handler.Next(w, r.WithContext(ctx))
}
