package handler

import (
	"context"
	"net/http"

	"github.com/filecoin-project/go-jsonrpc/auth"
)

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

	h.handler.ServeHTTP(w, r.WithContext(ctx))
}
