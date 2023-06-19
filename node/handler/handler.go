package handler

import (
	"context"
	"net/http"
	"strings"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("handler")

type (
	// RemoteAddr client address
	RemoteAddr struct{}
	// user id (node id)
	ID struct{}
)

// Handler represents an HTTP handler that also adds remote client address and node ID to the request context
type Handler struct {
	// handler *auth.Handler
	verify func(ctx context.Context, token string) (*types.JWTPayload, error)
	next   http.HandlerFunc
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
	// check role
	if !api.HasPerm(ctx, api.RoleDefault, api.RoleEdge) && !api.HasPerm(ctx, api.RoleDefault, api.RoleCandidate) {
		log.Warnf("client is not edge and candidate")
		return ""
	}

	v, ok := ctx.Value(ID{}).(string)
	if !ok {
		return ""
	}

	return v
}

// GetUserID returns the user ID of the client
func GetUserID(ctx context.Context) string {
	// check role
	if !api.HasPerm(ctx, api.RoleDefault, api.RoleUser) {
		return ""
	}

	v, ok := ctx.Value(ID{}).(string)
	if !ok {
		return ""
	}
	return v
}

// New returns a new HTTP handler with the given auth handler and additional request context fields
func New(verify func(ctx context.Context, token string) (*types.JWTPayload, error), next http.HandlerFunc) http.Handler {
	return &Handler{verify, next}
}

// ServeHTTP serves an HTTP request with the added client remote address and node ID in the request context
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	remoteAddr := r.Header.Get("X-Remote-Addr")
	if remoteAddr == "" {
		remoteAddr = r.RemoteAddr
	}

	ctx := r.Context()
	ctx = context.WithValue(ctx, RemoteAddr{}, remoteAddr)

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

		payload, err := h.verify(ctx, token)
		if err != nil {
			log.Warnf("JWT Verification failed (originating from %s): %s, token:%s", r.RemoteAddr, err, token)
			w.WriteHeader(401)
			return
		}

		ctx = context.WithValue(ctx, ID{}, payload.ID)
		ctx = api.WithPerm(ctx, payload.Allow)
	}

	h.next(w, r.WithContext(ctx))
}
