package httpserver

import (
	"net/http"

	"github.com/Filecoin-Titan/titan/api/types"
)

// serveCodec serves requests for codec endpoints.
func (hs *HttpServer) serveCodec(w http.ResponseWriter, r *http.Request, credentials *types.Credentials) {
	http.Error(w, "not implement", http.StatusBadRequest)
}
