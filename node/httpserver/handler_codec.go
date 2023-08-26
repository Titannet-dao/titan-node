package httpserver

import (
	"fmt"
	"net/http"
)

// serveCodec serves requests for codec endpoints.
func (hs *HttpServer) serveCodec(w http.ResponseWriter, r *http.Request, assetCID string) (int, error) {
	return http.StatusBadRequest, fmt.Errorf("not implement")
}
