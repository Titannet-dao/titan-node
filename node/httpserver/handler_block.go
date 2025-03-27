package httpserver

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

// serveRawBlock retrieves a raw block from the asset using the given tokenPayload and URL path,
// sets the appropriate headers, and serves the block to the client.
func (hs *HttpServer) serveRawBlock(w http.ResponseWriter, r *http.Request, assetCID string) (int, error) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	root, err := cid.Decode(assetCID)
	if err != nil {
		log.Debugw("unable to decode baseCid", "error", err)
		return http.StatusBadRequest, fmt.Errorf("decode root cid %s error: %s", assetCID, err.Error())
	}

	contentPath := path.New(r.URL.Path)
	resolvedPath, err := hs.resolvePath(ctx, contentPath, root)
	if err != nil {
		log.Debugw("path resolve failed", "error", err)
		return http.StatusBadRequest, fmt.Errorf("can not resolved path: %s", err.Error())
	}

	c := resolvedPath.Cid()
	block, err := hs.asset.GetBlock(ctx, root, c)
	if err != nil {
		log.Debugw("error while getting content", "error", err)
		return http.StatusInternalServerError, fmt.Errorf("can not get block %s, %s", c.String(), err.Error())
	}

	// TODO: limit rate
	content := bytes.NewReader(block.RawData())

	// Set Content-Disposition
	var name string
	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = c.String() + ".bin"
	}
	setContentDispositionHeader(w, name, "attachment")

	// Set remaining headers
	w.Header().Set("Content-Type", "application/vnd.ipld.raw")
	w.Header().Set("X-Content-Type-Options", "nosniff") // no funny business in the browsers :^)

	modtime := addCacheControlHeaders(w, r, contentPath, c)
	// ServeContent will take care of
	// If-None-Match+Etag, Content-Length and range requests
	http.ServeContent(w, r, name, modtime, content)
	return 0, nil
}
