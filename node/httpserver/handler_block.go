package httpserver

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

// serveRawBlock retrieves a raw block from the asset using the given credentials and URL path,
// sets the appropriate headers, and serves the block to the client.
func (hs *HttpServer) serveRawBlock(w http.ResponseWriter, r *http.Request, credentials *types.Credentials) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	root, err := cid.Decode(credentials.AssetCID)
	if err != nil {
		http.Error(w, fmt.Sprintf("decode root cid %s error: %s", credentials.AssetCID, err.Error()), http.StatusBadRequest)
		return
	}

	contentPath := path.New(r.URL.Path)
	resolvedPath, err := hs.resolvePath(ctx, contentPath, root)
	if err != nil {
		http.Error(w, fmt.Sprintf("can not resolved path: %s", err.Error()), http.StatusBadRequest)
		return
	}

	c := resolvedPath.Cid()
	block, err := hs.asset.GetBlock(ctx, root, c)
	if err != nil {
		http.Error(w, fmt.Sprintf("can not get block %s, %s", c.String(), err.Error()), http.StatusInternalServerError)
		return
	}

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

	// TODO: limit rate and report to scheduler
}
