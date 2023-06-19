package httpserver

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

// serveRawBlock retrieves a raw block from the asset using the given tokenPayload and URL path,
// sets the appropriate headers, and serves the block to the client.
func (hs *HttpServer) serveRawBlock(w http.ResponseWriter, r *http.Request, tkPayload *types.TokenPayload) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	startTime := time.Now()

	root, err := cid.Decode(tkPayload.AssetCID)
	if err != nil {
		http.Error(w, fmt.Sprintf("decode root cid %s error: %s", tkPayload.AssetCID, err.Error()), http.StatusBadRequest)
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

	// if not come from sdk, don't send workload
	if len(tkPayload.ID) == 0 {
		return
	}

	duration := time.Since(startTime)
	speed := float64(0)
	if duration > 0 {
		speed = float64(len(block.RawData())) / float64(duration) * float64(time.Second)
	}

	workload := &types.Workload{
		DownloadSpeed: int64(speed),
		DownloadSize:  int64(len(block.RawData())),
		StartTime:     startTime.Unix(),
		EndTime:       time.Now().Unix(),
	}

	report := &report{
		TokenID:  tkPayload.ID,
		ClientID: tkPayload.ClientID,
		Workload: workload,
	}
	hs.reporter.addReport(report)
}
