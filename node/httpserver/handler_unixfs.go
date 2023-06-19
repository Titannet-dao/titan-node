package httpserver

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

func (hs *HttpServer) serveUnixFS(w http.ResponseWriter, r *http.Request, tkPayload *types.TokenPayload) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	root, err := cid.Decode(tkPayload.AssetCID)
	if err != nil {
		http.Error(w, fmt.Sprintf("decode car cid error: %s", err.Error()), http.StatusBadRequest)
		return
	}

	contentPath := path.New(r.URL.Path)
	resolvedPath, err := hs.resolvePath(ctx, contentPath, root)
	if err != nil {
		http.Error(w, fmt.Sprintf("can not resolved path: %s", err.Error()), http.StatusBadRequest)
		return
	}

	node, err := hs.getUnixFsNode(ctx, resolvedPath, root)
	if err != nil {
		http.Error(w, fmt.Sprintf("error while getting UnixFS node: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	defer node.Close() //nolint:errcheck // ignore error

	// Handling Unixfs file
	if f, ok := node.(files.File); ok {
		log.Debugw("serving unixfs file", "path", contentPath)
		hs.serveFile(w, r, tkPayload, f)
		return
	}

	// Handling Unixfs directory
	dir, ok := node.(files.Directory)
	if !ok {
		http.Error(w, "unsupported UnixFS type", http.StatusInternalServerError)
		return
	}

	log.Debugw("serving unixfs directory", "path", contentPath)
	hs.serveDirectory(w, r, tkPayload, dir)
}
