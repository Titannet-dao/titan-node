package httpserver

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

func (hs *HttpServer) serveUnixFS(w http.ResponseWriter, r *http.Request, credentials *types.Credentials) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	root, err := cid.Decode(credentials.AssetCID)
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

	dr, err := hs.getUnixFsNode(ctx, resolvedPath, root)
	if err != nil {
		http.Error(w, fmt.Sprintf("error while getting UnixFS node: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	defer dr.Close() //nolint:errcheck // ignore error

	// Handling Unixfs file
	if f, ok := dr.(files.File); ok {
		log.Debugw("serving unixfs file", "path", contentPath)
		hs.serveFile(w, r, credentials, f)
		return
	}

	// Handling Unixfs directory
	dir, ok := dr.(files.Directory)
	if !ok {
		http.Error(w, "unsupported UnixFS type", http.StatusInternalServerError)
		return
	}

	log.Debugw("serving unixfs directory", "path", contentPath)
	hs.serveDirectory(w, r, credentials, dir)
}

func (hs *HttpServer) serveDirectory(w http.ResponseWriter, r *http.Request, ticket *types.Credentials, dir files.Directory) {
	http.Error(w, "dir list not support now", http.StatusBadRequest)
}
