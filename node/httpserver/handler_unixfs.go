package httpserver

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

// return true if is directory
func (hs *HttpServer) serveUnixFS(w http.ResponseWriter, r *http.Request, assetCID string, filePass []byte) (bool, int, error) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	root, err := cid.Decode(assetCID)
	if err != nil {
		return false, http.StatusBadRequest, fmt.Errorf("decode car cid error: %s", err.Error())
	}

	contentPath := path.New(r.URL.Path)
	resolvedPath, err := hs.resolvePath(ctx, contentPath, root)
	if err != nil {
		return false, http.StatusBadRequest, fmt.Errorf("can not resolved path: %s", err.Error())
	}

	node, err := hs.getUnixFsNode(ctx, resolvedPath, root)
	if err != nil {
		return false, http.StatusInternalServerError, fmt.Errorf("error while getting UnixFS node: %s", err.Error())
	}
	defer node.Close() //nolint:errcheck // ignore error

	// Handling Unixfs file
	if f, ok := node.(files.File); ok {
		log.Debugw("serving unixfs file", "path", contentPath, "assetCid", assetCID, "filePass", filePass)
		statusCode, err := hs.serveFile(w, r, assetCID, f, filePass)
		return false, statusCode, err
	}

	// Handling Unixfs directory
	dir, ok := node.(files.Directory)
	if !ok {
		return false, http.StatusInternalServerError, fmt.Errorf("unsupported UnixFS type")
	}

	log.Debugw("serving unixfs directory", "path", contentPath)
	statusCode, err := hs.serveDirectory(w, r, assetCID, dir)
	return true, statusCode, err
}
