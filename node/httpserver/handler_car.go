package httpserver

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

// ServeCar handles HTTP requests for serving CAR files
func (hs *HttpServer) serveCar(w http.ResponseWriter, r *http.Request, credentials *types.Credentials, carVersion string) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	switch carVersion {
	case "": // noop, client does not care about version
	case "1":
	case "2":
	default:
		http.Error(w, fmt.Sprintf("not support car version %s", carVersion), http.StatusBadRequest)
		return
	}

	root, err := cid.Decode(credentials.AssetCID)
	if err != nil {
		http.Error(w, fmt.Sprintf("decode root cid error: %s", err.Error()), http.StatusBadRequest)
		return
	}

	contentPath := path.New(r.URL.Path)
	resolvedPath, err := hs.resolvePath(ctx, contentPath, root)
	if err != nil {
		http.Error(w, fmt.Sprintf("can not resolved path: %s", err.Error()), http.StatusBadRequest)
		return
	}
	rootCID := resolvedPath.Cid()

	has, err := hs.asset.AssetExists(rootCID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !has {
		http.Error(w, fmt.Sprintf("can not found car %s", contentPath.String()), http.StatusNotFound)
		return
	}

	// Set Content-Disposition
	var name string
	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = rootCID.String() + ".car"
	}
	setContentDispositionHeader(w, name, "attachment")

	// Set Cache-Control (same logic as for a regular files)
	addCacheControlHeaders(w, r, contentPath, rootCID)

	// Weak Etag W/ because we can't guarantee byte-for-byte identical
	// responses, but still want to benefit from HTTP Caching. Two CAR
	// responses for the same CID and selector will be logically equivalent,
	// but when CAR is streamed, then in theory, blocks may arrive from
	// datastore in non-deterministic order.
	etag := `W/` + getEtag(r, rootCID)
	w.Header().Set("Etag", etag)

	// Finish early if Etag match
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.Header().Set("Content-Type", "application/vnd.ipld.car; version=1")
	w.Header().Set("X-Content-Type-Options", "nosniff") // no funny business in the browsers :^)

	modtime := addCacheControlHeaders(w, r, contentPath, rootCID)

	reader, err := hs.asset.GetAsset(rootCID)
	if err != nil {
		http.Error(w, fmt.Sprintf("not support car version %s", carVersion), http.StatusInternalServerError)
		return
	}
	defer reader.Close() //nolint:errcheck  // ignore error

	// If-None-Match+Etag, Content-Length and range requests
	http.ServeContent(w, r, name, modtime, reader)

	// TODO: limit rate and report to scheduler
}
