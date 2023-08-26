package httpserver

import (
	"context"
	"fmt"
	"html"
	"net/http"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

// serveTAR responds to an HTTP request with the content at the specified path in a TAR archive format.
func (hs *HttpServer) serveTAR(w http.ResponseWriter, r *http.Request, assetCID string) (int, error) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	rootCID, err := cid.Decode(assetCID)
	if err != nil {
		return http.StatusBadRequest, fmt.Errorf("decode car cid error: %s", err.Error())
	}

	contentPath := path.New(r.URL.Path)
	resolvedPath, err := hs.resolvePath(ctx, contentPath, rootCID)
	if err != nil {
		return http.StatusBadRequest, fmt.Errorf("can not resolved path: %s", err.Error())
	}

	// Get Unixfs file
	file, err := hs.getUnixFsNode(ctx, resolvedPath, rootCID)
	if err != nil {
		return http.StatusInternalServerError, fmt.Errorf("error getting UnixFS node for %s: %w", html.EscapeString(contentPath.String()), err)
	}
	defer file.Close() //nolint:errcheck // ignore error

	targetCID := resolvedPath.Cid()

	// Set Cache-Control and read optional Last-Modified time
	modtime := addCacheControlHeaders(w, r, contentPath, targetCID)

	// Weak Etag W/ because we can't guarantee byte-for-byte identical
	// responses, but still want to benefit from HTTP Caching. Two TAR
	// responses for the same CID will be logically equivalent,
	// but when TAR is streamed, then in theory, files and directories
	// may arrive in different order (depends on TAR lib and filesystem/inodes).
	etag := `W/` + getEtag(r, targetCID)
	w.Header().Set("Etag", etag)

	// Finish early if Etag match
	if r.Header.Get("If-None-Match") == etag {
		return http.StatusNotModified, fmt.Errorf("header If-None-Match == %s", etag)
	}

	// Set Content-Disposition
	var name string
	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = rootCID.String() + ".tar"
	}
	setContentDispositionHeader(w, name, "attachment")

	// Construct the TAR writer
	tarw, err := files.NewTarWriter(w)
	if err != nil {
		return http.StatusInternalServerError, fmt.Errorf("could not build tar writer: %s", err.Error())
	}
	defer tarw.Close() //nolint:errcheck // ignore error

	// Sets correct Last-Modified header. This code is borrowed from the standard
	// library (net/http/server.go) as we cannot use serveFile without throwing the entire
	// TAR into the memory first.
	if !(modtime.IsZero() || modtime.Equal(time.Unix(0, 0))) {
		w.Header().Set("Last-Modified", modtime.UTC().Format(http.TimeFormat))
	}

	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("X-Content-Type-Options", "nosniff") // no funny business in the browsers :^)

	// The TAR has a top-level directory (or file) named by the CID.
	err = tarw.WriteFile(file, rootCID.String())
	if err != nil {
		return http.StatusInternalServerError, err
	}

	return 0, nil
}
