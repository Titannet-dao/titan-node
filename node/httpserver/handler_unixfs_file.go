package httpserver

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	gopath "path"
	"strings"

	"github.com/gabriel-vasile/mimetype"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

// serveFile serves a single file to the client.
func (hs *HttpServer) serveFile(w http.ResponseWriter, r *http.Request, assetCID string, file files.File) (int, error) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	root, err := cid.Decode(assetCID)
	if err != nil {
		return http.StatusBadRequest, fmt.Errorf("decode car cid error: %s", err.Error())
	}

	contentPath := path.New(r.URL.Path)
	resolvedPath, err := hs.resolvePath(ctx, contentPath, root)
	if err != nil {
		return http.StatusBadRequest, fmt.Errorf("can not resolved path: %s", err.Error())
	}

	// Set Cache-Control and read optional Last-Modified time
	modtime := addCacheControlHeaders(w, r, contentPath, resolvedPath.Cid())

	// Set Content-Disposition
	name := addContentDispositionHeader(w, r, contentPath)

	// Prepare size value for Content-Length HTTP header (set inside of http.ServeContent)
	size, err := file.Size()
	if err != nil {
		return http.StatusBadGateway, fmt.Errorf("cannot get files with unknown sizes: %s", err.Error())
	}

	if size == 0 {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		return 0, nil
	}

	// Lazy seeker enables efficient range-requests and HTTP HEAD responses
	content := &lazySeeker{
		size:   size,
		reader: file,
	}

	// Calculate deterministic value for Content-Type HTTP header
	// (we prefer to do it here, rather than using implicit sniffing in http.ServeContent)
	var ctype string
	if _, isSymlink := file.(*files.Symlink); isSymlink {
		// We should be smarter about resolving symlinks but this is the
		// "most correct" we can be without doing that.
		ctype = "inode/symlink"
	} else {
		ctype = mime.TypeByExtension(gopath.Ext(name))
		if ctype == "" {
			// uses https://github.com/gabriel-vasile/mimetype library to determine the content type.
			// Fixes https://github.com/ipfs/kubo/issues/7252
			mimeType, err := mimetype.DetectReader(content)
			if err != nil {
				return http.StatusInternalServerError, fmt.Errorf("cannot detect content-type: %s", err.Error())
			}

			ctype = mimeType.String()
			_, err = content.Seek(0, io.SeekStart)
			if err != nil {
				return http.StatusInternalServerError, fmt.Errorf("seeker can't seek")
			}
		}
		// Strip the encoding from the HTML Content-Type header and let the
		// browser figure it out.
		if strings.HasPrefix(ctype, "text/html;") {
			ctype = "text/html"
		}
	}
	// Setting explicit Content-Type to avoid mime-type sniffing on the client
	// (unifies behavior across gateways and web browsers)
	w.Header().Set("Content-Type", ctype)
	http.ServeContent(w, r, name, modtime, content)

	if countWrite, ok := w.(*SpeedCountWriter); ok {
		if countWrite.dataSize == 0 {
			countWrite.dataSize = size
		}
	}
	return 0, nil
}
