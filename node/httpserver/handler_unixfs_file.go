package httpserver

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	gopath "path"
	"strings"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/gabriel-vasile/mimetype"
	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

// serveFile serves a single file to the client.
func (hs *HttpServer) serveFile(w http.ResponseWriter, r *http.Request, credentials *types.Credentials, file files.File) {
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

	// Set Cache-Control and read optional Last-Modified time
	modtime := addCacheControlHeaders(w, r, contentPath, resolvedPath.Cid())

	// Set Content-Disposition
	name := addContentDispositionHeader(w, r, contentPath)

	// Prepare size value for Content-Length HTTP header (set inside of http.ServeContent)
	size, err := file.Size()
	if err != nil {
		http.Error(w, fmt.Sprintf("cannot serve files with unknown sizes: %s", err.Error()), http.StatusBadGateway)
		return
	}

	if size == 0 {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		return
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
				http.Error(w, fmt.Sprintf("cannot detect content-type: %s", err.Error()), http.StatusInternalServerError)
			}

			ctype = mimeType.String()
			_, err = content.Seek(0, io.SeekStart)
			if err != nil {
				http.Error(w, "seeker can't seek", http.StatusInternalServerError)
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
}
