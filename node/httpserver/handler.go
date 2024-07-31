package httpserver

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/lib/limiter"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

var log = logging.Logger("httpserver")

const (
	reqIpfs   = "/ipfs"
	reqUpload = "/upload"
	// upload files
	reqUploadv2           = "/uploadv2"
	reqRpc                = "/rpc"
	reqLease              = "/lease"
	immutableCacheControl = "public, max-age=29030400, immutable"
	domainFields          = 4
)

var onlyASCII = regexp.MustCompile("[[:^ascii:]]")

type Handler struct {
	handler http.Handler
	hs      *HttpServer
}

func resetPath(r *http.Request) {
	host, _, err := net.SplitHostPort(r.Host)
	if err != nil {
		log.Debugf("SplitHostPort error :%s", err.Error())
		return
	}

	ip := net.ParseIP(host)
	if ip != nil {
		return
	}

	fields := strings.Split(host, ".")
	if len(fields) >= domainFields {
		r.URL.Path = "/ipfs/" + fields[0] + r.URL.Path
	}

}

func getFirstPathSegment(path string) string {
	// Trim leading and trailing slashes
	path = strings.Trim(path, "/")
	// Split the path into segments
	segments := strings.Split(path, "/")
	// Return the first segment
	if len(segments) > 0 {
		return "/" + segments[0]
	}
	return ""
}

// ServeHTTP checks if the request path starts with the IPFS path prefix and delegates to the appropriate handler
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.URL.Path, reqIpfs) &&
		!strings.Contains(r.URL.Path, reqUpload) &&
		!strings.Contains(r.URL.Path, reqRpc) &&
		!strings.Contains(r.URL.Path, reqLease) &&
		!strings.Contains(r.URL.Path, reqUploadv2) {
		resetPath(r)
	}

	reqPath := getFirstPathSegment(r.URL.Path)

	switch reqPath {
	case reqIpfs:
		h.hs.handler(w, r)
	case reqUpload:
		h.hs.uploadHandler(w, r)
	case reqUploadv2:
		h.hs.uploadv2Handler(w, r)
	case reqLease:
		h.hs.LeaseShellHandler(w, r)
	default:
		h.handler.ServeHTTP(w, r)
	}
}

// NewHandler creates a new Handler with the given HTTP handler
func (hs *HttpServer) NewHandler(handler http.Handler) http.Handler {
	return &Handler{handler, hs}
}

// handler is the main request handler for serving titan content
func (hs *HttpServer) handler(w http.ResponseWriter, r *http.Request) {
	log.Debugf("handler path: %s", r.URL.Path)

	setAccessControlAllowForHeader(w)

	switch r.Method {
	case http.MethodOptions:
		return
	case http.MethodHead:
		hs.headHandler(w, r)
	case http.MethodGet:
		limiter := limiter.NewWriter(w, hs.rateLimiter.BandwidthUpLimiter)
		hs.getHandler(limiter, r)
	default:
		http.Error(w, fmt.Sprintf("method %s not allowed", r.Method), http.StatusBadRequest)
	}
}

// getEtag generates an Etag value based on the HTTP request and CID
func getEtag(r *http.Request, cid cid.Cid) string {
	prefix := `"`
	suffix := `"`
	responseFormat, _, err := customResponseFormat(r)
	if err == nil && responseFormat != "" {
		// application/vnd.ipld.foo → foo
		// application/x-bar → x-bar
		shortFormat := responseFormat[strings.LastIndexAny(responseFormat, "/.")+1:]
		// Etag: "cid.shortFmt" (gives us nice compression together with Content-Disposition in block (raw) and car responses)
		suffix = `.` + shortFormat + suffix
	}
	return prefix + cid.String() + suffix
}

// setContentDispositionHeader sets the Content-Disposition header to an arbitrary filename and disposition
func setContentDispositionHeader(w http.ResponseWriter, filename string, disposition string) {
	utf8Name := url.PathEscape(filename)
	asciiName := url.PathEscape(onlyASCII.ReplaceAllLiteralString(filename, "_"))
	w.Header().Set("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"; filename*=UTF-8''%s", disposition, asciiName, utf8Name))
}

// addCacheControlHeaders sets the Cache-Control and Last-Modified headers based on the contentPath properties
func addCacheControlHeaders(w http.ResponseWriter, r *http.Request, contentPath path.Path, fileCid cid.Cid) (modtime time.Time) {
	// Set Etag to based on CID (override whatever was set before)
	w.Header().Set("Etag", getEtag(r, fileCid))

	// Set Cache-Control and Last-Modified based on contentPath properties
	if contentPath.Mutable() {
		modtime = time.Now()
	} else {
		// immutable! CACHE ALL THE THINGS, FOREVER! wolololol
		// w.Header().Set("Cache-Control", immutableCacheControl)

		// Set modtime to 'zero time' to disable Last-Modified header (superseded by Cache-Control)
		modtime = time.Unix(0, 0)
	}
	return modtime
}

// getFilename returns the filename component of a path
func getFilename(contentPath path.Path) string {
	s := contentPath.String()
	if strings.HasPrefix(s, reqIpfs) {
		// Don't want to treat ipfs.io in /ipns/ipfs.io as a filename.
		return ""
	}
	return s
}

// addContentDispositionHeader set Content-Disposition if filename URL query param is present, return preferred filename
func addContentDispositionHeader(w http.ResponseWriter, r *http.Request, contentPath path.Path) string {
	/* This logic enables:
	 * - creation of HTML links that trigger "Save As.." dialog instead of being rendered by the browser
	 * - overriding the filename used when saving subresource assets on HTML page
	 * - providing a default filename for HTTP clients when downloading direct /ipfs/CID without any subpath
	 */

	// URL param ?filename=cat.jpg triggers Content-Disposition: [..] filename
	// which impacts default name used in "Save As.." dialog
	name := getFilename(contentPath)
	urlFilename := r.URL.Query().Get("filename")
	if urlFilename != "" {
		disposition := "inline"
		// URL param ?download=true triggers Content-Disposition: [..] attachment
		// which skips rendering and forces "Save As.." dialog in browsers
		if r.URL.Query().Get("download") == "true" {
			disposition = "attachment"
		}
		setContentDispositionHeader(w, urlFilename, disposition)
		name = urlFilename
	}
	return name
}

func setAccessControlAllowForHeader(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Authorization")
}
