package httpserver

import (
	"context"
	"fmt"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"mime"
	"net/http"
	gopath "path"
	"strconv"
	"strings"

	fscrypto "github.com/Filecoin-Titan/titan/node/httpserver/crypto"
	"github.com/gabriel-vasile/mimetype"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/nfnt/resize"
	ffmpeg "github.com/u2takey/ffmpeg-go"
	"golang.org/x/image/bmp"
	"golang.org/x/image/tiff"
)

func init() {
	mime.AddExtensionType(".apk", "application/vnd.android.package-archive")
}

// serveFile serves a single file to the client.
func (hs *HttpServer) serveFile(w http.ResponseWriter, r *http.Request, assetCID string, file files.File, filePass []byte) (int, error) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	root, err := cid.Decode(assetCID)
	if err != nil {
		log.Debugw("Failed to parse asset CID", "cid", assetCID)
		return http.StatusBadRequest, fmt.Errorf("decode car cid error: %s", err.Error())
	}

	contentPath := path.New(r.URL.Path)
	resolvedPath, err := hs.resolvePath(ctx, contentPath, root)
	if err != nil {
		log.Debugw("Failed to resolve file from path", "error", err, "path", r.URL.Path)
		return http.StatusBadRequest, fmt.Errorf("can not resolved path: %s", err.Error())
	}

	// Set Cache-Control and read optional Last-Modified time
	modtime := addCacheControlHeaders(w, r, contentPath, resolvedPath.Cid())

	// Set Content-Disposition
	name := addContentDispositionHeader(w, r, contentPath)

	// Prepare size value for Content-Length HTTP header (set inside of http.ServeContent)
	size, err := file.Size()
	if err != nil {
		log.Debugw("failed to get file size", "error", err, "path", contentPath)
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

	if len(filePass) > 0 {
		streamer, err := fscrypto.NewDecryptPartIPFSStreamer(file, filePass)
		if err != nil {
			log.Debugw("failed to prepare decryption stream", "error", err)
			return http.StatusInternalServerError, fmt.Errorf("decrypt file error: %s", err.Error())
		}
		content.ctr = streamer
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

	log.Debugw("content-type", "name", name, "path", contentPath, "cid", resolvedPath.Cid(), "content-type", ctype, "filesize", size, "modtime", modtime, "cidPath", resolvedPath.String())
	// Setting explicit Content-Type to avoid mime-type sniffing on the client
	// (unifies behavior across gateways and web browsers)
	w.Header().Set("Content-Type", ctype)

	// Check if thumbnail is wanted
	if r.URL.Query().Get("thumb") == "true" {
		hs.serveThumbnail(w, r, content, ctype)
		return 0, nil
	}

	http.ServeContent(w, r, name, modtime, content)

	log.Debugf("Served asset (%s)", contentPath)
	return 0, nil
}

func (hs *HttpServer) serveThumbnail(w http.ResponseWriter, r *http.Request, cnt *lazySeeker, ctype string) {
	widthStr, heightStr := r.URL.Query().Get("width"), r.URL.Query().Get("height")
	width, _ := strconv.Atoi(widthStr)
	height, _ := strconv.Atoi(heightStr)

	if width == 0 {
		width = 150
	}
	if height == 0 {
		height = 150
	}

	var (
		thumbImage image.Image
		err        error
	)

	switch {
	case strings.HasPrefix(ctype, "image"):
		thumbImage, err = resizeImage(cnt, width, height)
	case strings.HasPrefix(ctype, "video"):
		thumbImage, err = extractFirstFrameInVideo(cnt, width, height)
	default:
		http.Error(w, "Unsupported media type for thumbnail", http.StatusUnsupportedMediaType)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = writeImageToResponse(w, thumbImage, ctype)
	if err != nil {
		http.Error(w, "Failed to encode thumbnail: "+err.Error(), http.StatusInternalServerError)
	}
}

func extractFirstFrameInVideo(f io.Reader, width, height int) (image.Image, error) {
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		err := ffmpeg.Input("pipe:0").
			Filter("scale", nil, ffmpeg.KwArgs{"w": width, "h": height}).
			Output("pipe:1", ffmpeg.KwArgs{"vframes": 1, "format": "image2", "vcodec": "mjpeg"}).
			WithInput(f).
			WithOutput(pw).
			Run()
		if err != nil {
			log.Errorf("Failed to extract ffmpeg frame from video: %v", err)
		}
	}()

	img, _, err := image.Decode(pr)
	if err != nil {
		return nil, fmt.Errorf("failed to decode video frame: %w", err)
	}

	return img, nil
	// return nil, fmt.Errorf("not implemented yet")
}

func resizeImage(f io.Reader, width, height int) (image.Image, error) {
	image, _, err := image.Decode(f)
	if err != nil {
		return nil, err
	}
	return resize.Resize(uint(width), uint(height), image, resize.Lanczos3), nil
}

func writeImageToResponse(w http.ResponseWriter, img image.Image, ctype string) error {
	encoder := getEncoder(ctype)
	return encoder(w, img)
}

func getEncoder(ctype string) func(io.Writer, image.Image) error {
	switch {
	case strings.Contains(ctype, "png"):
		return func(w io.Writer, i image.Image) error {
			return png.Encode(w, i)
		}
	case strings.Contains(ctype, "jpeg") || strings.Contains(ctype, "jpg"):
		return func(w io.Writer, i image.Image) error {
			return jpeg.Encode(w, i, &jpeg.Options{Quality: jpeg.DefaultQuality})
		}
	case strings.Contains(ctype, "gif"):
		return func(w io.Writer, i image.Image) error {
			return gif.Encode(w, i, nil)
		}
	case strings.Contains(ctype, "tiff"):
		return func(w io.Writer, i image.Image) error {
			return tiff.Encode(w, i, &tiff.Options{Compression: tiff.Deflate, Predictor: true})
		}
	case strings.Contains(ctype, "bmp"):
		return func(w io.Writer, i image.Image) error {
			return bmp.Encode(w, i)
		}
	// case strings.Contains(ctype, "webp"):
	// return func(w io.Writer, i image.Image) error {
	// 	return webp.Encode(w, i, &webp.Options{Lossless: true, Quality: webp.DefaulQuality})
	// }
	default:
		return func(w io.Writer, i image.Image) error {
			return fmt.Errorf("unsupported image format: %s", ctype)
		}
	}
}
