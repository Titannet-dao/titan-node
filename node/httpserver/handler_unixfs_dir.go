package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

const (
	defaultCount = 10
	maxCount     = 10000
)

// SubAsset the content of directory
type FileProperty struct {
	FileName string
	CID      string
	Size     int64
	Link     string
}

type FileList struct {
	FileProperties []FileProperty
	Name           string
	Size           int64
	Total          int
}

type DirectoryResult struct {
	Data FileList `json:"data"`
	Code int      `json:"code"`
	Msg  string   `json:"msg"`
}

func (hs *HttpServer) serveDirectory(w http.ResponseWriter, r *http.Request, assetCID string, dir files.Directory) (int, error) {
	ret := DirectoryResult{}
	result, statusCode, err := hs.listFile(r, assetCID, dir)
	if err != nil {
		ret.Code = -1
		ret.Msg = fmt.Sprintf("Status code %d, error %s", statusCode, err.Error())
	}
	ret.Data = *result

	buf, err := json.Marshal(ret)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)

	return 0, nil
}

func (hs *HttpServer) listFile(r *http.Request, assetCID string, dir files.Directory) (*FileList, int, error) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	root, err := cid.Decode(assetCID)
	if err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("decode car cid error: %s", err.Error())
	}

	contentPath := path.New(r.URL.Path)
	resolvedPath, err := hs.resolvePath(ctx, contentPath, root)
	if err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("can not resolved path: %s", err.Error())
	}

	directory, err := hs.lsUnixFsDir(ctx, resolvedPath, root)
	if err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("ls unix file dir: %s", err.Error())
	}

	// TODO get all links is cost cpu and memory
	links, err := directory.Links(context.Background())
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("directory get links error: %s", err.Error())
	}

	page := getPage(r)
	count := getCount(r)
	start := page * count
	end := start + count

	if end > len(links) {
		end = len(links)
	}

	fileList := make([]FileProperty, 0)
	for i := start; i < end; i++ {
		l := links[i]
		fileProperty := FileProperty{
			FileName: l.Name,
			CID:      l.Cid.String(),
			Size:     int64(l.Size),
			Link:     fmt.Sprintf("%s%s%s", getCurrentPath(r), l.Name, addQueryString(r, "filename", l.Name)),
		}
		directory.Find(context.Background(), l.Name)
		fileList = append(fileList, fileProperty)

	}

	// if err != nil {
	// 	log.Errorf("directory foreach error %s", err.Error())
	// }

	size, err := dir.Size()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	dirName := r.URL.Query().Get("filename")

	return &FileList{FileProperties: fileList, Name: dirName, Size: size, Total: len(links)}, 0, nil
}

func getCurrentPath(r *http.Request) string {
	if strings.HasSuffix(r.URL.Path, "/") {
		return r.URL.Path
	}
	return r.URL.Path + "/"
}

func addQueryString(r *http.Request, k, v string) string {
	q := r.URL.Query()
	q.Set(k, v)
	return "?" + q.Encode()
}

func getPage(r *http.Request) int {
	page := r.URL.Query().Get("page")
	i, err := strconv.Atoi(page)
	if err != nil {
		log.Infof("get page error %s", err.Error())
		return 0
	}
	return i
}

func getCount(r *http.Request) int {
	count := r.URL.Query().Get("count")
	i, err := strconv.Atoi(count)
	if err != nil {
		log.Infof("get count error %s", err.Error())
		return defaultCount
	}

	if i > maxCount {
		i = maxCount
	}
	return i
}
