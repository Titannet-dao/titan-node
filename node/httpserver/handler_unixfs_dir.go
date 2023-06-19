package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

const (
	maxLen = 50
)

func (hs *HttpServer) serveDirectory(w http.ResponseWriter, r *http.Request, tkPayload *types.TokenPayload, dir files.Directory) {
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

	directory, err := hs.lsUnixFsDir(ctx, resolvedPath, root)
	if err != nil {
		http.Error(w, fmt.Sprintf("ls unix file dir: %s", err.Error()), http.StatusBadRequest)
		return
	}

	var count = 0
	var fileList string
	err = directory.ForEachLink(ctx, func(l *format.Link) error {
		log.Debugf("name %s cid %s", l.Name, l.Cid)
		downloadLink := fmt.Sprintf("<a href=\"%s%s\">%s</a>", getCurrentPath(r), l.Name, l.Name)

		// 构建文件列表项
		fileList += fmt.Sprintf(`
			<tr>
				<td>%s</td>
				<td>%s</td>
			</tr>`, downloadLink, l.Cid)

		count++
		if count >= maxLen {
			return fmt.Errorf("directory link out of max len %d", maxLen)
		}

		return nil
	})

	if err != nil {
		log.Errorf("directory forEach error %s", err.Error())
	}

	htmlText := fmt.Sprintf(webHTML, "100%", resolvedPath.Cid().String(), fileList)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, htmlText)
}

func getCurrentPath(r *http.Request) string {
	if strings.HasSuffix(r.URL.Path, "/") {
		return r.URL.Path
	}
	return r.URL.Path + "/"
}
