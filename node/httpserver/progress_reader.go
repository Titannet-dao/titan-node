package httpserver

import (
	"context"
	"io"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/ipfs/go-cid"
)

type ProgressReader struct {
	r          io.Reader
	httpServer *HttpServer
	updateTime time.Time
	root       cid.Cid
	totalSize  int64
	readLength int64
}

func newProgressReader(r io.Reader, httpServer *HttpServer, root cid.Cid, totalSize int64) *ProgressReader {
	return &ProgressReader{
		r:          r,
		httpServer: httpServer,
		root:       root,
		totalSize:  totalSize,
		updateTime: time.Now(),
	}
}

func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	n, err = pr.r.Read(p)
	if err != nil {
		return
	}

	pr.readLength += int64(n)

	if time.Since(pr.updateTime) > time.Second && pr.readLength != pr.totalSize {
		progress := &types.UploadProgress{TotalSize: pr.totalSize, DoneSize: pr.readLength}
		if err = pr.httpServer.asset.SetAssetUploadProgress(context.Background(), pr.root, progress); err != nil {
			log.Errorf("SetAssetUploadProgress error %s", err.Error())
		}
		pr.updateTime = time.Now()
	}

	return
}
