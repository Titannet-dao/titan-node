package fetcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/ipfs/go-cid"
	httpapi "github.com/ipfs/go-ipfs-http-client"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

var log = logging.Logger("asset/fetcher")

// IPFSClient
type IPFSClient struct {
	httpAPI *httpapi.HttpApi
}

// NewIPFSClient creates a new IPFSClient with the given API URL, timeout, and retry count
func NewIPFSClient(ipfsAPIURL string) *IPFSClient {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 10
	t.IdleConnTimeout = 120 * time.Second

	httpClient := &http.Client{
		Transport: t,
	}

	fmt.Println("ipfsAPIURL ", ipfsAPIURL)
	httpAPI, err := httpapi.NewURLApiWithClient(ipfsAPIURL, httpClient)
	if err != nil {
		log.Panicf("new ipfs error:%s, url:%s", err.Error(), ipfsAPIURL)
	}

	return &IPFSClient{httpAPI: httpAPI}
}

// FetchBlocks retrieves blocks from IPFSClient using the provided context, CIDs, and download info
func (ipfs *IPFSClient) FetchBlocks(ctx context.Context, cids []string, sdis []*types.SourceDownloadInfo) ([]*ErrMsg, []*types.Workload, []blocks.Block, error) {
	return ipfs.retrieveBlocks(ctx, cids)
}

// retrieveBlock gets a block from IPFSClient with the specified CID
func (ipfs *IPFSClient) retrieveBlock(ctx context.Context, cidStr string) (blocks.Block, error) {
	reader, err := ipfs.httpAPI.Block().Get(ctx, path.New(cidStr))
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return createBlock(cidStr, data)
}

// retrieveBlocks gets multiple blocks from IPFSClient using the provided context and CIDs
func (ipfs *IPFSClient) retrieveBlocks(ctx context.Context, cids []string) ([]*ErrMsg, []*types.Workload, []blocks.Block, error) {
	blks := make([]blocks.Block, 0, len(cids))
	blksLock := &sync.Mutex{}

	startTime := time.Now()
	workload := types.Workload{SourceID: "ipfs"}
	errMsgs := make([]*ErrMsg, 0)
	var wg sync.WaitGroup

	for _, cid := range cids {
		wg.Add(1)

		go func(cid string) {
			defer wg.Done()

			b, err := ipfs.retrieveBlock(ctx, cid)
			if err != nil {
				errMsgs = append(errMsgs, &ErrMsg{Cid: cid, Source: "ipfs", Msg: err.Error()})
				return
			}

			blksLock.Lock()
			blks = append(blks, b)
			workload.DownloadSize += int64(len(b.RawData()))
			blksLock.Unlock()
		}(cid)
	}
	wg.Wait()

	costTime := time.Since(startTime) / time.Millisecond
	workload.CostTime = int64(costTime)

	if errors.Is(ctx.Err(), context.Canceled) {
		return errMsgs, nil, blks, ctx.Err()
	}

	return errMsgs, []*types.Workload{&workload}, blks, nil
}

// createBlock creates a new block with the specified CID and data
func createBlock(cidStr string, data []byte) (blocks.Block, error) {
	cid, err := cid.Decode(cidStr)
	if err != nil {
		return nil, err
	}

	basicBlock, err := blocks.NewBlockWithCid(data, cid)
	if err != nil {
		return nil, err
	}

	return basicBlock, nil
}
