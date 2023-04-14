package fetcher

import (
	"context"
	"io/ioutil"
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
	httpAPI    *httpapi.HttpApi
	timeout    int
	retryCount int
}

// NewIPFSClient creates a new IPFSClient with the given API URL, timeout, and retry count
func NewIPFSClient(ipfsAPIURL string, timeout, retryCount int) *IPFSClient {
	httpAPI, err := httpapi.NewURLApiWithClient(ipfsAPIURL, &http.Client{})
	if err != nil {
		log.Panicf("new ipfs error:%s, url:%s", err.Error(), ipfsAPIURL)
	}

	return &IPFSClient{httpAPI: httpAPI, timeout: timeout, retryCount: retryCount}
}

// FetchBlocks retrieves blocks from IPFSClient using the provided context, CIDs, and download info
func (ipfs *IPFSClient) FetchBlocks(ctx context.Context, cids []string, dss []*types.CandidateDownloadInfo) ([]blocks.Block, error) {
	return ipfs.retrieveBlocks(ctx, cids)
}

// retrieveBlock gets a block from IPFSClient with the specified CID
func (ipfs *IPFSClient) retrieveBlock(cidStr string) (blocks.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ipfs.timeout)*time.Second)
	defer cancel()

	reader, err := ipfs.httpAPI.Block().Get(ctx, path.New(cidStr))
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return createBlock(cidStr, data)
}

// retrieveBlocks gets multiple blocks from IPFSClient using the provided context and CIDs
func (ipfs *IPFSClient) retrieveBlocks(ctx context.Context, cids []string) ([]blocks.Block, error) {
	blks := make([]blocks.Block, 0, len(cids))
	blksLock := &sync.Mutex{}

	var wg sync.WaitGroup

	for _, cid := range cids {
		cidStr := cid
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < ipfs.retryCount; i++ {
				b, err := ipfs.retrieveBlock(cidStr)
				if err != nil {
					log.Errorf("getBlock error:%s, cid:%s", err.Error(), cidStr)
					continue
				}

				blksLock.Lock()
				blks = append(blks, b)
				blksLock.Unlock()
				return
			}
		}()
	}
	wg.Wait()

	return blks, nil
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
