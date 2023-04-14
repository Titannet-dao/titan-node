package fetcher

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

// CandidateFetcher
type CandidateFetcher struct {
	retryCount int
	httpClient *http.Client
}

// NewCandidateFetcher creates a new CandidateFetcher with the specified timeout and retry count
func NewCandidateFetcher(timeout, retryCount int) *CandidateFetcher {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 10
	t.IdleConnTimeout = 120 * time.Second

	httpClient := &http.Client{
		Timeout:   time.Duration(timeout) * time.Second,
		Transport: t,
	}

	return &CandidateFetcher{retryCount: retryCount, httpClient: httpClient}
}

// FetchBlocks fetches blocks for the given cids and candidate download info
func (c *CandidateFetcher) FetchBlocks(ctx context.Context, cids []string, dss []*types.CandidateDownloadInfo) ([]blocks.Block, error) {
	return c.retrieveBlocks(cids, dss)
}

// fetchSingleBlock fetches a single block for the given candidate download info and cid string
func (c *CandidateFetcher) fetchSingleBlock(ds *types.CandidateDownloadInfo, cidStr string) (blocks.Block, error) {
	if len(ds.URL) == 0 {
		return nil, fmt.Errorf("candidate address can not empty")
	}
	buf, err := encode(ds.Credentials)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("http://%s/ipfs/%s?format=raw", ds.URL, cidStr)

	req, err := http.NewRequest(http.MethodGet, url, buf)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck // ignore error

	if resp.StatusCode != http.StatusOK {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("http status code: %d, error msg: %s", resp.StatusCode, string(data))
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

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

// retrieveBlocks retrieves multiple blocks using the given cids and candidate download info
func (c *CandidateFetcher) retrieveBlocks(cids []string, dss []*types.CandidateDownloadInfo) ([]blocks.Block, error) {
	if len(dss) == 0 {
		return nil, fmt.Errorf("download infos can not empty")
	}

	blks := make([]blocks.Block, 0, len(cids))
	// candidates := make(map[string]api.CandidateFetcher)
	blksLock := &sync.Mutex{}

	var wg sync.WaitGroup

	for index, cid := range cids {
		cidStr := cid
		i := index % len(dss)
		ds := dss[i]

		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < c.retryCount; i++ {
				b, err := c.fetchSingleBlock(ds, cidStr)
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

func encode(gwCredentials *types.GatewayCredentials) (*bytes.Buffer, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(gwCredentials)
	if err != nil {
		return nil, err
	}

	return &buffer, nil
}
