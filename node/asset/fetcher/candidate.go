package fetcher

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

// CandidateFetcher
type CandidateFetcher struct {
	httpClient *http.Client
}

// NewCandidateFetcher creates a new CandidateFetcher with the specified timeout and retry count
func NewCandidateFetcher(httpClient *http.Client) *CandidateFetcher {
	return &CandidateFetcher{httpClient: httpClient}
}

// FetchBlocks fetches blocks for the given cids and candidate download info
func (c *CandidateFetcher) FetchBlocks(ctx context.Context, cids []string, sdis []*types.SourceDownloadInfo) ([]*ErrMsg, []*types.Workload, []blocks.Block, error) {
	return c.retrieveBlocks(ctx, cids, sdis)
}

// fetchSingleBlock fetches a single block for the given candidate download info and cid string
func (c *CandidateFetcher) fetchSingleBlock(ctx context.Context, downloadSource *types.SourceDownloadInfo, cidStr string) (blocks.Block, error) {
	if len(downloadSource.Address) == 0 {
		return nil, fmt.Errorf("candidate address can not empty")
	}

	if downloadSource.Tk == nil {
		return nil, fmt.Errorf("token can not empty")
	}

	buf, err := encode(downloadSource.Tk)
	if err != nil {
		return nil, fmt.Errorf("encode %s", err.Error())
	}
	url := fmt.Sprintf("https://%s/ipfs/%s?format=raw", downloadSource.Address, cidStr)

	req, err := http.NewRequest(http.MethodGet, url, buf)
	if err != nil {
		return nil, fmt.Errorf("newRequest %s", err.Error())
	}
	req = req.WithContext(ctx)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("doRequest %s", err.Error())
	}
	defer resp.Body.Close() //nolint:errcheck // ignore error

	if resp.StatusCode != http.StatusOK {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("http status code: %d, read body error %s", resp.StatusCode, err.Error())
		}
		return nil, fmt.Errorf("http status code: %d, error msg: %s", resp.StatusCode, string(data))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body %s", err.Error())
	}

	cid, err := cid.Decode(cidStr)
	if err != nil {
		return nil, fmt.Errorf("decode %s", err.Error())
	}

	basicBlock, err := blocks.NewBlockWithCid(data, cid)
	if err != nil {
		return nil, err
	}

	return basicBlock, nil
}

// retrieveBlocks retrieves multiple blocks using the given cids and candidate download info
func (c *CandidateFetcher) retrieveBlocks(ctx context.Context, cids []string, sdis []*types.SourceDownloadInfo) ([]*ErrMsg, []*types.Workload, []blocks.Block, error) {
	if len(sdis) == 0 {
		return nil, nil, nil, fmt.Errorf("download infos can not empty")
	}

	workloads := make([]*types.Workload, 0, len(cids))
	blks := make([]blocks.Block, 0, len(cids))
	lock := &sync.Mutex{}

	errMsgs := make([]*ErrMsg, 0)
	var wg sync.WaitGroup

	for index, cid := range cids {
		cidStr := cid
		i := index % len(sdis)
		ds := sdis[i]

		wg.Add(1)

		go func() {
			defer wg.Done()
			startTime := time.Now()
			b, err := c.fetchSingleBlock(ctx, ds, cidStr)
			if err != nil {
				errMsgs = append(errMsgs, &ErrMsg{Cid: cidStr, Source: ds.NodeID, Msg: err.Error()})
				return
			}

			costTime := time.Since(startTime) / time.Millisecond
			workload := &types.Workload{SourceID: ds.NodeID, DownloadSize: int64(len(b.RawData())), CostTime: int64(costTime)}

			lock.Lock()
			blks = append(blks, b)
			workloads = append(workloads, workload)
			lock.Unlock()
		}()
	}
	wg.Wait()

	if errors.Is(ctx.Err(), context.Canceled) {
		return errMsgs, nil, blks, ctx.Err()
	}

	return errMsgs, workloads, blks, nil
}

func encode(esc *types.Token) (*bytes.Buffer, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(esc)
	if err != nil {
		return nil, err
	}

	return &buffer, nil
}
