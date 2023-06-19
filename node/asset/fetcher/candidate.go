package fetcher

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
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
	httpClient *http.Client
}

// NewCandidateFetcher creates a new CandidateFetcher with the specified timeout and retry count
func NewCandidateFetcher() *CandidateFetcher {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 10
	t.IdleConnTimeout = 120 * time.Second

	httpClient := &http.Client{
		Transport: t,
	}

	return &CandidateFetcher{httpClient: httpClient}
}

// FetchBlocks fetches blocks for the given cids and candidate download info
func (c *CandidateFetcher) FetchBlocks(ctx context.Context, cids []string, dss []*types.CandidateDownloadInfo) ([]*types.WorkloadReport, []blocks.Block, error) {
	return c.retrieveBlocks(ctx, cids, dss)
}

// fetchSingleBlock fetches a single block for the given candidate download info and cid string
func (c *CandidateFetcher) fetchSingleBlock(ctx context.Context, downloadSource *types.CandidateDownloadInfo, cidStr string) (blocks.Block, error) {
	if len(downloadSource.Address) == 0 {
		return nil, fmt.Errorf("candidate address can not empty")
	}

	if downloadSource.Tk == nil {
		return nil, fmt.Errorf("token can not empty")
	}

	buf, err := encode(downloadSource.Tk)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("http://%s/ipfs/%s?format=raw", downloadSource.Address, cidStr)

	req, err := http.NewRequest(http.MethodGet, url, buf)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

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
func (c *CandidateFetcher) retrieveBlocks(ctx context.Context, cids []string, dss []*types.CandidateDownloadInfo) ([]*types.WorkloadReport, []blocks.Block, error) {
	if len(dss) == 0 {
		return nil, nil, fmt.Errorf("download infos can not empty")
	}

	workloadReports := make([]*types.WorkloadReport, 0, len(cids))
	blks := make([]blocks.Block, 0, len(cids))
	lock := &sync.Mutex{}

	var wg sync.WaitGroup

	for index, cid := range cids {
		cidStr := cid
		i := index % len(dss)
		ds := dss[i]

		wg.Add(1)

		go func() {
			defer wg.Done()
			startTime := time.Now()
			b, err := c.fetchSingleBlock(ctx, ds, cidStr)
			if err != nil {
				log.Errorf("fetch single block error:%s, cid:%s", err.Error(), cidStr)
				return
			}

			downloadSpeed := float64(0)
			duration := time.Since(startTime)
			if duration > 0 {
				downloadSpeed = float64(len(b.RawData())) / float64(duration) * float64(time.Second)
			}

			workload := &types.Workload{DownloadSpeed: int64(downloadSpeed), DownloadSize: int64(len(b.RawData())), StartTime: startTime.Unix(), EndTime: time.Now().Unix()}
			workloadReport := &types.WorkloadReport{TokenID: ds.Tk.ID, NodeID: ds.NodeID, Workload: workload}

			lock.Lock()
			blks = append(blks, b)
			workloadReports = append(workloadReports, workloadReport)
			lock.Unlock()
		}()
	}
	wg.Wait()

	if errors.Is(ctx.Err(), context.Canceled) {
		return nil, blks, ctx.Err()
	}

	return workloadReports, blks, nil
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
