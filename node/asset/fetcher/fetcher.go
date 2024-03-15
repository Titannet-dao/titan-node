package fetcher

import (
	"context"
	"encoding/json"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/ipfs/go-libipfs/blocks"
)

// BlockFetcher is an interface for fetching blocks from remote sources
type BlockFetcher interface {
	// FetchBlocks retrieves blocks with the given cids from remote sources using the provided CandidateDownloadInfo
	FetchBlocks(ctx context.Context, cids []string, dss []*types.CandidateDownloadInfo) ([]*ErrMsg, []*types.WorkloadReport, []blocks.Block, error)
}

type ErrMsg struct {
	Cid    string
	Source string
	Msg    string
}

func (e *ErrMsg) toString() string {
	buf, err := json.Marshal(e)
	if err != nil {
		log.Error("marshal error %s", err.Error())
		return ""
	}
	return string(buf)
}
