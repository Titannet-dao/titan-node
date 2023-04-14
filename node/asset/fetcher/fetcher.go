package fetcher

import (
	"context"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/ipfs/go-libipfs/blocks"
)

// BlockFetcher is an interface for fetching blocks from remote sources
type BlockFetcher interface {
	// FetchBlocks retrieves blocks with the given cids from remote sources using the provided CandidateDownloadInfo
	FetchBlocks(ctx context.Context, cids []string, dss []*types.CandidateDownloadInfo) ([]blocks.Block, error)
}
