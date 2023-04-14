package httpserver

import (
	"context"
	"sync/atomic"

	"github.com/ipfs/go-cid"
	ipldformat "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
)

// format.NodeGetter interface implement
type nodeGetter struct {
	hs   *HttpServer
	root cid.Cid
}

// Get retrieves nodes by CID. Depending on the NodeGetter
// implementation, this may involve fetching the Node from a remote
// machine; consider setting a deadline in the context.
func (ng *nodeGetter) Get(ctx context.Context, block cid.Cid) (ipldformat.Node, error) {
	blk, err := ng.hs.asset.GetBlock(ctx, ng.root, block)
	if err != nil {
		return nil, err
	}

	return legacy.DecodeNode(context.Background(), blk)
}

// GetMany returns a channel of NodeOptions given a set of CIDs.
func (ng *nodeGetter) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipldformat.NodeOption {
	var count uint64
	ch := make(chan *ipldformat.NodeOption, len(cids))
	for _, cid := range cids {
		c := cid
		go func() {
			node, err := ng.Get(ctx, c)
			ch <- &ipldformat.NodeOption{Node: node, Err: err}

			atomic.AddUint64(&count, 1)

			// TODO: will be ch = nil ?
			if int(count) == len(cids) {
				close(ch)
			}
		}()
	}
	return ch
}
