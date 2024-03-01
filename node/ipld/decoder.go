package ipld

import (
	"context"

	"github.com/ipfs/go-cid"
	legacy "github.com/ipfs/go-ipld-legacy"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-merkledag"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

func init() {
	legacy.RegisterCodec(cid.DagProtobuf, dagpb.Type.PBNode, merkledag.ProtoNodeConverter)
	legacy.RegisterCodec(cid.Raw, basicnode.Prototype.Bytes, merkledag.RawNodeConverter)

}

// DecodeNode handle codec not match case
func DecodeNode(ctx context.Context, b blocks.Block) (legacy.UniversalNode, error) {
	node, err := legacy.DecodeNode(ctx, b)
	if err == nil {
		return node, nil
	}

	codec := cid.Raw
	if b.Cid().Type() == uint64(cid.Raw) {
		codec = cid.DagProtobuf
	}

	c := cid.NewCidV1(uint64(codec), b.Cid().Hash())
	block, err := blocks.NewBlockWithCid(b.RawData(), c)
	if err != nil {
		return nil, err
	}

	return legacy.DecodeNode(ctx, block)
}
