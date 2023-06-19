package storage

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"golang.org/x/xerrors"
)

var totalSize = int64(0)
var dongSize = int64(0)

func TestScanAsset(t *testing.T) {
	filePath := "C:/Users/aaa/.titancandidate-1/storage/assets/12205a8ebc6e33c8bccef097cc9816304e3c65145489da1064aae29e75e52d8a14bf.car"
	f, err := os.Open(filePath)
	if err != nil {
		t.Errorf("open file %s error %s", filePath, err.Error())
		return
	}
	defer f.Close()

	bs, err := blockstore.NewReadOnly(f, nil, carv2.ZeroLengthSectionAsEOF(true))
	if err != nil {
		t.Errorf("NewReadOnly error %s", err.Error())
		return
	}

	root, err := cid.Decode("QmUS9e1GisPtRtB6NAda23jtCEB5Go2SGyZy5Yf9YBLd8a")
	if err != nil {
		t.Errorf("Decode error %s", err.Error())
		return
	}

	block, err := bs.Get(context.Background(), root)
	if err != nil {
		t.Errorf("get block error %s", err.Error())
		return
	}

	node, err := legacy.DecodeNode(context.Background(), block)
	if err != nil {
		t.Errorf("DecodeNode error %s", err.Error())
		return
	}

	totalSize = int64(len(block.RawData()))
	dongSize += int64(len(block.RawData()))
	fmt.Printf("block cid: %s, blockSzie:%d\n", block.Cid().String(), len(block.RawData()))

	for _, link := range node.Links() {
		totalSize += int64(link.Size)
		fmt.Printf("block cid: %s, link size:%d\n", link.Cid.String(), link.Size)
	}

	if len(node.Links()) > 0 {
		if err = getBlocks(context.Background(), bs, node.Links()); err != nil {
			t.Errorf("get blocks error %s", err.Error())
			return
		}
	}

	fmt.Printf("totalSize: %d, doneSize: %d", totalSize, dongSize)

}

func getBlocks(ctx context.Context, bs *blockstore.ReadOnly, links []*format.Link) error {
	for _, link := range links {
		block, err := bs.Get(context.Background(), link.Cid)
		if err != nil {
			return xerrors.Errorf("get block %s error %w", link.Cid.String(), err)
		}
		dongSize += int64(len(block.RawData()))
		// fmt.Printf("totalSize: %d, dongSize:%d, blockSzie:%d, linkSize:%d\n", totalSize, dongSize, len(block.RawData()), link.Size)
		node, err := legacy.DecodeNode(context.Background(), block)
		if err != nil {
			return err
		}
		fmt.Printf("block cid: %s, block size:%d, linkSize:%d\n", block.Cid().String(), len(block.RawData()), link.Size)
		// if len(node.Links()) > 0 {
		// 	fmt.Printf("---block cid: %s, link size:%d --\n", link.Cid.String(), link.Size)
		// }
		// // fmt.Printf("block cid: %s, link size:%d\n", link.Cid.String(), link.Size)
		// for _, link := range node.Links() {
		// 	totalSize += int64(link.Size)
		// 	fmt.Printf("block cid: %s, link size:%d\n", link.Cid.String(), link.Size)
		// }

		if len(node.Links()) > 0 {
			if err = getBlocks(ctx, bs, node.Links()); err != nil {
				return err
			}
		}

	}

	return nil
}
