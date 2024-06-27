package carutil

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"path"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data/builder"
	carv1 "github.com/ipld/go-car"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	carstorage "github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

// createCar creates a car
func CreateCar(inputPath string, outputPath string) (cid.Cid, error) {
	// make a cid with the right length that we eventually will patch with the root.
	hasher, err := multihash.GetHasher(multihash.SHA2_256)
	if err != nil {
		return cid.Cid{}, err
	}
	digest := hasher.Sum([]byte{})
	hash, err := multihash.Encode(digest, multihash.SHA2_256)
	if err != nil {
		return cid.Cid{}, err
	}
	proxyRoot := cid.NewCidV1(uint64(multicodec.DagPb), hash)

	cdest, err := blockstore.OpenReadWrite(outputPath, []cid.Cid{proxyRoot})
	if err != nil {
		return cid.Cid{}, err
	}

	// Write the unixfs blocks into the store.
	root, err := writeFiles(context.TODO(), true, cdest, inputPath)
	if err != nil {
		return cid.Cid{}, err
	}

	if err := cdest.Finalize(); err != nil {
		return cid.Cid{}, err
	}

	return root, car.ReplaceRootsInFile(outputPath, []cid.Cid{root})
}

// writeFiles writes files to the blockstore and returns the root CID.
func writeFiles(ctx context.Context, noWrap bool, bs *blockstore.ReadWrite, paths ...string) (cid.Cid, error) {
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true
	ls.StorageReadOpener = func(_ ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		cl, ok := l.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("not a cidlink")
		}
		blk, err := bs.Get(ctx, cl.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(blk.RawData()), nil
	}
	ls.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(l ipld.Link) error {
			cl, ok := l.(cidlink.Link)
			if !ok {
				return fmt.Errorf("not a cidlink")
			}
			blk, err := blocks.NewBlockWithCid(buf.Bytes(), cl.Cid)
			if err != nil {
				return err
			}
			bs.Put(ctx, blk)
			return nil
		}, nil
	}

	topLevel := make([]dagpb.PBLink, 0, len(paths))
	for _, p := range paths {
		l, size, err := builder.BuildUnixFSRecursive(p, &ls)
		if err != nil {
			return cid.Undef, err
		}
		if noWrap {
			rcl, ok := l.(cidlink.Link)
			if !ok {
				return cid.Undef, fmt.Errorf("could not interpret %s", l)
			}
			return rcl.Cid, nil
		}
		name := path.Base(p)
		entry, err := builder.BuildUnixFSDirectoryEntry(name, int64(size), l)
		if err != nil {
			return cid.Undef, err
		}
		topLevel = append(topLevel, entry)
	}

	// make a directory for the file(s).

	root, _, err := builder.BuildUnixFSDirectory(topLevel, &ls)
	if err != nil {
		return cid.Undef, nil
	}
	rcl, ok := root.(cidlink.Link)
	if !ok {
		return cid.Undef, fmt.Errorf("could not interpret %s", root)
	}

	return rcl.Cid, nil
}

// CalculateCid calculates the CID for the given reader.
func CalculateCid(r io.Reader) (cid.Cid, error) {
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true

	ls.StorageReadOpener = func(_ ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		return nil, nil
	}

	ls.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(l ipld.Link) error {
			return nil
		}, nil
	}

	link, _, err := builder.BuildUnixFSFile(r, "", &ls)
	if err != nil {
		return cid.Cid{}, err
	}

	root := link.(cidlink.Link)
	return root.Cid, nil
}

// CarStream is an interface that combines io.ReadWriter, io.ReaderAt, io.WriterAt, io.Seeker.
type CarStream interface {
	io.ReadWriter
	io.ReaderAt
	io.WriterAt
	io.Seeker
}

// createCarStream creates a car using a CarStream.
func createCarStream(r io.Reader, carStream CarStream) (cid.Cid, error) {
	// make a cid with the right length that we eventually will patch with the root.
	hasher, err := multihash.GetHasher(multihash.SHA2_256)
	if err != nil {
		return cid.Cid{}, err
	}
	digest := hasher.Sum([]byte{})
	hash, err := multihash.Encode(digest, multihash.SHA2_256)
	if err != nil {
		return cid.Cid{}, err
	}
	proxyRoot := cid.NewCidV1(uint64(multicodec.DagPb), hash)
	// proxyRoot, err := CalculateCID(r)
	// if err != nil {
	// 	return cid.Cid{}, err
	// }

	wCar, err := carstorage.NewWritable(carStream, []cid.Cid{proxyRoot})
	if err != nil {
		return cid.Cid{}, err
	}

	// Write the unixfs blocks into the store.
	root, err := writeBuffer(context.TODO(), r, wCar)
	if err != nil {
		return cid.Cid{}, err
	}

	if err := wCar.Finalize(); err != nil {
		return cid.Cid{}, err
	}

	return root, replaceRootsInCar(carStream, []cid.Cid{root})
}

// writeBuffer writes data to the car using a CarStream.
func writeBuffer(ctx context.Context, r io.Reader, wCar carstorage.WritableCar) (cid.Cid, error) {
	sCar := wCar.(*carstorage.StorageCar)
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true

	ls.StorageReadOpener = func(_ ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		cl, ok := l.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("not a cidlink")
		}
		blk, err := sCar.Get(ctx, cl.Cid.KeyString())
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(blk), nil
	}

	ls.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(l ipld.Link) error {
			cl, ok := l.(cidlink.Link)
			if !ok {
				return fmt.Errorf("not a cidlink")
			}
			blk, err := blocks.NewBlockWithCid(buf.Bytes(), cl.Cid)
			if err != nil {
				return err
			}
			sCar.Put(ctx, blk.Cid().KeyString(), blk.RawData())
			return nil
		}, nil
	}

	link, _, err := builder.BuildUnixFSFile(r, "", &ls)
	if err != nil {
		return cid.Cid{}, err
	}

	root := link.(cidlink.Link)
	return root.Cid, nil
}

// replaceRootsInCar replaces the roots in the car header.
func replaceRootsInCar(carStream CarStream, roots []cid.Cid, opts ...car.Option) (err error) {
	var v2h car.Header
	if _, err = v2h.ReadFrom(carStream); err != nil {
		return err
	}
	var newHeaderOffset = int64(v2h.DataOffset)
	if _, err = carStream.Seek(newHeaderOffset, io.SeekStart); err != nil {
		return err
	}
	var innerV1Header *carv1.CarHeader
	innerV1Header, err = carv1.ReadHeader(bufio.NewReader(carStream))
	if err != nil {
		return err
	}
	fmt.Println("innerV1Header", innerV1Header)
	if innerV1Header.Version != 1 {
		err = fmt.Errorf("invalid data payload header: expected version 1, got %d", innerV1Header.Version)
		return err
	}
	var readSoFar int64
	readSoFar, err = carStream.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	var currentSize = readSoFar - newHeaderOffset

	newHeader := &carv1.CarHeader{
		Roots:   roots,
		Version: 1,
	}

	size, _ := carv1.HeaderSize(newHeader)
	fmt.Printf("newHeader %v, size %d", newHeader, size)
	// Serialize the new header straight up instead of using carv1.HeaderSize.
	// Because, carv1.HeaderSize serialises it to calculate size anyway.
	// By serializing straight up we get the replacement bytes and size.
	// Otherwise, we end up serializing the new header twice:
	// once through carv1.HeaderSize, and
	// once to write it out.
	var buf bytes.Buffer
	if err = carv1.WriteHeader(newHeader, &buf); err != nil {
		return err
	}
	// Assert the header sizes are consistent.
	newSize := int64(buf.Len())
	if currentSize < newSize {
		return fmt.Errorf("current header size (%d) must match replacement header size (%d)", currentSize, newSize)
	}
	// Seek to the offset at which the new header should be written.
	if _, err = carStream.Seek(newHeaderOffset, io.SeekStart); err != nil {
		return err
	}
	_, err = carStream.Write(buf.Bytes())
	return err
}
