package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/Filecoin-Titan/titan/node/ipld"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-libipfs/blocks"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"golang.org/x/xerrors"
)

// asset save asset file
type asset struct {
	assetsPaths *assetsPaths
	suffix      string
}

// newAsset initializes a new asset instance.
func newAsset(assetsPaths *assetsPaths, suffix string) (*asset, error) {
	return &asset{assetsPaths: assetsPaths, suffix: suffix}, nil
}

// generateAssetName creates a new asset file name.
func (a *asset) generateAssetName(root cid.Cid) string {
	return root.Hash().String() + a.suffix
}

// storeBlocks stores blocks to the file system.
func (a *asset) storeBlocks(ctx context.Context, root cid.Cid, blks []blocks.Block) error {
	baseDir, err := a.assetsPaths.allocatePathWithBlocks(root, blks)
	if err != nil {
		return err
	}

	assetDir := filepath.Join(baseDir, root.Hash().String())
	err = os.MkdirAll(assetDir, 0o755)
	if err != nil {
		return err
	}

	for _, blk := range blks {
		filePath := filepath.Join(assetDir, blk.Cid().Hash().String())
		if err := os.WriteFile(filePath, blk.RawData(), 0o644); err != nil {
			return err
		}
	}

	return nil
}

// storeBlocksToCar stores the asset to the file system.
func (a *asset) storeBlocksToCar(ctx context.Context, root cid.Cid) error {
	baseDir, err := a.assetsPaths.findPath(root)
	if err != nil {
		return err
	}

	assetDir := filepath.Join(baseDir, root.Hash().String())
	entries, err := os.ReadDir(assetDir)
	if err != nil {
		return err
	}

	name := a.generateAssetName(root)
	path := filepath.Join(baseDir, name)

	rw, err := blockstore.OpenReadWrite(path, []cid.Cid{root})
	if err != nil {
		return err
	}

	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(assetDir, entry.Name()))
		if err != nil {
			return err
		}

		blk := blocks.NewBlock(data)
		if err = rw.Put(ctx, blk); err != nil {
			return err
		}
	}

	if err = rw.Finalize(); err != nil {
		return err
	}

	return os.RemoveAll(assetDir)
}

func (a *asset) verifyBlocks(ctx context.Context, bs *blockstore.ReadOnly, links []*format.Link) error {
	for _, link := range links {
		block, err := bs.Get(context.Background(), link.Cid)
		if err != nil {
			return xerrors.Errorf("get block %s error %w", link.Cid.String(), err)
		}

		node, err := ipld.DecodeNode(context.Background(), block)
		if err != nil {
			return err
		}

		if len(node.Links()) > 0 {
			if err = a.verifyBlocks(ctx, bs, node.Links()); err != nil {
				return err
			}
		}

	}

	return nil
}

func (a *asset) verifyAsset(filePath string, root cid.Cid) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	bs, err := blockstore.NewReadOnly(f, nil, carv2.ZeroLengthSectionAsEOF(true))
	if err != nil {
		return err
	}

	block, err := bs.Get(context.Background(), root)
	if err != nil {
		return xerrors.Errorf("get block %s error %w", root.String(), err)
	}

	node, err := ipld.DecodeNode(context.Background(), block)
	if err != nil {
		log.Errorf("decode block error:%s", err.Error())
		return err
	}

	if len(node.Links()) > 0 {
		return a.verifyBlocks(context.Background(), bs, node.Links())
	}

	return nil
}

func (a *asset) saveAsset(filePath string, assetSize int64, r io.Reader) error {
	destFile, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, r)
	if err != nil {
		return err
	}

	stat, err := destFile.Stat()
	if err != nil {
		return err
	}

	if stat.Size() != assetSize {
		return fmt.Errorf("require asset size is %d, but upload asset size is %d", assetSize, stat.Size())
	}

	return nil
}

func (a *asset) isCarV1(filePath string) (bool, error) {
	src, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer src.Close()

	// Open the CARv2 file
	cr, err := carv2.NewReader(src)
	if err != nil {
		return false, err
	}
	defer cr.Close()

	// check if car v1
	if cr.Version == 1 {
		return true, nil
	}
	return false, nil
}

// storeAssetToCar stores the asset to the file system.
func (a *asset) saveUserAsset(ctx context.Context, userID string, root cid.Cid, assetSize int64, r io.Reader) error {
	if ok, err := a.exists(root); err != nil {
		return err
	} else if ok {
		return nil
	}

	baseDir, err := a.assetsPaths.allocatePathWithSize(root, assetSize)
	if err != nil {
		return err
	}

	tempAssetDir := filepath.Join(baseDir, userID)
	err = os.MkdirAll(tempAssetDir, 0o755)
	if err != nil {
		return err
	}

	// create file
	name := a.generateAssetName(root)
	tempAssetPath := filepath.Join(tempAssetDir, name)
	if err := a.saveAsset(tempAssetPath, assetSize, r); err != nil {
		return err
	}

	assetPath := filepath.Join(baseDir, name)

	isV1, err := a.isCarV1(tempAssetPath)
	if err != nil {
		return err
	}

	if isV1 {
		if err := carv2.WrapV1File(tempAssetPath, assetPath); err != nil {
			return err
		}
	} else {
		if err = os.Rename(tempAssetPath, assetPath); err != nil {
			return err
		}
	}

	if err = os.RemoveAll(tempAssetDir); err != nil {
		return err
	}

	if err = a.verifyAsset(assetPath, root); err != nil {
		// remove asset if verify failed
		if e := os.Remove(assetPath); e != nil {
			log.Errorf("remove asset %s error %s", assetPath, e.Error())
		}
		return xerrors.Errorf("verify car error: %w", err)
	}

	return nil
}

// get returns a ReadSeekCloser for the given asset root.
// The caller must close the reader.
func (a *asset) get(root cid.Cid) (io.ReadSeekCloser, error) {
	baseDir, err := a.assetsPaths.findPath(root)
	if err != nil {
		return nil, err
	}

	// check if put asset complete
	assetDir := filepath.Join(baseDir, root.Hash().String())
	if _, err := os.Stat(assetDir); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	} else {
		return nil, xerrors.Errorf("putting asset, not ready")
	}

	name := a.generateAssetName(root)
	filePath := filepath.Join(baseDir, name)
	return os.Open(filePath)
}

// exists checks if the asset exists in the file system.
func (a *asset) exists(root cid.Cid) (bool, error) {
	if ok := a.assetsPaths.exists(root); !ok {
		return false, nil
	}

	baseDir, err := a.assetsPaths.findPath(root)
	if err != nil {
		return false, err
	}

	assetDir := filepath.Join(baseDir, root.Hash().String())
	if _, err := os.Stat(assetDir); err != nil {
		if !os.IsNotExist(err) {
			return false, err
		}
	} else {
		return false, nil
	}

	name := a.generateAssetName(root)
	filePath := filepath.Join(baseDir, name)

	_, err = os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// remove deletes the asset from the file system.
func (a *asset) remove(root cid.Cid) error {
	baseDir, err := a.assetsPaths.findPath(root)
	if err != nil {
		return err
	}

	a.assetsPaths.releasePath(root)

	assetDir := filepath.Join(baseDir, root.Hash().String())
	if err := os.RemoveAll(assetDir); err != nil {
		if e, ok := err.(*os.PathError); !ok {
			return err
		} else if e.Err != syscall.ENOENT {
			return err
		}
	}

	name := a.generateAssetName(root)
	path := filepath.Join(baseDir, name)

	// remove file
	return os.Remove(path)
}

// count returns the number of assets in the file system.
func (a *asset) count() (int, error) {
	count := 0
	for _, baseDir := range a.assetsPaths.baseDirs {
		entries, err := os.ReadDir(baseDir)
		if err != nil {
			return 0, err
		}
		count += len(entries)
	}

	return count, nil
}
