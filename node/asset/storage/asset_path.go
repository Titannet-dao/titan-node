package storage

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/node/ipld"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/shirou/gopsutil/v3/disk"
)

type assetsPaths struct {
	baseDirs   []string
	assetPaths map[string]string
	rand       *rand.Rand
}

func newAssetsPaths(diskPaths []string, assetsDir string) (*assetsPaths, error) {
	baseDirs := make([]string, 0, len(diskPaths))
	for _, path := range diskPaths {
		baseDir := filepath.Join(path, assetsDir)
		err := os.MkdirAll(baseDir, 0o755)
		if err != nil {
			return nil, err
		}
		baseDirs = append(baseDirs, baseDir)
	}

	asPaths := &assetsPaths{baseDirs: baseDirs, assetPaths: make(map[string]string), rand: rand.New(rand.NewSource(time.Now().Unix()))}
	if len(baseDirs) > 1 {
		if err := asPaths.loadAssets(); err != nil {
			return nil, err
		}
	}

	log.Debugf("assetPaths:%#v", asPaths.assetPaths)
	return asPaths, nil
}

func (ap *assetsPaths) findPath(root cid.Cid) (string, error) {
	if len(ap.baseDirs) == 1 {
		return ap.baseDirs[0], nil
	}

	baseDir, ok := ap.assetPaths[root.Hash().String()]
	if ok {
		return baseDir, nil
	}

	return "", fmt.Errorf("asset %s not exist", root.String())

}

func (ap *assetsPaths) exists(root cid.Cid) bool {
	if len(ap.baseDirs) == 1 {
		return true
	}

	if _, ok := ap.assetPaths[root.Hash().String()]; ok {
		return true
	}

	return false
}

func (ap *assetsPaths) allocatePathWithBlocks(root cid.Cid, blks []blocks.Block) (string, error) {
	if len(ap.baseDirs) == 1 {
		return ap.baseDirs[0], nil
	}

	baseDir, ok := ap.assetPaths[root.Hash().String()]
	if ok {
		return baseDir, nil
	}

	isRoot := false
	var rootBlk blocks.Block
	for _, blk := range blks {
		if blk.Cid().Hash().String() == root.Hash().String() {
			isRoot = true
			rootBlk = blk
			break
		}
	}

	if !isRoot {
		return "", fmt.Errorf("can not allocate disk for none root asset ")
	}

	node, err := ipld.DecodeNode(context.Background(), rootBlk)
	if err != nil {
		return "", err
	}

	linksSize := uint64(len(rootBlk.RawData()))
	for _, link := range node.Links() {
		linksSize += link.Size
	}

	validPaths, err := ap.filterValidPaths(linksSize * 2)
	if err != nil {
		return "", err
	}
	if len(validPaths) == 0 {
		return "", fmt.Errorf("no free space enough for asset %s", root.String())
	}

	n := ap.rand.Intn(len(validPaths))
	path := validPaths[n]
	ap.assetPaths[root.Hash().String()] = path

	return path, nil
}

func (ap *assetsPaths) allocatePathWithSize(root cid.Cid, size int64) (string, error) {
	if len(ap.baseDirs) == 1 {
		return ap.baseDirs[0], nil
	}

	baseDir, ok := ap.assetPaths[root.Hash().String()]
	if ok {
		return baseDir, nil
	}

	validPaths, err := ap.filterValidPaths(uint64(size) * 2)
	if err != nil {
		return "", err
	}
	if len(validPaths) == 0 {
		return "", fmt.Errorf("no free space enough for asset %s", root.String())
	}

	n := ap.rand.Intn(len(validPaths))
	path := validPaths[n]
	ap.assetPaths[root.Hash().String()] = path

	return path, nil
}

func (ap *assetsPaths) filterValidPaths(freeSize uint64) ([]string, error) {
	validPaths := make([]string, 0)
	for _, baseDir := range ap.baseDirs {
		usageStat, err := disk.Usage(baseDir)
		if err != nil {
			log.Errorf("get disk usage stat error: %s", err)
			return nil, err
		}

		if usageStat.Free > freeSize {
			validPaths = append(validPaths, baseDir)
		}
	}

	return validPaths, nil
}

func (ap *assetsPaths) releasePath(root cid.Cid) {
	delete(ap.assetPaths, root.String())
}

func (ap *assetsPaths) loadAssets() error {
	for _, baseDir := range ap.baseDirs {
		entries, err := os.ReadDir(baseDir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			assetName := entry.Name()
			if !entry.IsDir() {
				assetName = strings.Replace(entry.Name(), ".car", "", 1)
			}
			ap.assetPaths[assetName] = baseDir
		}

	}

	return nil
}
