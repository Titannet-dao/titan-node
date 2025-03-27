package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type trace = string

// assetSliceManager is responsible for managing the slices of an asset.
// It is responsible for creating new slices, combining them and deleting them.
type assetSliceManager struct {
	slices map[trace]slicePathInfo

	dir          string
	maxChunkSize int64
	maxTotalSize int64
}

type sliceChunk struct {
	start     int64
	end       int64
	createdAt int64
	path      string
}

type fileInfo struct {
	traceID   string
	fileName  string
	totalSize int64
	isCar     bool
}

var expireDelIn24HourFunc = func(path string, expireTime time.Time) {
	time.AfterFunc(time.Until(expireTime), func() {
		if err := os.Remove(path); err != nil {
			log.Errorf("Failed to remove slice %s: %v", path, err)
		}
	})
}

type chunks []sliceChunk

// path is asset-slices/<traceid>/chunk-<start>-<end>.
// Example: asset-slices/393dfb06-b9fa-4c31-a17d-803ab22231c6/chunk-0-1023.
// slices in path contains a .info file
type slicePathInfo struct {
	fileInfo fileInfo
	chunks   chunks
}

func newAssetSliceManager(dir string) (*assetSliceManager, error) {

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %v", dir, err)
		}
	}

	asm := &assetSliceManager{
		slices: make(map[trace]slicePathInfo),
		dir:    dir,
	}

	// load existing slices
	if err := asm.loadSlices(); err != nil {
		return nil, fmt.Errorf("failed to load slices: %v", err)
	}

	log.Infof("Asset slice manager initialized with directory: %s", dir)

	return asm, nil
}

func (a *assetSliceManager) addSlice(traceID string, expireTime time.Time, r io.Reader, chunk sliceChunk, fs fileInfo) error {
	traceDir := filepath.Join(a.dir, traceID)

	if _, err := os.Stat(traceDir); os.IsNotExist(err) {
		if err := os.MkdirAll(traceDir, 0o755); err != nil {
			return fmt.Errorf("failed to create trace directory %s: %v", traceDir, err)
		}

		go expireDelIn24HourFunc(traceDir, expireTime)

		a.slices[traceID] = slicePathInfo{
			fileInfo: fs,
		}
	}

	path := filepath.Join(traceDir, fmt.Sprintf("chunk-%d-%d", chunk.start, chunk.end))

	chunkFile, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create slice file %s: %v", path, err)
	}
	defer chunkFile.Close()

	if _, err := io.Copy(chunkFile, r); err != nil {
		return fmt.Errorf("failed to write slice file %s: %v", path, err)
	}

	chunk.path = path

	chunksInfo := a.slices[traceID].chunks
	chunksInfo = append(chunksInfo, chunk)

	sort.Slice(chunksInfo, func(i, j int) bool {
		return chunksInfo[i].start < chunksInfo[j].start
	})

	a.slices[traceID] = slicePathInfo{
		fileInfo: fs,
		chunks:   chunksInfo,
	}

	log.Infof("Added slice %s %d-%d to trace %s", path, chunk.start, chunk.end, traceID)

	return nil
}

func (a *assetSliceManager) loadSlices() error {
	entries, err := os.ReadDir(a.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			log.Errorf("Unexpected entry in slices directory: %s", entry.Name())
			continue
		}

		trace := entry.Name()
		contents, err := os.ReadDir(a.dir + "/" + trace)
		if err != nil {
			log.Errorf("Failed to read slices for %s: %v", trace, err)
			continue
		}

		var (
			chunks chunks
			fsInfo fileInfo
		)

		for _, content := range contents {

			name := content.Name()
			path := a.dir + "/" + trace + "/" + name

			if name == ".info" {
				infoBytes, err := os.ReadFile(path)
				if err != nil {
					log.Errorf("Failed to read slice info for %s: %v", trace, err)
					break
				}
				if err := json.Unmarshal(infoBytes, &fsInfo); err != nil {
					log.Errorf("Failed to parse slice info for %s: %v", trace, err)
					break
				}
				continue
			}
			// path is asset-slices/<traceid>/chunk-<start>-<end>.
			// Example: asset-slices/393dfb06-b9fa-4c31-a17d-803ab22231c6/chunk-0-1023.
			if strings.Contains(name, "chunk") {
				var start, end int64
				if _, err := fmt.Scanf(name, "chunk-%d-%d", &start, &end); err != nil {
					log.Errorf("Failed to parse chunk name for %s: %v", trace, err)
					break
				}

				fileInfo, err := os.Stat(path)
				if err != nil {
					log.Errorf("Failed to stat chunk file %s: %v", name, err)
					break
				}

				chunk := sliceChunk{
					start:     start,
					end:       end,
					createdAt: fileInfo.ModTime().Unix(),
					path:      path,
					// expireAt:   0,
					// expireFunc: expireDelIn24HourFunc,
				}

				chunks = append(chunks, chunk)
			}
		}

		if len(chunks) > 0 && fsInfo.traceID != "" {
			a.slices[trace] = slicePathInfo{fileInfo: fsInfo, chunks: chunks}
		}

		log.Infof("Loaded %d slices for trace %s", len(chunks), trace)
	}

	return nil
}
