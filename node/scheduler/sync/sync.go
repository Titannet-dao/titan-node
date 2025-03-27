package sync

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("data-sync")

const (
	syncInterval = 24 * time.Hour
)

// DataSync asset synchronization manager
type DataSync struct {
	nodeList    []string
	lock        *sync.Mutex
	waitChannel chan bool
	nodeManager *node.Manager
}

// NewDataSync creates a new NewDataSync instance and starts the synchronization process.
func NewDataSync(nodeManager *node.Manager) *DataSync {
	dataSync := &DataSync{
		nodeList:    make([]string, 0),
		lock:        &sync.Mutex{},
		waitChannel: make(chan bool),
		nodeManager: nodeManager,
	}

	go dataSync.startSyncLoop()
	go dataSync.startCheckNodeTimer()

	return dataSync
}

func (ds *DataSync) startCheckNodeTimer() {
	// now := time.Now()

	// nextTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 1, 0, 0, now.Location())
	// if now.After(nextTime) {
	// 	nextTime = nextTime.Add(syncInterval)
	// }

	// duration := nextTime.Sub(now)
	// timer := time.NewTimer(duration)

	timer := time.NewTicker(12 * time.Hour)
	defer timer.Stop()

	for {
		<-timer.C

		ds.notifyCandidateSyncAsset()

		// timer.Reset(syncInterval)
	}
}

func (ds *DataSync) notifyCandidateSyncAsset() {
	_, cList := ds.nodeManager.GetValidCandidateNodes()
	for _, node := range cList {
		ds.AddNodeToList(node.NodeID)
	}
}

// AddNodeToList adds a nodeID to the nodeList.
func (ds *DataSync) AddNodeToList(nodeID string) {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	for _, id := range ds.nodeList {
		if id == nodeID {
			return
		}
	}

	ds.nodeList = append(ds.nodeList, nodeID)

	ds.notifySyncLoop()
}

// runs the syncData function continuously when notified.
func (ds *DataSync) startSyncLoop() {
	for {
		<-ds.waitChannel
		ds.syncData()
	}
}

// syncData processes the nodeList to perform data synchronization.
func (ds *DataSync) syncData() {
	for len(ds.nodeList) > 0 {
		nodeID := ds.removeFirstNode()
		err := ds.performDataSync(nodeID)
		if err != nil {
			log.Errorf("do data sync error:%s", err.Error())
		}
	}
}

// notifies the startSyncLoop to process nodeList.
func (ds *DataSync) notifySyncLoop() {
	select {
	case ds.waitChannel <- true:
	default:
	}
}

// removes the first node from nodeList.
func (ds *DataSync) removeFirstNode() string {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	if len(ds.nodeList) == 0 {
		return ""
	}

	nodeID := ds.nodeList[0]
	ds.nodeList = ds.nodeList[1:]
	return nodeID
}

// synchronizes data for the given nodeID.
func (ds *DataSync) performDataSync(nodeID string) error {
	node := ds.nodeManager.GetNode(nodeID)
	if node == nil {
		return xerrors.Errorf("could not get node %s data sync api", nodeID)
	}

	topChecksum, err := ds.fetchTopHash(nodeID)
	if err != nil {
		return xerrors.Errorf("get top hash %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if ok, err := node.CompareTopHash(ctx, topChecksum); err != nil {
		return xerrors.Errorf("compare top hash %w", err)
	} else if ok {
		log.Infof("node %s asset is sync", nodeID)
		return nil
	}

	checksums, err := ds.fetchBucketHashes(nodeID)
	if err != nil {
		return xerrors.Errorf("get hashes of buckets %w", err)
	}

	mismatchBuckets, err := node.CompareBucketHashes(ctx, checksums)
	if err != nil {
		return xerrors.Errorf("compare bucket hashes %w", err)
	}

	if err = ds.nodeManager.UpdateSyncTime(nodeID); err != nil {
		log.Errorf("UpdateSyncTime error %s", err.Error())
	}

	log.Warnf("node %s mismatch buckets len:%d", nodeID, len(mismatchBuckets))
	return nil
}

// retrieves the top hash for a nodeID.
func (ds *DataSync) fetchTopHash(nodeID string) (string, error) {
	return ds.nodeManager.LoadTopHash(nodeID)
}

// retrieves the hashes of buckets for a nodeID.
func (ds *DataSync) fetchBucketHashes(nodeID string) (map[uint32]string, error) {
	hashBytes, err := ds.nodeManager.LoadBucketHashes(nodeID)
	if err != nil {
		return nil, err
	}

	if len(hashBytes) == 0 {
		return make(map[uint32]string), nil
	}

	buffer := bytes.NewBuffer(hashBytes)
	dec := gob.NewDecoder(buffer)

	out := make(map[uint32]string)
	if err = dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}
