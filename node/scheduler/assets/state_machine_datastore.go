package assets

import (
	"bytes"
	"context"
	"strings"
	"sync"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/jmoiron/sqlx"
)

// Datastore represents the asset datastore
type Datastore struct {
	sync.RWMutex
	assetDB *db.SQLDB
	assetDS *datastore.MapDatastore
	dtypes.ServerID
}

// NewDatastore creates a new AssetDatastore
func NewDatastore(db *db.SQLDB, serverID dtypes.ServerID) *Datastore {
	return &Datastore{
		assetDB:  db,
		assetDS:  datastore.NewMapDatastore(),
		ServerID: serverID,
	}
}

// Close closes the asset datastore
func (d *Datastore) Close() error {
	return d.assetDS.Close()
}

func trimPrefix(key datastore.Key) string {
	return strings.Trim(key.String(), "/")
}

// Get retrieves data from the datastore
func (d *Datastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	d.RLock()
	defer d.RUnlock()
	return d.assetDS.Get(ctx, key)
}

// Has  checks if the key exists in the datastore
func (d *Datastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	d.RLock()
	defer d.RUnlock()
	return d.assetDS.Has(ctx, key)
}

// GetSize gets the data size from the datastore
func (d *Datastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	d.RLock()
	defer d.RUnlock()
	return d.assetDS.GetSize(ctx, key)
}

// Query queries asset records from the datastore
func (d *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	var rows *sqlx.Rows
	var err error

	state := append(FailedStates, PullingStates...)

	rows, err = d.assetDB.LoadAssetRecords(state, q.Limit, q.Offset, d.ServerID)
	if err != nil {
		log.Errorf("LoadAssets :%s", err.Error())
		return nil, err
	}
	defer rows.Close()

	d.Lock()
	defer d.Unlock()

	re := make([]query.Entry, 0)
	// loading assets to local
	for rows.Next() {
		cInfo := &types.AssetRecord{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("asset StructScan err: %s", err.Error())
			continue
		}

		cInfo.ReplicaInfos, err = d.assetDB.LoadAssetReplicas(cInfo.Hash)
		if err != nil {
			log.Errorf("asset %s load replicas err: %s", cInfo.CID, err.Error())
			continue
		}

		asset := assetPullingInfoFrom(cInfo)
		valueBuf := new(bytes.Buffer)
		if err = asset.MarshalCBOR(valueBuf); err != nil {
			log.Errorf("asset marshal cbor: %s", err.Error())
			continue
		}

		prefix := "/"
		entry := query.Entry{
			Key: prefix + asset.Hash.String(), Size: len(valueBuf.Bytes()),
		}

		if err = d.assetDS.Put(ctx, datastore.NewKey(entry.Key), valueBuf.Bytes()); err != nil {
			log.Errorf("datastore loading assets: %v", err)
		}

		if !q.KeysOnly {
			entry.Value = valueBuf.Bytes()
		}

		re = append(re, entry)
	}

	r := query.ResultsWithEntries(q, re)
	r = query.NaiveQueryApply(q, r)

	return r, nil
}

// Put update asset record info
func (d *Datastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	d.Lock()
	defer d.Unlock()

	if err := d.assetDS.Put(ctx, key, value); err != nil {
		log.Errorf("datastore local put: %v", err)
	}
	aInfo := &AssetPullingInfo{}
	if err := aInfo.UnmarshalCBOR(bytes.NewReader(value)); err != nil {
		return err
	}
	if aInfo.Hash == "" {
		return nil
	}

	info := aInfo.ToAssetRecord()
	info.ServerID = d.ServerID

	return d.assetDB.SaveAssetRecord(info)
}

// Delete delete asset record info
func (d *Datastore) Delete(ctx context.Context, key datastore.Key) error {
	d.Lock()
	defer d.Unlock()

	if err := d.assetDS.Delete(ctx, key); err != nil {
		log.Errorf("datastore local delete: %v", err)
	}
	return d.assetDB.DeleteAssetRecord(trimPrefix(key))
}

// Sync sync
func (d *Datastore) Sync(ctx context.Context, prefix datastore.Key) error {
	return nil
}

// Batch batch
func (d *Datastore) Batch(ctx context.Context) (datastore.Batch, error) {
	return d.assetDS.Batch(ctx)
}

var _ datastore.Batching = (*Datastore)(nil)
