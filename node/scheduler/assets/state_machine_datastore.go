package assets

import (
	"bytes"
	"context"
	"strings"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/jmoiron/sqlx"
)

// Datastore represents the asset datastore
type Datastore struct {
	assetDB *db.SQLDB
	dtypes.ServerID
}

// NewDatastore creates a new AssetDatastore
func NewDatastore(db *db.SQLDB, serverID dtypes.ServerID) *Datastore {
	return &Datastore{
		assetDB:  db,
		ServerID: serverID,
	}
}

// Close closes the asset datastore
func (d *Datastore) Close() error {
	return nil
}

func trimPrefix(key datastore.Key) string {
	return strings.Trim(key.String(), "/")
}

// Get retrieves data from the datastore
func (d *Datastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	cInfo, err := d.assetDB.LoadAssetRecord(trimPrefix(key))
	if err != nil {
		return nil, err
	}

	cInfo.ReplicaInfos, err = d.assetDB.LoadReplicasByStatus(cInfo.Hash, types.ReplicaStatusAll)
	if err != nil {
		return nil, err
	}

	asset := assetPullingInfoFrom(cInfo)

	valueBuf := new(bytes.Buffer)
	if err := asset.MarshalCBOR(valueBuf); err != nil {
		return nil, err
	}

	return valueBuf.Bytes(), nil
}

// Has  checks if the key exists in the datastore
func (d *Datastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	return d.assetDB.AssetExists(trimPrefix(key), d.ServerID)
}

// GetSize gets the data size from the datastore
func (d *Datastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	return d.assetDB.LoadAssetCount(d.ServerID, Remove.String())
}

// Query queries asset records from the datastore
func (d *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	log.Debugln("------------Datastore Query-------------")

	var rows *sqlx.Rows
	var err error

	rows, err = d.assetDB.LoadAllAssetRecords(d.ServerID, checkAssetReplicaLimit, 0, PullingStates)
	if err != nil {
		log.Errorf("LoadAssets :%s", err.Error())
		return nil, err
	}
	defer rows.Close()

	re := make([]query.Entry, 0)
	// loading assets to local
	for rows.Next() {
		cInfo := &types.AssetRecord{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("asset StructScan err: %s", err.Error())
			continue
		}

		cInfo.ReplicaInfos, err = d.assetDB.LoadReplicasByStatus(cInfo.Hash, types.ReplicaStatusAll)
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
	aInfo := &AssetPullingInfo{}
	if err := aInfo.UnmarshalCBOR(bytes.NewReader(value)); err != nil {
		return err
	}

	aInfo.Hash = AssetHash(trimPrefix(key))

	return d.assetDB.UpdateAssetInfo(aInfo.Hash.String(), aInfo.State.String(), aInfo.Blocks, aInfo.Size, aInfo.RetryCount, aInfo.ReplenishReplicas, d.ServerID)
}

// Delete delete asset record info (This func has no place to call it)
func (d *Datastore) Delete(ctx context.Context, key datastore.Key) error {
	return nil
}

// Sync sync
func (d *Datastore) Sync(ctx context.Context, prefix datastore.Key) error {
	return nil
}

// Batch batch
func (d *Datastore) Batch(ctx context.Context) (datastore.Batch, error) {
	return nil, nil
}

var _ datastore.Batching = (*Datastore)(nil)
