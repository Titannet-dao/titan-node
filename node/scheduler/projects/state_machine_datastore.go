package projects

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

// Datastore represents the project datastore
type Datastore struct {
	projectDB *db.SQLDB
	dtypes.ServerID
}

// NewDatastore creates a new projectDatastore
func NewDatastore(db *db.SQLDB, serverID dtypes.ServerID) *Datastore {
	return &Datastore{
		projectDB: db,
		ServerID:  serverID,
	}
}

// Close closes the project datastore
func (d *Datastore) Close() error {
	return nil
}

func trimPrefix(key datastore.Key) string {
	return strings.Trim(key.String(), "/")
}

// Get retrieves data from the datastore
func (d *Datastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	cInfo, err := d.projectDB.LoadProjectInfo(trimPrefix(key))
	if err != nil {
		return nil, err
	}

	cInfo.DetailsList, err = d.projectDB.LoadProjectReplicaInfos(cInfo.UUID)
	if err != nil {
		return nil, err
	}

	project := projectInfoFrom(cInfo)

	valueBuf := new(bytes.Buffer)
	if err := project.MarshalCBOR(valueBuf); err != nil {
		return nil, err
	}

	return valueBuf.Bytes(), nil
}

// Has  checks if the key exists in the datastore
func (d *Datastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	return d.projectDB.ProjectExists(trimPrefix(key), d.ServerID)
}

// GetSize gets the data size from the datastore
func (d *Datastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	return d.projectDB.LoadProjectCount(d.ServerID, Remove.String())
}

// Query queries project records from the datastore
func (d *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	var rows *sqlx.Rows
	var err error

	rows, err = d.projectDB.LoadAllProjectInfos(d.ServerID, 500, 0, []string{Deploying.String()})
	if err != nil {
		log.Errorf("Load projects :%s", err.Error())
		return nil, err
	}
	defer rows.Close()

	re := make([]query.Entry, 0)
	// loading projects to local
	for rows.Next() {
		cInfo := &types.ProjectInfo{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("project StructScan err: %s", err.Error())
			continue
		}

		cInfo.DetailsList, err = d.projectDB.LoadProjectReplicaInfos(cInfo.UUID)
		if err != nil {
			log.Errorf("project %s load replicas err: %s", cInfo.UUID, err.Error())
			continue
		}

		project := projectInfoFrom(cInfo)
		valueBuf := new(bytes.Buffer)
		if err = project.MarshalCBOR(valueBuf); err != nil {
			log.Errorf("project marshal cbor: %s", err.Error())
			continue
		}

		prefix := "/"
		entry := query.Entry{
			Key: prefix + project.UUID.String(), Size: len(valueBuf.Bytes()),
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

// Put update project record info
func (d *Datastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	aInfo := &ProjectInfo{}
	if err := aInfo.UnmarshalCBOR(bytes.NewReader(value)); err != nil {
		return err
	}

	aInfo.UUID = ProjectID(trimPrefix(key))

	return d.projectDB.UpdateProjectStateInfo(aInfo.UUID.String(), aInfo.State.String(), aInfo.RetryCount, aInfo.ReplenishReplicas, d.ServerID)
}

// Delete delete project record info (This func has no place to call it)
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
