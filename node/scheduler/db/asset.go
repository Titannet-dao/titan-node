package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/jmoiron/sqlx"
	"golang.org/x/xerrors"
)

// UpdateUnfinishedReplica update unfinished replica info , return an error if the replica is finished
func (n *SQLDB) UpdateUnfinishedReplica(cInfo *types.ReplicaInfo) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=?, done_size=? WHERE hash=? AND node_id=? AND (status=? or status=?)`, replicaInfoTable)
	result, err := n.db.Exec(query, cInfo.Status, cInfo.DoneSize, cInfo.Hash, cInfo.NodeID, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)
	if err != nil {
		return err
	}

	r, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if r < 1 {
		return xerrors.New("nothing to update")
	}

	return nil
}

// UpdateReplicasStatusToFailed updates the status of unfinished asset replicas
func (n *SQLDB) UpdateReplicasStatusToFailed(hash string) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=? WHERE hash=? AND (status=? or status=?)`, replicaInfoTable)
	_, err := n.db.Exec(query, types.ReplicaStatusFailed, hash, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)

	return err
}

// BatchInitReplicas inserts or updates replica information in batch
func (n *SQLDB) BatchInitReplicas(infos []*types.ReplicaInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (hash, node_id, status, is_candidate) 
				VALUES (:hash, :node_id, :status, :is_candidate) 
				ON DUPLICATE KEY UPDATE status=VALUES(status)`, replicaInfoTable)

	_, err := n.db.NamedExec(query, infos)

	return err
}

// UpdateStateOfAsset update asset state information
func (n *SQLDB) UpdateStateOfAsset(hash, state string, totalBlock, totalSize, retryCount, replenishReplicas int64, serverID dtypes.ServerID) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("SaveAssetRecord Rollback err:%s", err.Error())
		}
	}()

	// update state table
	query := fmt.Sprintf(
		`UPDATE %s SET state=?,retry_count=?,replenish_replicas=? WHERE hash=?`, assetStateTable(serverID))
	_, err = tx.Exec(query, state, retryCount, replenishReplicas, hash)
	if err != nil {
		return err
	}

	// update record table
	dQuery := fmt.Sprintf(`UPDATE %s SET total_size=?, total_blocks=?, end_time=NOW() WHERE hash=?`, assetRecordTable)
	_, err = tx.Exec(dQuery, totalSize, totalBlock, hash)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// LoadAssetRecord load asset record information
func (n *SQLDB) LoadAssetRecord(hash string) (*types.AssetRecord, error) {
	var info types.AssetRecord
	query := fmt.Sprintf("SELECT * FROM %s WHERE hash=?", assetRecordTable)
	err := n.db.Get(&info, query, hash)
	if err != nil {
		return nil, err
	}

	stateInfo, err := n.LoadAssetState(hash, info.ServerID)
	if err != nil {
		return nil, err
	}

	info.State = stateInfo.State
	info.RetryCount = stateInfo.RetryCount
	info.ReplenishReplicas = stateInfo.ReplenishReplicas

	return &info, nil
}

// LoadAssetRecords load the asset records from the incoming scheduler
func (n *SQLDB) LoadAssetRecords(statuses []string, limit, offset int, serverID dtypes.ServerID) (*sqlx.Rows, error) {
	if limit > loadAssetRecordsDefaultLimit || limit == 0 {
		limit = loadAssetRecordsDefaultLimit
	}
	sQuery := fmt.Sprintf(`SELECT * FROM %s a LEFT JOIN %s b ON a.hash = b.hash WHERE state in (?) order by a.hash asc LIMIT ? OFFSET ?`, assetStateTable(serverID), assetRecordTable)
	query, args, err := sqlx.In(sQuery, statuses, limit, offset)
	if err != nil {
		return nil, err
	}

	query = n.db.Rebind(query)
	return n.db.QueryxContext(context.Background(), query, args...)
}

// LoadReplicasByHash load asset replica information based on hash and statuses.
func (n *SQLDB) LoadReplicasByHash(hash string, statuses []types.ReplicaStatus) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE hash=? AND status in (?)`, replicaInfoTable)
	query, args, err := sqlx.In(sQuery, hash, statuses)
	if err != nil {
		return nil, err
	}

	query = n.db.Rebind(query)
	return n.db.QueryxContext(context.Background(), query, args...)
}

// LoadAssetReplicas load asset replica information based on hash.
func (n *SQLDB) LoadAssetReplicas(hash string) ([]*types.ReplicaInfo, error) {
	var out []*types.ReplicaInfo
	query := fmt.Sprintf(`SELECT * FROM %s WHERE hash=? `, replicaInfoTable)
	if err := n.db.Select(&out, query, hash); err != nil {
		return nil, err
	}

	return out, nil
}

// LoadSucceededReplicaCount retrieves the succeeded replica count
func (n *SQLDB) LoadSucceededReplicaCount() (int, error) {
	// TODO Initialize to cache
	query := fmt.Sprintf(`SELECT count(hash) FROM %s WHERE status=?`, replicaInfoTable)

	var count int
	err := n.db.Get(&count, query, types.ReplicaStatusSucceeded)

	return count, err
}

// UpdateAssetRecordExpiry resets asset record expiration time based on hash and eTime
func (n *SQLDB) UpdateAssetRecordExpiry(hash string, eTime time.Time, serverID dtypes.ServerID) error {
	query := fmt.Sprintf(`UPDATE %s SET expiration=? WHERE hash=? AND scheduler_sid=?`, assetRecordTable)
	_, err := n.db.Exec(query, eTime, hash, serverID)

	return err
}

// LoadExpiredAssetRecords load all expired asset records based on serverID.
func (n *SQLDB) LoadExpiredAssetRecords(serverID dtypes.ServerID) ([]*types.AssetRecord, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE scheduler_sid=? AND expiration <= NOW() LIMIT ?`, assetRecordTable)

	var out []*types.AssetRecord
	if err := n.db.Select(&out, query, serverID, loadExpiredAssetRecordsDefaultLimit); err != nil {
		return nil, err
	}

	return out, nil
}

// LoadUnfinishedPullAssetNodes retrieves the node IDs for all nodes that have not yet finished pulling an asset for a given asset hash.
func (n *SQLDB) LoadUnfinishedPullAssetNodes(hash string) ([]string, error) {
	var nodes []string
	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE hash=? AND (status=? or status=?)`, replicaInfoTable)
	err := n.db.Select(&nodes, query, hash, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)
	return nodes, err
}

// DeleteAssetRecord removes all records associated with a given asset hash from the database.
func (n *SQLDB) DeleteAssetRecord(hash string, serverID dtypes.ServerID, info *types.AssetEventInfo) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("DeleteAssetRecord Rollback err:%s", err.Error())
		}
	}()

	// asset record info
	dQuery := fmt.Sprintf(`DELETE FROM %s WHERE hash=? AND scheduler_sid=?`, assetRecordTable)
	result, err := tx.Exec(dQuery, hash, serverID)
	if err != nil {
		return err
	}

	r, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if r < 1 {
		return xerrors.New("nothing to delete")
	}

	// asset info
	dQuery = fmt.Sprintf(`DELETE FROM %s WHERE hash=?`, assetStateTable(serverID))
	_, err = tx.Exec(dQuery, hash)
	if err != nil {
		return err
	}

	// replica info
	cQuery := fmt.Sprintf(`DELETE FROM %s WHERE hash=? `, replicaInfoTable)
	_, err = tx.Exec(cQuery, hash)
	if err != nil {
		return err
	}

	// asset event
	query := fmt.Sprintf(`INSERT INTO %s (hash, event, requester, details)
	VALUES (?, ?, ?, ?)`, assetEventsTable)

	_, err = tx.Exec(query, info.Hash, info.Event, info.Requester, info.Details)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// DeleteAssetReplica remove a replica associated with a given asset hash from the database.
func (n *SQLDB) DeleteAssetReplica(hash, nodeID string, info *types.AssetEventInfo) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("DeleteAssetReplica Rollback err:%s", err.Error())
		}
	}()

	query := fmt.Sprintf(`DELETE FROM %s WHERE hash=? AND node_id=?`, replicaInfoTable)
	_, err = tx.Exec(query, hash, nodeID)
	if err != nil {
		return err
	}

	// asset event
	eQuery := fmt.Sprintf(`INSERT INTO %s (hash, event, requester, details)
		VALUES (?, ?, ?, ?)`, assetEventsTable)

	_, err = tx.Exec(eQuery, info.Hash, info.Event, info.Requester, info.Details)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// DeleteUnfinishedReplicas deletes the incomplete replicas with the given hash from the database.
func (n *SQLDB) DeleteUnfinishedReplicas(hash string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE hash=? AND status!=?`, replicaInfoTable)
	_, err := n.db.Exec(query, hash, types.ReplicaStatusSucceeded)

	return err
}

// LoadReplicas load information about all replicas whose end_time is between startTime and endTime, limited to "count" results and starting from "cursor".
func (n *SQLDB) LoadReplicas(startTime time.Time, endTime time.Time, cursor, count int) (*types.ListReplicaInfosRsp, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(hash) FROM %s WHERE end_time between ? and ?`, replicaInfoTable)
	if err := n.db.Get(&total, countSQL, startTime, endTime); err != nil {
		return nil, err
	}

	if count > loadReplicaInfosDefaultLimit {
		count = loadReplicaInfosDefaultLimit
	}

	query := fmt.Sprintf(`SELECT * FROM %s WHERE end_time between ? and ? limit ?,?`, replicaInfoTable)

	var out []*types.ReplicaInfo
	if err := n.db.Select(&out, query, startTime, endTime, cursor, count); err != nil {
		return nil, err
	}

	return &types.ListReplicaInfosRsp{Replicas: out, Total: total}, nil
}

// AssetExists checks if an asset exists in the state machine table of the specified server.
func (n *SQLDB) AssetExists(hash string, serverID dtypes.ServerID) (bool, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(hash) FROM %s WHERE hash=?`, assetStateTable(serverID))
	if err := n.db.Get(&total, countSQL, hash); err != nil {
		return false, err
	}

	return total > 0, nil
}

// LoadAssetCount count asset
func (n *SQLDB) LoadAssetCount(serverID dtypes.ServerID) (int, error) {
	var size int
	cmd := fmt.Sprintf("SELECT count(hash) FROM %s ;", assetStateTable(serverID))
	err := n.db.Get(&size, cmd)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// LoadAllAssetRecords loads all asset records for a given server ID.
func (n *SQLDB) LoadAllAssetRecords(serverID dtypes.ServerID) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s a LEFT JOIN %s b ON a.hash = b.hash`, assetStateTable(serverID), assetRecordTable)

	return n.db.QueryxContext(context.Background(), sQuery)
}

// LoadAssetState loads the state of the asset for a given server ID.
func (n *SQLDB) LoadAssetState(hash string, serverID dtypes.ServerID) (*types.AssetStateInfo, error) {
	var info types.AssetStateInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE hash=?", assetStateTable(serverID))
	if err := n.db.Get(&info, query, hash); err != nil {
		return nil, err
	}
	return &info, nil
}

// SaveAssetRecord  saves an asset record into the database.
func (n *SQLDB) SaveAssetRecord(rInfo *types.AssetRecord, eInfo *types.AssetEventInfo) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("SaveRecordOfAsset Rollback err:%s", err.Error())
		}
	}()

	// asset record
	query := fmt.Sprintf(
		`INSERT INTO %s (hash, scheduler_sid, cid, edge_replicas, candidate_replicas, expiration) 
		        VALUES (:hash, :scheduler_sid, :cid, :edge_replicas, :candidate_replicas, :expiration)
				ON DUPLICATE KEY UPDATE scheduler_sid=:scheduler_sid, edge_replicas=:edge_replicas, candidate_replicas=:candidate_replicas, expiration=:expiration`, assetRecordTable)
	_, err = tx.NamedExec(query, rInfo)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(
		`INSERT INTO %s (hash, state, replenish_replicas) 
		        VALUES (?, ?, ?) 
				ON DUPLICATE KEY UPDATE state=?, replenish_replicas=?`, assetStateTable(rInfo.ServerID))
	_, err = tx.Exec(query, rInfo.Hash, "", rInfo.ReplenishReplicas, "", rInfo.ReplenishReplicas)
	if err != nil {
		return err
	}

	// asset event
	eQuery := fmt.Sprintf(`INSERT INTO %s (hash, event, requester, details)
		VALUES (?, ?, ?, ?)`, assetEventsTable)

	_, err = tx.Exec(eQuery, eInfo.Hash, eInfo.Event, eInfo.Requester, eInfo.Details)
	if err != nil {
		return err
	}

	return tx.Commit()
}
