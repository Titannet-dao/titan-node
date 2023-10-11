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

// UpdateReplicaInfo update unfinished replica info , return an error if the replica is finished
func (n *SQLDB) UpdateReplicaInfo(cInfo *types.ReplicaInfo) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("Rollback err:%s", err.Error())
		}
	}()

	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=?, done_size=? WHERE hash=? AND node_id=? AND (status=? or status=?)`, replicaInfoTable)
	result, err := tx.Exec(query, cInfo.Status, cInfo.DoneSize, cInfo.Hash, cInfo.NodeID, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)
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

	return tx.Commit()
}

func (n *SQLDB) SaveReplicaEvent(hash, cid, nodeID string, size int64, expiration time.Time, event types.ReplicaEvent) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("Rollback err:%s", err.Error())
		}
	}()

	// replica event
	query := fmt.Sprintf(
		`INSERT INTO %s (hash, event, node_id, total_size, cid, expiration) 
			VALUES (?, ?, ?, ?, ?, ?)`, replicaEventTable)

	_, err = tx.Exec(query, hash, event, nodeID, size, cid, expiration)
	if err != nil {
		return err
	}

	// update node asset count
	query = fmt.Sprintf(`UPDATE %s SET asset_count=asset_count+?,download_traffic=download_traffic+? WHERE node_id=?`, nodeInfoTable)
	_, err = tx.Exec(query, 1, size, nodeID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// UpdateReplicasStatusToFailed updates the status of unfinished asset replicas
func (n *SQLDB) UpdateReplicasStatusToFailed(hash string) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=? WHERE hash=? AND (status=? or status=?)`, replicaInfoTable)
	_, err := n.db.Exec(query, types.ReplicaStatusFailed, hash, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)

	return err
}

// SaveReplicasStatus inserts or updates replicas status
func (n *SQLDB) SaveReplicasStatus(infos []*types.ReplicaInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (hash, node_id, status, is_candidate) 
				VALUES (:hash, :node_id, :status, :is_candidate) 
				ON DUPLICATE KEY UPDATE status=VALUES(status)`, replicaInfoTable)

	_, err := n.db.NamedExec(query, infos)

	return err
}

// UpdateAssetInfo update asset information
func (n *SQLDB) UpdateAssetInfo(hash, state string, totalBlock, totalSize, retryCount, replenishReplicas int64, serverID dtypes.ServerID) error {
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

	stateInfo, err := n.LoadAssetStateInfo(hash, info.ServerID)
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

// LoadReplicasByStatus load asset replica information based on hash and statuses.
func (n *SQLDB) LoadReplicasByStatus(hash string, statuses []types.ReplicaStatus) ([]*types.ReplicaInfo, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE hash=? AND status in (?)`, replicaInfoTable)
	query, args, err := sqlx.In(sQuery, hash, statuses)
	if err != nil {
		return nil, err
	}

	var out []*types.ReplicaInfo
	query = n.db.Rebind(query)
	if err := n.db.Select(&out, query, args...); err != nil {
		return nil, err
	}

	return out, nil
}

// LoadAllHashesOfNode load asset replica information based on node.
func (n *SQLDB) LoadAllHashesOfNode(nodeID string) ([]string, error) {
	var out []string
	query := fmt.Sprintf(`SELECT hash FROM %s WHERE node_id=? AND status=?`, replicaInfoTable)
	if err := n.db.Select(&out, query, nodeID, types.ReplicaStatusSucceeded); err != nil {
		return nil, err
	}

	return out, nil
}

// LoadReplicasByHash load replicas of asset hash.
func (n *SQLDB) LoadReplicasByHash(hash string, limit, offset int) (*types.ListReplicaRsp, error) {
	res := new(types.ListReplicaRsp)
	var infos []*types.ReplicaInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE hash=? AND status=? order by node_id desc LIMIT ? OFFSET ?", replicaInfoTable)
	if limit > loadReplicaDefaultLimit {
		limit = loadReplicaDefaultLimit
	}

	err := n.db.Select(&infos, query, hash, types.ReplicaStatusSucceeded, limit, offset)
	if err != nil {
		return nil, err
	}

	res.ReplicaInfos = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE hash=? AND status=?", replicaInfoTable)
	var count int
	err = n.db.Get(&count, countQuery, hash, types.ReplicaStatusSucceeded)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// LoadReplicasByNodeID load replicas of node.
func (n *SQLDB) LoadReplicasByNodeID(nodeID string, limit, offset int) (*types.ListNodeAssetRsp, error) {
	res := new(types.ListNodeAssetRsp)
	var infos []*types.NodeAssetInfo
	query := fmt.Sprintf("SELECT a.hash,a.end_time,b.cid,b.total_size,b.expiration FROM %s a LEFT JOIN %s b ON a.hash = b.hash WHERE a.node_id=? AND a.status=? order by a.end_time desc LIMIT ? OFFSET ?", replicaInfoTable, assetRecordTable)
	if limit > loadReplicaDefaultLimit {
		limit = loadReplicaDefaultLimit
	}

	err := n.db.Select(&infos, query, nodeID, types.ReplicaStatusSucceeded, limit, offset)
	if err != nil {
		return nil, err
	}

	res.NodeAssetInfos = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE node_id=? AND status=?", replicaInfoTable)
	var count int
	err = n.db.Get(&count, countQuery, nodeID, types.ReplicaStatusSucceeded)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// UpdateAssetRecordExpiration resets asset record expiration time based on hash and eTime
func (n *SQLDB) UpdateAssetRecordExpiration(hash string, eTime time.Time) error {
	query := fmt.Sprintf(`UPDATE %s SET expiration=? WHERE hash=?`, assetRecordTable)
	_, err := n.db.Exec(query, eTime, hash)

	return err
}

// LoadExpiredAssetRecords load all expired asset records based on serverID.
func (n *SQLDB) LoadExpiredAssetRecords(serverID dtypes.ServerID, statuses []string) ([]*types.AssetRecord, error) {
	var hs []string
	hQuery := fmt.Sprintf(`SELECT hash FROM %s WHERE state in (?) `, assetStateTable(serverID))
	shQuery, args, err := sqlx.In(hQuery, statuses)
	if err != nil {
		return nil, err
	}

	shQuery = n.db.Rebind(shQuery)

	if err := n.db.Select(&hs, shQuery, args...); err != nil {
		return nil, err
	}

	rQuery := fmt.Sprintf(`SELECT * FROM %s WHERE hash in (?) AND expiration <= NOW() LIMIT ?`, assetRecordTable)
	var out []*types.AssetRecord

	srQuery, args, err := sqlx.In(rQuery, hs, loadExpiredAssetRecordsDefaultLimit)
	if err != nil {
		return nil, err
	}

	srQuery = n.db.Rebind(srQuery)
	if err := n.db.Select(&out, srQuery, args...); err != nil {
		return nil, err
	}

	return out, nil
}

// DeleteAssetReplica remove a replica associated with a given asset hash from the database.
func (n *SQLDB) DeleteAssetReplica(hash, nodeID string) error {
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

	// replica info
	query := fmt.Sprintf(`DELETE FROM %s WHERE hash=? AND node_id=?`, replicaInfoTable)
	_, err = tx.Exec(query, hash, nodeID)
	if err != nil {
		return err
	}

	// replica event
	query = fmt.Sprintf(
		`INSERT INTO %s (hash, event, node_id) 
			VALUES (?, ?, ?)`, replicaEventTable)

	_, err = tx.Exec(query, hash, types.ReplicaEventRemove, nodeID)
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

// AssetExists checks if an asset exists in the state machine table of the specified server.
func (n *SQLDB) AssetExists(hash string, serverID dtypes.ServerID) (bool, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(hash) FROM %s WHERE hash=? `, assetStateTable(serverID))
	if err := n.db.Get(&total, countSQL, hash); err != nil {
		return false, err
	}

	return total > 0, nil
}

// LoadAssetCount count asset
func (n *SQLDB) LoadAssetCount(serverID dtypes.ServerID, filterState string) (int, error) {
	var size int
	cmd := fmt.Sprintf("SELECT count(hash) FROM %s WHERE state!=?", assetStateTable(serverID))
	err := n.db.Get(&size, cmd, filterState)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// LoadAllAssetRecords loads all asset records for a given server ID.
func (n *SQLDB) LoadAllAssetRecords(serverID dtypes.ServerID, limit, offset int, statuses []string) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s a LEFT JOIN %s b ON a.hash = b.hash WHERE a.state in (?) order by a.hash asc limit ? offset ?`, assetStateTable(serverID), assetRecordTable)
	query, args, err := sqlx.In(sQuery, statuses, limit, offset)
	if err != nil {
		return nil, err
	}

	query = n.db.Rebind(query)
	return n.db.QueryxContext(context.Background(), query, args...)
}

// LoadAssetStateInfo loads the state of the asset for a given server ID.
func (n *SQLDB) LoadAssetStateInfo(hash string, serverID dtypes.ServerID) (*types.AssetStateInfo, error) {
	var info types.AssetStateInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE hash=?", assetStateTable(serverID))
	if err := n.db.Get(&info, query, hash); err != nil {
		return nil, err
	}
	return &info, nil
}

// SaveAssetRecord  saves an asset record into the database.
func (n *SQLDB) SaveAssetRecord(rInfo *types.AssetRecord) error {
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

	// asset record
	query := fmt.Sprintf(
		`INSERT INTO %s (hash, scheduler_sid, cid, edge_replicas, candidate_replicas, expiration, bandwidth, total_size, created_time) 
		        VALUES (:hash, :scheduler_sid, :cid, :edge_replicas, :candidate_replicas, :expiration, :bandwidth, :total_size, :created_time)
				ON DUPLICATE KEY UPDATE scheduler_sid=:scheduler_sid, edge_replicas=:edge_replicas, created_time=:created_time,
				candidate_replicas=:candidate_replicas, expiration=:expiration, bandwidth=:bandwidth, total_size=:total_size`, assetRecordTable)
	_, err = tx.NamedExec(query, rInfo)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(
		`INSERT INTO %s (hash, state, replenish_replicas) 
		        VALUES (?, ?, ?) 
				ON DUPLICATE KEY UPDATE state=?, replenish_replicas=?`, assetStateTable(rInfo.ServerID))
	_, err = tx.Exec(query, rInfo.Hash, rInfo.State, rInfo.ReplenishReplicas, rInfo.State, rInfo.ReplenishReplicas)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// LoadReplicaEventsOfNode Load replica event
func (n *SQLDB) LoadReplicaEventsOfNode(nodeID string, limit, offset int) (*types.ListReplicaEventRsp, error) {
	res := new(types.ListReplicaEventRsp)

	var infos []*types.ReplicaEventInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE node_id=? AND event!=? order by end_time desc LIMIT ? OFFSET ? ", replicaEventTable)
	if limit > loadReplicaEventDefaultLimit {
		limit = loadReplicaEventDefaultLimit
	}

	err := n.db.Select(&infos, query, nodeID, types.ReplicaEventRemove, limit, offset)
	if err != nil {
		return nil, err
	}

	res.ReplicaEvents = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE node_id=? AND event!=?", replicaEventTable)
	var count int
	err = n.db.Get(&count, countQuery, nodeID, types.ReplicaEventRemove)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// LoadReplicaEvents Load replica event
func (n *SQLDB) LoadReplicaEvents(start, end time.Time, limit, offset int) (*types.ListReplicaEventRsp, error) {
	res := new(types.ListReplicaEventRsp)

	var infos []*types.ReplicaEventInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE end_time BETWEEN ? AND ? order by end_time desc LIMIT ? OFFSET ? ", replicaEventTable)
	if limit > loadReplicaEventDefaultLimit {
		limit = loadReplicaEventDefaultLimit
	}

	err := n.db.Select(&infos, query, start, end, limit, offset)
	if err != nil {
		return nil, err
	}

	res.ReplicaEvents = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE end_time BETWEEN ? AND ? ", replicaEventTable)
	var count int
	err = n.db.Get(&count, countQuery, start, end)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// SaveReplenishBackup Save assets that require replenish backups
func (n *SQLDB) SaveReplenishBackup(hashes []string) error {
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

	for _, hash := range hashes {
		query := fmt.Sprintf(
			`INSERT INTO %s (hash) 
		        VALUES (?) 
				ON DUPLICATE KEY UPDATE hash=?`, replenishBackupTable)
		_, err := tx.Exec(query, hash, hash)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DeleteReplenishBackup delete
func (n *SQLDB) DeleteReplenishBackup(hash string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE hash=? `, replenishBackupTable)
	_, err := n.db.Exec(query, hash)

	return err
}

// LoadReplenishBackups load asset replica information
func (n *SQLDB) LoadReplenishBackups() ([]string, error) {
	var out []string
	query := fmt.Sprintf(`SELECT hash FROM %s`, replenishBackupTable)
	if err := n.db.Select(&out, query); err != nil {
		return nil, err
	}

	return out, nil
}

// DeleteAssetRecordsOfNode clean asset records of node
func (n *SQLDB) DeleteAssetRecordsOfNode(nodeID string) error {
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

	query := fmt.Sprintf(`DELETE FROM %s WHERE node_id=? `, replicaInfoTable)
	_, err = tx.Exec(query, nodeID)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE node_id=?`, assetsViewTable)
	_, err = tx.Exec(query, nodeID)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE bucket_id LIKE ?`, bucketTable)
	_, err = tx.Exec(query, nodeID+"%")
	if err != nil {
		return err
	}

	return tx.Commit()
}
