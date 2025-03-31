package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"golang.org/x/xerrors"
)

// UpdateReplicaInfo updates information of unfinished replicas in the database.
// It sets the end time and updates the status and done size of the replica specified by hash and node_id.
// It returns an error if no rows were affected (i.e., if the replica is already finished or does not exist).
func (n *SQLDB) UpdateReplicaInfo(cInfo *types.ReplicaInfo) error {
	query := `UPDATE ` + replicaInfoTable + ` 
              SET end_time = NOW(), 
                  status = ?, 
                  done_size = ?`

	args := []interface{}{cInfo.Status, cInfo.DoneSize}

	if cInfo.Speed > 0 {
		query += `, speed = ?`
		args = append(args, cInfo.Speed)
	}

	if cInfo.ClientID != "" {
		query += `, client_id = ?`
		args = append(args, cInfo.ClientID)
	}

	query += ` WHERE hash = ? AND node_id = ? AND (status = ? OR status = ?)`
	args = append(args, cInfo.Hash, cInfo.NodeID, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)

	result, err := n.db.Exec(query, args...)
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

// LoadNodesOfPullingReplica retrieves a list of node IDs for replicas that are either pulling or waiting.
func (n *SQLDB) LoadNodesOfPullingReplica(hash string) ([]string, error) {
	var nodes []string
	query := fmt.Sprintf("SELECT node_id FROM %s WHERE hash=? AND (status=? or status=?)", replicaInfoTable)
	err := n.db.Select(&nodes, query, hash, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

// UpdateReplicasStatusToFailed updates the status of unfinished asset replicas to 'failed'.
func (n *SQLDB) UpdateReplicasStatusToFailed(hash string) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=? WHERE hash=? AND (status=? or status=?)`, replicaInfoTable)
	_, err := n.db.Exec(query, types.ReplicaStatusFailed, hash, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)

	return err
}

// SaveReplicaStatus inserts or updates the status of a single replica in the database.
func (n *SQLDB) SaveReplicaStatus(info *types.ReplicaInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (hash, node_id, status, is_candidate, start_time, total_size, workload_id)
				VALUES (:hash, :node_id, :status, :is_candidate, NOW(), :total_size, :workload_id)
				ON DUPLICATE KEY UPDATE status=:status, start_time=NOW(), total_size=:total_size, workload_id=:workload_id`, replicaInfoTable)

	_, err := n.db.NamedExec(query, info)
	return err
}

// UpdateAssetInfo updates the asset information including state, retry count, and replicas in multiple tables within a transaction.
func (n *SQLDB) UpdateAssetInfo(hash, state string, totalBlock, totalSize, retryCount, replenishReplicas int64, serverID dtypes.ServerID) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("UpdateAssetInfo Rollback err:%s", err.Error())
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

// LoadAssetRecord retrieves asset record information based on the hash and merges it with state information.
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

// LoadAssetRecords retrieves all asset records from a specified server ID filtered by state.
func (n *SQLDB) LoadAssetRecords(statuses []string, serverID dtypes.ServerID) ([]*types.AssetRecord, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s a LEFT JOIN %s b ON a.hash = b.hash WHERE state in (?) `, assetStateTable(serverID), assetRecordTable)
	query, args, err := sqlx.In(sQuery, statuses)
	if err != nil {
		return nil, err
	}

	out := make([]*types.AssetRecord, 0)

	query = n.db.Rebind(query)
	n.db.Select(&out, query, args...)

	return out, nil
}

// LoadAssetRecordRowsWithCID retrieves a paginated list of all asset records with cid
func (n *SQLDB) LoadAssetRecordRowsWithCID(cids []string, serverID dtypes.ServerID) (*sqlx.Rows, error) {
	if len(cids) > loadAssetRecordsDefaultLimit {
		return nil, fmt.Errorf("limit:%d", loadAssetRecordsDefaultLimit)
	}

	sQuery := fmt.Sprintf(`SELECT * FROM %s a LEFT JOIN %s b ON a.hash = b.hash WHERE cid in (?) `, assetRecordTable, assetStateTable(serverID))
	query, args, err := sqlx.In(sQuery, cids)
	if err != nil {
		return nil, err
	}

	query = n.db.Rebind(query)
	return n.db.QueryxContext(context.Background(), query, args...)
}

// LoadAssetRecordRows retrieves a paginated list of all asset records from a specified server ID filtered by state.
func (n *SQLDB) LoadAssetRecordRows(statuses []string, limit, offset int, serverID dtypes.ServerID) (*sqlx.Rows, error) {
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

// LoadReplicasByStatus retrieves a list of replica information for a specific hash filtered by status.
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

// LoadReplicasByHashes retrieves replica information based on the provided hashes and node ID.
func (n *SQLDB) LoadReplicasByHashes(hashes []string, nodeID string) ([]*types.ReplicaInfo, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE node_id=? AND hash in (?)`, replicaInfoTable)
	query, args, err := sqlx.In(sQuery, nodeID, hashes)
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

// LoadReplicaCountByStatus retrieves a count of replica information for a specific hash filtered by status.
func (n *SQLDB) LoadReplicaCountByStatus(hash string, statuses []types.ReplicaStatus) (int, error) {
	sQuery := fmt.Sprintf(`SELECT count(*) FROM %s WHERE hash=? AND status in (?)`, replicaInfoTable)
	query, args, err := sqlx.In(sQuery, hash, statuses)
	if err != nil {
		return 0, err
	}

	var out int
	query = n.db.Rebind(query)
	if err := n.db.Get(&out, query, args...); err != nil {
		return 0, err
	}

	return out, nil
}

// LoadAllHashesOfNode retrieves all hash identifiers for a given node ID where the replica status is 'succeeded'.
func (n *SQLDB) LoadAllHashesOfNode(nodeID string) ([]string, error) {
	var out []string
	query := fmt.Sprintf(`SELECT hash FROM %s WHERE node_id=? AND status=?`, replicaInfoTable)
	if err := n.db.Select(&out, query, nodeID, types.ReplicaStatusSucceeded); err != nil {
		return nil, err
	}

	return out, nil
}

// LoadReplicasByHash retrieves a paginated list of replicas for a specific hash where the status is 'succeeded'.
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

// LoadSucceedReplicaCountNodeID retrieves the count of successful replicas for a given node ID.
func (n *SQLDB) LoadSucceedReplicaCountNodeID(nodeID string) (int64, error) {
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE node_id=? AND status=?", replicaInfoTable)
	var count int64
	err := n.db.Get(&count, countQuery, nodeID, types.ReplicaStatusSucceeded)
	if err != nil {
		return count, err
	}

	return count, nil
}

// LoadPullingReplicaCountNodeID retrieves the count of pulling replicas for a given node ID.
func (n *SQLDB) LoadPullingReplicaCountNodeID(nodeID string) (int64, error) {
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE node_id=? AND (status=? or status=?)", replicaInfoTable)
	var count int64
	err := n.db.Get(&count, countQuery, nodeID, types.ReplicaStatusWaiting, types.ReplicaStatusPulling)
	if err != nil {
		return count, err
	}

	return count, nil
}

// LoadSucceedReplicasByNodeID retrieves a list of successful replica information for a given node ID, with pagination support.
func (n *SQLDB) LoadSucceedReplicasByNodeID(nodeID string, limit, offset int) (*types.ListNodeAssetRsp, error) {
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

// LoadFailedReplicas retrieves a list of replicas that have failed.
func (n *SQLDB) LoadFailedReplicas() ([]*types.ReplicaInfo, error) {
	var infos []*types.ReplicaInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE status=? ", replicaInfoTable)

	err := n.db.Select(&infos, query, types.ReplicaStatusFailed)
	if err != nil {
		return nil, err
	}

	return infos, nil
}

// LoadNodeReplicasByNode retrieves all replicas for a specified node ID, filtered by status, with pagination support.
func (n *SQLDB) LoadNodeReplicasByNode(nodeID string, limit, offset int, statues []types.ReplicaStatus) (*types.ListNodeReplicaRsp, error) {
	res := new(types.ListNodeReplicaRsp)
	query := fmt.Sprintf("SELECT a.hash,a.start_time,a.status,a.done_size,a.end_time,b.cid,b.total_size FROM %s a LEFT JOIN %s b ON a.hash = b.hash WHERE a.node_id=? AND a.status in (?) order by a.start_time desc LIMIT ? OFFSET ?", replicaInfoTable, assetRecordTable)
	if limit > loadReplicaDefaultLimit {
		limit = loadReplicaDefaultLimit
	}

	srQuery, args, err := sqlx.In(query, nodeID, statues, limit, offset)
	if err != nil {
		return nil, err
	}

	var infos []*types.NodeReplicaInfo
	srQuery = n.db.Rebind(srQuery)
	err = n.db.Select(&infos, srQuery, args...)
	if err != nil {
		return nil, err
	}

	res.NodeReplicaInfos = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE node_id=? AND status in (?)", replicaInfoTable)
	srQuery, args, err = sqlx.In(countQuery, nodeID, statues)
	if err != nil {
		return nil, err
	}

	var count int
	srQuery = n.db.Rebind(srQuery)
	err = n.db.Get(&count, srQuery, args...)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// LoadReplicaSizeByNodeID calculates the total size of successful replicas for a specific node ID.
func (n *SQLDB) LoadReplicaSizeByNodeID(nodeID string) (int64, error) {
	// SELECT SUM(b.total_size) AS total_size FROM replica_info a JOIN asset_record b ON a.hash = b.hash WHERE a.status = 3 AND a.node_id='e_77dafc142748480bb38b5f45628807bd';
	size := int64(0)
	query := fmt.Sprintf("SELECT COALESCE(SUM(b.total_size), 0) FROM %s a JOIN %s b ON a.hash = b.hash WHERE a.status=? AND a.node_id=?", replicaInfoTable, assetRecordTable)
	err := n.db.Get(&size, query, types.ReplicaStatusSucceeded, nodeID)
	if err != nil {
		return 0, err
	}

	return size, nil
}

// UpdateAssetRecordExpiration updates the expiration time for an asset record based on the provided hash and new expiration time.
func (n *SQLDB) UpdateAssetRecordExpiration(hash string, eTime time.Time) error {
	query := fmt.Sprintf(`UPDATE %s SET expiration=? WHERE hash=?`, assetRecordTable)
	_, err := n.db.Exec(query, eTime, hash)

	return err
}

// LoadExpiredAssetRecords retrieves asset records that have expired according to a given server ID and asset statuses.
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

// DeleteAssetReplica removes a replica associated with a given asset hash from the database.
func (n *SQLDB) DeleteAssetReplica(hash, nodeID, cid string) error {
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
	err = n.saveReplicaEvent(tx, &types.AssetReplicaEventInfo{Hash: hash, Event: types.ReplicaEventRemove, NodeID: nodeID, Cid: cid})
	if err != nil {
		return err
	}

	return tx.Commit()
}

// AssetExists checks if an asset exists in the state table for a specific server ID.
func (n *SQLDB) AssetExists(hash string, serverID dtypes.ServerID) (bool, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(hash) FROM %s WHERE hash=? `, assetStateTable(serverID))
	if err := n.db.Get(&total, countSQL, hash); err != nil {
		return false, err
	}

	return total > 0, nil
}

// GetNodePullingCount counts the number of assets being pulled or waiting by a node, filtered by specific asset statuses.
func (n *SQLDB) GetNodePullingCount(serverID dtypes.ServerID, nodeID string, statuses []string) (int64, error) {
	var hs []string
	hQuery := fmt.Sprintf(`SELECT hash FROM %s WHERE state in (?) `, assetStateTable(serverID))
	shQuery, args, err := sqlx.In(hQuery, statuses)
	if err != nil {
		return 0, err
	}

	shQuery = n.db.Rebind(shQuery)
	if err := n.db.Select(&hs, shQuery, args...); err != nil {
		return 0, err
	}

	var total int64
	hQuery = fmt.Sprintf(`SELECT count(*) FROM %s WHERE node_id=? AND (status=? OR status=?) AND hash in (?)`, replicaInfoTable)
	shQuery, args, err = sqlx.In(hQuery, nodeID, types.ReplicaStatusPulling, types.ReplicaStatusWaiting, hs)
	if err != nil {
		return 0, err
	}

	shQuery = n.db.Rebind(shQuery)
	if err := n.db.Get(&total, shQuery, args...); err != nil {
		return 0, err
	}

	return total, nil
}

// DeleteReplicaOfTimeout deletes replicas that are either pulling or waiting and not included in a specified list of hashes.
func (n *SQLDB) DeleteReplicaOfTimeout(statuses, hs []string) error {
	if len(hs) > 0 {
		hQuery := fmt.Sprintf(`DELETE FROM %s WHERE  (status=? OR status=?) AND hash not in (?)`, replicaInfoTable)
		shQuery, args, err := sqlx.In(hQuery, types.ReplicaStatusPulling, types.ReplicaStatusWaiting, hs)
		if err != nil {
			return err
		}

		shQuery = n.db.Rebind(shQuery)
		if _, err = n.db.Exec(shQuery, args...); err != nil {
			return err
		}
	} else {
		hQuery := fmt.Sprintf(`DELETE FROM %s WHERE  (status=? OR status=?) `, replicaInfoTable)
		_, err := n.db.Exec(hQuery, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)

		return err
	}

	return nil
}

// LoadAssetCount retrieves the number of asset records for a given server ID, excluding a specific state.
func (n *SQLDB) LoadAssetCount(serverID dtypes.ServerID, filterState string) (int, error) {
	var size int
	cmd := fmt.Sprintf("SELECT count(hash) FROM %s WHERE state!=?", assetStateTable(serverID))
	err := n.db.Get(&size, cmd, filterState)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// LoadActiveAssetRecords retrieves detailed information about nodes, including their type and last seen time.
func (n *SQLDB) LoadActiveAssetRecords(serverID dtypes.ServerID, limit, offset int) (*sqlx.Rows, int64, error) {
	t := time.Now().Add(-(time.Hour * 6))

	var total int64
	cQuery := fmt.Sprintf(`SELECT count(hash) FROM %s  where end_time>?`, assetRecordTable)
	err := n.db.Get(&total, cQuery, t)
	if err != nil {
		return nil, 0, err
	}

	if limit > loadAssetRecordsDefaultLimit || limit == 0 {
		limit = loadAssetRecordsDefaultLimit
	}

	sQuery := fmt.Sprintf(`SELECT * FROM %s a LEFT JOIN %s b ON a.hash = b.hash WHERE a.end_time>? order by a.hash asc LIMIT ? OFFSET ?`, assetRecordTable, assetStateTable(serverID))
	rows, err := n.db.QueryxContext(context.Background(), sQuery, t, limit, offset)
	return rows, total, err
}

// LoadAssetRecordsByDateRange retrieves detailed information about nodes, including their type and last seen time.
func (n *SQLDB) LoadAssetRecordsByDateRange(serverID dtypes.ServerID, limit, offset int, start, end time.Time) (*sqlx.Rows, int64, error) {
	var total int64
	cQuery := fmt.Sprintf(`SELECT count(hash) FROM %s  where end_time BETWEEN ? AND ?`, assetRecordTable)
	err := n.db.Get(&total, cQuery, start, end)
	if err != nil {
		return nil, 0, err
	}

	if limit > loadAssetRecordsDefaultLimit || limit == 0 {
		limit = loadAssetRecordsDefaultLimit
	}

	sQuery := fmt.Sprintf(`SELECT * FROM %s a LEFT JOIN %s b ON a.hash = b.hash WHERE a.end_time BETWEEN ? AND ? order by a.hash asc LIMIT ? OFFSET ?`, assetRecordTable, assetStateTable(serverID))
	rows, err := n.db.QueryxContext(context.Background(), sQuery, start, end, limit, offset)
	return rows, total, err
}

// LoadAllAssetRecords retrieves all asset records for a specified server ID, applying a filter by state with pagination.
func (n *SQLDB) LoadAllAssetRecords(serverID dtypes.ServerID, limit, offset int, statuses []string) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s a LEFT JOIN %s b ON a.hash = b.hash WHERE a.state in (?) order by a.hash asc limit ? offset ?`, assetStateTable(serverID), assetRecordTable)
	query, args, err := sqlx.In(sQuery, statuses, limit, offset)
	if err != nil {
		return nil, err
	}

	query = n.db.Rebind(query)
	return n.db.QueryxContext(context.Background(), query, args...)
}

// LoadNeedRefillAssetRecords finds an asset record that needs replenishing of replicas below a specified threshold, filtered by asset state.
func (n *SQLDB) LoadNeedRefillAssetRecords(serverID dtypes.ServerID, replicas int64, status string) (*types.AssetRecord, error) {
	var info types.AssetRecord

	sQuery := fmt.Sprintf(`SELECT * FROM %s a LEFT JOIN %s b ON a.hash = b.hash WHERE a.state=? AND edge_replicas<? limit 1;`, assetStateTable(serverID), assetRecordTable)
	query, args, err := sqlx.In(sQuery, status, replicas)
	if err != nil {
		return nil, err
	}

	query = n.db.Rebind(query)
	err = n.db.Get(&info, query, args...)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

// LoadAssetStateInfo fetches the state information of an asset for a given server ID based on the asset's hash.
func (n *SQLDB) LoadAssetStateInfo(hash string, serverID dtypes.ServerID) (*types.AssetStateInfo, error) {
	var info types.AssetStateInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE hash=?", assetStateTable(serverID))
	if err := n.db.Get(&info, query, hash); err != nil {
		return nil, err
	}
	return &info, nil
}

// UpdateAssetRecordReplicaCount updates the count of edge replicas for a specific content identifier in the asset record table.
func (n *SQLDB) UpdateAssetRecordReplicaCount(hash string, count int) error {
	query := fmt.Sprintf(`UPDATE %s SET edge_replicas=? WHERE hash=?`, assetRecordTable)
	_, err := n.db.Exec(query, count, hash)

	return err
}

// SaveAssetRecord saves or updates an asset record in the database within a transaction, ensuring atomicity of asset data and state updates.
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
		`INSERT INTO %s (hash, scheduler_sid, cid, edge_replicas, candidate_replicas, expiration, bandwidth, total_size, created_time, note, source, owner) 
		        VALUES (:hash, :scheduler_sid, :cid, :edge_replicas, :candidate_replicas, :expiration, :bandwidth, :total_size, :created_time, :note, :source, :owner)
				ON DUPLICATE KEY UPDATE scheduler_sid=:scheduler_sid, edge_replicas=:edge_replicas, created_time=:created_time, cid=:cid,
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

// SaveReplenishBackup adds hashes that require replenishment to a dedicated backup table, updating existing entries as needed.
func (n *SQLDB) SaveReplenishBackup(hashes []string) error {
	stmt, err := n.db.Preparex(`INSERT INTO ` + replenishBackupTable + ` (hash)
        VALUES (?)
        ON DUPLICATE KEY UPDATE hash=?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, hash := range hashes {
		if _, err := stmt.Exec(hash, hash); err != nil {
			log.Errorf("SaveReplenishBackup %s err:%s", hash, err.Error())
		}
	}

	return nil
}

// DeleteReplenishBackup removes an entry from the replenish backup table based on the specified hash.
func (n *SQLDB) DeleteReplenishBackup(hash string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE hash=? `, replenishBackupTable)
	_, err := n.db.Exec(query, hash)

	return err
}

// LoadReplenishBackups retrieves a list of hashes from the replenish backup table, limited by the specified maximum count.
func (n *SQLDB) LoadReplenishBackups(limit int) ([]string, error) {
	var out []string
	query := fmt.Sprintf(`SELECT hash FROM %s LIMIT ?`, replenishBackupTable)
	if err := n.db.Select(&out, query, limit); err != nil {
		return nil, err
	}

	return out, nil
}

// DeleteAssetRecordsOfNode cleans up all asset records associated with a specific node ID, ensuring all related entries across multiple tables are removed within a transaction.
func (n *SQLDB) DeleteAssetRecordsOfNode(nodeID string) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("DeleteAssetRecordsOfNode Rollback err:%s", err.Error())
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

// SaveAssetDownloadResult records the result of an asset download for a node, including traffic and bandwidth metrics.
func (n *SQLDB) SaveAssetDownloadResult(info *types.AssetDownloadResult) error {
	query := fmt.Sprintf(`INSERT INTO %s (hash, node_id, total_traffic, peak_bandwidth, user_id) VALUES (:hash, :node_id, :total_traffic, :peak_bandwidth, :user_id) `, assetDownloadTable)
	_, err := n.db.NamedExec(query, info)

	return err
}

// LoadDownloadResultsFromAsset retrieves download metrics for specified asset hashes over a given time range, aggregated to show peak values.
func (n *SQLDB) LoadDownloadResultsFromAsset(ctx context.Context, hashes []string, start, end time.Time) ([]*types.AssetDownloadResultRsp, error) {
	var list []*types.AssetDownloadResultRsp

	sq := squirrel.Select("user_id,hash,COALESCE(SUM(total_traffic), 0) AS total_traffic,COALESCE(MAX(peak_bandwidth), 0) AS peak_bandwidth").
		From(assetDownloadTable).Where("created_time BETWEEN ? AND ?", start, end)
	if len(hashes) > 0 {
		sq = sq.Where(squirrel.Eq{"hash": hashes})
	}
	query, args, err := sq.GroupBy("hash", "user_id").ToSql()
	if err != nil {
		return nil, fmt.Errorf("generate sql of get DownloadResultsFromAssets error:%w", err)
	}

	err = n.db.SelectContext(ctx, &list, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get DownloadResultsFromAssets error:%w", err)
	}

	return list, nil
}

// LoadAssetDownloadResults fetches download results for a specified hash or all hashes within a certain time frame, compiling them into a list with the total count.
func (n *SQLDB) LoadAssetDownloadResults(hash string, start, end time.Time) (*types.ListAssetDownloadRsp, error) {
	res := new(types.ListAssetDownloadRsp)

	var infos []*types.AssetDownloadResult
	// var count int

	if hash != "" {
		query := fmt.Sprintf("SELECT * FROM %s WHERE hash=? AND created_time BETWEEN ? AND ? ", assetDownloadTable)
		err := n.db.Select(&infos, query, hash, start, end)
		if err != nil {
			return nil, err
		}
	} else {
		query := fmt.Sprintf("SELECT * FROM %s WHERE created_time BETWEEN ? AND ?", assetDownloadTable)
		err := n.db.Select(&infos, query, start, end)
		if err != nil {
			return nil, err
		}
	}
	res.AssetDownloadResults = infos

	res.Total = len(infos)

	return res, nil
}

// LoadSucceededReplicasByNode retrieves a paginated list of replicas for a specific hash where the status is 'succeeded'.
func (n *SQLDB) LoadSucceededReplicasByNode(nodeID string, limit, offset int) (*types.ListReplicaRsp, error) {
	res := new(types.ListReplicaRsp)
	var infos []*types.ReplicaInfo

	query := fmt.Sprintf("SELECT * FROM %s WHERE node_id=? AND status=? order by start_time asc LIMIT ? OFFSET ?", replicaInfoTable)
	if limit > loadReplicaDefaultLimit {
		limit = loadReplicaDefaultLimit
	}

	err := n.db.Select(&infos, query, nodeID, types.ReplicaStatusSucceeded, limit, offset)
	if err != nil {
		return nil, err
	}

	res.ReplicaInfos = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE node_id=? AND status=?", replicaInfoTable)
	var count int
	err = n.db.Get(&count, countQuery, nodeID, types.ReplicaStatusSucceeded)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}
