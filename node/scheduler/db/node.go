package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/jmoiron/sqlx"
	"golang.org/x/xerrors"
)

// UpdatePortMapping updates the port mapping for a given node in the database.
func (n *SQLDB) UpdatePortMapping(nodeID, port string) error {
	// update
	query := fmt.Sprintf(`UPDATE %s SET port_mapping=? WHERE node_id=?`, nodeInfoTable)
	_, err := n.db.Exec(query, port, nodeID)
	return err
}

// SaveValidationResultInfos stores multiple validation result records in the database.
func (n *SQLDB) SaveValidationResultInfos(infos []*types.ValidationResultInfo) error {
	query := fmt.Sprintf(`INSERT INTO %s (round_id, node_id, validator_id, status, cid, start_time, end_time, calculated_profit, file_saved, node_count) 
	VALUES (:round_id, :node_id, :validator_id, :status, :cid, :start_time, :end_time, :calculated_profit, :file_saved, :node_count)`, validationResultTable)
	_, err := n.db.NamedExec(query, infos)

	return err
}

// LoadNodeValidationInfo retrieves a specific validation result by round and node ID.
func (n *SQLDB) LoadNodeValidationInfo(roundID, nodeID string) (*types.ValidationResultInfo, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE round_id=? AND node_id=?", validationResultTable)
	var info types.ValidationResultInfo
	err := n.db.Get(&info, query, roundID, nodeID)
	return &info, err
}

// UpdateValidationResultInfo updates existing validation result details in the database.
func (n *SQLDB) UpdateValidationResultInfo(info *types.ValidationResultInfo) error {
	query := fmt.Sprintf(`UPDATE %s SET block_number=:block_number,status=:status, duration=:duration, bandwidth=:bandwidth, end_time=NOW(), profit=:profit, token_id=:token_id WHERE round_id=:round_id AND node_id=:node_id`, validationResultTable)
	_, err := n.db.NamedExec(query, info)
	if err != nil {
		return err
	}

	return nil
}

// UpdateValidationResultStatus marks the validation results as timeout for a given round and node.
func (n *SQLDB) UpdateValidationResultStatus(roundID, nodeID string, status types.ValidationStatus) error {
	query := fmt.Sprintf(`UPDATE %s SET status=?, end_time=NOW() WHERE round_id=? AND node_id=?`, validationResultTable)
	_, err := n.db.Exec(query, status, roundID, nodeID)
	return err
}

// LoadCreateValidationResultInfos fetches validation results that are in the 'create' status.
func (n *SQLDB) LoadCreateValidationResultInfos() ([]*types.ValidationResultInfo, error) {
	var infos []*types.ValidationResultInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE status=?", validationResultTable)

	err := n.db.Select(&infos, query, types.ValidationStatusCreate)
	if err != nil {
		return nil, err
	}

	return infos, nil
}

// LoadValidationResultInfos retrieves a paginated list of validation results for a specific node.
func (n *SQLDB) LoadValidationResultInfos(nodeID string, limit, offset int) (*types.ListValidationResultRsp, error) {
	res := new(types.ListValidationResultRsp)
	var infos []types.ValidationResultInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE node_id=? order by start_time desc LIMIT ? OFFSET ?", validationResultTable)

	if limit > loadValidationResultsDefaultLimit {
		limit = loadValidationResultsDefaultLimit
	}

	err := n.db.Select(&infos, query, nodeID, limit, offset)
	if err != nil {
		return nil, err
	}

	res.ValidationResultInfos = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE node_id=?", validationResultTable)
	var count int
	err = n.db.Get(&count, countQuery, nodeID)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// SaveEdgeUpdateConfig records or updates edge update configuration details.
func (n *SQLDB) SaveEdgeUpdateConfig(info *api.EdgeUpdateConfig) error {
	sqlString := fmt.Sprintf(`INSERT INTO %s (node_type, app_name, version, hash, download_url) VALUES (:node_type, :app_name, :version, :hash, :download_url) ON DUPLICATE KEY UPDATE app_name=:app_name, version=:version, hash=:hash, download_url=:download_url`, edgeUpdateTable)
	_, err := n.db.NamedExec(sqlString, info)
	return err
}

// LoadEdgeUpdateConfigs fetches all edge update configurations, returning them as a map keyed by node type.
func (n *SQLDB) LoadEdgeUpdateConfigs() (map[int]*api.EdgeUpdateConfig, error) {
	query := fmt.Sprintf(`SELECT * FROM %s`, edgeUpdateTable)

	var out []*api.EdgeUpdateConfig
	if err := n.db.Select(&out, query); err != nil {
		return nil, err
	}

	ret := make(map[int]*api.EdgeUpdateConfig)
	for _, info := range out {
		ret[info.NodeType] = info
	}
	return ret, nil
}

// DeleteEdgeUpdateConfig removes an edge update configuration for a specified node type.
func (n *SQLDB) DeleteEdgeUpdateConfig(nodeType int) error {
	deleteString := fmt.Sprintf(`DELETE FROM %s WHERE node_type=?`, edgeUpdateTable)
	_, err := n.db.Exec(deleteString, nodeType)
	return err
}

// SaveNodeInfo upserts a node's information into the database.
func (n *SQLDB) SaveNodeInfo(info *types.NodeInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, mac_location, cpu_cores, memory, node_name, cpu_info, available_disk_space, titan_disk_usage, gpu_info, version, last_seen,
			    disk_type, io_system, system_version, disk_space, bandwidth_up, bandwidth_down, netflow_up, netflow_down, scheduler_sid) 
				VALUES (:node_id, :mac_location, :cpu_cores, :memory, :node_name, :cpu_info, :available_disk_space, :titan_disk_usage, :gpu_info, :version, :last_seen,
				:disk_type, :io_system, :system_version, :disk_space, :bandwidth_up, :bandwidth_down, :netflow_up, :netflow_down, :scheduler_sid) 
				ON DUPLICATE KEY UPDATE node_id=:node_id, scheduler_sid=:scheduler_sid, system_version=:system_version, cpu_cores=:cpu_cores, titan_disk_usage=:titan_disk_usage, gpu_info=:gpu_info,
				memory=:memory, node_name=:node_name, disk_space=:disk_space, cpu_info=:cpu_info, available_disk_space=:available_disk_space, available_disk_space=:available_disk_space, last_seen=:last_seen,
				netflow_up=:netflow_up, netflow_down=:netflow_down ,version=:version`, nodeInfoTable)

	_, err := n.db.NamedExec(query, info)
	return err
}

// UpdateNodeOnlineCount updates the online count of nodes for a given date.
func (n *SQLDB) UpdateNodeOnlineCount(nodeOnlineCount map[string]int, date time.Time) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt2, err := tx.Prepare(`INSERT INTO ` + onlineCountTable + ` (node_id, created_time, online_count)
	VALUES (?, ?, ?)
	ON DUPLICATE KEY UPDATE online_count=online_count+?`)
	if err != nil {
		return err
	}

	defer stmt2.Close()

	for nodeID, count := range nodeOnlineCount {
		_, err = stmt2.Exec(nodeID, date, count, count)
		if err != nil {
			log.Errorf("UpdateOnlineCount %s err:%s", nodeID, err.Error())
		}
	}
	return tx.Commit()
}

// UpdateNodeDynamicInfo updates various dynamic information fields for multiple nodes.
func (n *SQLDB) UpdateNodeDynamicInfo(infos []*types.NodeDynamicInfo) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareNamed(`UPDATE ` + nodeInfoTable + ` SET nat_type=:nat_type, last_seen=:last_seen, online_duration=:online_duration, disk_usage=:disk_usage, bandwidth_up=:bandwidth_up, bandwidth_down=:bandwidth_down, titan_disk_usage=:titan_disk_usage, available_disk_space=:available_disk_space, download_traffic=:download_traffic, upload_traffic=:upload_traffic WHERE node_id=:node_id`)
	if err != nil {
		return err
	}
	// stmt2, err := tx.Prepare(`INSERT INTO ` + onlineCountTable + ` (node_id, created_time, online_count)
	// VALUES (?, ?, ?)
	// ON DUPLICATE KEY UPDATE online_count=online_count+?`)
	// if err != nil {
	// 	return err
	// }

	defer func() {
		stmt.Close()
		// stmt2.Close()
	}()

	batchSize := 500
	for i := 0; i < len(infos); i += batchSize {
		end := i + batchSize
		if end > len(infos) {
			end = len(infos)
		}
		batch := infos[i:end]

		for _, info := range batch {
			if _, err := stmt.Exec(info); err != nil {
				log.Errorf("UpdateNodeDynamicInfo %s, %.4f,%d,%d,%.4f,%.4f err:%s", info.NodeID, info.DiskUsage, info.BandwidthUp, info.BandwidthDown, info.TitanDiskUsage, info.AvailableDiskSpace, err.Error())
			}

			// _, err := stmt2.Exec(info.NodeID, date, info.TodayOnlineTimeWindow, info.TodayOnlineTimeWindow)
			// if err != nil {
			// 	log.Errorf("UpdateOnlineCount %s err:%s", info.NodeID, err.Error())
			// }
		}
	}

	return tx.Commit()
}

// // UpdateNodeDynamicInfo updates various dynamic information fields for multiple nodes.
// func (n *SQLDB) UpdateNodeDynamicInfo(infos []*types.NodeDynamicInfo, date time.Time) error {
// 	stmt, err := n.db.Preparex(`UPDATE ` + nodeInfoTable + ` SET last_seen=?,online_duration=?,disk_usage=?,bandwidth_up=?,bandwidth_down=?,
// 		    titan_disk_usage=?,available_disk_space=?,download_traffic=?,upload_traffic=? WHERE node_id=?`)
// 	if err != nil {
// 		return err
// 	}
// 	defer stmt.Close()

// 	for _, info := range infos {
// 		if _, err := stmt.Exec(info.LastSeen, info.OnlineDuration, info.DiskUsage, info.BandwidthUp, info.BandwidthDown, info.TitanDiskUsage,
// 			info.AvailableDiskSpace, info.DownloadTraffic, info.UploadTraffic, info.NodeID); err != nil {
// 			log.Errorf("SaveReplenishBackup %s, %.4f,%d,%d,%.4f,%.4f err:%s", info.NodeID, info.DiskUsage, info.BandwidthUp, info.BandwidthDown, info.TitanDiskUsage, info.AvailableDiskSpace, err.Error())
// 		}
// 	}

// 	stmt2, err := n.db.Preparex(`INSERT INTO ` + onlineCountTable + ` (node_id, created_time, online_count)
// 	VALUES (?, ?, ?)
// 	ON DUPLICATE KEY UPDATE online_count=?`)
// 	if err != nil {
// 		return err
// 	}
// 	defer stmt2.Close()

// 	for _, info := range infos {
// 		if _, err := stmt2.Exec(info.NodeID, date, info.TodayOnlineTimeWindow, info.TodayOnlineTimeWindow); err != nil {
// 			log.Errorf("UpdateOnlineCount %s err:%s", info.NodeID, err.Error())
// 		}
// 	}

// 	return nil
// }

// SaveNodeRegisterInfos stores registration details for multiple nodes.
func (n *SQLDB) SaveNodeRegisterInfos(details []*types.ActivationDetail) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, created_time, node_type, activation_key, ip)
				VALUES (:node_id, NOW(), :node_type, :activation_key, :ip)`, nodeRegisterTable)

	_, err := n.db.NamedExec(query, details)

	return err
}

// LoadNodeRegisterInfo retrieves registration details for a specific node.
func (n *SQLDB) LoadNodeRegisterInfo(nodeID string) (*types.ActivationDetail, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE node_id=?`, nodeRegisterTable)

	var out types.ActivationDetail
	err := n.db.Get(&out, query, nodeID)
	if err != nil {
		return nil, err
	}

	return &out, nil
}

// SaveNodePublicKey updates the public key associated with a node.
func (n *SQLDB) SaveNodePublicKey(pKey, nodeID string) error {
	query := fmt.Sprintf(`UPDATE %s SET public_key=? WHERE node_id=? `, nodeRegisterTable)
	_, err := n.db.Exec(query, pKey, nodeID)

	return err
}

// UpdateNodeType updates the node type  with a node.
func (n *SQLDB) UpdateNodeType(nodeID string, t types.NodeType) error {
	query := fmt.Sprintf(`UPDATE %s SET node_type=? WHERE node_id=? `, nodeRegisterTable)
	_, err := n.db.Exec(query, t, nodeID)

	return err
}

// SaveMigrateKey updates the migration key associated with a node.
func (n *SQLDB) SaveMigrateKey(pKey, nodeID string) error {
	query := fmt.Sprintf(`UPDATE %s SET migrate_key=? WHERE node_id=? `, nodeRegisterTable)
	_, err := n.db.Exec(query, pKey, nodeID)

	return err
}

// DeleteNodeInfo removes all information related to a node from the database.
func (n *SQLDB) DeleteNodeInfo(nodeID string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE node_id=?`, nodeInfoTable)
	_, err := n.db.Exec(query, nodeID)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE node_id=?`, nodeRegisterTable)
	_, err = n.db.Exec(query, nodeID)
	return err
}

// LoadNodeActivationKey fetches the activation key for a specific node.
func (n *SQLDB) LoadNodeActivationKey(nodeID string) (string, error) {
	var pKey string

	query := fmt.Sprintf(`SELECT activation_key FROM %s WHERE node_id=?`, nodeRegisterTable)
	if err := n.db.Get(&pKey, query, nodeID); err != nil {
		return pKey, err
	}

	return pKey, nil
}

// LoadNodePublicKey retrieves the public key for a specific node.
func (n *SQLDB) LoadNodePublicKey(nodeID string) (string, error) {
	var pKey string

	query := fmt.Sprintf(`SELECT public_key FROM %s WHERE node_id=?`, nodeRegisterTable)
	if err := n.db.Get(&pKey, query, nodeID); err != nil {
		return pKey, err
	}

	return pKey, nil
}

// LoadNodeType fetches the type of a node.
func (n *SQLDB) LoadNodeType(nodeID string) (types.NodeType, error) {
	var nodeType types.NodeType

	query := fmt.Sprintf(`SELECT node_type FROM %s WHERE node_id=?`, nodeRegisterTable)
	if err := n.db.Get(&nodeType, query, nodeID); err != nil {
		return nodeType, err
	}

	return nodeType, nil
}

// NodeExistsFromType checks if a node of a specific type exists.
func (n *SQLDB) NodeExistsFromType(nodeID string, nodeType types.NodeType) error {
	var count int
	cQuery := fmt.Sprintf(`SELECT count(*) FROM %s WHERE node_id=? AND node_type=?`, nodeRegisterTable)
	err := n.db.Get(&count, cQuery, nodeID, nodeType)
	if err != nil {
		return err
	}

	if count < 1 {
		return xerrors.New("node not exists")
	}

	return nil
}

// NodeExists verifies the existence of a node in the database.
func (n *SQLDB) NodeExists(nodeID string) error {
	var count int
	cQuery := fmt.Sprintf(`SELECT count(*) FROM %s WHERE node_id=? `, nodeRegisterTable)
	err := n.db.Get(&count, cQuery, nodeID)
	if err != nil {
		return err
	}

	if count < 1 {
		return xerrors.New("node not exists")
	}

	return nil
}

// RegisterCount calculates the number of registrations from a specific IP address for the current day.
func (n *SQLDB) RegisterCount(ip string) (int, error) {
	var count int
	cQuery := fmt.Sprintf(`SELECT count(*) FROM %s WHERE ip=?`, nodeRegisterTable)
	err := n.db.Get(&count, cQuery, ip)
	if err != nil {
		if err != sql.ErrNoRows {
			return 0, err
		}
	}

	return count, nil
}

// LoadActiveNodeInfos retrieves detailed information about nodes, including their type and last seen time.
func (n *SQLDB) LoadActiveNodeInfos(limit, offset int) (*sqlx.Rows, int64, error) {
	t := time.Now().Add(-(time.Hour * 6))

	var total int64
	cQuery := fmt.Sprintf(`SELECT count(node_id) FROM %s  where last_seen>?`, nodeInfoTable)
	err := n.db.Get(&total, cQuery, t)
	if err != nil {
		return nil, 0, err
	}

	if limit > loadNodeInfosDefaultLimit || limit == 0 {
		limit = loadNodeInfosDefaultLimit
	}

	sQuery := fmt.Sprintf(`SELECT a.*,b.node_type as type FROM %s a LEFT JOIN %s b ON a.node_id = b.node_id where a.last_seen>? order by node_id asc LIMIT ? OFFSET ?`, nodeInfoTable, nodeRegisterTable)
	rows, err := n.db.QueryxContext(context.Background(), sQuery, t, limit, offset)
	return rows, total, err
}

// LoadNodeInfosOfType fetches node information filtered by node type.
func (n *SQLDB) LoadNodeInfosOfType(nodeType int) ([]*types.NodeInfo, error) {
	query := fmt.Sprintf(`SELECT a.*,b.node_type as type FROM %s a LEFT JOIN %s b ON a.node_id = b.node_id where b.node_type=?;`, nodeInfoTable, nodeRegisterTable)

	var out []*types.NodeInfo
	if err := n.db.Select(&out, query, nodeType); err != nil {
		return nil, err
	}

	return out, nil
}

// LoadNodeInfo retrieves detailed information for a specific node.
func (n *SQLDB) LoadNodeInfo(nodeID string) (*types.NodeInfo, error) {
	err := n.NodeExists(nodeID)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`SELECT * FROM %s WHERE node_id=?`, nodeInfoTable)

	var out types.NodeInfo
	err = n.db.Get(&out, query, nodeID)
	if err != nil {
		return nil, err
	}

	sInfo, err := n.LoadNodeStatisticsInfo(nodeID)
	if err != nil {
		log.Warnf("LoadNodeInfo %s load statistics info error:%s \n", nodeID, err.Error())
	}

	out.NodeStatisticsInfo = sInfo

	return &out, nil
}

// LoadNodeLastSeenTime fetches the last seen timestamp for a specific node.
func (n *SQLDB) LoadNodeLastSeenTime(nodeID string) (time.Time, error) {
	var t time.Time
	query := fmt.Sprintf(`SELECT last_seen FROM %s WHERE node_id=?`, nodeInfoTable)
	err := n.db.Get(&t, query, nodeID)
	return t, err
}

// LoadTopHash retrieves the top hash from an asset view for a specific node.
func (n *SQLDB) LoadTopHash(nodeID string) (string, error) {
	query := fmt.Sprintf(`SELECT top_hash FROM %s WHERE node_id=?`, assetsViewTable)

	var out string
	err := n.db.Get(&out, query, nodeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}

	return out, nil
}

// LoadSyncTime fetches the last synchronization time for an asset view related to a specific node.
func (n *SQLDB) LoadSyncTime(nodeID string) (time.Time, error) {
	query := fmt.Sprintf(`SELECT sync_time FROM %s WHERE node_id=?`, assetsViewTable)

	var out time.Time
	err := n.db.Get(&out, query, nodeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}, nil
		}
		return time.Now(), err
	}

	return out, nil
}

// UpdateSyncTime updates the synchronization time for an asset view related to a specific node.
func (n *SQLDB) UpdateSyncTime(nodeID string) error {
	query := fmt.Sprintf(`UPDATE %s SET sync_time=? WHERE node_id=?`, assetsViewTable)
	_, err := n.db.Exec(query, time.Now(), nodeID)
	return err
}

// LoadBucketHashes retrieves the bucket hashes for a specific node from the assets view.
func (n *SQLDB) LoadBucketHashes(nodeID string) ([]byte, error) {
	query := fmt.Sprintf(`SELECT bucket_hashes FROM %s WHERE node_id=?`, assetsViewTable)

	var data []byte
	err := n.db.Get(&data, query, nodeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return data, nil
}

// SaveAssetsView stores or updates the top hash and bucket hashes in the assets view for a node.
func (n *SQLDB) SaveAssetsView(nodeID string, topHash string, bucketHashes []byte) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, top_hash, bucket_hashes) VALUES (?, ?, ?) 
				ON DUPLICATE KEY UPDATE top_hash=?, bucket_hashes=?`, assetsViewTable)

	_, err := n.db.Exec(query, nodeID, topHash, bucketHashes, topHash, bucketHashes)
	return err
}

// DeleteAssetsView removes the assets view record for a specific node.
func (n *SQLDB) DeleteAssetsView(nodeID string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE node_id=?`, assetsViewTable)
	_, err := n.db.Exec(query, nodeID)
	return err
}

// LoadBucket fetches asset identifiers stored in a specific bucket.
func (n *SQLDB) LoadBucket(bucketID string) ([]byte, error) {
	query := fmt.Sprintf(`SELECT asset_hashes FROM %s WHERE bucket_id=?`, bucketTable)

	var data []byte
	err := n.db.Get(&data, query, bucketID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return data, nil
}

// SaveBucket stores or updates asset identifiers in a specific bucket.
func (n *SQLDB) SaveBucket(bucketID string, assetHashes []byte) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (bucket_id, asset_hashes) VALUES (?, ?) 
				ON DUPLICATE KEY UPDATE asset_hashes=?`, bucketTable)

	_, err := n.db.Exec(query, bucketID, assetHashes, assetHashes)
	return err
}

// DeleteBucket removes a bucket record from the database.
func (n *SQLDB) DeleteBucket(bucketID string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE bucket_id=?`, bucketTable)
	_, err := n.db.Exec(query, bucketID)
	return err
}

// SaveWorkloadRecord stores workload records in the database.
func (n *SQLDB) SaveWorkloadRecord(records []*types.WorkloadRecord) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (workload_id, asset_cid, client_id, asset_size, workloads, status, event) 
				VALUES (:workload_id, :asset_cid, :client_id, :asset_size, :workloads, :status, :event)`, workloadRecordTable)

	_, err := n.db.NamedExec(query, records)
	return err
}

// UpdateWorkloadRecord updates a specific workload record's status and details.
func (n *SQLDB) UpdateWorkloadRecord(record *types.WorkloadRecord, status types.WorkloadStatus) error {
	query := fmt.Sprintf(`UPDATE %s SET workloads=?, client_end_time=NOW(), status=? WHERE workload_id=? AND status=?`, workloadRecordTable)
	result, err := n.db.Exec(query, record.Workloads, record.Status, record.WorkloadID, status)
	if err != nil {
		return err
	}

	r, err := result.RowsAffected()
	if r < 1 {
		return xerrors.New("nothing to update")
	}

	return err
}

// LoadWorkloadRecord retrieves workload records based on asset CID, client ID, and status.
func (n *SQLDB) LoadWorkloadRecord(workload *types.WorkloadRecord) ([]*types.WorkloadRecord, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE asset_cid=? AND client_id=? AND status=?`, workloadRecordTable)
	var records []*types.WorkloadRecord
	err := n.db.Select(&records, query, workload.AssetCID, workload.ClientID, workload.Status)
	if err != nil {
		return nil, err
	}

	return records, nil
}

// LoadWorkloadRecordOfID fetches a specific workload record by its identifier.
func (n *SQLDB) LoadWorkloadRecordOfID(workloadID string) (*types.WorkloadRecord, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE workload_id=? `, workloadRecordTable)
	var record types.WorkloadRecord
	err := n.db.Get(&record, query, workloadID)
	if err != nil {
		return nil, err
	}

	return &record, nil
}

// LoadWorkloadRecords retrieves a paginated list of workload records for a specific node.
func (n *SQLDB) LoadWorkloadRecords(nodeID string, limit, offset int) (*types.ListWorkloadRecordRsp, error) {
	res := new(types.ListWorkloadRecordRsp)

	var infos []*types.WorkloadRecord
	query := fmt.Sprintf("SELECT * FROM %s WHERE client_id=? order by client_end_time desc LIMIT ? OFFSET ? ", workloadRecordTable)

	if limit > loadWorkloadDefaultLimit {
		limit = loadWorkloadDefaultLimit
	}

	err := n.db.Select(&infos, query, nodeID, limit, offset)
	if err != nil {
		return nil, err
	}

	res.WorkloadRecordInfos = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE client_id=? ", workloadRecordTable)
	var count int
	err = n.db.Get(&count, countQuery, nodeID)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// LoadUnCalculatedValidationResults fetches validation results that have not yet had profits calculated.
func (n *SQLDB) LoadUnCalculatedValidationResults(maxTime time.Time, limit int) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE calculated_profit=? AND start_time<? order by start_time asc LIMIT ?`, validationResultTable)
	return n.db.QueryxContext(context.Background(), sQuery, false, maxTime, limit)
}

// LoadUnSavedValidationResults retrieves validation results that have not been saved to file.
func (n *SQLDB) LoadUnSavedValidationResults(limit int) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE file_saved=? AND calculated_profit=? order by start_time asc LIMIT ?`, validationResultTable)
	return n.db.QueryxContext(context.Background(), sQuery, false, true, limit)
}

// RemoveInvalidValidationResult deletes validation results that are deemed invalid.
func (n *SQLDB) RemoveInvalidValidationResult(sIDs []int) error {
	rQuery := fmt.Sprintf(`DELETE FROM %s WHERE id in (?)`, validationResultTable)

	srQuery, args, err := sqlx.In(rQuery, sIDs)
	if err != nil {
		return err
	}

	srQuery = n.db.Rebind(srQuery)
	_, err = n.db.Exec(srQuery, args...)
	return err
}

// processes and commits a batch of node profit details within a transaction.
func (n *SQLDB) processNodeProfitBatch(batch []*types.ProfitDetails) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = tx.Rollback()
			if err != nil && err != sql.ErrTxDone {
				log.Errorf("nodeProfitBatch Rollback err:%s", err.Error())
			}
		}
	}()

	for _, profitInfo := range batch {
		// add profit details
		sqlString := fmt.Sprintf(`INSERT INTO %s (node_id, profit, profit_type, size, note, cid, rate) VALUES (:node_id, :profit, :profit_type, :size, :note, :cid, :rate)`, profitDetailsTable)
		_, err = tx.NamedExec(sqlString, profitInfo)
		if err != nil {
			log.Errorf("nodeProfitBatch Exec INSERT %s err:%s", profitInfo.NodeID, err.Error())
			continue
		}

		iQuery := fmt.Sprintf(`UPDATE %s SET profit=profit+?,penalty_profit=penalty_profit+? WHERE node_id=?`, nodeInfoTable)
		_, err = tx.Exec(iQuery, profitInfo.Profit, profitInfo.Penalty, profitInfo.NodeID)
		if err != nil {
			log.Errorf("nodeProfitBatch Exec UPDATE %s err:%s", profitInfo.NodeID, err.Error())
		}
	}

	return tx.Commit()
}

// AddNodeProfitDetails adds profit details for nodes in batches to manage large sets of data efficiently.
func (n *SQLDB) AddNodeProfitDetails(profitInfos []*types.ProfitDetails) error {
	if profitInfos == nil {
		return nil
	}

	const batchSize = 100
	for i := 0; i < len(profitInfos); i += batchSize {
		end := i + batchSize
		if end > len(profitInfos) {
			end = len(profitInfos)
		}
		if err := n.processNodeProfitBatch(profitInfos[i:end]); err != nil {
			return err
		}
	}

	return nil
}

// LoadTodayProfitsForNode calculates the total profit for a specific node for the current day.
func (n *SQLDB) LoadTodayProfitsForNode(nodeID string) (float64, error) {
	start := time.Now()
	start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, start.Location())

	size := 0.0
	query := fmt.Sprintf("SELECT COALESCE(SUM(profit), 0) FROM %s WHERE node_id=? AND created_time>?", profitDetailsTable)
	err := n.db.Get(&size, query, nodeID, start)
	if err != nil {
		return 0, err
	}

	return size, nil
}

// LoadNodeProfits retrieves a paginated list of profit details for a specific node, filtered by profit types.
func (n *SQLDB) LoadNodeProfits(nodeID string, limit, offset int, ts []int) (*types.ListNodeProfitDetailsRsp, error) {
	res := new(types.ListNodeProfitDetailsRsp)
	query := fmt.Sprintf("SELECT * FROM %s WHERE node_id=? AND profit_type in (?) order by created_time desc LIMIT ? OFFSET ?", profitDetailsTable)
	if limit > loadReplicaDefaultLimit {
		limit = loadReplicaDefaultLimit
	}

	srQuery, args, err := sqlx.In(query, nodeID, ts, limit, offset)
	if err != nil {
		return nil, err
	}

	var infos []*types.ProfitDetails
	srQuery = n.db.Rebind(srQuery)
	err = n.db.Select(&infos, srQuery, args...)
	if err != nil {
		return nil, err
	}

	res.Infos = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE node_id=? AND profit_type in (?)", profitDetailsTable)
	srQuery, args, err = sqlx.In(countQuery, nodeID, ts)
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

// SaveDeactivateNode records the time a node was deactivated and adjusts the node's profit and penalty.
func (n *SQLDB) SaveDeactivateNode(nodeID string, time int64, penaltyPoint float64, oldTime int64) error {
	query := fmt.Sprintf(`UPDATE %s SET deactivate_time=?, profit=profit-?,penalty_profit=penalty_profit+? WHERE node_id=? and deactivate_time=?`, nodeInfoTable)
	result, err := n.db.Exec(query, time, penaltyPoint, penaltyPoint, nodeID, oldTime)

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return xerrors.New("optimistic lock failed: version mismatch or node not found")
	}

	return nil
}

// SaveForceOffline updates the force offline status of a node.
func (n *SQLDB) SaveForceOffline(nodeID string, value bool) error {
	query := fmt.Sprintf(`UPDATE %s SET force_offline=? WHERE node_id=?`, nodeInfoTable)
	_, err := n.db.Exec(query, value, nodeID)
	return err
}

// SaveWSServerID updates the WebSocket server ID associated with a node.
func (n *SQLDB) SaveWSServerID(nodeID, wID string) error {
	query := fmt.Sprintf(`UPDATE %s SET ws_server_id=? WHERE node_id=?`, nodeInfoTable)
	_, err := n.db.Exec(query, wID, nodeID)
	return err
}

// LoadWSServerID retrieves the WebSocket server ID associated with a specific node.
func (n *SQLDB) LoadWSServerID(nodeID string) (string, error) {
	var wID string
	query := fmt.Sprintf(`SELECT ws_server_id FROM %s WHERE node_id=?`, nodeInfoTable)
	err := n.db.Get(&wID, query, nodeID)
	return wID, err
}

// LoadDeactivateNodeTime fetches the time at which a node was last deactivated.
func (n *SQLDB) LoadDeactivateNodeTime(nodeID string) (int64, error) {
	query := fmt.Sprintf(`SELECT deactivate_time FROM %s WHERE node_id=?`, nodeInfoTable)

	var time int64
	err := n.db.Get(&time, query, nodeID)
	if err != nil {
		return 0, err
	}

	return time, nil
}

// SaveFreeUpDiskTime records the time at which disk space was last freed up on a node.
func (n *SQLDB) SaveFreeUpDiskTime(nodeID string, time time.Time) error {
	query := fmt.Sprintf(`UPDATE %s SET free_up_disk_time=? WHERE node_id=?`, nodeInfoTable)
	_, err := n.db.Exec(query, time, nodeID)
	return err
}

// LoadFreeUpDiskTime retrieves the time at which disk space was last freed up on a specific node.
func (n *SQLDB) LoadFreeUpDiskTime(nodeID string) (time.Time, error) {
	query := fmt.Sprintf(`SELECT free_up_disk_time FROM %s WHERE node_id=?`, nodeInfoTable)

	var time time.Time
	err := n.db.Get(&time, query, nodeID)
	if err != nil {
		return time, err
	}

	return time, nil
}

// LoadDeactivateNodes fetches the IDs of nodes that have been deactivated before a specified time.
func (n *SQLDB) LoadDeactivateNodes(time int64) ([]string, error) {
	var out []string
	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE deactivate_time>0 AND deactivate_time<?`, nodeInfoTable)
	if err := n.db.Select(&out, query, time); err != nil {
		return nil, err
	}

	return out, nil
}

// CleanData performs a cleanup of outdated records across various tables based on predefined intervals.
func (n *SQLDB) CleanData() {
	query := fmt.Sprintf(`DELETE FROM %s WHERE created_time<DATE_SUB(NOW(), INTERVAL 30 DAY) `, replicaEventTable)
	_, err := n.db.Exec(query)
	if err != nil {
		log.Warnf("CleanData replicaEventTable err:%s", err.Error())
	}

	// cleanTime := time.Now().Add(-5).Unix()
	// query = fmt.Sprintf(`DELETE FROM %s WHERE end_time<? `, retrieveEventTable)
	// _, err = n.db.Exec(query, cleanTime)
	// if err != nil {
	// 	log.Warnf("CleanData retrieveEventTable err:%s", err.Error())
	// }

	query = fmt.Sprintf(`DELETE FROM %s WHERE client_end_time<DATE_SUB(NOW(), INTERVAL 7 DAY) `, workloadRecordTable)
	_, err = n.db.Exec(query)
	if err != nil {
		log.Warnf("CleanData workloadRecordTable err:%s", err.Error())
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE start_time<DATE_SUB(NOW(), INTERVAL 7 DAY) `, validationResultTable)
	_, err = n.db.Exec(query)
	if err != nil {
		log.Warnf("CleanData validationResultTable err:%s", err.Error())
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE created_time<DATE_SUB(NOW(), INTERVAL 10 DAY) `, profitDetailsTable)
	_, err = n.db.Exec(query)
	if err != nil {
		log.Warnf("CleanData profitDetailsTable err:%s", err.Error())
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE created_time<DATE_SUB(NOW(), INTERVAL 8 DAY) `, onlineCountTable)
	_, err = n.db.Exec(query)
	if err != nil {
		log.Warnf("CleanData onlineCountTable err:%s", err.Error())
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE created_time<DATE_SUB(NOW(), INTERVAL 2 DAY) `, assetDownloadTable)
	_, err = n.db.Exec(query)
	if err != nil {
		log.Warnf("CleanData assetDownloadTable err:%s", err.Error())
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE created_time<DATE_SUB(NOW(), INTERVAL 30 DAY) `, projectEventTable)
	_, err = n.db.Exec(query)
	if err != nil {
		log.Warnf("CleanData projectEventTable err:%s", err.Error())
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE created_time<DATE_SUB(NOW(), INTERVAL 30 DAY) `, nodeRetrieveTable)
	_, err = n.db.Exec(query)
	if err != nil {
		log.Warnf("CleanData nodeRetrieveTable err:%s", err.Error())
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE start_time<DATE_SUB(NOW(), INTERVAL 30 DAY) `, serviceEventTable)
	_, err = n.db.Exec(query)
	if err != nil {
		log.Warnf("CleanData serviceEventTable err:%s", err.Error())
	}

	// query = fmt.Sprintf(`DELETE FROM %s WHERE created_time<DATE_SUB(NOW(), INTERVAL 30 DAY) `, bandwidthScoreEventTable)
	// _, err = n.db.Exec(query)
	// if err != nil {
	// 	log.Warnf("CleanData bandwidthScoreEventTable err:%s", err.Error())
	// }
}

// SaveCandidateCodeInfo stores information related to candidate codes used in node verification or testing.
func (n *SQLDB) SaveCandidateCodeInfo(infos []*types.CandidateCodeInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (code, expiration, node_type, is_test)
			VALUES (:code, :expiration, :node_type, :is_test)`, candidateCodeTable)

	_, err := n.db.NamedExec(query, infos)
	return err
}

// GetCandidateCodeInfos retrieves all candidate code information from the database.
func (n *SQLDB) GetCandidateCodeInfos() ([]*types.CandidateCodeInfo, error) {
	var infos []*types.CandidateCodeInfo
	query := fmt.Sprintf("SELECT * FROM %s ", candidateCodeTable)

	err := n.db.Select(&infos, query)
	if err != nil {
		return nil, err
	}

	return infos, nil
}

// GetCandidateCodeInfoForNodeID fetches candidate code information associated with a specific node ID.
func (n *SQLDB) GetCandidateCodeInfoForNodeID(nodeID string) (*types.CandidateCodeInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE node_id=?`, candidateCodeTable)

	var out types.CandidateCodeInfo
	err := n.db.Get(&out, query, nodeID)
	if err != nil {
		return nil, err
	}

	return &out, nil
}

// GetCandidateCodeInfo retrieves candidate code information based on a specific code.
func (n *SQLDB) GetCandidateCodeInfo(code string) (*types.CandidateCodeInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE code=?`, candidateCodeTable)

	var out types.CandidateCodeInfo
	err := n.db.Get(&out, query, code)
	if err != nil {
		return nil, err
	}

	return &out, nil
}

// UpdateCandidateCodeInfo associates a candidate code with a node if not already associated.
func (n *SQLDB) UpdateCandidateCodeInfo(code, nodeID string) error {
	query := fmt.Sprintf(`UPDATE %s SET node_id=? WHERE code=? AND node_id=''`, candidateCodeTable)
	result, err := n.db.Exec(query, nodeID, code)
	if err != nil {
		return err
	}

	r, err := result.RowsAffected()
	if r < 1 {
		return xerrors.New("nothing to update")
	}

	return err
}

// ResetCandidateCodeInfo resets the node ID associated with a candidate code.
func (n *SQLDB) ResetCandidateCodeInfo(code, nodeID string) error {
	query := fmt.Sprintf(`UPDATE %s SET node_id=? WHERE code=? `, candidateCodeTable)
	result, err := n.db.Exec(query, nodeID, code)
	if err != nil {
		return err
	}

	r, err := result.RowsAffected()
	if r < 1 {
		return xerrors.New("nothing to update")
	}

	return err
}

// DeleteCandidateCodeInfo removes candidate code information from the database.
func (n *SQLDB) DeleteCandidateCodeInfo(code string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE code=?`, candidateCodeTable)
	_, err := n.db.Exec(query, code)
	return err
}

// // UpdateServerOnlineCount updates the online count for nodes on a specific date, incrementing counts where applicable.
// func (n *SQLDB) UpdateServerOnlineCount(serverID string, count int, date time.Time) error {
// 	stmt, err := n.db.Preparex(`INSERT INTO ` + onlineCountTable + ` (node_id, created_time, online_count)
//         VALUES (?, ?, ?)
//         ON DUPLICATE KEY UPDATE online_count=online_count+?`)
// 	if err != nil {
// 		return err
// 	}
// 	defer stmt.Close()

// 	if _, err := stmt.Exec(serverID, date, count, count); err != nil {
// 		log.Errorf("UpdateServerOnlineCount %s err:%s", serverID, err.Error())
// 	}

// 	return nil
// }

// // UpdateOnlineCount updates the online count for nodes on a specific date, incrementing counts where applicable.
// func (n *SQLDB) UpdateOnlineCount(nodes []string, countIncr int, date time.Time) error {
// 	stmt, err := n.db.Preparex(`INSERT INTO ` + onlineCountTable + ` (node_id, created_time, online_count)
//         VALUES (?, ?, ?)
//         ON DUPLICATE KEY UPDATE online_count=online_count+?`)
// 	if err != nil {
// 		return err
// 	}
// 	defer stmt.Close()

// 	for _, nodeID := range nodes {
// 		if _, err := stmt.Exec(nodeID, date, countIncr, countIncr); err != nil {
// 			log.Errorf("UpdateOnlineCount %s err:%s", nodeID, err.Error())
// 		}
// 	}

// 	return nil
// }

// GetOnlineCount retrieves the online count for a specific node on a given date.
func (n *SQLDB) GetOnlineCount(node string, date time.Time) (int, error) {
	count := 0
	query := fmt.Sprintf("SELECT online_count FROM %s WHERE node_id=? AND created_time=? ", onlineCountTable)

	err := n.db.Get(&count, query, node, date)
	if err != nil && err != sql.ErrNoRows {
		return count, err
	}

	return count, nil
}

// UpdateNodePenalty updates the penalty duration and last seen time for nodes based on their online status.
func (n *SQLDB) UpdateNodePenalty(nodePns map[string]float64) error {
	stmt, err := n.db.Preparex(`UPDATE ` + nodeInfoTable + ` SET offline_duration=offline_duration+1,last_seen=NOW() WHERE node_id=?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for nodeID := range nodePns {
		if _, err := stmt.Exec(nodeID); err != nil {
			log.Errorf("UpdateNodePenalty %s, err:%s", nodeID, err.Error())
		}
	}

	return nil
}

// MigrateNodeDetails migrates comprehensive node-related details into the database, including registration, node info, profit details, and online counts within a transaction.
func (n *SQLDB) MigrateNodeDetails(info *types.NodeMigrateInfo) error {
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

	nodeID := info.NodeInfo.NodeID

	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, created_time, node_type, activation_key, ip, public_key)
				VALUES (:node_id, :created_time, :node_type, :activation_key, :ip, :public_key)`, nodeRegisterTable)
	_, err = tx.NamedExec(query, info.ActivationInfo)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(
		`INSERT INTO %s (node_id, online_duration, offline_duration, disk_usage, last_seen, profit, available_disk_space, titan_disk_usage, penalty_profit, port_mapping,
			    download_traffic, upload_traffic, bandwidth_up, bandwidth_down, netflow_up, netflow_down, scheduler_sid,first_login_time) 
				VALUES (:node_id, :online_duration, :offline_duration, :disk_usage, :last_seen, :profit, :available_disk_space, :titan_disk_usage, :penalty_profit, :port_mapping,
				:download_traffic, :upload_traffic, :bandwidth_up, :bandwidth_down, :netflow_up, :netflow_down, :scheduler_sid, :first_login_time)`, nodeInfoTable)
	_, err = tx.NamedExec(query, info.NodeInfo)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(
		`INSERT INTO %s (node_id, retrieve_count, retrieve_succeeded_count, retrieve_failed_count, asset_count, asset_succeeded_count, asset_failed_count) VALUES (?, ?, ?, ?, ?, ?, ?) `, nodeStatisticsTable)
	_, err = tx.Exec(query, nodeID, info.NodeInfo.RetrieveCount, info.NodeInfo.RetrieveSucceededCount, info.NodeInfo.RetrieveFailedCount, info.NodeInfo.AssetCount, info.NodeInfo.AssetSucceededCount, info.NodeInfo.AssetFailedCount)
	if err != nil {
		return err
	}

	if info.CodeInfo != nil {
		query = fmt.Sprintf(
			`INSERT INTO %s (code, expiration, node_type, is_test, node_id)
			VALUES (:code, :expiration, :node_type, :is_test, :node_id)`, candidateCodeTable)
		_, err = tx.NamedExec(query, info.CodeInfo)
		if err != nil {
			return err
		}
	}

	for t, i := range info.OnlineCounts {
		query := fmt.Sprintf(
			`INSERT INTO %s (node_id, created_time, online_count)
			    VALUES (?, ?, ?)
				ON DUPLICATE KEY UPDATE online_count=online_count+?`, onlineCountTable)

		_, err = tx.Exec(query, nodeID, t, i, i)
		if err != nil {
			return err
		}
	}

	query = fmt.Sprintf(`INSERT INTO %s (node_id, profit, profit_type, size, note, cid, rate) VALUES (:node_id, :profit, :profit_type, :size, :note, :cid, :rate)`, profitDetailsTable)
	tx.NamedExec(query, info.ProfitList)

	// Commit
	return tx.Commit()
}

// CleanNodeInfo removes all data related to a specific node across multiple tables in a single transaction to ensure data consistency.
func (n *SQLDB) CleanNodeInfo(nodeID string) error {
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

	query := fmt.Sprintf(`DELETE FROM %s WHERE node_id=?`, nodeRegisterTable)
	_, err = n.db.Exec(query, nodeID)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE node_id=?`, nodeInfoTable)
	_, err = tx.Exec(query, nodeID)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE node_id=?`, nodeStatisticsTable)
	_, err = tx.Exec(query, nodeID)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE node_id=?`, candidateCodeTable)
	_, err = tx.Exec(query, nodeID)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE node_id=?`, onlineCountTable)
	_, err = tx.Exec(query, nodeID)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE node_id=?`, profitDetailsTable)
	_, err = tx.Exec(query, nodeID)
	if err != nil {
		return err
	}

	// Commit
	return tx.Commit()
}
