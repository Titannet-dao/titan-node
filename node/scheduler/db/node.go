package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/jmoiron/sqlx"
	"golang.org/x/xerrors"
)

// UpdatePortMapping sets the node's mapping port.
func (n *SQLDB) UpdatePortMapping(nodeID, port string) error {
	// update
	query := fmt.Sprintf(`UPDATE %s SET port_mapping=? WHERE node_id=?`, nodeInfoTable)
	_, err := n.db.Exec(query, port, nodeID)
	return err
}

// SaveValidationResultInfos inserts validation result information.
func (n *SQLDB) SaveValidationResultInfos(infos []*types.ValidationResultInfo) error {
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

	// query := fmt.Sprintf(`INSERT INTO %s (round_id, node_id, validator_id, status, cid, start_time, end_time, calculated_profit, file_saved)
	//                         VALUES (:round_id, :node_id, :validator_id, :status, :cid, :start_time, :end_time, :calculated_profit, :file_saved)`, validationResultTable)
	// _, err := n.db.NamedExec(query, infos)

	for _, info := range infos {
		query := fmt.Sprintf(`INSERT INTO %s (round_id, node_id, validator_id, status, cid, start_time, end_time, calculated_profit, file_saved, node_count) 
								VALUES (:round_id, :node_id, :validator_id, :status, :cid, :start_time, :end_time, :calculated_profit, :file_saved, :node_count)`, validationResultTable)
		tx.NamedExec(query, info)
	}

	// Commit
	return tx.Commit()
}

// LoadNodeValidationInfo load the cid of a validation result.
func (n *SQLDB) LoadNodeValidationInfo(roundID, nodeID string) (*types.ValidationResultInfo, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE round_id=? AND node_id=?", validationResultTable)
	var info types.ValidationResultInfo
	err := n.db.Get(&info, query, roundID, nodeID)
	return &info, err
}

// UpdateValidationResultInfo updates the validation result information.
func (n *SQLDB) UpdateValidationResultInfo(info *types.ValidationResultInfo) error {
	query := fmt.Sprintf(`UPDATE %s SET block_number=:block_number,status=:status, duration=:duration, bandwidth=:bandwidth, end_time=NOW(), profit=:profit, token_id=:token_id WHERE round_id=:round_id AND node_id=:node_id`, validationResultTable)
	_, err := n.db.NamedExec(query, info)
	if err != nil {
		return err
	}

	return nil
}

// UpdateValidationResultsTimeout sets the validation results' status as timeout.
func (n *SQLDB) UpdateValidationResultStatus(roundID, nodeID string, status types.ValidationStatus) error {
	query := fmt.Sprintf(`UPDATE %s SET status=?, end_time=NOW() WHERE round_id=? AND node_id=?`, validationResultTable)
	_, err := n.db.Exec(query, status, roundID, nodeID)
	return err
}

// LoadCreateValidationResultInfos load validation results.
func (n *SQLDB) LoadCreateValidationResultInfos() ([]*types.ValidationResultInfo, error) {
	var infos []*types.ValidationResultInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE status=?", validationResultTable)

	err := n.db.Select(&infos, query, types.ValidationStatusCreate)
	if err != nil {
		return nil, err
	}

	return infos, nil
}

// LoadValidationResultInfos load validation results.
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

// SaveEdgeUpdateConfig inserts edge update information.
func (n *SQLDB) SaveEdgeUpdateConfig(info *api.EdgeUpdateConfig) error {
	sqlString := fmt.Sprintf(`INSERT INTO %s (node_type, app_name, version, hash, download_url) VALUES (:node_type, :app_name, :version, :hash, :download_url) ON DUPLICATE KEY UPDATE app_name=:app_name, version=:version, hash=:hash, download_url=:download_url`, edgeUpdateTable)
	_, err := n.db.NamedExec(sqlString, info)
	return err
}

// LoadEdgeUpdateConfigs load edge update information.
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

// DeleteEdgeUpdateConfig delete edge update info
func (n *SQLDB) DeleteEdgeUpdateConfig(nodeType int) error {
	deleteString := fmt.Sprintf(`DELETE FROM %s WHERE node_type=?`, edgeUpdateTable)
	_, err := n.db.Exec(deleteString, nodeType)
	return err
}

// UpdateValidators update validators
func (n *SQLDB) UpdateValidators(nodeIDs []string, serverID dtypes.ServerID, cleanOld bool) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("UpdateValidators Rollback err:%s", err.Error())
		}
	}()

	if cleanOld {
		// clean old validators
		dQuery := fmt.Sprintf(`DELETE FROM %s WHERE scheduler_sid=? `, validatorsTable)
		_, err = tx.Exec(dQuery, serverID)
		if err != nil {
			return err
		}
	}

	for _, nodeID := range nodeIDs {
		iQuery := fmt.Sprintf(`INSERT INTO %s (node_id, scheduler_sid) VALUES (?, ?)`, validatorsTable)
		tx.Exec(iQuery, nodeID, serverID)
	}

	return tx.Commit()
}

// LoadValidators load validators information.
func (n *SQLDB) LoadValidators(serverID dtypes.ServerID) ([]string, error) {
	sQuery := fmt.Sprintf(`SELECT node_id FROM %s WHERE scheduler_sid=?`, validatorsTable)

	var out []string
	err := n.db.Select(&out, sQuery, serverID)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// IsValidator Determine whether the node is a validator
func (n *SQLDB) IsValidator(nodeID string) (bool, error) {
	var count int64
	sQuery := fmt.Sprintf("SELECT count(node_id) FROM %s WHERE node_id=?", validatorsTable)
	err := n.db.Get(&count, sQuery, nodeID)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// UpdateValidatorInfo reset scheduler server id for validator
func (n *SQLDB) UpdateValidatorInfo(serverID dtypes.ServerID, nodeID string) error {
	uQuery := fmt.Sprintf(`UPDATE %s SET scheduler_sid=? WHERE node_id=?`, validatorsTable)
	_, err := n.db.Exec(uQuery, serverID, nodeID)

	return err
}

// SaveNodeInfo Insert or update node info
func (n *SQLDB) SaveNodeInfo(info *types.NodeInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, mac_location, cpu_cores, memory, node_name, cpu_info, available_disk_space, titan_disk_usage, gpu_info,
			    disk_type, io_system, system_version, disk_space, bandwidth_up, bandwidth_down, netflow_up, netflow_down, scheduler_sid) 
				VALUES (:node_id, :mac_location, :cpu_cores, :memory, :node_name, :cpu_info, :available_disk_space, :titan_disk_usage, gpu_info,
				:disk_type, :io_system, :system_version, :disk_space, :bandwidth_up, :bandwidth_down, :netflow_up, :netflow_down, :scheduler_sid) 
				ON DUPLICATE KEY UPDATE node_id=:node_id, scheduler_sid=:scheduler_sid, system_version=:system_version, cpu_cores=:cpu_cores, titan_disk_usage=:titan_disk_usage, gpu_info=:gpu_info,
				memory=:memory, node_name=:node_name, disk_space=:disk_space, cpu_info=:cpu_info, available_disk_space=:available_disk_space, available_disk_space=:available_disk_space,
				netflow_up=:netflow_up, netflow_down=:netflow_down `, nodeInfoTable)

	_, err := n.db.NamedExec(query, info)
	return err
}

// UpdateNodeDynamicInfo update node online time , last time , disk usage ...
func (n *SQLDB) UpdateNodeDynamicInfo(infos []*types.NodeDynamicInfo) ([]string, error) {
	errorList := make([]string, 0)

	tx, err := n.db.Beginx()
	if err != nil {
		return errorList, err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("Rollback err:%s", err.Error())
		}
	}()

	for _, info := range infos {
		query := fmt.Sprintf(`UPDATE %s SET last_seen=?,online_duration=?,disk_usage=?,bandwidth_up=?,bandwidth_down=?,
		    titan_disk_usage=?,available_disk_space=?,download_traffic=?,upload_traffic=? WHERE node_id=?`, nodeInfoTable)
		_, err = tx.Exec(query, info.LastSeen, info.OnlineDuration, info.DiskUsage, info.BandwidthUp, info.BandwidthDown, info.TitanDiskUsage,
			info.AvailableDiskSpace, info.DownloadTraffic, info.UploadTraffic, info.NodeID)
		if err != nil {
			errorList = append(errorList, fmt.Sprintf("UpdateOnlineDuration %s, %.4f,%d,%d,%.4f,%.4f err:%s", info.NodeID, info.DiskUsage, info.BandwidthUp, info.BandwidthDown, info.TitanDiskUsage, info.AvailableDiskSpace, err.Error()))
		}
	}

	// Commit
	return errorList, tx.Commit()
}

// SaveNodeRegisterInfos Insert Node register info
func (n *SQLDB) SaveNodeRegisterInfos(details []*types.ActivationDetail) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, created_time, node_type, activation_key, ip)
				VALUES (:node_id, NOW(), :node_type, :activation_key, :ip)`, nodeRegisterTable)

	_, err := n.db.NamedExec(query, details)

	return err
}

// SaveNodePublicKey update node public key
func (n *SQLDB) SaveNodePublicKey(pKey, nodeID string) error {
	query := fmt.Sprintf(`UPDATE %s SET public_key=? WHERE node_id=? `, nodeRegisterTable)
	_, err := n.db.Exec(query, pKey, nodeID)

	return err
}

// DeleteNodeInfo delete Node info
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

// LoadNodeActivationKey load activation key of node.
func (n *SQLDB) LoadNodeActivationKey(nodeID string) (string, error) {
	var pKey string

	query := fmt.Sprintf(`SELECT activation_key FROM %s WHERE node_id=?`, nodeRegisterTable)
	if err := n.db.Get(&pKey, query, nodeID); err != nil {
		return pKey, err
	}

	return pKey, nil
}

// LoadNodePublicKey load public key of node.
func (n *SQLDB) LoadNodePublicKey(nodeID string) (string, error) {
	var pKey string

	query := fmt.Sprintf(`SELECT public_key FROM %s WHERE node_id=?`, nodeRegisterTable)
	if err := n.db.Get(&pKey, query, nodeID); err != nil {
		return pKey, err
	}

	return pKey, nil
}

// LoadNodeType load type of node.
func (n *SQLDB) LoadNodeType(nodeID string) (types.NodeType, error) {
	var nodeType types.NodeType

	query := fmt.Sprintf(`SELECT node_type FROM %s WHERE node_id=?`, nodeRegisterTable)
	if err := n.db.Get(&nodeType, query, nodeID); err != nil {
		return nodeType, err
	}

	return nodeType, nil
}

// NodeExists is node exists
func (n *SQLDB) NodeExists(nodeID string, nodeType types.NodeType) error {
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

// TodayRegisterCount get the number of registrations for this ip today
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

// LoadNodeInfos load nodes information.
func (n *SQLDB) LoadNodeInfos(limit, offset int) (*sqlx.Rows, int64, error) {
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

// LoadNodeInfosOfType load node infos
func (n *SQLDB) LoadNodeInfosOfType(nodeType int) ([]*types.NodeInfo, error) {
	query := fmt.Sprintf(`SELECT a.*,b.node_type as type FROM %s a LEFT JOIN %s b ON a.node_id = b.node_id where b.node_type=?;`, nodeInfoTable, nodeRegisterTable)

	var out []*types.NodeInfo
	if err := n.db.Select(&out, query, nodeType); err != nil {
		return nil, err
	}

	return out, nil
}

// LoadNodeInfo load node information.
func (n *SQLDB) LoadNodeInfo(nodeID string) (*types.NodeInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE node_id=?`, nodeInfoTable)

	var out types.NodeInfo
	err := n.db.Get(&out, query, nodeID)
	if err != nil {
		return nil, err
	}

	return &out, nil
}

// LoadNodeLastSeenTime loads the last seen time of a node
func (n *SQLDB) LoadNodeLastSeenTime(nodeID string) (time.Time, error) {
	var t time.Time
	query := fmt.Sprintf(`SELECT last_seen FROM %s WHERE node_id=?`, nodeInfoTable)
	err := n.db.Get(&t, query, nodeID)
	return t, err
}

// LoadTopHash load assets view top hash
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

// LoadSyncTime load assets view sync time
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

// UpdateSyncTime update assets view sync time
func (n *SQLDB) UpdateSyncTime(nodeID string) error {
	query := fmt.Sprintf(`UPDATE %s SET sync_time=? WHERE node_id=?`, assetsViewTable)
	_, err := n.db.Exec(query, time.Now(), nodeID)
	return err
}

// LoadBucketHashes load assets view buckets hashes
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

// SaveAssetsView update or insert top hash and buckets hashes to assets view
// bucketHashes key is number of bucket, value is bucket hash
// TODO save bucketHashes as array
func (n *SQLDB) SaveAssetsView(nodeID string, topHash string, bucketHashes []byte) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, top_hash, bucket_hashes) VALUES (?, ?, ?) 
				ON DUPLICATE KEY UPDATE top_hash=?, bucket_hashes=?`, assetsViewTable)

	_, err := n.db.Exec(query, nodeID, topHash, bucketHashes, topHash, bucketHashes)
	return err
}

// DeleteAssetsView delete the asset view for node
func (n *SQLDB) DeleteAssetsView(nodeID string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE node_id=?`, assetsViewTable)
	_, err := n.db.Exec(query, nodeID)
	return err
}

// LoadBucket load assets ids from bucket
// return hashes of asset
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

// SaveBucket update or insert assets ids to bucket
func (n *SQLDB) SaveBucket(bucketID string, assetHashes []byte) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (bucket_id, asset_hashes) VALUES (?, ?) 
				ON DUPLICATE KEY UPDATE asset_hashes=?`, bucketTable)

	_, err := n.db.Exec(query, bucketID, assetHashes, assetHashes)
	return err
}

// DeleteBucket delete the bucket
func (n *SQLDB) DeleteBucket(bucketID string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE bucket_id=?`, bucketTable)
	_, err := n.db.Exec(query, bucketID)
	return err
}

// SaveWorkloadRecord save workload record
func (n *SQLDB) SaveWorkloadRecord(records []*types.WorkloadRecord) error {
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

	for _, info := range records {
		if info == nil {
			continue
		}

		query := fmt.Sprintf(
			`INSERT INTO %s (workload_id, asset_cid, client_id, asset_size, workloads, status, event) 
					VALUES (:workload_id, :asset_cid, :client_id, :asset_size, :workloads, :status, :event)`, workloadRecordTable)

		tx.NamedExec(query, info)
	}

	// Commit
	return tx.Commit()
}

// UpdateWorkloadRecord update workload record
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

// LoadWorkloadRecord load workload record
func (n *SQLDB) LoadWorkloadRecord(workload *types.WorkloadRecord) ([]*types.WorkloadRecord, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE asset_cid=? AND client_id=? AND status=?`, workloadRecordTable)
	var records []*types.WorkloadRecord
	err := n.db.Select(&records, query, workload.AssetCID, workload.ClientID, workload.Status)
	if err != nil {
		return nil, err
	}

	return records, nil
}

// LoadWorkloadRecordOfID load workload record
func (n *SQLDB) LoadWorkloadRecordOfID(workloadID string) (*types.WorkloadRecord, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE workload_id=? `, workloadRecordTable)
	var record types.WorkloadRecord
	err := n.db.Get(&record, query, workloadID)
	if err != nil {
		return nil, err
	}

	return &record, nil
}

// LoadWorkloadRecords Load workload records
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

// LoadUnCalculatedValidationResults Load not calculated profit validation results
func (n *SQLDB) LoadUnCalculatedValidationResults(maxTime time.Time, limit int) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE calculated_profit=? AND start_time<? order by start_time asc LIMIT ?`, validationResultTable)
	return n.db.QueryxContext(context.Background(), sQuery, false, maxTime, limit)
}

// LoadUnSavedValidationResults Load not save to file validation results
func (n *SQLDB) LoadUnSavedValidationResults(limit int) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE file_saved=? AND calculated_profit=? order by start_time asc LIMIT ?`, validationResultTable)
	return n.db.QueryxContext(context.Background(), sQuery, false, true, limit)
}

// RemoveInvalidValidationResult Remove invalid validation results
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

// UpdateFileSavedStatus update the file saved in validation result
func (n *SQLDB) UpdateFileSavedStatus(ids []int) error {
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

	for _, id := range ids {
		uQuery := fmt.Sprintf(`UPDATE %s SET file_saved=? WHERE id=?`, validationResultTable)
		_, err = tx.Exec(uQuery, true, id)
		if err != nil {
			return err
		}
	}

	// Commit
	return tx.Commit()
}

// SaveRetrieveEventInfo save retrieve event and update node info
func (n *SQLDB) SaveRetrieveEventInfo(cInfo *types.RetrieveEvent) error {
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

	query := fmt.Sprintf(
		`INSERT INTO %s (token_id, node_id, client_id, cid, size, created_time, end_time, profit ) 
				VALUES (:token_id, :node_id, :client_id, :cid, :size, :created_time, :end_time, :profit )`, retrieveEventTable)
	_, err = tx.NamedExec(query, cInfo)
	if err != nil {
		return err
	}

	// update node info
	query = fmt.Sprintf(`UPDATE %s SET retrieve_count=retrieve_count+? WHERE node_id=?`, nodeInfoTable)
	_, err = tx.Exec(query, 1, cInfo.NodeID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// LoadRetrieveEvent load retrieve event
func (n *SQLDB) LoadRetrieveEvent(tokenID string) (*types.RetrieveEvent, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE token_id=?`, retrieveEventTable)
	var record types.RetrieveEvent
	err := n.db.Get(&record, query, tokenID)
	if err != nil {
		return nil, err
	}

	return &record, nil
}

// LoadRetrieveEventRecords Load retrieve events
func (n *SQLDB) LoadRetrieveEventRecords(nodeID string, limit, offset int) (*types.ListRetrieveEventRsp, error) {
	res := new(types.ListRetrieveEventRsp)

	var infos []*types.RetrieveEvent
	query := fmt.Sprintf("SELECT * FROM %s WHERE node_id=? order by created_time desc LIMIT ? OFFSET ? ", retrieveEventTable)

	if limit > loadRetrieveDefaultLimit {
		limit = loadRetrieveDefaultLimit
	}

	err := n.db.Select(&infos, query, nodeID, limit, offset)
	if err != nil {
		return nil, err
	}

	res.RetrieveEventInfos = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE node_id=? ", retrieveEventTable)
	var count int
	err = n.db.Get(&count, countQuery, nodeID)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

func (n *SQLDB) AddNodeProfit(profitInfo *types.ProfitDetails) error {
	if profitInfo == nil {
		return nil
	}

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

	// add profit details
	sqlString := fmt.Sprintf(`INSERT INTO %s (node_id, profit, profit_type, size, note, cid) VALUES (:node_id, :profit, :profit_type, :size, :note, :cid)`, profitDetailsTable)
	_, err = tx.NamedExec(sqlString, profitInfo)
	if err != nil {
		return err
	}

	iQuery := fmt.Sprintf(`UPDATE %s SET profit=profit+? WHERE node_id=?`, nodeInfoTable)
	_, err = tx.Exec(iQuery, profitInfo.Profit, profitInfo.NodeID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (n *SQLDB) LoadTodayProfitsForNode(nodeID string, startTime, endTime time.Time) (float64, error) {
	size := 0.0
	query := fmt.Sprintf("SELECT COALESCE(SUM(profit), 0) FROM %s WHERE node_id=? AND created_time BETWEEN ? AND ? ", profitDetailsTable)
	err := n.db.Get(&size, query, nodeID, startTime, endTime)
	if err != nil {
		return 0, err
	}

	return size, nil
}

// LoadNodeProfits load profit info.
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

// SaveDeactivateNode save deactivate node time
func (n *SQLDB) SaveDeactivateNode(nodeID string, time int64, penaltyPoint float64) error {
	query := fmt.Sprintf(`UPDATE %s SET deactivate_time=?, point=point-? WHERE node_id=?`, nodeInfoTable)
	_, err := n.db.Exec(query, time, penaltyPoint, nodeID)
	return err
}

// SaveWSServerID save id
func (n *SQLDB) SaveWSServerID(nodeID, wID string) error {
	query := fmt.Sprintf(`UPDATE %s SET ws_server_id=? WHERE node_id=?`, nodeInfoTable)
	_, err := n.db.Exec(query, wID, nodeID)
	return err
}

// LoadWSServerID
func (n *SQLDB) LoadWSServerID(nodeID string) (string, error) {
	var wID string
	query := fmt.Sprintf(`SELECT ws_server_id FROM %s WHERE node_id=?`, nodeInfoTable)
	err := n.db.Get(&wID, query, nodeID)
	return wID, err
}

// LoadDeactivateNodeTime Get node deactivate time
func (n *SQLDB) LoadDeactivateNodeTime(nodeID string) (int64, error) {
	query := fmt.Sprintf(`SELECT deactivate_time FROM %s WHERE node_id=?`, nodeInfoTable)

	var time int64
	err := n.db.Get(&time, query, nodeID)
	if err != nil {
		return 0, err
	}

	return time, nil
}

// SaveFreeUpDiskTime save free up disk time
func (n *SQLDB) SaveFreeUpDiskTime(nodeID string, time time.Time) error {
	query := fmt.Sprintf(`UPDATE %s SET free_up_disk_time=? WHERE node_id=?`, nodeInfoTable)
	_, err := n.db.Exec(query, time, nodeID)
	return err
}

// LoadFreeUpDiskTime Get free up disk time
func (n *SQLDB) LoadFreeUpDiskTime(nodeID string) (time.Time, error) {
	query := fmt.Sprintf(`SELECT free_up_disk_time FROM %s WHERE node_id=?`, nodeInfoTable)

	var time time.Time
	err := n.db.Get(&time, query, nodeID)
	if err != nil {
		return time, err
	}

	return time, nil
}

// LoadDeactivateNodes load all deactivate node.
func (n *SQLDB) LoadDeactivateNodes(time int64) ([]string, error) {
	var out []string
	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE deactivate_time>0 AND deactivate_time<?`, nodeInfoTable)
	if err := n.db.Select(&out, query, time); err != nil {
		return nil, err
	}

	return out, nil
}

// func(n *SQLDB)

// CleanData delete events
func (n *SQLDB) CleanData() error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE end_time<DATE_SUB(NOW(), INTERVAL 30 DAY) `, replicaEventTable)
	_, err := n.db.Exec(query)
	if err != nil {
		return err
	}

	cleanTime := time.Now().Add(-30).Unix()
	query = fmt.Sprintf(`DELETE FROM %s WHERE end_time<? `, retrieveEventTable)
	_, err = n.db.Exec(query, cleanTime)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE client_end_time<DATE_SUB(NOW(), INTERVAL 10 DAY) `, workloadRecordTable)
	_, err = n.db.Exec(query)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE start_time<DATE_SUB(NOW(), INTERVAL 30 DAY) `, validationResultTable)
	_, err = n.db.Exec(query)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE created_time<DATE_SUB(NOW(), INTERVAL 30 DAY) `, profitDetailsTable)
	_, err = n.db.Exec(query)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE created_time<DATE_SUB(NOW(), INTERVAL 30 DAY) `, projectEventTable)
	_, err = n.db.Exec(query)
	return err
}

// SaveCandidateCodeInfo Insert Node code info
func (n *SQLDB) SaveCandidateCodeInfo(infos []*types.CandidateCodeInfo) error {
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

	for _, info := range infos {
		query := fmt.Sprintf(
			`INSERT INTO %s (code, expiration, node_type, is_test)
				VALUES (:code, :expiration, :node_type, :is_test)`, candidateCodeTable)

		tx.NamedExec(query, info)
	}

	return tx.Commit()
}

// GetCandidateCodeInfos code info
func (n *SQLDB) GetCandidateCodeInfos() ([]*types.CandidateCodeInfo, error) {
	var infos []*types.CandidateCodeInfo
	query := fmt.Sprintf("SELECT * FROM %s ", candidateCodeTable)

	err := n.db.Select(&infos, query)
	if err != nil {
		return nil, err
	}

	return infos, nil
}

// GetCandidateCodeInfoForNodeID code info
func (n *SQLDB) GetCandidateCodeInfoForNodeID(nodeID string) (*types.CandidateCodeInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE node_id=?`, candidateCodeTable)

	var out types.CandidateCodeInfo
	err := n.db.Get(&out, query, nodeID)
	if err != nil {
		return nil, err
	}

	return &out, nil
}

// GetCandidateCodeInfo code info
func (n *SQLDB) GetCandidateCodeInfo(code string) (*types.CandidateCodeInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE code=?`, candidateCodeTable)

	var out types.CandidateCodeInfo
	err := n.db.Get(&out, query, code)
	if err != nil {
		return nil, err
	}

	return &out, nil
}

// UpdateCandidateCodeInfo code info
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

// DeleteCandidateCodeInfo code info
func (n *SQLDB) DeleteCandidateCodeInfo(code string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE code=?`, candidateCodeTable)
	_, err := n.db.Exec(query, code)
	return err
}

func (n *SQLDB) UpdateOnlineCount(nodes []string, countIncr int, date time.Time) error {
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

	for _, nodeID := range nodes {
		query := fmt.Sprintf(
			`INSERT INTO %s (node_id, create_time, online_count)
			    VALUES (?, ?, ?)
				ON DUPLICATE KEY UPDATE online_count=online_count+?`, onlineCountTable)

		_, err := tx.Exec(query, nodeID, date, countIncr, countIncr)
		if err != nil {
			log.Errorf("UpdateOnlineCount %s err:%s", nodeID, err.Error())
		}
	}

	return tx.Commit()
}

// GetOnlineCount
func (n *SQLDB) GetOnlineCount(node string, date time.Time) (int, error) {
	count := 0
	query := fmt.Sprintf("SELECT online_count FROM %s WHERE node_id=? AND create_time=? ", onlineCountTable)

	err := n.db.Get(&count, query, node, date)
	if err != nil {
		return count, err
	}

	return count, nil
}

// UpdateNodePenalty
func (n *SQLDB) UpdateNodePenalty(nodePns map[string]float64) error {
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

	for nodeID, pn := range nodePns {
		uQuery := fmt.Sprintf(`UPDATE %s SET offline_duration=offline_duration+1,profit=profit-?,last_seen=NOW() WHERE node_id=?`, nodeInfoTable)
		_, err := tx.Exec(uQuery, pn, nodeID)
		if err != nil {
			log.Errorf("UpdateNodePenalty %s, %.4f err:%s", nodeID, pn, err.Error())
		}
	}

	// Commit
	return tx.Commit()
}
