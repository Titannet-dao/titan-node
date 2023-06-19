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
	query := fmt.Sprintf(`INSERT INTO %s (round_id, node_id, validator_id, status, cid, start_time, end_time, calculated_profit, file_saved) 
	                        VALUES (:round_id, :node_id, :validator_id, :status, :cid, :start_time, :end_time, :calculated_profit, :file_saved)`, validationResultTable)
	_, err := n.db.NamedExec(query, infos)

	return err
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
	if info.Status == types.ValidationStatusSuccess {
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

		query := fmt.Sprintf(`UPDATE %s SET block_number=:block_number,status=:status, duration=:duration, bandwidth=:bandwidth, end_time=NOW(), profit=:profit, token_id=:token_id WHERE round_id=:round_id AND node_id=:node_id`, validationResultTable)
		_, err = tx.NamedExec(query, info)
		if err != nil {
			return err
		}

		bandwidth := int64(info.Bandwidth) * info.Duration
		// update node bandwidth traffic info
		iQuery := fmt.Sprintf(`UPDATE %s SET download_traffic=download_traffic+? WHERE node_id=?`, nodeInfoTable)
		_, err = tx.Exec(iQuery, bandwidth, info.ValidatorID)
		if err != nil {
			return err
		}

		iQuery = fmt.Sprintf(`UPDATE %s SET upload_traffic=upload_traffic+? WHERE node_id=?`, nodeInfoTable)
		_, err = tx.Exec(iQuery, bandwidth, info.NodeID)
		if err != nil {
			return err
		}

		return tx.Commit()
	}

	query := fmt.Sprintf(`UPDATE %s SET status=:status, end_time=NOW(), profit=:profit, token_id=:token_id WHERE round_id=:round_id AND node_id=:node_id`, validationResultTable)
	_, err := n.db.NamedExec(query, info)

	return err
}

// UpdateValidationResultsTimeout sets the validation results' status as timeout.
func (n *SQLDB) UpdateValidationResultsTimeout(roundID string) error {
	query := fmt.Sprintf(`UPDATE %s SET status=?, end_time=NOW() WHERE round_id=? AND status=?`, validationResultTable)
	_, err := n.db.Exec(query, types.ValidationStatusValidatorTimeOut, roundID, types.ValidationStatusCreate)
	return err
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
func (n *SQLDB) UpdateValidators(nodeIDs []string, serverID dtypes.ServerID) error {
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

	// clean old validators
	dQuery := fmt.Sprintf(`DELETE FROM %s WHERE scheduler_sid=? `, validatorsTable)
	_, err = tx.Exec(dQuery, serverID)
	if err != nil {
		return err
	}

	for _, nodeID := range nodeIDs {
		iQuery := fmt.Sprintf(`INSERT INTO %s (node_id, scheduler_sid) VALUES (?, ?)`, validatorsTable)
		_, err = tx.Exec(iQuery, nodeID, serverID)
		if err != nil {
			return err
		}
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
		`INSERT INTO %s (node_id, mac_location, cpu_cores, memory, node_name,
			    disk_type, io_system, system_version, nat_type, disk_space, bandwidth_up, bandwidth_down, scheduler_sid) 
				VALUES (:node_id, :mac_location, :cpu_cores, :memory, :node_name,
				:disk_type, :io_system, :system_version, :nat_type, :disk_space, :bandwidth_up, :bandwidth_down, :scheduler_sid) 
				ON DUPLICATE KEY UPDATE node_id=:node_id, scheduler_sid=:scheduler_sid`, nodeInfoTable)

	_, err := n.db.NamedExec(query, info)
	return err
}

// UpdateOnlineDuration update node online time , last time , disk usage
func (n *SQLDB) UpdateOnlineDuration(infos []*types.NodeDynamicInfo) error {
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
		query := fmt.Sprintf(`UPDATE %s SET last_seen=?,online_duration=?,disk_usage=? WHERE node_id=?`, nodeInfoTable)
		tx.Exec(query, info.LastSeen, info.OnlineDuration, info.DiskUsage, info.NodeID)
	}

	// Commit
	return tx.Commit()
}

// SaveNodeRegisterInfos Insert Node register info
func (n *SQLDB) SaveNodeRegisterInfos(details []*types.ActivationDetail) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, created_time, node_type, activation_key)
				VALUES (:node_id, NOW(), :node_type, :activation_key)`, nodeRegisterTable)

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
	err := n.db.Get(&count, cQuery, count, nodeType)
	if err != nil {
		return err
	}

	if count < 1 {
		return xerrors.New("node not exists")
	}

	return nil
}

// LoadNodeInfos load nodes information.
func (n *SQLDB) LoadNodeInfos(limit, offset int) (*sqlx.Rows, int64, error) {
	var total int64
	cQuery := fmt.Sprintf(`SELECT count(node_id) FROM %s`, nodeInfoTable)
	err := n.db.Get(&total, cQuery)
	if err != nil {
		return nil, 0, err
	}

	if limit > loadNodeInfosDefaultLimit || limit == 0 {
		limit = loadNodeInfosDefaultLimit
	}

	sQuery := fmt.Sprintf(`SELECT a.*,b.node_type as type FROM %s a LEFT JOIN %s b ON a.node_id = b.node_id order by node_id asc LIMIT ? OFFSET ?`, nodeInfoTable, nodeRegisterTable)
	rows, err := n.db.QueryxContext(context.Background(), sQuery, limit, offset)
	return rows, total, err
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
	query := fmt.Sprintf(
		`INSERT INTO %s (token_id, node_id, client_id, asset_id, limit_rate, created_time, expiration, client_workload, node_workload, status) 
				VALUES (:token_id, :node_id, :client_id, :asset_id, :limit_rate, :created_time, :expiration, :client_workload, :node_workload, :status)`, workloadRecordTable)

	_, err := n.db.NamedExec(query, records)
	return err
}

// UpdateWorkloadRecord update workload record
func (n *SQLDB) UpdateWorkloadRecord(record *types.WorkloadRecord) error {
	query := fmt.Sprintf(`UPDATE %s SET client_workload=:client_workload, node_workload=:node_workload WHERE token_id=:token_id`, workloadRecordTable)
	_, err := n.db.NamedExec(query, record)
	return err
}

// LoadWorkloadRecord load workload record
func (n *SQLDB) LoadWorkloadRecord(tokenID string) (*types.WorkloadRecord, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE token_id=?`, workloadRecordTable)
	var record types.WorkloadRecord
	err := n.db.Get(&record, query, tokenID)
	if err != nil {
		return nil, err
	}

	return &record, nil
}

// LoadUnprocessedWorkloadResults Load unprocessed workload results
func (n *SQLDB) LoadUnprocessedWorkloadResults(limit int) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE status=? AND expiration<? order by created_time asc LIMIT ?`, workloadRecordTable)
	return n.db.QueryxContext(context.Background(), sQuery, types.WorkloadStatusCreate, time.Now(), limit)
}

// LoadWorkloadRecords Load workload records
func (n *SQLDB) LoadWorkloadRecords(nodeID string, limit, offset int) (*types.ListWorkloadRecordRsp, error) {
	res := new(types.ListWorkloadRecordRsp)

	var infos []*types.WorkloadRecord
	query := fmt.Sprintf("SELECT * FROM %s WHERE node_id=? AND status=? order by created_time desc LIMIT ? OFFSET ? ", workloadRecordTable)

	if limit > loadWorkloadDefaultLimit {
		limit = loadWorkloadDefaultLimit
	}

	err := n.db.Select(&infos, query, nodeID, types.WorkloadStatusSucceeded, limit, offset)
	if err != nil {
		return nil, err
	}

	res.WorkloadRecordInfos = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE node_id=? AND status=?", workloadRecordTable)
	var count int
	err = n.db.Get(&count, countQuery, nodeID, types.WorkloadStatusSucceeded)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// RemoveInvalidWorkloadResult Remove invalid workload certificates
func (n *SQLDB) RemoveInvalidWorkloadResult(sIDs []string) error {
	rQuery := fmt.Sprintf(`DELETE FROM %s WHERE token_id in (?)`, workloadRecordTable)

	srQuery, args, err := sqlx.In(rQuery, sIDs)
	if err != nil {
		return err
	}

	srQuery = n.db.Rebind(srQuery)
	_, err = n.db.Exec(srQuery, args...)
	return err
}

// UpdateNodeInfosByWorkloadResult Update the info value of the node, and set the processed flag to the workload record
func (n *SQLDB) UpdateNodeInfosByWorkloadResult(sIDs map[string]types.WorkloadStatus, nodeInfos map[string]*types.NodeDynamicInfo) error {
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

	for tokenID, status := range sIDs {
		query := fmt.Sprintf(`UPDATE %s SET status=? WHERE token_id=?`, workloadRecordTable)
		_, err = tx.Exec(query, status, tokenID)
		if err != nil {
			return err
		}
	}

	for nodeID, info := range nodeInfos {
		// update node profit
		query := fmt.Sprintf(`UPDATE %s SET profit=profit+?,download_traffic=download_traffic+?,upload_traffic=upload_traffic+?,retrieve_count=retrieve_count+? WHERE node_id=?`, nodeInfoTable)
		_, err = tx.Exec(query, info.Profit, info.DownloadTraffic, info.UploadTraffic, info.RetrieveCount, nodeID)
		if err != nil {
			return err
		}
	}

	// Commit
	return tx.Commit()
}

// LoadUnCalculatedValidationResults Load not calculated profit validation results
func (n *SQLDB) LoadUnCalculatedValidationResults(maxTime time.Time, limit int) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE calculated_profit=? AND end_time<? order by end_time asc LIMIT ?`, validationResultTable)
	return n.db.QueryxContext(context.Background(), sQuery, false, maxTime, limit)
}

// LoadUnSavedValidationResults Load not save to file validation results
func (n *SQLDB) LoadUnSavedValidationResults(limit int) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE file_saved=? AND calculated_profit=? order by end_time asc LIMIT ?`, validationResultTable)
	return n.db.QueryxContext(context.Background(), sQuery, false, true, limit)
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

// UpdateNodeInfosByValidationResult Update the info value of the node, and set the calculated flag to the validation record
func (n *SQLDB) UpdateNodeInfosByValidationResult(infos []*types.ValidationResultInfo, nodeProfits map[string]float64) error {
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

	// update validation result info
	for _, info := range infos {
		// update node profit
		uQuery := fmt.Sprintf(`UPDATE %s SET calculated_profit=?,profit=? WHERE id=?`, validationResultTable)
		_, err = tx.Exec(uQuery, true, info.Profit, info.ID)
		if err != nil {
			return err
		}
	}

	for nodeID, profit := range nodeProfits {
		// update node profit
		uQuery := fmt.Sprintf(`UPDATE %s SET profit=profit+? WHERE node_id=?`, nodeInfoTable)
		_, err = tx.Exec(uQuery, profit, nodeID)
		if err != nil {
			return err
		}
	}

	// Commit
	return tx.Commit()
}
