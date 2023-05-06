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

// LoadPortMapping load the mapping port of a node.
func (n *SQLDB) LoadPortMapping(nodeID string) (string, error) {
	var port string
	query := fmt.Sprintf("SELECT port_mapping FROM %s WHERE node_id=?", nodeInfoTable)
	if err := n.db.Get(&port, query, nodeID); err != nil {
		return "", err
	}

	return port, nil
}

// UpdatePortMapping sets the node's mapping port.
func (n *SQLDB) UpdatePortMapping(nodeID, port string) error {
	info := types.NodeInfo{
		NodeID:      nodeID,
		PortMapping: port,
	}
	// update
	query := fmt.Sprintf(`UPDATE %s SET port_mapping=:port_mapping WHERE node_id=:node_id`, nodeInfoTable)
	_, err := n.db.NamedExec(query, info)
	return err
}

// SaveValidationResultInfos inserts validation result information.
func (n *SQLDB) SaveValidationResultInfos(infos []*types.ValidationResultInfo) error {
	query := fmt.Sprintf(`INSERT INTO %s (round_id, node_id, validator_id, status, cid, start_time, end_time, processed) VALUES (:round_id, :node_id, :validator_id, :status, :cid, :start_time, :end_time, :processed)`, validationResultTable)
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
		query := fmt.Sprintf(`UPDATE %s SET block_number=:block_number,status=:status, duration=:duration, bandwidth=:bandwidth, end_time=NOW(), profit=:profit WHERE round_id=:round_id AND node_id=:node_id`, validationResultTable)
		_, err := n.db.NamedExec(query, info)
		return err
	}

	query := fmt.Sprintf(`UPDATE %s SET status=:status, end_time=NOW(), profit=:profit WHERE round_id=:round_id AND node_id=:node_id`, validationResultTable)
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
func (n *SQLDB) LoadValidationResultInfos(startTime, endTime time.Time, pageNumber, pageSize int) (*types.ListValidationResultRsp, error) {
	// TODO problematic from web
	res := new(types.ListValidationResultRsp)
	var infos []types.ValidationResultInfo
	query := fmt.Sprintf("SELECT *, (duration/1e3 * bandwidth) AS `upload_traffic` FROM %s WHERE start_time between ? and ? order by start_time asc  LIMIT ?,? ", validationResultTable)

	if pageSize > loadValidationResultsDefaultLimit {
		pageSize = loadValidationResultsDefaultLimit
	}

	err := n.db.Select(&infos, query, startTime, endTime, (pageNumber-1)*pageSize, pageSize)
	if err != nil {
		return nil, err
	}

	res.ValidationResultInfos = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE start_time between ? and ?", validationResultTable)
	var count int
	err = n.db.Get(&count, countQuery, startTime, endTime)
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
		`INSERT INTO %s (node_id, mac_location, product_type, cpu_cores, memory, node_name, latitude, disk_usage,
			    longitude, disk_type, io_system, system_version, nat_type, disk_space, bandwidth_up, bandwidth_down, blocks, scheduler_sid) 
				VALUES (:node_id, :mac_location, :product_type, :cpu_cores, :memory, :node_name, :latitude, :disk_usage,
				:longitude, :disk_type, :io_system, :system_version, :nat_type, :disk_space, :bandwidth_up, :bandwidth_down, :blocks, :scheduler_sid) 
				ON DUPLICATE KEY UPDATE node_id=:node_id, last_seen=:last_seen, disk_usage=:disk_usage, blocks=:blocks, scheduler_sid=:scheduler_sid`, nodeInfoTable)

	_, err := n.db.NamedExec(query, info)
	return err
}

// UpdateNodeOnlineTime update node online time and last time
func (n *SQLDB) UpdateNodeOnlineTime(nodeID string, onlineTime int) error {
	query := fmt.Sprintf(`UPDATE %s SET last_seen=NOW(),online_duration=? WHERE node_id=?`, nodeInfoTable)
	// update
	_, err := n.db.Exec(query, onlineTime, nodeID)
	return err
}

// SaveNodeRegisterInfo Insert Node register info
func (n *SQLDB) SaveNodeRegisterInfo(pKey, nodeID string, nodeType types.NodeType) error {
	query := fmt.Sprintf(`INSERT INTO %s (node_id, public_key, create_time, node_type)
	VALUES (?, ?, NOW(), ?)`, nodeRegisterTable)

	_, err := n.db.Exec(query, nodeID, pKey, nodeType)

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

// LoadNodeOnlineDuration load online duration of node.
func (n *SQLDB) LoadNodeOnlineDuration(nodeID string) (int, error) {
	var t int

	query := fmt.Sprintf(`SELECT online_duration FROM %s WHERE node_id=?`, nodeInfoTable)
	if err := n.db.Get(&t, query, nodeID); err != nil {
		return t, err
	}

	return t, nil
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

// TODO save bucketHashes as array
// SaveAssetsView update or insert top hash and buckets hashes to assets view
// bucketHashes key is number of bucket, value is bucket hash
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

func (n *SQLDB) SaveTokenPayload(tks []*types.TokenPayload) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (token_id, node_id, client_id, asset_id, limit_rate, create_time, expiration) 
				VALUES (:token_id, :node_id, :client_id, :asset_id, :limit_rate, :create_time, :expiration)`, workloadReportTable)

	_, err := n.db.NamedExec(query, tks)
	return err
}

func (n *SQLDB) UpdateWorkloadReport(tokenID string, isClient bool, workloads []byte) error {
	query := fmt.Sprintf(`UPDATE %s SET node_workload=? WHERE token_id=?`, workloadReportTable)
	if isClient {
		query = fmt.Sprintf(`UPDATE %s SET client_workload=? WHERE token_id=?`, workloadReportTable)
	}

	_, err := n.db.Exec(query, workloads, tokenID)
	return err
}

func (n *SQLDB) LoadTokenPayloadAndWorkloads(tokenID string, isClient bool) (*types.TokenPayload, []byte, error) {
	query := fmt.Sprintf(`SELECT token_id, node_id, client_id, asset_id, limit_rate, create_time, expiration FROM %s WHERE token_id=?`, workloadReportTable)
	var tkPayload types.TokenPayload
	err := n.db.Get(&tkPayload, query, tokenID)
	if err != nil {
		return nil, nil, err
	}

	query = fmt.Sprintf(`SELECT node_workload FROM %s WHERE token_id=?`, workloadReportTable)
	if isClient {
		query = fmt.Sprintf(`SELECT client_workload FROM %s WHERE token_id=?`, workloadReportTable)
	}
	var workload []byte
	err = n.db.Get(&workload, query, tokenID)
	if err != nil {
		if err == sql.ErrNoRows {
			return &tkPayload, nil, nil
		}
		return nil, nil, err
	}
	return &tkPayload, workload, nil
}

// LoadValidationResults Load unprocessed validation results
func (n *SQLDB) LoadValidationResults(maxTime time.Time, limit int) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE processed=? AND end_time<? LIMIT ?`, validationResultTable)
	return n.db.QueryxContext(context.Background(), sQuery, false, maxTime, limit)
}

// UpdateValidationResultProfit update profit for node validation
func (n *SQLDB) UpdateValidationResultProfit(id int, profit float64) error {
	uQuery := fmt.Sprintf(`UPDATE %s SET profit=? WHERE id=? AND processed=?`, validationResultTable)
	_, err := n.db.Exec(uQuery, profit, id, false)
	return err
}

// UpdateNodeProfitsByValidationResult Update the gain value of the node, and set the processed flag to the validation record
func (n *SQLDB) UpdateNodeProfitsByValidationResult(vIDs []int, nodeProfits map[string]float64) error {
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
	uQuery := fmt.Sprintf(`UPDATE %s SET processed=? WHERE id in (?)`, validationResultTable)
	query, args, err := sqlx.In(uQuery, true, vIDs)
	if err != nil {
		return err
	}

	query = n.db.Rebind(query)
	_, err = n.db.QueryxContext(context.Background(), query, args...)
	if err != nil {
		return err
	}

	for nodeID, profit := range nodeProfits {
		// update node profit
		uQuery = fmt.Sprintf(`UPDATE %s SET profit=profit+? WHERE node_id=?`, nodeInfoTable)
		_, err = tx.Exec(uQuery, profit, nodeID)
		if err != nil {
			return err
		}
	}

	// Commit
	return tx.Commit()
}

// SaveNodeProfit Modify and return the node profit value
func (n *SQLDB) SaveNodeProfit(nodeID string, cProfit float64) (oldProfit, newValue float64, err error) {
	tx, err := n.db.Beginx()
	if err != nil {
		return
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("Rollback err:%s", err.Error())
		}
	}()
	// select node info
	nQuery := fmt.Sprintf(`SELECT profit FROM %s WHERE node_id=? FOR UPDATE`, nodeInfoTable)
	err = tx.Get(&oldProfit, nQuery, nodeID)
	if err != nil {
		return
	}

	newValue = oldProfit + float64(cProfit)

	// update node profit
	uQuery := fmt.Sprintf(`UPDATE %s SET profit=? WHERE node_id=?`, nodeInfoTable)
	_, err = tx.Exec(uQuery, newValue, nodeID)
	if err != nil {
		return
	}

	// Commit
	err = tx.Commit()

	return
}
