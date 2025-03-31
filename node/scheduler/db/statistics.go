package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/jmoiron/sqlx"
)

// LoadBandwidthUpFromRetrieve retrieves the bandwidth up data for a specific node
func (n *SQLDB) LoadBandwidthUpFromRetrieve(nodeID string, start, end time.Time) ([]int64, error) {
	var out []int64
	query := fmt.Sprintf("SELECT speed FROM %s WHERE created_time BETWEEN ? AND ? AND node_id=? AND status=? AND speed>0", nodeRetrieveTable)
	err := n.db.Select(&out, query, start, end, nodeID, types.EventStatusSucceed)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// LoadBandwidthDownFromReplica retrieves the bandwidth down data for a specific node
func (n *SQLDB) LoadBandwidthDownFromReplica(nodeID string, start, end time.Time) ([]int64, error) {
	var out []int64
	query := fmt.Sprintf("SELECT speed FROM %s WHERE end_time BETWEEN ? AND ? AND node_id=? AND status=?", replicaInfoTable)
	err := n.db.Select(&out, query, start, end, nodeID, types.ReplicaStatusSucceeded)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// LoadBandwidthDownFromValidation retrieves the bandwidth down data for a specific node
func (n *SQLDB) LoadBandwidthDownFromValidation(nodeID string, start, end time.Time) ([]float64, error) {
	var out []float64
	query := fmt.Sprintf("SELECT bandwidth FROM %s WHERE end_time BETWEEN ? AND ? AND validator_id=? AND status=?", validationResultTable)
	err := n.db.Select(&out, query, start, end, nodeID, types.ValidationStatusSuccess)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// SaveRetrieveEventInfo records a retrieval event and updates the associated node information in the database.
func (n *SQLDB) SaveRetrieveEventInfo(info *types.RetrieveEvent, succeededCount, failedCount int) error {
	total := succeededCount + failedCount
	// update node info
	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, retrieve_count, retrieve_succeeded_count, retrieve_failed_count) VALUES (?, ?, ?, ?) 
				ON DUPLICATE KEY UPDATE retrieve_count=retrieve_count+? ,retrieve_succeeded_count=retrieve_succeeded_count+?, retrieve_failed_count=retrieve_failed_count+?, update_time=NOW()`, nodeStatisticsTable)
	_, err := n.db.Exec(query, info.NodeID, total, succeededCount, failedCount, total, succeededCount, failedCount)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(
		`INSERT INTO %s (trace_id, node_id, client_id, hash, size, speed, status ) 
				VALUES (:trace_id, :node_id, :client_id, :hash, :size, :speed, :status )`, nodeRetrieveTable)
	_, err = n.db.NamedExec(query, info)

	return err
}

// SaveReplicaEvent logs a replica event with detailed event information into the database.
func (n *SQLDB) SaveReplicaEvent(info *types.AssetReplicaEventInfo, succeededCount, failedCount int) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("SaveReplicaEvent Rollback err:%s", err.Error())
		}
	}()

	total := succeededCount + failedCount

	// update node asset count
	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, asset_count, asset_succeeded_count, asset_failed_count) VALUES (?, ?, ?, ?) 
				ON DUPLICATE KEY UPDATE asset_count=asset_count+? ,asset_succeeded_count=asset_succeeded_count+?, asset_failed_count=asset_failed_count+?, update_time=NOW()`, nodeStatisticsTable)
	_, err = tx.Exec(query, info.NodeID, total, succeededCount, failedCount, total, succeededCount, failedCount)
	if err != nil {
		return err
	}

	// replica event
	err = n.saveReplicaEvent(tx, info)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (n *SQLDB) saveReplicaEvent(tx *sqlx.Tx, info *types.AssetReplicaEventInfo) error {
	qry := fmt.Sprintf(`INSERT INTO %s (node_id, event, hash, source, client_id, speed, cid, total_size, done_size, trace_id, msg) 
		        VALUES (:node_id, :event, :hash, :source, :client_id, :speed, :cid, :total_size, :done_size, :trace_id, :msg)`, replicaEventTable)
	_, err := tx.NamedExec(qry, info)

	return err
}

// SaveProjectEvent logs a replica event with detailed event information into the database.
func (n *SQLDB) SaveProjectEvent(info *types.ProjectReplicaEventInfo, succeededCount, failedCount int) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("SaveProjectEvent Rollback err:%s", err.Error())
		}
	}()

	// update node project count
	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, project_count, project_succeeded_count, project_failed_count) VALUES (?, ?, ?, ?) 
				ON DUPLICATE KEY UPDATE project_count=project_count+? ,project_succeeded_count=project_succeeded_count+?, project_failed_count=project_failed_count+?, update_time=NOW()`, nodeStatisticsTable)
	_, err = tx.Exec(query, info.NodeID, 1, succeededCount, failedCount, 1, succeededCount, failedCount)
	if err != nil {
		return err
	}

	// replica event
	err = n.saveProjectReplicaEvent(tx, info)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (n *SQLDB) saveProjectReplicaEvent(tx *sqlx.Tx, info *types.ProjectReplicaEventInfo) error {
	qry := fmt.Sprintf(`INSERT INTO %s (node_id, event, id) 
		        VALUES (:node_id, :event, :id)`, projectEventTable)
	_, err := tx.NamedExec(qry, info)

	return err
}

// LoadNodeStatisticsInfo retrieves statistics information for a given node ID.
func (n *SQLDB) LoadNodeStatisticsInfo(nodeID string) (types.NodeStatisticsInfo, error) {
	sInfo := types.NodeStatisticsInfo{}
	query := fmt.Sprintf(`SELECT asset_count,asset_succeeded_count,asset_failed_count,retrieve_count,retrieve_succeeded_count,retrieve_failed_count,
	    project_count,project_succeeded_count,project_failed_count 
	    FROM %s WHERE node_id=?`, nodeStatisticsTable)
	err := n.db.Get(&sInfo, query, nodeID)

	return sInfo, err
}

// LoadReplicaEventCountByStatus retrieves a count of replica for a specific hash filtered by status.
func (n *SQLDB) LoadReplicaEventCountByStatus(hash string, statuses []types.ReplicaEvent) (int, error) {
	sQuery := fmt.Sprintf(`SELECT count(*) FROM %s WHERE hash=? AND event in (?)`, replicaEventTable)
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

// LoadReplicaEventsByNode retrieves replica events for a specific node ID, excluding the removal events, with pagination support.
func (n *SQLDB) LoadReplicaEventsByNode(nodeID string, status types.ReplicaEvent, limit, offset int) (*types.ListAssetReplicaEventRsp, error) {
	res := new(types.ListAssetReplicaEventRsp)

	var infos []*types.AssetReplicaEventInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE node_id=? AND event=? order by created_time desc LIMIT ? OFFSET ? ", replicaEventTable)
	if limit > loadReplicaEventDefaultLimit {
		limit = loadReplicaEventDefaultLimit
	}

	err := n.db.Select(&infos, query, nodeID, status, limit, offset)
	if err != nil {
		return nil, err
	}

	res.List = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE node_id=? AND event=?", replicaEventTable)
	var count int
	err = n.db.Get(&count, countQuery, nodeID, status)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// LoadReplicaEventsOfNode retrieves replica events for a specific node ID, excluding the removal events, with pagination support.
func (n *SQLDB) LoadReplicaEventsOfNode(nodeID string, limit, offset int) (*types.ListAssetReplicaEventRsp, error) {
	res := new(types.ListAssetReplicaEventRsp)

	var infos []*types.AssetReplicaEventInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE node_id=? AND event!=? order by created_time desc LIMIT ? OFFSET ? ", replicaEventTable)
	if limit > loadReplicaEventDefaultLimit {
		limit = loadReplicaEventDefaultLimit
	}

	err := n.db.Select(&infos, query, nodeID, types.ReplicaEventRemove, limit, offset)
	if err != nil {
		return nil, err
	}

	res.List = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE node_id=? AND event!=?", replicaEventTable)
	var count int
	err = n.db.Get(&count, countQuery, nodeID, types.ReplicaEventRemove)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// LoadReplicaEventsByHash retrieves replica events for a specific node ID, excluding the removal events, with pagination support.
func (n *SQLDB) LoadReplicaEventsByHash(hash string, status types.ReplicaEvent, limit, offset int) (*types.ListAssetReplicaEventRsp, error) {
	res := new(types.ListAssetReplicaEventRsp)

	var infos []*types.AssetReplicaEventInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE hash=? AND event=? order by created_time desc LIMIT ? OFFSET ? ", replicaEventTable)
	if limit > loadReplicaEventDefaultLimit {
		limit = loadReplicaEventDefaultLimit
	}

	err := n.db.Select(&infos, query, hash, status, limit, offset)
	if err != nil {
		return nil, err
	}

	res.List = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE hash=? AND event=?", replicaEventTable)
	var count int
	err = n.db.Get(&count, countQuery, hash, status)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// func (n *SQLDB) saveServiceEvent(tx *sqlx.Tx, info *types.ServiceEvent) error {
// 	qry := fmt.Sprintf(`INSERT INTO %s (trace_id, node_id, info, size, status, peak, end_time, start_time, speed, score)
// 		        VALUES (:trace_id, :node_id, :info, :size, :status, :peak, :end_time, :start_time, :speed, :score)`, serviceEventTable)
// 	_, err := tx.NamedExec(qry, info)

// 	return err
// }

// SaveServiceEvent logs a service event with detailed event information into the database.
func (n *SQLDB) SaveServiceEvent(info *types.ServiceEvent) error {
	qry := fmt.Sprintf(`INSERT INTO %s (trace_id, node_id, info, size, status, peak, end_time, start_time, speed, score) 
		        VALUES (:trace_id, :node_id, :info, :size, :status, :peak, :end_time, :start_time, :speed, :score)`, serviceEventTable)
	_, err := n.db.NamedExec(qry, info)
	return err
}

func (n *SQLDB) LoadServiceEventByNode(nodeID string, start, end time.Time) ([]*types.ServiceEvent, error) {
	var infos []*types.ServiceEvent
	query := fmt.Sprintf("SELECT * FROM %s WHERE node_id=? AND start_time BETWEEN ? AND ?", serviceEventTable)

	err := n.db.Select(&infos, query, nodeID, start, end)
	if err != nil {
		return nil, err
	}

	return infos, nil
}

func (n *SQLDB) LoadServiceEventCount(nodeID string, start, end time.Time) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT count(*) FROM %s WHERE node_id=? AND start_time BETWEEN ? AND ?", serviceEventTable)

	err := n.db.Get(&count, query, nodeID, start, end)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (n *SQLDB) LoadServiceEventCountByStatus(nodeID string, status types.ServiceStatus, start, end time.Time) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT count(*) FROM %s WHERE node_id=? AND status=? AND start_time BETWEEN ? AND ?", serviceEventTable)

	err := n.db.Get(&count, query, nodeID, status, start, end)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (n *SQLDB) LoadServiceEvents(startTime, endTime time.Time) ([]*types.ServiceEvent, error) {
	var infos []*types.ServiceEvent
	query := fmt.Sprintf("SELECT * FROM %s WHERE start_time BETWEEN ? AND ?", serviceEventTable)

	err := n.db.Select(&infos, query, startTime, endTime)
	if err != nil {
		return nil, err
	}

	return infos, nil
}

// SaveBandwidthEvent logs a bandwidth event with detailed event information into the database.
func (n *SQLDB) SaveBandwidthEvent(infos []*types.BandwidthEvent) error {
	qry := fmt.Sprintf(`INSERT INTO %s (node_id, b_up_peak, b_down_peak, b_up_free, b_down_free, b_up_load, b_down_load, size, task_success, task_total, score, created_time) 
		        VALUES (:node_id, :b_up_peak, :b_down_peak, :b_up_free, :b_down_free, :b_up_load, :b_down_load, :size, :task_success, :task_total, :score, :created_time)`, bandwidthEventTable)
	_, err := n.db.NamedExec(qry, infos)

	return err
}

func (n *SQLDB) CleanBandwidthEvent(startTime, endTime time.Time) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE start_time BETWEEN ? AND ? `, bandwidthEventTable)
	_, err := n.db.Exec(query, startTime, endTime)
	return err
}

func (n *SQLDB) LoadBandwidthEvents(startTime, endTime time.Time) ([]*types.BandwidthEvent, error) {
	var infos []*types.BandwidthEvent
	query := fmt.Sprintf("SELECT * FROM %s WHERE start_time BETWEEN ? AND ?", bandwidthEventTable)

	err := n.db.Select(&infos, query, startTime, endTime)
	if err != nil {
		return nil, err
	}

	return infos, nil
}
