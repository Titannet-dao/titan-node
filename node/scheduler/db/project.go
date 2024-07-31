package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/jmoiron/sqlx"
)

// ProjectExists checks if an project exists in the state machine table of the specified server.
func (n *SQLDB) ProjectExists(id string, serverID dtypes.ServerID) (bool, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(id) FROM %s WHERE id=? `, projectStateTable(serverID))
	if err := n.db.Get(&total, countSQL, id); err != nil {
		return false, err
	}

	return total > 0, nil
}

func (n *SQLDB) UpdateProjectInfo(info *types.ProjectInfo) error {
	query := fmt.Sprintf(`UPDATE %s SET name=:name,bundle_url=:bundle_url, replicas=:replicas, scheduler_sid=:scheduler_sid WHERE id=:id `, projectInfoTable)
	_, err := n.db.NamedExec(query, info)
	if err != nil {
		return err
	}

	return nil
}

// SaveProjectInfo inserts
func (n *SQLDB) SaveProjectInfo(info *types.ProjectInfo) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("SaveProjectInfo Rollback err:%s", err.Error())
		}
	}()

	query := fmt.Sprintf(
		`INSERT INTO %s (id, name, bundle_url, user_id, replicas, scheduler_sid, cpu_cores, memory, area_id, expiration)
				VALUES (:id, :name, :bundle_url, :user_id, :replicas, :scheduler_sid, :cpu_cores, :memory, :area_id, :expiration)				
				ON DUPLICATE KEY UPDATE scheduler_sid=:scheduler_sid, replicas=:replicas, user_id=:user_id,
				bundle_url=:bundle_url, expiration=:expiration, name=:name`, projectInfoTable)

	_, err = tx.NamedExec(query, info)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(
		`INSERT INTO %s (id, state, replenish_replicas) 
		        VALUES (?, ?, ?) 
				ON DUPLICATE KEY UPDATE state=?, replenish_replicas=?`, projectStateTable(info.ServerID))
	_, err = tx.Exec(query, info.UUID, info.State, info.ReplenishReplicas, info.State, info.ReplenishReplicas)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// DeleteProjectInfo deletes
func (n *SQLDB) DeleteProjectInfo(serverID dtypes.ServerID, id string) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("DeleteWorkerdProjectInfo Rollback err:%s", err.Error())
		}
	}()

	query := fmt.Sprintf(`DELETE FROM %s WHERE id=?`, projectStateTable(serverID))
	_, err = tx.Exec(query, id)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE id=?`, projectInfoTable)
	_, err = tx.Exec(query, id)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// LoadProjectInfo load project record information
func (n *SQLDB) LoadProjectInfo(id string) (*types.ProjectInfo, error) {
	var info types.ProjectInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE id=?", projectInfoTable)
	err := n.db.Get(&info, query, id)
	if err != nil {
		return nil, err
	}

	stateInfo, err := n.LoadProjectStateInfo(id, info.ServerID)
	if err != nil {
		return nil, err
	}

	info.State = stateInfo.State
	info.RetryCount = stateInfo.RetryCount
	info.ReplenishReplicas = stateInfo.ReplenishReplicas

	return &info, nil
}

// LoadProjectCount count project
func (n *SQLDB) LoadProjectCount(serverID dtypes.ServerID, filterState string) (int, error) {
	var size int
	cmd := fmt.Sprintf("SELECT count(id) FROM %s WHERE state!=?", projectStateTable(serverID))
	err := n.db.Get(&size, cmd, filterState)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// LoadAllProjectInfos loads all project records for a given server ID.
func (n *SQLDB) LoadAllProjectInfos(serverID dtypes.ServerID, limit, offset int, statuses []string) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s a LEFT JOIN %s b ON a.id = b.id WHERE a.state in (?) order by a.id asc limit ? offset ?`, projectStateTable(serverID), projectInfoTable)
	query, args, err := sqlx.In(sQuery, statuses, limit, offset)
	if err != nil {
		return nil, err
	}

	query = n.db.Rebind(query)
	return n.db.QueryxContext(context.Background(), query, args...)
}

// LoadProjectInfos load project record information
func (n *SQLDB) LoadProjectInfos(serverID dtypes.ServerID, userID string, limit, offset int) ([]*types.ProjectInfo, error) {
	var infos []*types.ProjectInfo

	if userID == "" {
		query := fmt.Sprintf("SELECT * FROM %s a LEFT JOIN %s b ON a.id = b.id order by b.created_time desc LIMIT ? OFFSET ?", projectStateTable(serverID), projectInfoTable)
		err := n.db.Select(&infos, query, limit, offset)
		if err != nil {
			return nil, err
		}
	} else {
		query := fmt.Sprintf("SELECT * FROM %s a LEFT JOIN %s b ON a.id = b.id WHERE user_id=? order by b.created_time desc LIMIT ? OFFSET ?", projectStateTable(serverID), projectInfoTable)
		err := n.db.Select(&infos, query, userID, limit, offset)
		if err != nil {
			return nil, err
		}
	}

	return infos, nil
}

// SaveProjectReplicasInfo inserts
func (n *SQLDB) SaveProjectReplicasInfo(info *types.ProjectReplicas) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (id, node_id, status, created_time, end_time)
		VALUES (:id, :node_id, :status, NOW(), NOW())
		ON DUPLICATE KEY UPDATE status=:status, end_time=NOW()`, projectReplicasTable)

	_, err := n.db.NamedExec(query, info)
	if err != nil {
		return err
	}

	if info.Status == types.ProjectReplicaStatusStarted {
		// replica event
		query = fmt.Sprintf(
			`INSERT INTO %s (id, event, node_id) 
			VALUES (?, ?, ?)`, projectEventTable)
		_, err = n.db.Exec(query, info.Id, types.ProjectEventAdd, info.NodeID)
		if err != nil {
			return err
		}
	}
	return nil
}

// LoadProjectReplicaInfos load project replica information based on id
func (n *SQLDB) LoadProjectReplicaInfos(id string) ([]*types.ProjectReplicas, error) {
	var out []*types.ProjectReplicas
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE id=? `, projectReplicasTable)
	if err := n.db.Select(&out, sQuery, id); err != nil {
		return nil, err
	}

	return out, nil
}

// LoadProjectReplicaInfo load project replica information based on id
func (n *SQLDB) LoadProjectReplicaInfo(id, nodeID string) (*types.ProjectReplicas, error) {
	var out types.ProjectReplicas
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE id=? AND node_id=?`, projectReplicasTable)
	if err := n.db.Get(&out, sQuery, id, nodeID); err != nil {
		return nil, err
	}

	return &out, nil
}

// LoadProjectReplicasForNode load project replica  information based on node
func (n *SQLDB) LoadProjectReplicasForNode(nodeID string) ([]*types.ProjectReplicas, error) {
	var out []*types.ProjectReplicas
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE node_id=? `, projectReplicasTable)
	if err := n.db.Select(&out, sQuery, nodeID); err != nil {
		return nil, err
	}

	return out, nil
}

// DeleteProjectReplica deletes
func (n *SQLDB) DeleteProjectReplica(id, nodeID string, event types.ProjectEvent) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("DeleteProjectReplica Rollback err:%s", err.Error())
		}
	}()

	query := fmt.Sprintf(`DELETE FROM %s WHERE id=? AND node_id=?`, projectReplicasTable)
	_, err = tx.Exec(query, id, nodeID)
	if err != nil {
		return err
	}

	// replica event
	query = fmt.Sprintf(
		`INSERT INTO %s (id, event, node_id) 
			VALUES (?, ?, ?)`, projectEventTable)

	_, err = tx.Exec(query, id, event, nodeID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// DeleteUnfinishedProjectReplicas deletes the incomplete replicas with the given hash from the database.
func (n *SQLDB) DeleteUnfinishedProjectReplicas(id string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id=? AND status!=?`, projectReplicasTable)
	_, err := n.db.Exec(query, id, types.ProjectReplicaStatusStarted)

	return err
}

// UpdateProjectReplicaStatusFromNode
func (n *SQLDB) UpdateProjectReplicaStatusFromNode(nodeID string, uuids []string, status types.ProjectReplicaStatus) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("UpdateProjectReplicaStatusToOffline Rollback err:%s", err.Error())
		}
	}()

	for _, uid := range uuids {
		// update state table
		query := fmt.Sprintf(
			`UPDATE %s SET status=?,end_time=NOW() WHERE id=? AND node_id=?`, projectReplicasTable)
		tx.Exec(query, status, uid, nodeID)
	}

	return tx.Commit()
}

// UpdateProjectReplicaStatusToFailed
func (n *SQLDB) UpdateProjectReplicaStatusToFailed(id string, nodes []string) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("UpdateProjectReplicaStatusToFailed Rollback err:%s", err.Error())
		}
	}()

	for _, nid := range nodes {
		// update state table
		query := fmt.Sprintf(
			`UPDATE %s SET status=?,end_time=NOW() WHERE id=? AND node_id=?`, projectReplicasTable)
		tx.Exec(query, types.ProjectReplicaStatusError, id, nid)
	}

	return tx.Commit()
}

// UpdateProjectStateInfo update project information
func (n *SQLDB) UpdateProjectStateInfo(id, state string, retryCount, replenishReplicas int64, serverID dtypes.ServerID) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("UpdateProjectInfo Rollback err:%s", err.Error())
		}
	}()

	// update state table
	query := fmt.Sprintf(
		`UPDATE %s SET state=?,retry_count=?,replenish_replicas=? WHERE id=?`, projectStateTable(serverID))
	_, err = tx.Exec(query, state, retryCount, replenishReplicas, id)
	if err != nil {
		return err
	}

	// // update record table
	// dQuery := fmt.Sprintf(`UPDATE %s SET end_time=NOW() WHERE id=?`, projectInfoTable)
	// _, err = tx.Exec(dQuery, id)
	// if err != nil {
	// 	return err
	// }

	return tx.Commit()
}

// LoadProjectStateInfo loads the state of the project for a given server ID.
func (n *SQLDB) LoadProjectStateInfo(id string, serverID dtypes.ServerID) (*types.ProjectStateInfo, error) {
	var info types.ProjectStateInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE id=?", projectStateTable(serverID))
	if err := n.db.Get(&info, query, id); err != nil {
		return nil, err
	}

	return &info, nil
}

// LoadNodesOfStartingReplica
func (n *SQLDB) LoadNodesOfStartingReplica(id string) ([]string, error) {
	var nodes []string
	query := fmt.Sprintf("SELECT node_id FROM %s WHERE id=? AND status=?", projectReplicasTable)
	err := n.db.Select(&nodes, query, id, types.ProjectReplicaStatusStarting)
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

// UpdateProjectReplicasStatusToFailed updates the status of unfinished project replicas
func (n *SQLDB) UpdateProjectReplicasStatusToFailed(id string) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=? WHERE id=? AND status=?`, projectReplicasTable)
	_, err := n.db.Exec(query, types.ProjectReplicaStatusError, id, types.ProjectReplicaStatusStarting)

	return err
}
