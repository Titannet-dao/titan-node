package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

// SaveProjectInfo inserts new project information into the database or updates it if it already exists, and also updates or inserts state information.
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
		`INSERT INTO %s (id, name, bundle_url, user_id, replicas, scheduler_sid, requirement, expiration, type)
				VALUES (:id, :name, :bundle_url, :user_id, :replicas, :scheduler_sid, :requirement, :expiration, :type)				
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

// DeleteProjectInfo deletes project information and its associated state information from the database.
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

// LoadProjectInfo loads project record and state information based on the provided project ID.
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

// ProjectExists checks if a project exists in the state machine table of the specified server.
func (n *SQLDB) ProjectExists(id string, serverID dtypes.ServerID) (bool, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(id) FROM %s WHERE id=? `, projectStateTable(serverID))
	if err := n.db.Get(&total, countSQL, id); err != nil {
		return false, err
	}

	return total > 0, nil
}

// UpdateProjectInfo updates project information in the database.
func (n *SQLDB) UpdateProjectInfo(info *types.ProjectInfo) error {
	query := fmt.Sprintf(`UPDATE %s SET name=:name,bundle_url=:bundle_url, replicas=:replicas, scheduler_sid=:scheduler_sid WHERE id=:id `, projectInfoTable)
	_, err := n.db.NamedExec(query, info)

	return err
}

// LoadProjectCount counts projects based on a server ID and a filter state.
func (n *SQLDB) LoadProjectCount(serverID dtypes.ServerID, filterState string) (int, error) {
	var size int
	cmd := fmt.Sprintf("SELECT count(id) FROM %s WHERE state!=?", projectStateTable(serverID))
	err := n.db.Get(&size, cmd, filterState)

	return size, err
}

// LoadAllProjectInfos loads all project records for a given server ID within specified parameters.
func (n *SQLDB) LoadAllProjectInfos(serverID dtypes.ServerID, limit, offset int, statuses []string) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s a LEFT JOIN %s b ON a.id = b.id WHERE a.state in (?) order by a.id asc limit ? offset ?`, projectStateTable(serverID), projectInfoTable)
	query, args, err := sqlx.In(sQuery, statuses, limit, offset)
	if err != nil {
		return nil, err
	}

	query = n.db.Rebind(query)
	return n.db.QueryxContext(context.Background(), query, args...)
}

// LoadProjectInfos loads project information based on a server ID and optionally a user ID.
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

// UpdateProjectReplicasInfo updates the replicas information for a project.
func (n *SQLDB) UpdateProjectReplicasInfo(info *types.ProjectReplicas) error {
	query := fmt.Sprintf(
		`UPDATE %s SET time=?,max_delay=?,min_delay=?,avg_delay=?,upload_traffic=?,download_traffic=? WHERE id=? AND node_id=?`, projectReplicasTable)
	_, err := n.db.Exec(query, info.Time, info.MaxDelay, info.MinDelay, info.AvgDelay, info.UploadTraffic, info.DownTraffic, info.Id, info.NodeID)

	return err
}

// SaveProjectReplicasInfo inserts or updates project replica information.
func (n *SQLDB) SaveProjectReplicasInfo(info *types.ProjectReplicas) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (id, node_id, status, created_time, end_time, type )
		VALUES (:id, :node_id, :status, NOW(), NOW(), :type)
		ON DUPLICATE KEY UPDATE status=:status, end_time=NOW()`, projectReplicasTable)

	_, err := n.db.NamedExec(query, info)
	if err != nil {
		return err
	}

	if info.Status == types.ProjectReplicaStatusStarted {
		err = n.SaveProjectEvent(&types.ProjectReplicaEventInfo{ID: info.Id, NodeID: info.NodeID, Event: types.ProjectEventAdd}, 1, 0)
		if err != nil {
			return err
		}
	} else if info.Status == types.ProjectReplicaStatusError {
		err = n.SaveProjectEvent(&types.ProjectReplicaEventInfo{ID: info.Id, NodeID: info.NodeID, Event: types.ProjectEventFailed}, 0, 1)
		if err != nil {
			return err
		}
	}

	return nil
}

// LoadProjectReplicaInfos loads multiple project replica information based on an ID.
func (n *SQLDB) LoadProjectReplicaInfos(id string) ([]*types.ProjectReplicas, error) {
	var out []*types.ProjectReplicas
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE id=? `, projectReplicasTable)
	err := n.db.Select(&out, sQuery, id)

	return out, err
}

// LoadProjectReplicaInfo loads specific project replica information based on an ID and node ID.
func (n *SQLDB) LoadProjectReplicaInfo(id, nodeID string) (*types.ProjectReplicas, error) {
	var out types.ProjectReplicas
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE id=? AND node_id=?`, projectReplicasTable)
	err := n.db.Get(&out, sQuery, id, nodeID)

	return &out, err
}

// LoadAllProjectReplicasForNode loads project replica information based on a node ID.
func (n *SQLDB) LoadAllProjectReplicasForNode(nodeID string) ([]*types.ProjectReplicas, error) {
	var out []*types.ProjectReplicas
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE node_id=? `, projectReplicasTable)
	err := n.db.Select(&out, sQuery, nodeID)
	return out, err
}

// LoadProjectReplicasForNode loads project replica information based on a node ID.
func (n *SQLDB) LoadProjectReplicasForNode(nodeID string, limit, offset int, uuid string, statuses []types.ProjectReplicaStatus) (*types.ListProjectReplicaRsp, error) {
	res := new(types.ListProjectReplicaRsp)
	var infos []*types.ProjectReplicas
	var count int

	sq := squirrel.Select("*").From(projectReplicasTable).Where(squirrel.Eq{"node_id": nodeID})
	sq2 := squirrel.Select("COUNT(*)").From(projectReplicasTable).Where(squirrel.Eq{"node_id": nodeID})
	if len(uuid) > 0 {
		sq = sq.Where(squirrel.Eq{"id": uuid})
		sq2 = sq2.Where(squirrel.Eq{"id": uuid})
	}
	if len(statuses) > 0 {
		sq = sq.Where(squirrel.Eq{"status": statuses})
		sq2 = sq2.Where(squirrel.Eq{"status": statuses})
	}

	query, args, err := sq.Limit(uint64(limit)).Offset(uint64(offset)).ToSql()
	if err != nil {
		return nil, err
	}

	err = n.db.Select(&infos, query, args...)
	if err != nil {
		return nil, err
	}

	query2, args2, err := sq2.ToSql()
	if err != nil {
		return nil, err
	}

	err = n.db.Get(&count, query2, args2...)
	if err != nil {
		return nil, err
	}

	res.List = infos
	res.Total = count

	return res, nil
}

// DeleteProjectReplica deletes a project replica from the database.
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

// DeleteUnfinishedProjectReplicas deletes incomplete replicas that have not started.
func (n *SQLDB) DeleteUnfinishedProjectReplicas(id string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id=? AND status!=?`, projectReplicasTable)
	_, err := n.db.Exec(query, id, types.ProjectReplicaStatusStarted)

	return err
}

// UpdateProjectReplicaStatusFromNode updates the status of project replicas from a specific node.
func (n *SQLDB) UpdateProjectReplicaStatusFromNode(nodeID string, uuids []string, status types.ProjectReplicaStatus) error {
	stmt, err := n.db.Preparex(`UPDATE ` + projectReplicasTable + ` SET status=?,end_time=NOW() WHERE id=? AND node_id=?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, uid := range uuids {
		if _, err := stmt.Exec(status, uid, nodeID); err != nil {
			log.Errorf("UpdateProjectReplicaStatusToFailed %s err:%s", nodeID, err.Error())
		}
	}

	return nil
}

// UpdateProjectStateInfo updates project state information in the database.
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

	return tx.Commit()
}

// LoadProjectStateInfo loads the state information of a specific project.
func (n *SQLDB) LoadProjectStateInfo(id string, serverID dtypes.ServerID) (*types.ProjectStateInfo, error) {
	var info types.ProjectStateInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE id=?", projectStateTable(serverID))
	if err := n.db.Get(&info, query, id); err != nil {
		return nil, err
	}

	return &info, nil
}

// LoadNodesOfStartingReplica loads the node IDs for replicas in the 'Starting' state.
func (n *SQLDB) LoadNodesOfStartingReplica(id string) ([]string, error) {
	var nodes []string
	query := fmt.Sprintf("SELECT node_id FROM %s WHERE id=? AND status=?", projectReplicasTable)
	err := n.db.Select(&nodes, query, id, types.ProjectReplicaStatusStarting)
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

// LoadProjectOverviews retrieves project overviews from the database.
func (n *SQLDB) LoadProjectOverviews() ([]*types.ProjectOverview, error) {
	var out []*types.ProjectOverview
	query := fmt.Sprintf(`SELECT node_id, SUM(upload_traffic) AS sum_upload_traffic,
	SUM(download_traffic) AS sum_download_traffic,
	SUM(time) AS sum_time,
	FLOOR(AVG(avg_delay)) AS avg_delay FROM %s WHERE type=0 GROUP BY node_id`, projectReplicasTable)
	err := n.db.Select(&out, query)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// UpdateProjectReplicasStatusToFailed updates the status of unfinished project replicas to 'Failed'.
func (n *SQLDB) UpdateProjectReplicasStatusToFailed(id string) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=? WHERE id=? AND status=?`, projectReplicasTable)
	_, err := n.db.Exec(query, types.ProjectReplicaStatusError, id, types.ProjectReplicaStatusStarting)

	return err
}
