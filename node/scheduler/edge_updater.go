package scheduler

import (
	"context"

	"github.com/Filecoin-Titan/titan/node/scheduler/db"

	"github.com/Filecoin-Titan/titan/api"
)

// EdgeUpdateManager manages information about edge node updates.
type EdgeUpdateManager struct {
	db          *db.SQLDB
	updateInfos map[int]*api.EdgeUpdateConfig
}

// NewEdgeUpdateManager creates a new EdgeUpdateManager with the given SQL database connection.
func NewEdgeUpdateManager(db *db.SQLDB) (*EdgeUpdateManager, error) {
	updater := &EdgeUpdateManager{
		db:          db,
		updateInfos: make(map[int]*api.EdgeUpdateConfig),
	}
	appUpdateInfo, err := db.LoadEdgeUpdateConfigs()
	if err != nil {
		log.Errorf("GetEdgeUpdateConfigs error:%s", err)
		return nil, err
	}
	updater.updateInfos = appUpdateInfo
	return updater, nil
}

// GetEdgeUpdateConfigs  returns the map of edge node update information.
func (eu *EdgeUpdateManager) GetEdgeUpdateConfigs(ctx context.Context) (map[int]*api.EdgeUpdateConfig, error) {
	return eu.updateInfos, nil
}

// SetEdgeUpdateConfig sets the EdgeUpdateConfig for the given node type.
func (eu *EdgeUpdateManager) SetEdgeUpdateConfig(ctx context.Context, info *api.EdgeUpdateConfig) error {
	if eu.updateInfos == nil {
		eu.updateInfos = make(map[int]*api.EdgeUpdateConfig)
	}
	eu.updateInfos[info.NodeType] = info
	return eu.db.SaveEdgeUpdateConfig(info)
}

// DeleteEdgeUpdateConfig deletes the EdgeUpdateConfig for the given node type.
func (eu *EdgeUpdateManager) DeleteEdgeUpdateConfig(ctx context.Context, nodeType int) error {
	delete(eu.updateInfos, nodeType)
	return eu.db.DeleteEdgeUpdateConfig(nodeType)
}
