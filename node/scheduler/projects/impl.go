package projects

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	xerrors "golang.org/x/xerrors"
)

// UpdateStatus updates the status of projects for a given node ID.
func (m *Manager) UpdateStatus(nodeID string, list []*types.Project) error {
	for _, info := range list {
		exist, _ := m.projectStateMachines.Has(ProjectID(info.ID))
		if !exist {
			continue
		}

		exist = m.isProjectTaskExist(info.ID)
		if !exist {
			continue
		}

		if info.Status == types.ProjectReplicaStatusError {
			log.Infof("UpdateStatus  %s,%s err:%s", nodeID, info.ID, info.Msg)
		}

		err := m.SaveProjectReplicasInfo(&types.ProjectReplicas{
			Id:     info.ID,
			NodeID: nodeID,
			Status: info.Status,
		})
		if err != nil {
			log.Errorf("UpdateStatus SaveProjectReplicasInfo %s,%s err:%s", nodeID, info.ID, err.Error())
		}
	}

	return nil
}

// Deploy deploys a project based on the provided request.
func (m *Manager) Deploy(req *types.DeployProjectReq) error {
	exist := m.isProjectTaskExist(req.UUID)
	if exist {
		return xerrors.Errorf("project %s is exist ", req.UUID)
	}

	if req.Replicas > edgeReplicasLimit {
		return xerrors.Errorf("The number of replicas %d exceeds the limit %d", req.Replicas, edgeReplicasLimit)
	}

	if len(req.Requirement.NodeIDs) > 20 {
		return xerrors.Errorf("The number of nodes %d exceeds the limit %d", req.Replicas, 20)
	}

	// Waiting for state machine initialization
	m.stateMachineWait.Wait()
	log.Infof("%s project event: %s, add project area:[%s], ver:[%d] node:[%s] ", req.UUID, req.Name, req.Requirement.AreaID, req.Requirement.Version, req.Requirement.NodeIDs)

	buffer := &bytes.Buffer{}
	enc := gob.NewEncoder(buffer)
	err := enc.Encode(req.Requirement)
	if err != nil {
		return xerrors.Errorf("Deploy encode error:%s", err.Error())
	}

	info := &types.ProjectInfo{
		UUID:            req.UUID,
		ServerID:        m.nodeMgr.ServerID,
		Expiration:      req.Expiration,
		State:           NodeSelect.String(),
		CreatedTime:     time.Now(),
		Name:            req.Name,
		BundleURL:       req.BundleURL,
		UserID:          req.UserID,
		Replicas:        req.Replicas,
		RequirementByte: buffer.Bytes(),
		Type:            req.Type,
	}

	err = m.SaveProjectInfo(info)
	if err != nil {
		return err
	}

	rInfo := ProjectForceState{
		State: NodeSelect,
		// NodeIDs: req.Requirement.NodeIDs,
	}

	// create project task
	return m.projectStateMachines.Send(ProjectID(info.UUID), rInfo)
}

// Update updates the project with the given request.
func (m *Manager) Update(req *types.ProjectReq) error {
	if req.Replicas > edgeReplicasLimit {
		return xerrors.Errorf("The number of replicas %d exceeds the limit %d", req.Replicas, edgeReplicasLimit)
	}

	info, err := m.LoadProjectInfo(req.UUID)
	if err != nil {
		return err
	}

	if req.Replicas == 0 {
		req.Replicas = info.Replicas
	}

	if req.Name == "" {
		req.Name = info.Name
	}

	if req.BundleURL == "" {
		req.BundleURL = info.BundleURL
	}

	err = m.UpdateProjectInfo(&types.ProjectInfo{
		UUID:      req.UUID,
		Name:      req.Name,
		BundleURL: req.BundleURL,
		ServerID:  m.nodeMgr.ServerID,
		Replicas:  req.Replicas,
	})
	if err != nil {
		return err
	}

	rInfo := ProjectForceState{
		State: Update,
	}

	// create project task
	return m.projectStateMachines.Send(ProjectID(req.UUID), rInfo)
}

// Delete removes a project based on the provided request.
func (m *Manager) Delete(req *types.ProjectReq) error {
	if req.UUID == "" {
		return xerrors.New("UUID is nil")
	}

	if req.NodeID != "" {
		return m.removeReplica(req.UUID, req.NodeID, types.ProjectEventRemove)
	}

	if exist, _ := m.projectStateMachines.Has(ProjectID(req.UUID)); !exist {
		return xerrors.Errorf("not found project %s", req.UUID)
	}

	return m.projectStateMachines.Send(ProjectID(req.UUID), ProjectForceState{State: Remove, Event: int64(types.ProjectEventRemove)})
}

// GetProjectInfo retrieves the project information for the given UUID.
func (m *Manager) GetProjectInfo(uuid string) (*types.ProjectInfo, error) {
	info, err := m.LoadProjectInfo(uuid)
	if err != nil {
		return nil, err
	}

	list, err := m.LoadProjectReplicaInfos(uuid)
	if err != nil {
		return nil, err
	}

	for _, dInfo := range list {
		dInfo.BundleURL = info.BundleURL
		node := m.nodeMgr.GetNode(dInfo.NodeID)
		if node == nil {
			continue
		}

		vNode := m.nodeMgr.GetNode(node.WSServerID)
		if vNode == nil {
			vNode, err = m.nodeMgr.UpdateTunserverURL(node.NodeID)
			if err != nil {
				log.Errorf("GetProjectInfo UpdateTunserverURL err:%s", err.Error())
				continue
			}
		}

		dInfo.WsURL = vNode.WsURL()
		dInfo.IP = node.ExternalIP
		dInfo.GeoID = node.AreaID
	}

	info.DetailsList = list

	return info, nil
}

// RestartDeployProjects restarts deploy projects
func (m *Manager) RestartDeployProjects(ids []string) error {
	for _, id := range ids {
		if exist, _ := m.projectStateMachines.Has(ProjectID(id)); !exist {
			continue
		}

		err := m.projectStateMachines.Send(ProjectID(id), ProjectRestart{})
		if err != nil {
			log.Errorf("RestartDeployProjects send err:%s", err.Error())
		}
	}

	return nil
}
