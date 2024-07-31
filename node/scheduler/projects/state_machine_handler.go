package projects

import (
	"context"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/filecoin-project/go-statemachine"
	xerrors "golang.org/x/xerrors"
)

var (
	// MinRetryTime defines the minimum time duration between retries
	MinRetryTime = 30 * time.Second

	// MaxRetryCount defines the maximum number of retries allowed
	MaxRetryCount = 1
)

// failedCoolDown is called when a retry needs to be attempted and waits for the specified time duration
func failedCoolDown(ctx statemachine.Context, info ProjectInfo, t time.Duration) error {
	retryStart := time.Now().Add(t)
	if time.Now().Before(retryStart) {
		log.Debugf("%s(%s), waiting %s before retrying", info.State, info.UUID, time.Until(retryStart))
		select {
		case <-time.After(time.Until(retryStart)):
		case <-ctx.Context().Done():
			return ctx.Context().Err()
		}
	}

	return nil
}

// handleNodeSelect handles the selection nodes for project
func (m *Manager) handleNodeSelect(ctx statemachine.Context, info ProjectInfo) error {
	log.Debugf("handle Select : %s", info.UUID)

	curCount := int64(len(info.EdgeReplicaSucceeds))

	filterMap := make(map[string]struct{})
	for _, nodeID := range info.EdgeReplicaSucceeds {
		filterMap[nodeID] = struct{}{}
	}

	if info.NodeIDs != nil && len(info.NodeIDs) > 0 {
		for _, nodeID := range info.NodeIDs {
			node := m.nodeMgr.GetEdgeNode(nodeID)
			if node == nil {
				continue
			}

			if _, exist := filterMap[node.NodeID]; exist {
				continue
			}

			node.BackProjectTime = time.Now().Unix()
			status := types.ProjectReplicaStatusStarting

			err := node.Deploy(context.Background(), &types.Project{ID: info.UUID.String(), Name: info.Name, BundleURL: info.BundleURL})
			if err != nil {
				log.Errorf("DeployProject Deploy %s err:%s", node.NodeID, err.Error())
				status = types.ProjectReplicaStatusError
			}

			err = m.SaveProjectReplicasInfo(&types.ProjectReplicas{
				Id:     info.UUID.String(),
				NodeID: node.NodeID,
				Status: status,
			})
			if err != nil {
				log.Errorf("DeployProject SaveWorkerdDetailsInfo %s err:%s", node.NodeID, err.Error())
				continue
			}

			if status == types.ProjectReplicaStatusStarting {
				curCount++
			}
		}
	} else {
		// select nodes
		needCount := int(info.Replicas) - len(info.EdgeReplicaSucceeds)
		if needCount <= 0 {
			return ctx.Send(SkipStep{})
		}

		list := m.chooseNodes(needCount, filterMap, info)
		for _, node := range list {
			node.BackProjectTime = time.Now().Unix()
			status := types.ProjectReplicaStatusStarting

			// node.API.d
			err := node.Deploy(context.Background(), &types.Project{ID: info.UUID.String(), Name: info.Name, BundleURL: info.BundleURL})
			if err != nil {
				log.Errorf("DeployProject Deploy %s err:%s", node.NodeID, err.Error())
				status = types.ProjectReplicaStatusError
			}

			err = m.SaveProjectReplicasInfo(&types.ProjectReplicas{
				Id:     info.UUID.String(),
				NodeID: node.NodeID,
				Status: status,
			})
			if err != nil {
				log.Errorf("DeployProject SaveWorkerdDetailsInfo %s err:%s", node.NodeID, err.Error())
				continue
			}

			if status == types.ProjectReplicaStatusStarting {
				curCount++
			}
		}
	}

	if curCount == 0 {
		return ctx.Send(CreateFailed{error: xerrors.New("node not found; ")})
	}

	m.startProjectTimeoutCounting(info.UUID.String(), 0)

	return ctx.Send(DeployRequestSent{})
}

// handleUpdate handles the selection nodes for project
func (m *Manager) handleUpdate(ctx statemachine.Context, info ProjectInfo) error {
	log.Debugf("handle Update : %s", info.UUID)

	curCount := int64(0)

	// select nodes
	for _, nodeID := range info.EdgeReplicaSucceeds {
		status := types.ProjectReplicaStatusError

		node := m.nodeMgr.GetNode(nodeID)
		if node != nil {
			status = types.ProjectReplicaStatusStarting
			err := node.Update(context.Background(), &types.Project{ID: info.UUID.String(), BundleURL: info.BundleURL, Name: info.Name})
			if err != nil {
				log.Errorf("Update %s err:%s", nodeID, err.Error())
				status = types.ProjectReplicaStatusError
			}
		}

		err := m.SaveProjectReplicasInfo(&types.ProjectReplicas{
			Id:     info.UUID.String(),
			NodeID: node.NodeID,
			Status: status,
		})
		if err != nil {
			log.Errorf("DeployProject SaveWorkerdDetailsInfo %s err:%s", node.NodeID, err.Error())
			continue
		}

		if status == types.ProjectReplicaStatusStarting {
			curCount++
		}
	}

	if curCount == 0 {
		return ctx.Send(UpdateFailed{error: xerrors.New("node not found; ")})
	}

	m.startProjectTimeoutCounting(info.UUID.String(), 0)

	return ctx.Send(UpdateRequestSent{})
}

// handleDeploying handles the project deploying process of seed nodes
func (m *Manager) handleDeploying(ctx statemachine.Context, info ProjectInfo) error {
	log.Debugf("handle deploying, %s", info.UUID)

	if info.EdgeWaitings > 0 {
		return nil
	}

	if int64(len(info.EdgeReplicaSucceeds)) >= info.Replicas {
		return ctx.Send(DeploySucceed{})
	}

	if info.EdgeWaitings == 0 {
		return ctx.Send(DeployFailed{error: xerrors.New("node deploying failed")})
	}

	return nil
}

// handleServicing project pull completed and in service status
func (m *Manager) handleServicing(ctx statemachine.Context, info ProjectInfo) error {
	log.Infof("handle servicing: %s", info.UUID)
	m.stopProjectTimeoutCounting(info.UUID.String())

	// remove fail replicas
	m.DeleteUnfinishedProjectReplicas(info.UUID.String())
	return nil
}

// handleDeploysFailed handles the failed state of project pulling and retries if necessary
func (m *Manager) handleDeploysFailed(ctx statemachine.Context, info ProjectInfo) error {
	m.stopProjectTimeoutCounting(info.UUID.String())

	// remove fail replicas
	m.DeleteUnfinishedProjectReplicas(info.UUID.String())

	if info.RetryCount >= int64(MaxRetryCount) {
		log.Infof("handle pulls failed: %s, retry count: %d", info.UUID, info.RetryCount)

		return nil
	}

	log.Debugf(": %s, retries: %d", info.UUID, info.RetryCount)

	if err := failedCoolDown(ctx, info, MinRetryTime); err != nil {
		return err
	}

	return ctx.Send(ProjectRedeploy{})
}

func (m *Manager) handleRemove(ctx statemachine.Context, info ProjectInfo) error {
	// log.Infof("handle remove: %s", info.Hash)
	m.stopProjectTimeoutCounting(info.UUID.String())

	list, err := m.LoadProjectReplicaInfos(info.UUID.String())
	if err != nil {
		return err
	}

	for _, replica := range list {
		// request nodes
		err = m.removeReplica(replica.Id, replica.NodeID, types.ProjectEvent(info.Event))
		if err != nil {
			log.Errorf("handleRemove %s , %s removeReplica err:%s", replica.Id, replica.NodeID, err.Error())
		}
	}

	return m.DeleteProjectInfo(m.nodeMgr.ServerID, info.UUID.String())
}
