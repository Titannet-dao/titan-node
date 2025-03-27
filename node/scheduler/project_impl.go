package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/handler"
	"golang.org/x/xerrors"
)

// RedeployFailedProjects retries the deploy process for a list of failed projects
func (s *Scheduler) RedeployFailedProjects(ctx context.Context, ids []string) error {
	return s.ProjectManager.RestartDeployProjects(ids)
}

// UpdateProjectStatus updates the status of the projects in the scheduler.
func (s *Scheduler) UpdateProjectStatus(ctx context.Context, list []*types.Project) error {
	nodeID := handler.GetNodeID(ctx)
	if len(nodeID) == 0 {
		return fmt.Errorf("invalid request")
	}

	return s.ProjectManager.UpdateStatus(nodeID, list)
}

// DeployProject deploys a project based on the provided request.
func (s *Scheduler) DeployProject(ctx context.Context, req *types.DeployProjectReq) error {
	if req.UUID == "" {
		return xerrors.New("UUID is nil")
	}

	if req.BundleURL == "" {
		return xerrors.New("BundleURL is nil")
	}

	if req.Replicas <= 0 {
		return xerrors.New("Replicas is 0")
	}

	if req.Expiration.Before(time.Now()) {
		return xerrors.Errorf("Expiration %s is before now", req.Expiration.String())
	}

	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		req.UserID = uID
	}

	return s.ProjectManager.Deploy(req)
}

// StartProject starts a project based on the provided request.
func (s *Scheduler) StartProject(ctx context.Context, req *types.ProjectReq) error {
	return nil
}

// DeleteProject removes a project based on the provided request.
func (s *Scheduler) DeleteProject(ctx context.Context, req *types.ProjectReq) error {
	return s.ProjectManager.Delete(req)
}

// UpdateProject updates a project with the given request.
func (s *Scheduler) UpdateProject(ctx context.Context, req *types.ProjectReq) error {
	if req.BundleURL == "" {
		return xerrors.New("BundleURL is nil")
	}

	if req.Replicas <= 0 {
		return xerrors.New("Replicas is 0")
	}

	if req.UUID == "" {
		return xerrors.New("UUID is nil")
	}

	return s.ProjectManager.Update(req)
}

// GetProjectInfo retrieves project information by UUID.
func (s *Scheduler) GetProjectInfo(ctx context.Context, uuid string) (*types.ProjectInfo, error) {
	return s.ProjectManager.GetProjectInfo(uuid)
}

// GetProjectInfos retrieves project information for a user with specified limits.
func (s *Scheduler) GetProjectInfos(ctx context.Context, userID string, limit, offset int) ([]*types.ProjectInfo, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	infos, err := s.db.LoadProjectInfos(s.ServerID, userID, limit, offset)
	if err != nil {
		return nil, err
	}

	for _, pInfo := range infos {
		list, err := s.db.LoadProjectReplicaInfos(pInfo.UUID)
		if err != nil {
			return nil, err
		}

		for _, dInfo := range list {
			dInfo.BundleURL = pInfo.BundleURL
			node := s.NodeManager.GetNode(dInfo.NodeID)
			if node == nil {
				continue
			}

			vNode := s.NodeManager.GetNode(node.WSServerID)
			if vNode == nil {
				continue
			}

			dInfo.WsURL = vNode.WsURL()
			dInfo.IP = node.ExternalIP
			dInfo.GeoID = node.AreaID
		}

		pInfo.DetailsList = list
	}

	return infos, nil
}

// GetProjectOverviewByNode retrieves the project overview for a specific node.
func (s *Scheduler) GetProjectOverviewByNode(ctx context.Context, req *types.NodeProjectReq) (*types.ListProjectOverviewRsp, error) {
	if req.Offset < 0 {
		req.Offset = 0
	}

	list, err := s.db.LoadProjectOverviews()
	if err != nil {
		return nil, err
	}

	if req.NodeID != "" {
		eList := make([]*types.ProjectOverview, 0)

		for _, info := range list {
			if info.NodeID == req.NodeID {
				eList = append(eList, info)
			}
		}

		list = eList
	}

	size := len(list)
	out := &types.ListProjectOverviewRsp{
		Total: size,
	}

	if req.Offset >= size {
		return out, nil
	}

	end := req.Offset + req.Limit
	if end >= size {
		end = size
	}

	out.List = list[req.Offset:end]

	return out, nil
}
