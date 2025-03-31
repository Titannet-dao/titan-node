package scheduler

import (
	"context"
	"database/sql"
	"errors"

	"github.com/Filecoin-Titan/titan/api/types"
)

// GetDeploymentProviderIP retrieves the provider IP for a given deployment ID.
func (s *Scheduler) GetDeploymentProviderIP(ctx context.Context, id types.DeploymentID) (string, error) {
	// return s.ContainerManager.GetDeploymentProviderIP(ctx, id)
	deploy, err := s.db.GetDeploymentByID(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrDeploymentNotFound
	}

	if err != nil {
		return "", err
	}

	providerAPI := s.NodeManager.GetNode(deploy.ProviderID)
	if providerAPI == nil {
		return "", ErrOfflineNode
	}

	return providerAPI.ExternalIP, nil
}

var (
	ErrOfflineNode          = errors.New("node offline")
	ErrDeploymentNotFound   = errors.New("deployment not found")
	ErrDomainAlreadyExist   = errors.New("domain already exist")
	ErrPermissionNotAllowed = errors.New("permission not allowed")
	ErrInvalidDomain        = errors.New("invalid domain")
	ErrInvalidAnnotations   = errors.New("invalid annotations")
)
