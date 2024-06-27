package api

import (
	"context"

	"github.com/Filecoin-Titan/titan/api/types"
)

type Workerd interface {
	// Deploy deploy and run a project on workerd
	Deploy(ctx context.Context, project *types.Project) error //perm:admin
	// Update update the project on workerd
	Update(ctx context.Context, project *types.Project) error //perm:admin
	// Delete remove the project on workerd
	Delete(ctx context.Context, projectID string) error //perm:admin
	// Query projects
	Query(ctx context.Context, ids []string) ([]*types.Project, error) //perm:admin
}
