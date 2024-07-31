package api

import (
	"context"

	"github.com/Filecoin-Titan/titan/api/types"
)

type ProviderAPI interface {
	GetLeaseAccessToken(ctx context.Context, id types.DeploymentID, userId string) (string, error)                                 //perm:admin
	GetStatistics(ctx context.Context) (*types.ResourcesStatistics, error)                                                         //perm:admin
	GetDeployment(ctx context.Context, id types.DeploymentID) (*types.Deployment, error)                                           //perm:admin
	CreateDeployment(ctx context.Context, deployment *types.Deployment) error                                                      //perm:admin
	UpdateDeployment(ctx context.Context, deployment *types.Deployment) error                                                      //perm:admin
	CloseDeployment(ctx context.Context, deployment *types.Deployment) error                                                       //perm:admin
	GetLogs(ctx context.Context, id types.DeploymentID) ([]*types.ServiceLog, error)                                               //perm:admin
	GetEvents(ctx context.Context, id types.DeploymentID) ([]*types.ServiceEvent, error)                                           //perm:admin
	GetDomains(ctx context.Context, id types.DeploymentID) ([]*types.DeploymentDomain, error)                                      //perm:admin
	AddDomain(ctx context.Context, id types.DeploymentID, cert *types.Certificate) error                                           //perm:admin
	DeleteDomain(ctx context.Context, id types.DeploymentID, hostname string) error                                                //perm:admin
	GetSufficientResourceNodes(ctx context.Context, reqResources *types.ComputeResources) ([]*types.SufficientResourceNode, error) //perm:admin
	GetIngress(ctx context.Context, id types.DeploymentID) (*types.Ingress, error)                                                 //perm:admin
	UpdateIngress(ctx context.Context, id types.DeploymentID, annotations map[string]string) error                                 //perm:admin
}
