package api

import (
	"context"

	"github.com/Filecoin-Titan/titan/api/types"
)

// ContainerAPI is an interface for node
type ContainerAPI interface {
	GetRemoteAddress(ctx context.Context) (string, error)                                                        //perm:admin
	GetStatistics(ctx context.Context, id string) (*types.ResourcesStatistics, error)                            //perm:admin
	GetProviderList(ctx context.Context, option *types.GetProviderOption) ([]*types.Provider, error)             //perm:admin
	GetDeploymentList(ctx context.Context, opt *types.GetDeploymentOption) (*types.GetDeploymentListResp, error) //perm:candidate
	GetDeploymentProviderIP(ctx context.Context, id types.DeploymentID) (string, error)                          //perm:edge,candidate,web,locator
	CreateDeployment(ctx context.Context, deployment *types.Deployment) error                                    //perm:admin
	UpdateDeployment(ctx context.Context, deployment *types.Deployment) error                                    //perm:admin
	CloseDeployment(ctx context.Context, deployment *types.Deployment, force bool) error                         //perm:admin
	GetLogs(ctx context.Context, deployment *types.Deployment) ([]*types.ServiceLog, error)                      //perm:admin
	GetEvents(ctx context.Context, deployment *types.Deployment) ([]*types.ServiceEvent, error)                  //perm:admin
	SetProperties(ctx context.Context, properties *types.Properties) error                                       //perm:admin
	GetDeploymentDomains(ctx context.Context, id types.DeploymentID) ([]*types.DeploymentDomain, error)          //perm:admin
	AddDeploymentDomain(ctx context.Context, id types.DeploymentID, cert *types.Certificate) error               //perm:admin
	DeleteDeploymentDomain(ctx context.Context, id types.DeploymentID, domain string) error                      //perm:admin
	GetLeaseShellEndpoint(ctx context.Context, id types.DeploymentID) (*types.LeaseEndpoint, error)              //perm:admin
	GetIngress(ctx context.Context, id types.DeploymentID) (*types.Ingress, error)                               //perm:admin
	UpdateIngress(ctx context.Context, id types.DeploymentID, annotations map[string]string) error               //perm:admin
}
