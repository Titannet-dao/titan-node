package scheduler

import (
	"context"
	"database/sql"
	"errors"

	"github.com/Filecoin-Titan/titan/api/types"
)

// import (
// 	"context"
// 	"net"

// 	"github.com/Filecoin-Titan/titan/api/types"
// 	"github.com/Filecoin-Titan/titan/node/handler"
// 	"github.com/pkg/errors"
// )

// const shellPath = "/deployment/shell"

// // ErrDeploymentNotFound is returned when a deployment cannot be found.
// var (
// 	ErrDeploymentNotFound = errors.New("deployment not found")
// 	ErrDomainAlreadyExist = errors.New("domain already exist")
// 	ErrInvalidDomain      = errors.New("invalid domain")
// 	ErrInvalidAnnotations = errors.New("invalid annotations")
// )

// // GetRemoteAddress returns the remote address from the context.
// func (s *Scheduler) GetRemoteAddress(ctx context.Context) (string, error) {
// 	remoteAddr := handler.GetRemoteAddr(ctx)
// 	return remoteAddr, nil
// }

// // GetStatistics retrieves the resource statistics for a given container ID.
// func (s *Scheduler) GetStatistics(ctx context.Context, id string) (*types.ResourcesStatistics, error) {
// 	return s.ContainerManager.GetStatistics(ctx, id)
// }

// // GetProviders retrieves a list of providers based on the given options.
// func (s *Scheduler) GetProviders(ctx context.Context, opt *types.GetProviderOption) ([]*types.Provider, error) {
// 	return s.ContainerManager.GetProviders(ctx, opt)
// }

// // GetProviderList retrieves the list of providers based on the given options.
// func (s *Scheduler) GetProviderList(ctx context.Context, opt *types.GetProviderOption) (*types.ProvidersResp, error) {
// 	return s.ContainerManager.GetProviderList(ctx, opt)
// }

// // GetDeploymentList retrieves the list of deployments based on the provided options.
// func (s *Scheduler) GetDeploymentList(ctx context.Context, opt *types.GetDeploymentOption) (*types.GetDeploymentListResp, error) {
// 	return s.ContainerManager.GetDeploymentList(ctx, opt)
// }

// // CreateDeployment creates a new deployment in the scheduler.
// func (s *Scheduler) CreateDeployment(ctx context.Context, deployment *types.Deployment) error {
// 	// fix expose ip
// 	return s.ContainerManager.CreateDeployment(ctx, deployment)
// }

// // UpdateDeployment updates the specified deployment in the scheduler.
// func (s *Scheduler) UpdateDeployment(ctx context.Context, deployment *types.Deployment) error {
// 	return s.ContainerManager.UpdateDeployment(ctx, deployment)
// }

// // CloseDeployment closes the specified deployment, optionally forcing the closure.
// func (s *Scheduler) CloseDeployment(ctx context.Context, deployment *types.Deployment, force bool) error {
// 	return s.ContainerManager.CloseDeployment(ctx, deployment, force)
// }

// // GetLogs retrieves the logs for a specified deployment.
// func (s *Scheduler) GetLogs(ctx context.Context, deployment *types.Deployment) ([]*types.ServiceLog, error) {
// 	return s.ContainerManager.GetLogs(ctx, deployment)
// }

// // GetEvents retrieves the service events for a given deployment.
// func (s *Scheduler) GetEvents(ctx context.Context, deployment *types.Deployment) ([]*types.ServiceEvent, error) {
// 	return s.ContainerManager.GetEvents(ctx, deployment)
// }

// // SetProperties sets the properties for the scheduler.
// func (s *Scheduler) SetProperties(ctx context.Context, properties *types.Properties) error {
// 	return s.ContainerManager.SetProperties(ctx, properties)
// }

// // StateInvalid indicates that the state is invalid.
// const (
// 	StateInvalid = "Invalid" // ...
// 	StateOk      = "OK"      // ...
// )

// // GetDeploymentDomains retrieves the deployment domains for a given deployment ID.
// func (s *Scheduler) GetDeploymentDomains(ctx context.Context, id types.DeploymentID) ([]*types.DeploymentDomain, error) {
// 	return s.ContainerManager.GetDeploymentDomains(ctx, id)
// }

// func includeIP(hostname string, expectedIP string) bool {
// 	ips, err := net.LookupHost(hostname)
// 	if err != nil {
// 		return false
// 	}

// 	for _, ip := range ips {
// 		if ip == expectedIP {
// 			return true
// 		}
// 	}

// 	return false
// }

// // AddDeploymentDomain adds a deployment domain to the scheduler.
// func (s *Scheduler) AddDeploymentDomain(ctx context.Context, id types.DeploymentID, cert *types.Certificate) error {
// 	return s.ContainerManager.AddDeploymentDomain(ctx, id, cert)
// }

// // DeleteDeploymentDomain removes the specified deployment domain for the given deployment ID.
// func (s *Scheduler) DeleteDeploymentDomain(ctx context.Context, id types.DeploymentID, domain string) error {
// 	return s.ContainerManager.DeleteDeploymentDomain(ctx, id, domain)
// }

// // GetLeaseShellEndpoint retrieves the lease shell endpoint for a given deployment ID.
// func (s *Scheduler) GetLeaseShellEndpoint(ctx context.Context, id types.DeploymentID) (*types.LeaseEndpoint, error) {
// 	return s.ContainerManager.GetLeaseShellEndpoint(ctx, id)
// }

// // GetIngress retrieves the Ingress for a given DeploymentID.
// func (s *Scheduler) GetIngress(ctx context.Context, id types.DeploymentID) (*types.Ingress, error) {
// 	return s.ContainerManager.GetIngress(ctx, id)
// }

// // UpdateIngress updates the ingress for a given deployment ID with the provided annotations.
// func (s *Scheduler) UpdateIngress(ctx context.Context, id types.DeploymentID, annotations map[string]string) error {
// 	return s.ContainerManager.UpdateIngress(ctx, id, annotations)
// }

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
