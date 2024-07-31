package scheduler

import (
	"context"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/pkg/errors"
	"net"
)

const shellPath = "/deployment/shell"

var (
	ErrDeploymentNotFound = errors.New("deployment not found")
	ErrDomainAlreadyExist = errors.New("domain already exist")
	ErrInvalidDomain      = errors.New("invalid domain")
	ErrInvalidAnnotations = errors.New("invalid annotations")
)

func (s *Scheduler) GetRemoteAddress(ctx context.Context) (string, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	return remoteAddr, nil
}

func (s *Scheduler) GetStatistics(ctx context.Context, id string) (*types.ResourcesStatistics, error) {
	return s.ContainerManager.GetStatistics(ctx, id)
}

func (s *Scheduler) GetProviderList(ctx context.Context, opt *types.GetProviderOption) ([]*types.Provider, error) {
	return s.ContainerManager.GetProviderList(ctx, opt)
}

func (s *Scheduler) GetDeploymentList(ctx context.Context, opt *types.GetDeploymentOption) (*types.GetDeploymentListResp, error) {
	return s.ContainerManager.GetDeploymentList(ctx, opt)
}

func (s *Scheduler) CreateDeployment(ctx context.Context, deployment *types.Deployment) error {
	// fix expose ip
	return s.ContainerManager.CreateDeployment(ctx, deployment)
}

func (s *Scheduler) UpdateDeployment(ctx context.Context, deployment *types.Deployment) error {
	return s.ContainerManager.UpdateDeployment(ctx, deployment)
}

func (s *Scheduler) CloseDeployment(ctx context.Context, deployment *types.Deployment, force bool) error {
	return s.ContainerManager.CloseDeployment(ctx, deployment, force)
}

func (s *Scheduler) GetLogs(ctx context.Context, deployment *types.Deployment) ([]*types.ServiceLog, error) {
	return s.ContainerManager.GetLogs(ctx, deployment)
}

func (s *Scheduler) GetEvents(ctx context.Context, deployment *types.Deployment) ([]*types.ServiceEvent, error) {
	return s.ContainerManager.GetEvents(ctx, deployment)
}

func (s *Scheduler) SetProperties(ctx context.Context, properties *types.Properties) error {
	return s.ContainerManager.SetProperties(ctx, properties)
}

const (
	StateInvalid = "Invalid"
	StateOk      = "OK"
)

func (s *Scheduler) GetDeploymentDomains(ctx context.Context, id types.DeploymentID) ([]*types.DeploymentDomain, error) {
	return s.ContainerManager.GetDeploymentDomains(ctx, id)
}

func includeIP(hostname string, expectedIP string) bool {
	ips, err := net.LookupHost(hostname)
	if err != nil {
		return false
	}

	for _, ip := range ips {
		if ip == expectedIP {
			return true
		}
	}

	return false
}

func (s *Scheduler) AddDeploymentDomain(ctx context.Context, id types.DeploymentID, cert *types.Certificate) error {
	return s.ContainerManager.AddDeploymentDomain(ctx, id, cert)
}

func (s *Scheduler) DeleteDeploymentDomain(ctx context.Context, id types.DeploymentID, domain string) error {
	return s.ContainerManager.DeleteDeploymentDomain(ctx, id, domain)
}

func (s *Scheduler) GetLeaseShellEndpoint(ctx context.Context, id types.DeploymentID) (*types.LeaseEndpoint, error) {
	return s.ContainerManager.GetLeaseShellEndpoint(ctx, id)
}

func (s *Scheduler) GetIngress(ctx context.Context, id types.DeploymentID) (*types.Ingress, error) {
	return s.ContainerManager.GetIngress(ctx, id)
}

func (s *Scheduler) UpdateIngress(ctx context.Context, id types.DeploymentID, annotations map[string]string) error {
	return s.ContainerManager.UpdateIngress(ctx, id, annotations)
}

func (s *Scheduler) GetDeploymentProviderIP(ctx context.Context, id types.DeploymentID) (string, error) {
	return s.ContainerManager.GetDeploymentProviderIP(ctx, id)
}
