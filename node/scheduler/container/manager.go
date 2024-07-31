package container

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/filecoin-project/pubsub"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/pkg/errors"
	"net"
	"net/url"
	"time"
)

var log = logging.Logger("manager")

// POST /lease/<lease-id>/shell
const leaseBashPath = "/lease"

var (
	ErrOfflineNode          = errors.New("node offline")
	ErrDeploymentNotFound   = errors.New("deployment not found")
	ErrDomainAlreadyExist   = errors.New("domain already exist")
	ErrPermissionNotAllowed = errors.New("permission not allowed")
	ErrInvalidDomain        = errors.New("invalid domain")
	ErrInvalidAnnotations   = errors.New("invalid annotations")
)

// Manager represents a manager service in a cloud computing system.
type Manager struct {
	nodeMgr *node.Manager
	DB      *db.SQLDB
	notify  *pubsub.PubSub
}

func NewManager(nm *node.Manager, db *db.SQLDB, notify *pubsub.PubSub) *Manager {
	return &Manager{nodeMgr: nm, DB: db, notify: notify}
}

func (m *Manager) AddNewProvider(ctx context.Context, provider *types.Provider) error {
	return m.DB.AddNewProvider(ctx, provider)
}

func (m *Manager) GetRemoteAddress(ctx context.Context) (string, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	return remoteAddr, nil
}

func (m *Manager) GetStatistics(ctx context.Context, id string) (*types.ResourcesStatistics, error) {
	node := m.nodeMgr.GetNode(id)
	if node == nil {
		return nil, ErrOfflineNode
	}

	return node.GetStatistics(ctx)
}

func (m *Manager) GetProviderList(ctx context.Context, opt *types.GetProviderOption) ([]*types.Provider, error) {
	return m.DB.GetAllProviders(ctx, opt)
}

func (m *Manager) GetDeploymentList(ctx context.Context, opt *types.GetDeploymentOption) (*types.GetDeploymentListResp, error) {
	total, deployments, err := m.DB.GetDeployments(ctx, opt)
	if err != nil {
		return nil, err
	}

	for _, deployment := range deployments {
		providerApi := m.nodeMgr.GetNode(opt.ProviderID)
		if providerApi == nil {
			continue
		}

		remoteDeployment, err := providerApi.GetDeployment(ctx, deployment.ID)
		if err != nil {
			continue
		}

		deployment.Services = remoteDeployment.Services
	}

	return &types.GetDeploymentListResp{
		Deployments: deployments,
		Total:       total,
	}, nil
}

func (m *Manager) CreateDeployment(ctx context.Context, deployment *types.Deployment) error {
	providerApi := m.nodeMgr.GetNode(deployment.ProviderID)
	if providerApi == nil {
		return ErrOfflineNode
	}

	// TODO: authority validation
	deployment.ProviderExposeIP = providerApi.ExternalIP
	deployment.ID = types.DeploymentID(uuid.New().String())
	deployment.State = types.DeploymentStateActive
	deployment.CreatedAt = time.Now()
	deployment.UpdatedAt = time.Now()
	if deployment.Expiration.IsZero() {
		deployment.Expiration = time.Now().AddDate(0, 1, 0)
	}

	err := providerApi.CreateDeployment(ctx, deployment)
	if err != nil {
		return err
	}

	successDeployment, err := providerApi.GetDeployment(ctx, deployment.ID)
	if err != nil {
		return err
	}

	deployment.Services = successDeployment.Services
	for _, service := range deployment.Services {
		service.DeploymentID = deployment.ID
		service.CreatedAt = time.Now()
		service.UpdatedAt = time.Now()
	}

	err = m.DB.CreateDeployment(ctx, deployment)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) UpdateDeployment(ctx context.Context, deployment *types.Deployment) error {
	deploy, err := m.DB.GetDeploymentById(ctx, deployment.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("deployment not found")
	}

	if err != nil {
		return err
	}

	if deploy.Owner != deployment.Owner {
		return errors.Errorf("update operation not allow")
	}

	deployment.ProviderID = deploy.ProviderID
	//providerApi, err := m.ProviderManager.Get(deployment.ProviderID)
	//if err != nil {
	//	return err
	//}
	//
	//providerExternalIP, err := m.ProviderManager.GetRemoteAddr(deployment.ProviderID)
	//if err != nil {
	//	return err
	//}

	//deployment.ProviderExposeIP = providerExternalIP

	if len(deployment.Name) == 0 {
		deployment.Name = deploy.Name
	}

	if deployment.Expiration.IsZero() {
		deployment.Expiration = deploy.Expiration
	}

	deployment.CreatedAt = deploy.CreatedAt
	deployment.UpdatedAt = time.Now()

	for _, service := range deployment.Services {
		service.DeploymentID = deployment.ID
		service.CreatedAt = time.Now()
		service.UpdatedAt = time.Now()
	}

	providerApi := m.nodeMgr.GetNode(deployment.ProviderID)
	if providerApi == nil {
		return ErrOfflineNode
	}

	err = m.DB.CreateDeployment(ctx, deployment)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) CloseDeployment(ctx context.Context, deployment *types.Deployment, force bool) error {
	remoteClose := func() error {
		deploy, err := m.DB.GetDeploymentById(ctx, deployment.ID)
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("deployment not found")
		}

		if err != nil {
			return err
		}

		if deploy.Owner != deployment.Owner {
			return errors.Errorf("delete operation not allow")
		}

		providerApi := m.nodeMgr.GetNode(deploy.ProviderID)
		if providerApi == nil {
			return ErrOfflineNode
		}

		err = providerApi.CloseDeployment(ctx, deploy)
		if err != nil {
			return err
		}

		return nil
	}

	if err := remoteClose(); err != nil && !force {
		return err
	}

	return m.DB.DeleteDeployment(ctx, deployment.ID)
}

func (m *Manager) GetLogs(ctx context.Context, deployment *types.Deployment) ([]*types.ServiceLog, error) {
	deploy, err := m.DB.GetDeploymentById(ctx, deployment.ID)
	if errors.As(err, sql.ErrNoRows) {
		return nil, ErrDeploymentNotFound
	}

	if err != nil {
		return nil, err
	}

	providerApi := m.nodeMgr.GetNode(deploy.ProviderID)
	if providerApi == nil {
		return nil, ErrOfflineNode
	}

	serverLogs, err := providerApi.GetLogs(ctx, deployment.ID)
	if err != nil {
		return nil, err
	}

	for _, sl := range serverLogs {
		if len(sl.Logs) > 300 {
			sl.Logs = sl.Logs[len(sl.Logs)-300:]
		}
	}

	return serverLogs, nil
}

func (m *Manager) GetEvents(ctx context.Context, deployment *types.Deployment) ([]*types.ServiceEvent, error) {
	deploy, err := m.DB.GetDeploymentById(ctx, deployment.ID)
	if errors.As(err, sql.ErrNoRows) {
		return nil, ErrDeploymentNotFound
	}

	if err != nil {
		return nil, err
	}

	providerApi := m.nodeMgr.GetNode(deploy.ProviderID)
	if providerApi == nil {
		return nil, ErrOfflineNode
	}

	return providerApi.GetEvents(ctx, deployment.ID)
}

func (m *Manager) SetProperties(ctx context.Context, properties *types.Properties) error {
	providerApi := m.nodeMgr.GetNode(properties.ProviderID)
	if providerApi == nil {
		return ErrOfflineNode
	}

	properties.CreatedAt = time.Now()
	properties.UpdatedAt = time.Now()
	return m.DB.AddProperties(ctx, properties)
}

const (
	StateInvalid = "Invalid"
	StateOk      = "OK"
)

func (m *Manager) GetDeploymentDomains(ctx context.Context, id types.DeploymentID) ([]*types.DeploymentDomain, error) {
	deploy, err := m.DB.GetDeploymentById(ctx, id)
	if errors.As(err, sql.ErrNoRows) {
		return nil, ErrDeploymentNotFound
	}

	if err != nil {
		return nil, err
	}

	providerApi := m.nodeMgr.GetNode(deploy.ProviderID)
	if providerApi == nil {
		return nil, ErrOfflineNode
	}

	provider, err := m.DB.GetProviderById(ctx, deploy.ProviderID)
	if err != nil {
		return nil, err
	}

	domains, err := providerApi.GetDomains(ctx, deploy.ID)
	if err != nil {
		return nil, err
	}

	for _, domain := range domains {
		if includeIP(domain.Name, provider.IP) {
			domain.State = StateOk
		} else {
			domain.State = StateInvalid
		}
	}

	return domains, nil
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

func (m *Manager) AddDeploymentDomain(ctx context.Context, id types.DeploymentID, cert *types.Certificate) error {
	if cert.Hostname == "" {
		return ErrInvalidDomain
	}

	domain, err := m.DB.GetDomain(ctx, cert.Hostname)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if domain != nil && domain.DeploymentID == string(id) {
		return ErrDomainAlreadyExist
	}

	deploy, err := m.DB.GetDeploymentById(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrDeploymentNotFound
	}

	if err != nil {
		return err
	}

	providerApi := m.nodeMgr.GetNode(deploy.ProviderID)
	if providerApi == nil {
		return ErrOfflineNode
	}

	err = providerApi.AddDomain(ctx, deploy.ID, cert)
	if err != nil {
		return err
	}

	return m.DB.AddDomain(ctx, &types.DeploymentDomain{
		DeploymentID: string(id),
		Name:         cert.Hostname,
		ProviderID:   deploy.ProviderID,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	})
}

func (m *Manager) DeleteDeploymentDomain(ctx context.Context, id types.DeploymentID, domain string) error {
	deploy, err := m.DB.GetDeploymentById(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrDeploymentNotFound
	}

	if err != nil {
		return err
	}

	providerApi := m.nodeMgr.GetNode(deploy.ProviderID)
	if providerApi == nil {
		return ErrOfflineNode
	}

	err = providerApi.DeleteDomain(ctx, deploy.ID, domain)
	if err != nil {
		return err
	}

	err = m.DB.DeleteDomain(ctx, domain)
	if err != nil {
		log.Errorf("delete domain: %v", err)
	}

	return nil
}

func (m *Manager) GetLeaseShellEndpoint(ctx context.Context, id types.DeploymentID) (*types.LeaseEndpoint, error) {
	deploy, err := m.DB.GetDeploymentById(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrDeploymentNotFound
	}

	if err != nil {
		return nil, err
	}

	providerApi := m.nodeMgr.GetNode(deploy.ProviderID)
	if providerApi == nil {
		return nil, ErrOfflineNode
	}

	token, err := providerApi.GetLeaseAccessToken(ctx, deploy.ID, deploy.Owner)
	if err != nil {
		return nil, err
	}

	address, err := url.Parse(deploy.ProviderExposeIP)
	if err != nil {
		return nil, err
	}

	endpoint := &types.LeaseEndpoint{
		Scheme:    address.Scheme,
		Host:      address.Host,
		ShellPath: fmt.Sprintf("%s/%s/shell", leaseBashPath, deploy.ID),
		Token:     token,
	}

	return endpoint, nil
}

func (m *Manager) GetIngress(ctx context.Context, id types.DeploymentID) (*types.Ingress, error) {
	deploy, err := m.DB.GetDeploymentById(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrDeploymentNotFound
	}

	if err != nil {
		return nil, err
	}

	providerApi := m.nodeMgr.GetNode(deploy.ProviderID)
	if providerApi == nil {
		return nil, ErrOfflineNode
	}

	ingress, err := providerApi.GetIngress(ctx, deploy.ID)
	if err != nil {
		return nil, err
	}

	return ingress, nil
}

func (m *Manager) UpdateIngress(ctx context.Context, id types.DeploymentID, annotations map[string]string) error {
	if len(annotations) == 0 {
		return ErrInvalidAnnotations
	}

	deploy, err := m.DB.GetDeploymentById(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrDeploymentNotFound
	}

	if err != nil {
		return err
	}

	providerApi := m.nodeMgr.GetNode(deploy.ProviderID)
	if providerApi == nil {
		return ErrOfflineNode
	}

	return providerApi.UpdateIngress(ctx, id, annotations)
}

func (m *Manager) GenerateTokenForLeaseShell(commonAPI api.Common, deploy *types.Deployment) (string, error) {
	buf, err := json.Marshal(deploy)
	if err != nil {
		return "", err
	}

	payload := types.JWTPayload{Extend: string(buf)}
	tk, err := commonAPI.AuthNew(context.Background(), &payload)
	if err != nil {
		return "", err
	}

	return tk, nil
}

func (m *Manager) GetDeploymentProviderIP(ctx context.Context, id types.DeploymentID) (string, error) {
	deploy, err := m.DB.GetDeploymentById(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrDeploymentNotFound
	}

	if err != nil {
		return "", err
	}

	providerApi := m.nodeMgr.GetNode(deploy.ProviderID)
	if providerApi == nil {
		return "", ErrOfflineNode
	}

	return providerApi.ExternalIP, nil
}

func (m *Manager) ListenNodeState(ctx context.Context) {
	sub := m.notify.Sub(types.EventNodeOffline.String())

	for {
		select {
		case msg := <-sub:
			nodeInfo := msg.(*node.Node)

			if nodeInfo.Type != types.NodeCandidate {
				continue
			}

			log.Infof("node offline notification: %s", nodeInfo.NodeID)

			err := m.DB.UpdateProviderState(context.Background(), nodeInfo.NodeID, types.ProviderStateOffline)
			if err != nil {
				log.Errorf("update proivder state: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}
