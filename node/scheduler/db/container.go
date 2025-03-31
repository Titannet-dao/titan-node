package db

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/Filecoin-Titan/titan/api/types"
)

// DeploymentService deployment
type DeploymentService struct {
	types.Deployment
	types.Service `db:"service"`
}

// GetDeployments retrieves deployments and their related service information based on filtering options, with pagination support.
func (m *SQLDB) GetDeployments(ctx context.Context, option *types.GetDeploymentOption) (int64, []*types.Deployment, error) {
	var ds []*DeploymentService
	qry := `SELECT d.*,
       		s.image as 'service.image',
			s.name as 'service.name',
			s.cpu as 'service.cpu',
			s.gpu as 'service.gpu',
			s.memory as 'service.memory',
			s.storage as 'service.storage',
			s.ports as 'service.ports',
			s.env as 'service.env',
			s.arguments as 'service.arguments',
			s.error_message  as 'service.error_message',
			s.replicas as 'service.replicas',
			p.remote_addr as 'provider_expose_ip'
		FROM (%s) as d LEFT JOIN services s ON d.id = s.deployment_id LEFT JOIN providers p ON d.provider_id = p.id`

	subQry := `SELECT * from deployments`
	countQry := `SELECT count(1) from deployments`

	var condition []string
	if option.DeploymentID != "" {
		condition = append(condition, fmt.Sprintf(`id = '%s'`, option.DeploymentID))
	}

	if option.Owner != "" {
		condition = append(condition, fmt.Sprintf(`owner = '%s'`, option.Owner))
	}

	if option.ProviderID != "" {
		condition = append(condition, fmt.Sprintf(`provider_id = '%s'`, option.ProviderID))
	}

	if len(option.State) > 0 {
		var states []string
		for _, s := range option.State {
			states = append(states, strconv.Itoa(int(s)))
		}
		condition = append(condition, fmt.Sprintf(`state in (%s)`, strings.Join(states, ",")))
	} else {
		condition = append(condition, fmt.Sprintf(`state <> 3`))
	}

	if len(condition) > 0 {
		subQry += ` WHERE `
		subQry += strings.Join(condition, ` AND `)

		countQry += ` WHERE `
		countQry += strings.Join(condition, ` AND `)
	}

	var total int64
	err := m.db.GetContext(ctx, &total, countQry)
	if err != nil {
		return 0, nil, err
	}

	if option.Page <= 0 {
		option.Page = 1
	}

	if option.Size <= 0 {
		option.Size = 10
	}

	offset := (option.Page - 1) * option.Size
	limit := option.Size
	subQry += fmt.Sprintf(" ORDER BY created_at DESC LIMIT %d OFFSET %d", limit, offset)

	qry = fmt.Sprintf(qry, subQry)

	err = m.db.SelectContext(ctx, &ds, qry)
	if err != nil {
		return 0, nil, err
	}

	var out []*types.Deployment
	deploymentToServices := make(map[types.DeploymentID]*types.Deployment)
	for _, d := range ds {
		_, ok := deploymentToServices[d.Deployment.ID]
		if !ok {
			deploymentToServices[d.Deployment.ID] = &d.Deployment
			deploymentToServices[d.Deployment.ID].Services = make([]*types.Service, 0)
			out = append(out, deploymentToServices[d.Deployment.ID])
		}
		deploymentToServices[d.Deployment.ID].Services = append(deploymentToServices[d.Deployment.ID].Services, &d.Service)
	}

	return total, out, nil
}

// GetDeploymentByID fetches a single deployment by its ID, leveraging the more general GetDeployments function for consistency.
func (m *SQLDB) GetDeploymentByID(ctx context.Context, id types.DeploymentID) (*types.Deployment, error) {
	_, out, err := m.GetDeployments(ctx, &types.GetDeploymentOption{
		DeploymentID: id,
	})
	if err != nil {
		return nil, err
	}

	if len(out) == 0 {
		return nil, sql.ErrNoRows
	}

	return out[0], nil
}
