package db

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/Filecoin-Titan/titan/api/types"
)

// import (
// 	"context"
// 	"database/sql"
// 	"fmt"
// 	"strconv"
// 	"strings"

// 	"github.com/Filecoin-Titan/titan/api/types"

// 	"github.com/jmoiron/sqlx"
// )

// // CreateDeployment initializes a new deployment and its associated services within a transaction, ensuring all or nothing is committed to the database.
// func (m *SQLDB) CreateDeployment(ctx context.Context, deployment *types.Deployment) error {
// 	tx, err := m.db.Beginx()
// 	if err != nil {
// 		return err
// 	}
// 	defer tx.Rollback()

// 	err = addNewDeployment(ctx, tx, deployment)
// 	if err != nil {
// 		return err
// 	}

// 	err = addNewServices(ctx, tx, deployment.Services)
// 	if err != nil {
// 		return err
// 	}

// 	return tx.Commit()
// }

// // addNewDeployment inserts a new deployment record into the database or updates it if it already exists based on the primary key.
// func addNewDeployment(ctx context.Context, tx *sqlx.Tx, deployment *types.Deployment) error {
// 	qry := `INSERT INTO deployments (id, name, owner, state, type, authority, version, balance, cost, expiration, provider_id, created_at)
// 		        VALUES (:id, :name, :owner, :state, :type, :authority, :version, :balance, :cost, :expiration, :provider_id, :created_at)
// 		         ON DUPLICATE KEY UPDATE  authority=:authority, version=:version, balance=:balance, cost=:cost, expiration=:expiration`
// 	_, err := tx.NamedExecContext(ctx, qry, deployment)

// 	return err
// }

// // addNewServices inserts multiple service records associated with a deployment into the database, updating them if they already exist.
// func addNewServices(ctx context.Context, tx *sqlx.Tx, services []*types.Service) error {
// 	qry := `INSERT INTO services (id, name, image, ports, cpu, gpu, memory, storage, deployment_id, env, arguments, error_message, replicas, created_at)
// 		        VALUES (:id,:name, :image, :ports, :cpu, :gpu, :memory, :storage, :deployment_id, :env, :arguments, :error_message, :replicas, :created_at)
// 		        ON DUPLICATE KEY UPDATE image=VALUES(image), ports=VALUES(ports), cpu=VALUES(cpu), gpu=VALUES(gpu), memory=VALUES(memory), storage=VALUES(storage),
// 		        env=VALUES(env), arguments=VALUES(arguments), error_message=VALUES(error_message), replicas=VALUES(replicas)`
// 	_, err := tx.NamedExecContext(ctx, qry, services)

// 	return err
// }

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

// // DeleteDeployment removes a deployment and all its associated services from the database, ensuring data consistency within a transaction.
// func (m *SQLDB) DeleteDeployment(ctx context.Context, id types.DeploymentID) error {
// 	tx, err := m.db.Beginx()
// 	if err != nil {
// 		return err
// 	}
// 	defer tx.Rollback()

// 	qry := `DELETE FROM deployments where id = ?`
// 	_, err = tx.ExecContext(ctx, qry, id)
// 	if err != nil {
// 		return err
// 	}

// 	qry2 := `DELETE FROM services where deployment_id = ?`
// 	_, err = tx.ExecContext(ctx, qry2, id)
// 	if err != nil {
// 		return err
// 	}

// 	return tx.Commit()
// }

// // AddProperties inserts or updates properties associated with a deployment, handling data through an upsert operation.
// func (m *SQLDB) AddProperties(ctx context.Context, properties *types.Properties) error {
// 	qry := `INSERT INTO properties (id, provider_id, app_id, app_type, created_at)
// 		        VALUES (:id, :provider_id, :app_id, :app_type, :created_at) ON DUPLICATE KEY UPDATE
// 		        app_id=:app_id, app_type=:app_type`
// 	_, err := m.db.NamedExecContext(ctx, qry, properties)

// 	return err
// }

// // GetDomain retrieves domain information based on its hostname, providing details necessary for deployment mappings.
// func (m *SQLDB) GetDomain(ctx context.Context, hostname string) (*types.DeploymentDomain, error) {
// 	qry := `SELECT * FROM domains where name = ?`
// 	var out types.DeploymentDomain
// 	if err := m.db.GetContext(ctx, &out, qry, hostname); err != nil {
// 		return nil, err
// 	}
// 	return &out, nil
// }

// // AddDomain adds or updates a domain entry in the database, linking it with deployments and providers.
// func (m *SQLDB) AddDomain(ctx context.Context, domain *types.DeploymentDomain) error {
// 	statement := `INSERT INTO domains (name, state, deployment_id, provider_id, created_at) VALUES (:name, :state, :deployment_id, :provider_id, :created_at)
// 		ON DUPLICATE KEY UPDATE deployment_id = VALUES(deployment_id), provider_id = values(provider_id);`
// 	_, err := m.db.NamedExecContext(ctx, statement, domain)
// 	return err
// }

// // DeleteDomain removes a domain record from the database based on its name, ensuring cleanup of unused domain entries.
// func (m *SQLDB) DeleteDomain(ctx context.Context, name string) error {
// 	statement := `DELETE from domains where name = ?`
// 	_, err := m.db.ExecContext(ctx, statement, name)
// 	return err
// }

// // AddNewProvider inserts a new provider record or updates an existing one in the database, maintaining up-to-date provider details.
// func (m *SQLDB) AddNewProvider(ctx context.Context, provider *types.Provider) error {
// 	qry := `INSERT INTO providers (id, owner, remote_addr, ip, state, created_at)
// 		        VALUES (:id, :owner, :remote_addr, :ip, :state, :created_at) ON DUPLICATE KEY UPDATE  owner=:owner, remote_addr=:remote_addr,
// 		            ip=:ip, state=:state`
// 	_, err := m.db.NamedExecContext(ctx, qry, provider)

// 	return err
// }

// // UpdateProviderState updates the state of a provider in the database, reflecting changes such as activation or deactivation.
// func (m *SQLDB) UpdateProviderState(ctx context.Context, id string, state types.ProviderState) error {
// 	statement := `UPDATE providers set state = ? where id = ?`
// 	_, err := m.db.ExecContext(ctx, statement, state, id)
// 	return err
// }

// // GetProviderByID fetches a provider's details from the database using the provider's ID, ensuring accurate retrieval of provider information.
// func (m *SQLDB) GetProviderByID(ctx context.Context, id string) (*types.Provider, error) {
// 	qry := `SELECT * FROM providers where id = ?`
// 	var out types.Provider
// 	if err := m.db.GetContext(ctx, &out, qry, id); err != nil {
// 		return nil, err
// 	}
// 	return &out, nil
// }

// // GetAllProviders retrieves a list of all providers based on filtering options, with pagination support, and returns the total count of providers matching the filters.
// func (m *SQLDB) GetAllProviders(ctx context.Context, option *types.GetProviderOption) (int64, []*types.Provider, error) {
// 	qry := `SELECT * from providers`
// 	var condition []string
// 	if option.ID != "" {
// 		condition = append(condition, fmt.Sprintf(`id = '%s'`, option.ID))
// 	}

// 	if option.Owner != "" {
// 		condition = append(condition, fmt.Sprintf(`owner = '%s'`, option.Owner))
// 	}

// 	if len(option.State) > 0 {
// 		var states []string
// 		for _, s := range option.State {
// 			states = append(states, strconv.Itoa(int(s)))
// 		}
// 		condition = append(condition, fmt.Sprintf(`state in (%s)`, strings.Join(states, ",")))
// 	}

// 	countSQL := `select count(id) from providers`

// 	if len(condition) > 0 {
// 		qry += ` WHERE `
// 		qry += strings.Join(condition, ` AND `)
// 		countSQL += ` WHERE `
// 		countSQL += strings.Join(condition, ` AND `)
// 	}

// 	var total int64
// 	if err := m.db.GetContext(ctx, &total, countSQL); err != nil {
// 		return 0, nil, err
// 	}

// 	if option.Page <= 0 {
// 		option.Page = 1
// 	}

// 	if option.Size <= 0 {
// 		option.Size = 10
// 	}

// 	offset := (option.Page - 1) * option.Size
// 	limit := option.Size
// 	qry += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)

// 	var out []*types.Provider
// 	err := m.db.SelectContext(ctx, &out, qry)
// 	if err != nil {
// 		return 0, nil, err
// 	}
// 	return total, out, nil
// }
