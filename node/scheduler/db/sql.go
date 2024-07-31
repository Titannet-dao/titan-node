package db

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/jmoiron/sqlx"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("db")

// SQLDB represents a scheduler sql database.
type SQLDB struct {
	db *sqlx.DB
}

// NewSQLDB creates a new SQLDB instance.
func NewSQLDB(db *sqlx.DB) (*SQLDB, error) {
	s := &SQLDB{db}

	return s, nil
}

const (
	// userAssetTable       = "user_asset"
	// userInfoTable        = "user_info"
	// userAssetGroupTable  = "user_asset_group"
	// assetVisitCountTable = "asset_visit_count"

	// Database table names.
	nodeRegisterTable  = "node_register_info"
	nodeInfoTable      = "node_info"
	onlineCountTable   = "online_count"
	candidateCodeTable = "candidate_code"

	assetRecordTable = "asset_record"
	replicaInfoTable = "replica_info"
	assetsViewTable  = "asset_view"
	bucketTable      = "bucket"

	projectEventTable     = "project_event"
	validationResultTable = "validation_result"
	replicaEventTable     = "replica_event"
	retrieveEventTable    = "retrieve_event"

	edgeUpdateTable      = "edge_update_info"
	validatorsTable      = "validators"
	workloadRecordTable  = "workload_record"
	replenishBackupTable = "replenish_backup"
	awsDataTable         = "aws_data"
	profitDetailsTable   = "profit_details"
	projectInfoTable     = "project_info"
	projectReplicasTable = "project_replicas"

	deploymentTable = "deployments"
	providersTable  = "providers"
	propertiesTable = "properties"
	servicesTable   = "services"
	domainsTable    = "domains"

	// Default limits for loading table entries.
	loadNodeInfosDefaultLimit           = 1000
	loadValidationResultsDefaultLimit   = 100
	loadAssetRecordsDefaultLimit        = 100
	loadExpiredAssetRecordsDefaultLimit = 100
	loadWorkloadDefaultLimit            = 100
	loadReplicaEventDefaultLimit        = 500
	loadRetrieveDefaultLimit            = 100
	loadReplicaDefaultLimit             = 100
	loadUserDefaultLimit                = 100
)

// assetStateTable returns the asset state table name for the given serverID.
func assetStateTable(serverID dtypes.ServerID) string {
	str := strings.ReplaceAll(string(serverID), "-", "")
	return fmt.Sprintf("asset_state_%s", str)
}

// projectStateTable returns the project state table name for the given serverID.
func projectStateTable(serverID dtypes.ServerID) string {
	str := strings.ReplaceAll(string(serverID), "-", "")
	return fmt.Sprintf("project_state_%s", str)
}

// InitTables initializes data tables.
func InitTables(d *SQLDB, serverID dtypes.ServerID) error {
	doExec(d, serverID)

	// init table
	tx, err := d.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("InitTables Rollback err:%s", err.Error())
		}
	}()

	// Execute table creation statements
	tx.MustExec(fmt.Sprintf(cAssetStateTable, assetStateTable(serverID)))
	tx.MustExec(fmt.Sprintf(cReplicaInfoTable, replicaInfoTable))
	tx.MustExec(fmt.Sprintf(cNodeInfoTable, nodeInfoTable))
	tx.MustExec(fmt.Sprintf(cValidationResultsTable, validationResultTable))
	tx.MustExec(fmt.Sprintf(cNodeRegisterTable, nodeRegisterTable))
	tx.MustExec(fmt.Sprintf(cAssetRecordTable, assetRecordTable))
	tx.MustExec(fmt.Sprintf(cEdgeUpdateTable, edgeUpdateTable))
	tx.MustExec(fmt.Sprintf(cValidatorsTable, validatorsTable))
	tx.MustExec(fmt.Sprintf(cAssetViewTable, assetsViewTable))
	tx.MustExec(fmt.Sprintf(cBucketTable, bucketTable))
	tx.MustExec(fmt.Sprintf(cWorkloadTable, workloadRecordTable))
	tx.MustExec(fmt.Sprintf(cReplicaEventTable, replicaEventTable))
	tx.MustExec(fmt.Sprintf(cRetrieveEventTable, retrieveEventTable))
	tx.MustExec(fmt.Sprintf(cReplenishBackupTable, replenishBackupTable))
	tx.MustExec(fmt.Sprintf(cAWSDataTable, awsDataTable))
	tx.MustExec(fmt.Sprintf(cProfitDetailsTable, profitDetailsTable))
	tx.MustExec(fmt.Sprintf(cCandidateCodeTable, candidateCodeTable))
	tx.MustExec(fmt.Sprintf(cProjectStateTable, projectStateTable(serverID)))
	tx.MustExec(fmt.Sprintf(cProjectInfosTable, projectInfoTable))
	tx.MustExec(fmt.Sprintf(cProjectReplicasTable, projectReplicasTable))
	tx.MustExec(fmt.Sprintf(cProjectEventTable, projectEventTable))
	tx.MustExec(fmt.Sprintf(cOnlineCountTable, onlineCountTable))
	tx.MustExec(fmt.Sprintf(cDeploymentTable, deploymentTable))
	tx.MustExec(fmt.Sprintf(cProviderTable, providersTable))
	tx.MustExec(fmt.Sprintf(cPropertiesTable, propertiesTable))
	tx.MustExec(fmt.Sprintf(cServicesTable, servicesTable))
	tx.MustExec(fmt.Sprintf(cDomainTable, domainsTable))

	return tx.Commit()
}

func doExec(d *SQLDB, serverID dtypes.ServerID) {
	// _, err := d.db.Exec(fmt.Sprintf("ALTER TABLE %s CHANGE create_time created_time    DATETIME      NOT NULL", onlineCountTable))
	// if err != nil {
	// 	log.Errorf("InitTables doExec err:%s", err.Error())
	// }
	// _, err := d.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD penalty_profit       DECIMAL(20, 6)  DEFAULT 0;", nodeInfoTable))
	// if err != nil {
	// 	log.Errorf("InitTables doExec err:%s", err.Error())
	// }
	// _, err := d.db.Exec(fmt.Sprintf("ALTER TABLE %s DROP COLUMN nat_type ;", nodeInfoTable))
	// if err != nil {
	// 	log.Errorf("InitTables doExec err:%s", err.Error())
	// }
	// _, err = d.db.Exec(fmt.Sprintf("UPDATE  %s AS ni SET ni.penalty_profit = (SELECT ABS(COALESCE(SUM(pd.profit), 0)) FROM profit_details AS pd  WHERE pd.node_id = ni.node_id AND pd.profit_type = 7);", nodeInfoTable))
	// if err != nil {
	// 	log.Errorf("InitTables doExec err:%s", err.Error())
	// }
}
