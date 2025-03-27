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
	// remove table
	retrieveEventTable = "retrieve_event"
	validatorsTable    = "validators"

	// Database table names.
	nodeRegisterTable    = "node_register_info"
	nodeInfoTable        = "node_info"
	onlineCountTable     = "online_count"
	candidateCodeTable   = "candidate_code"
	assetRecordTable     = "asset_record"
	replicaInfoTable     = "replica_info"
	assetsViewTable      = "asset_view"
	bucketTable          = "bucket"
	assetDataTable       = "asset_data"
	profitDetailsTable   = "profit_details"
	projectInfoTable     = "project_info"
	projectReplicasTable = "project_replicas"

	assetDownloadTable    = "asset_download"
	projectEventTable     = "project_event"
	validationResultTable = "validation_result"
	replicaEventTable     = "replica_event"
	edgeUpdateTable       = "edge_update_info"
	workloadRecordTable   = "workload_record"
	replenishBackupTable  = "replenish_backup"
	awsDataTable          = "aws_data"
	nodeStatisticsTable   = "node_statistics"
	nodeRetrieveTable     = "node_retrieve"
	serviceEventTable     = "service_event"
	bandwidthEventTable   = "bandwidth_event"

	// Default limits for loading table entries.
	loadNodeInfosDefaultLimit           = 1000
	loadValidationResultsDefaultLimit   = 100
	loadAssetRecordsDefaultLimit        = 1000
	loadExpiredAssetRecordsDefaultLimit = 100
	loadWorkloadDefaultLimit            = 100
	loadReplicaEventDefaultLimit        = 500
	loadRetrieveDefaultLimit            = 100
	loadReplicaDefaultLimit             = 100
	loadAssetDownloadLimit              = 500
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
	// tx.MustExec(fmt.Sprintf(cRetrieveEventTable, retrieveEventTable))
	tx.MustExec(fmt.Sprintf(cReplenishBackupTable, replenishBackupTable))
	tx.MustExec(fmt.Sprintf(cAWSDataTable, awsDataTable))
	tx.MustExec(fmt.Sprintf(cProfitDetailsTable, profitDetailsTable))
	tx.MustExec(fmt.Sprintf(cCandidateCodeTable, candidateCodeTable))
	tx.MustExec(fmt.Sprintf(cProjectStateTable, projectStateTable(serverID)))
	tx.MustExec(fmt.Sprintf(cProjectInfosTable, projectInfoTable))
	tx.MustExec(fmt.Sprintf(cProjectReplicasTable, projectReplicasTable))
	tx.MustExec(fmt.Sprintf(cProjectEventTable, projectEventTable))
	tx.MustExec(fmt.Sprintf(cOnlineCountTable, onlineCountTable))
	// tx.MustExec(fmt.Sprintf(cDeploymentTable, deploymentTable))
	// tx.MustExec(fmt.Sprintf(cProviderTable, providersTable))
	// tx.MustExec(fmt.Sprintf(cPropertiesTable, propertiesTable))
	// tx.MustExec(fmt.Sprintf(cServicesTable, servicesTable))
	// tx.MustExec(fmt.Sprintf(cDomainTable, domainsTable))
	tx.MustExec(fmt.Sprintf(cAssetDownloadTable, assetDownloadTable))
	tx.MustExec(fmt.Sprintf(cNodeStatisticsTable, nodeStatisticsTable))
	tx.MustExec(fmt.Sprintf(cNodeRetrieveTable, nodeRetrieveTable))
	tx.MustExec(fmt.Sprintf(cAssetDataTable, assetDataTable))
	tx.MustExec(fmt.Sprintf(cServiceEventTable, serviceEventTable))
	tx.MustExec(fmt.Sprintf(cBandwidthEventTable, bandwidthEventTable))

	return tx.Commit()
}

func doExec(d *SQLDB, serverID dtypes.ServerID) {
	// _, err := d.db.Exec(fmt.Sprintf("ALTER TABLE %s CHANGE speed speed         BIGINT       DEFAULT 0", replicaEventTable))
	// if err != nil {
	// 	log.Errorf("InitTables doExec err:%s", err.Error())
	// }
	// _, err = d.db.Exec(fmt.Sprintf("ALTER TABLE %s CHANGE speed speed         BIGINT       DEFAULT 0", nodeRetrieveTable))
	// if err != nil {
	// 	log.Errorf("InitTables doExec err:%s", err.Error())
	// }

	// _, err = d.db.Exec(fmt.Sprintf("ALTER TABLE %s CHANGE speed speed         BIGINT       DEFAULT 0", replicaInfoTable))
	// if err != nil {
	// 	log.Errorf("InitTables doExec err:%s", err.Error())
	// }
	// _, err := d.db.Exec(fmt.Sprintf("ALTER TABLE %s DROP COLUMN cpu_cores ;", projectInfoTable))
	// if err != nil {
	// 	log.Errorf("InitTables doExec err:%s", err.Error())
	// }
	// _, err = d.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD workload_id   VARCHAR(128) DEFAULT ''", replicaInfoTable))
	// if err != nil {
	// 	log.Errorf("InitTables doExec err:%s", err.Error())
	// }
	// _, err := d.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD nat_type             VARCHAR(32)     DEFAULT 'UnknowNAT'", nodeInfoTable))
	// if err != nil {
	// 	log.Errorf("InitTables doExec err:%s", err.Error())
	// }

	// ALTER TABLE node_info ADD nat_type             VARCHAR(32)     DEFAULT 'UnknowNAT';
	// Drop table service_event;
}
