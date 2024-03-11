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
	// Database table names.
	assetRecordTable      = "asset_record"
	replicaInfoTable      = "replica_info"
	edgeUpdateTable       = "edge_update_info"
	nodeInfoTable         = "node_info"
	validatorsTable       = "validators"
	nodeRegisterTable     = "node_register_info"
	validationResultTable = "validation_result"
	assetsViewTable       = "asset_view"
	bucketTable           = "bucket"
	workloadRecordTable   = "workload_record"
	userAssetTable        = "user_asset"
	userInfoTable         = "user_info"
	replicaEventTable     = "replica_event"
	retrieveEventTable    = "retrieve_event"
	assetVisitCountTable  = "asset_visit_count"
	replenishBackupTable  = "replenish_backup"
	userAssetGroupTable   = "user_asset_group"
	awsDataTable          = "aws_data"

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

// InitTables initializes data tables.
func InitTables(d *SQLDB, serverID dtypes.ServerID) error {
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
	tx.MustExec(fmt.Sprintf(cUserAssetTable, userAssetTable))
	tx.MustExec(fmt.Sprintf(cUserInfoTable, userInfoTable))
	tx.MustExec(fmt.Sprintf(cReplicaEventTable, replicaEventTable))
	tx.MustExec(fmt.Sprintf(cRetrieveEventTable, retrieveEventTable))
	tx.MustExec(fmt.Sprintf(cAssetVisitCountTable, assetVisitCountTable))
	tx.MustExec(fmt.Sprintf(cReplenishBackupTable, replenishBackupTable))
	tx.MustExec(fmt.Sprintf(cUserAssetGroupTable, userAssetGroupTable))
	tx.MustExec(fmt.Sprintf(cAWSDataTable, awsDataTable))

	return tx.Commit()
}
