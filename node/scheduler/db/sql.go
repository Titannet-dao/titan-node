package db

import (
	"github.com/jmoiron/sqlx"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("db")

// SQLDB scheduler sql db
type SQLDB struct {
	db *sqlx.DB
}

// NewSQLDB new scheduler sql db
func NewSQLDB(db *sqlx.DB) *SQLDB {
	return &SQLDB{db}
}

const (
	// tables
	assetRecordTable      = "asset_record"
	replicaInfoTable      = "replica_info"
	edgeUpdateTable       = "edge_update_info"
	nodeInfoTable         = "node_info"
	validatorsTable       = "validators"
	nodeRegisterTable     = "node_register_info"
	validationResultTable = "validation_result"
	assetsViewTable       = "asset_view"
	bucketTable           = "bucket"

	loadNodeInfosLimit           = 100
	loadReplicaInfosLimit        = 100
	loadValidationResultsLimit   = 100
	loadAssetRecordsLimit        = 100
	loadExpiredAssetRecordsLimit = 100
)
