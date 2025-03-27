package db

import (
	"database/sql"
	"fmt"

	"github.com/Filecoin-Titan/titan/api/types"
	"golang.org/x/xerrors"
)

// SaveAWSData saves multiple AWS data entries within a transaction, ensuring data integrity. It checks the validity of the data size for each entry before saving.
func (n *SQLDB) SaveAWSData(infos []types.AWSDataInfo) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("SaveAWSData Rollback err:%s", err.Error())
		}
	}()

	for _, info := range infos {
		if info.Size <= 0 {
			return xerrors.Errorf("%s SaveAWSData size %.2f ", info.Bucket, info.Size)
		}

		sqlString := fmt.Sprintf(`INSERT INTO %s (bucket, replicas, cid, size) VALUES (:bucket, :replicas, :cid, :size) `, awsDataTable)
		tx.NamedExec(sqlString, info)

	}
	return tx.Commit()
}

// UpdateAWSData updates specific AWS data entries, setting new CID and distribution status along with the current timestamp.
func (n *SQLDB) UpdateAWSData(info *types.AWSDataInfo) error {
	query := fmt.Sprintf(`UPDATE %s SET cid=?, is_distribute=?, distribute_time=NOW() WHERE bucket=?`, awsDataTable)
	_, err := n.db.Exec(query, info.Cid, info.IsDistribute, info.Bucket)
	return err
}

// ListAWSData retrieves a list of AWS data entries based on their distribution status, with pagination support.
func (n *SQLDB) ListAWSData(limit, offset int, isDistribute bool) ([]*types.AWSDataInfo, error) {
	var infos []*types.AWSDataInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE is_distribute=? LIMIT ? OFFSET ?", awsDataTable)
	err := n.db.Select(&infos, query, isDistribute, limit, offset)
	if err != nil {
		return nil, err
	}

	return infos, nil
}

// SaveAssetData saves multiple ipfs data entries within a transaction, ensuring data integrity. It checks the validity of the data size for each entry before saving.
func (n *SQLDB) SaveAssetData(infos []types.AssetDataInfo) error {
	// query := fmt.Sprintf(
	// 	`INSERT INTO %s (owner, replicas, cid, expiration, hash) VALUES (:owner, :replicas, :cid, :expiration, :hash)`, assetDataTable)

	query := fmt.Sprintf(
		`INSERT INTO %s (owner, replicas, cid, expiration, hash)
					VALUES (:owner, :replicas, :cid, :expiration, :hash)
					ON DUPLICATE KEY UPDATE status=:status, expiration=:expiration, replicas=:replicas, owner=:owner`, assetDataTable)

	_, err := n.db.NamedExec(query, infos)
	return err
}

// UpdateAssetData updates specific ipfs data entries, setting new CID and distribution status along with the current timestamp.
func (n *SQLDB) UpdateAssetData(info *types.AssetDataInfo) error {
	query := fmt.Sprintf(`UPDATE %s SET  status=?, distribute_time=NOW() WHERE cid=?`, assetDataTable)
	_, err := n.db.Exec(query, info.Status, info.Cid)
	return err
}

// ListAssetData retrieves a list of ipfs data entries based on their distribution status, with pagination support.
func (n *SQLDB) ListAssetData(limit int, status int) ([]*types.AssetDataInfo, error) {
	var infos []*types.AssetDataInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE status=? order by distribute_time asc LIMIT ? ", assetDataTable)
	err := n.db.Select(&infos, query, status, limit)
	if err != nil {
		return nil, err
	}

	return infos, nil
}
