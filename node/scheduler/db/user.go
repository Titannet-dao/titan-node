package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/jmoiron/sqlx"
)

// SaveAssetUser save asset and user info
func (n *SQLDB) SaveAssetUser(hash, userID, assetName string, size int64) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("DeleteAssetUser Rollback err:%s", err.Error())
		}
	}()

	query := fmt.Sprintf(
		`INSERT INTO %s (hash, user_id, asset_name) 
		        VALUES (?, ?, ?) `, userAssetTable)
	result, err := tx.Exec(query, hash, userID, assetName)
	r, err := result.RowsAffected()
	if err != nil || r < 1 {
		return err
	}

	query = fmt.Sprintf(
		`UPDATE %s SET used_storage_size= used_storage_size+? WHERE user_id=?`, userAPIKeyStorageTable)
	_, err = tx.Exec(query, size, userID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// DeleteAssetUser delete asset and user info
func (n *SQLDB) DeleteAssetUser(hash, userID string, size int64) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("DeleteAssetUser Rollback err:%s", err.Error())
		}
	}()

	query := fmt.Sprintf(`DELETE FROM %s WHERE hash=? AND user_id=? `, userAssetTable)
	result, err := tx.Exec(query, hash, userID)
	r, err := result.RowsAffected()
	if err != nil || r < 1 {
		return err
	}

	query = fmt.Sprintf(
		`UPDATE %s SET used_storage_size= used_storage_size-? WHERE user_id=?`, userAPIKeyStorageTable)
	_, err = tx.Exec(query, size, userID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// ListUsersForAsset Get a list of users by asset
func (n *SQLDB) ListUsersForAsset(hash string) ([]string, error) {
	var infos []string
	query := fmt.Sprintf("SELECT user_id FROM %s WHERE hash=?", userAssetTable)

	err := n.db.Select(&infos, query, hash)
	if err != nil {
		return nil, err
	}

	return infos, nil
}

// ListAssetsForUser Get a list of assets by user
func (n *SQLDB) ListAssetsForUser(user string, limit, offset int) (*sqlx.Rows, error) {
	query := fmt.Sprintf("SELECT a.*,b.asset_name as asset_name FROM %s a LEFT JOIN %s b ON a.hash=b.hash WHERE b.user_id=? order by a.hash asc LIMIT ? OFFSET ?", assetRecordTable, userAssetTable)

	return n.db.QueryxContext(context.Background(), query, user, limit, offset)
}

// GetUserStorageSize Get the Storage Size already used by the user
func (n *SQLDB) GetUserStorageSize(user string) (int64, error) {
	var hashes []string

	query := fmt.Sprintf("SELECT hash FROM %s WHERE user_id=? ", userAssetTable)
	err := n.db.Select(&hashes, query, user)
	if err != nil {
		return 0, err
	}

	if len(hashes) > 0 {
		var size int64
		sQuery := fmt.Sprintf(`SELECT sum(total_size) FROM %s WHERE hash in (?)`, assetRecordTable)
		query, args, err := sqlx.In(sQuery, hashes)
		if err != nil {
			return 0, err
		}

		query = n.db.Rebind(query)
		err = n.db.Get(&size, query, args...)
		return size, err
	}

	return 0, nil
}

// SaveUserTotalStorageSize save user storage size
func (n *SQLDB) SaveUserTotalStorageSize(userID string, totalSize int64) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (user_id, total_storage_size) VALUES (?, ?) ON DUPLICATE KEY UPDATE total_storage_size=?`, userAPIKeyStorageTable)
	_, err := n.db.Exec(query, userID, totalSize, totalSize)

	return err
}

// LoadUserStorageSize load user storage size
func (n *SQLDB) LoadUserStorageSize(userID string) (*types.StorageSize, error) {
	query := fmt.Sprintf("SELECT total_storage_size, used_storage_size FROM %s WHERE user_id=?", userAPIKeyStorageTable)
	storageSize := types.StorageSize{}
	err := n.db.Get(&storageSize, query, userID)
	return &storageSize, err
}

// SaveUserAPIKeys save user api keys, apiKeys is map[string]string encode by gob
func (n *SQLDB) SaveUserAPIKeys(userID string, apiKeys []byte) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (user_id, api_keys) VALUES (?, ?) ON DUPLICATE KEY UPDATE api_keys=?`, userAPIKeyStorageTable)
	_, err := n.db.Exec(query, userID, apiKeys, apiKeys)
	return err
}

// LoadUserAPIKeys load user api keys
func (n *SQLDB) LoadUserAPIKeys(userID string) ([]byte, error) {
	query := fmt.Sprintf("SELECT api_keys FROM %s WHERE user_id=?", userAPIKeyStorageTable)
	var apiKeys []byte
	err := n.db.Get(&apiKeys, query, userID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return apiKeys, err
}
