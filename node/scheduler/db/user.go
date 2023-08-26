package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/jmoiron/sqlx"
	"golang.org/x/xerrors"
)

// SaveAssetUser save asset and user info
func (n *SQLDB) SaveAssetUser(hash, userID, assetName, assetType string, size int64, expiration time.Time) error {
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
		`INSERT INTO %s (hash, user_id, asset_name, total_size, asset_type, expiration) 
		        VALUES (?, ?, ?, ?, ?, ?) `, userAssetTable)
	_, err = tx.Exec(query, hash, userID, assetName, size, assetType, expiration)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(
		`UPDATE %s SET used_storage_size=used_storage_size+? WHERE user_id=?`, userInfoTable)
	_, err = tx.Exec(query, size, userID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// UpdateAssetShareStatus save share status
func (n *SQLDB) UpdateAssetShareStatus(hash, userID string, status int64) error {
	query := fmt.Sprintf(
		`UPDATE %s SET share_status=? WHERE user_id=? AND hash=?`, userAssetTable)
	_, err := n.db.Exec(query, status, userID, hash)
	return err
}

// DeleteAssetUser delete asset and user info
func (n *SQLDB) DeleteAssetUser(hash, userID string) error {
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

	var size int64
	query := fmt.Sprintf("SELECT total_size FROM %s WHERE hash=? AND user_id=?", userAssetTable)
	err = tx.Get(&size, query, hash, userID)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DELETE FROM %s WHERE hash=? AND user_id=? `, userAssetTable)
	result, err := tx.Exec(query, hash, userID)
	if err != nil {
		return err
	}

	r, err := result.RowsAffected()
	if r < 1 {
		return xerrors.New("nothing to update")
	}

	query = fmt.Sprintf(
		`UPDATE %s SET used_storage_size=used_storage_size-? WHERE user_id=?`, userInfoTable)
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

// AssetExistsOfUser checks if an asset exists
func (n *SQLDB) AssetExistsOfUser(hash, userID string) (bool, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(hash) FROM %s WHERE hash=? AND user_id=?`, userAssetTable)
	if err := n.db.Get(&total, countSQL, hash, userID); err != nil {
		return false, err
	}

	return total > 0, nil
}

// GetSizeForUserAsset Get asset size of user
func (n *SQLDB) GetSizeForUserAsset(hash, userID string) (int64, error) {
	var size int64
	query := fmt.Sprintf("SELECT total_size FROM %s WHERE hash=? AND user_id=?", userAssetTable)

	err := n.db.Get(&size, query, hash, userID)
	if err != nil {
		return 0, err
	}

	return size, nil
}

// ListAssetsForUser Get a list of assets by user
func (n *SQLDB) ListAssetsForUser(user string, limit, offset int) ([]*types.UserAssetDetail, error) {
	if limit > loadAssetRecordsDefaultLimit {
		limit = loadAssetRecordsDefaultLimit
	}

	var infos []*types.UserAssetDetail
	query := fmt.Sprintf("SELECT * FROM %s WHERE user_id=? order by created_time desc LIMIT ? OFFSET ?", userAssetTable)
	err := n.db.Select(&infos, query, user, limit, offset)
	if err != nil {
		return nil, err
	}

	return infos, nil
}

// GetAssetCountsForUser Get asset count
func (n *SQLDB) GetAssetCountsForUser(user string) (int, error) {
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE user_id=? ", userAssetTable)
	var count int
	err := n.db.Get(&count, countQuery, user)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// GetAssetName Get asset name of user
func (n *SQLDB) GetAssetName(hash, userID string) (string, error) {
	var assetName string
	query := fmt.Sprintf("SELECT asset_name FROM %s WHERE hash=? AND user_id=?", userAssetTable)

	err := n.db.Get(&assetName, query, hash, userID)
	if err != nil {
		return "", err
	}

	return assetName, nil
}

func (n *SQLDB) GetAssetExpiration(hash, userID string) (time.Time, error) {
	var expiration time.Time
	query := fmt.Sprintf("SELECT expiration FROM %s WHERE hash=? AND user_id=?", userAssetTable)

	err := n.db.Get(&expiration, query, hash, userID)
	if err != nil {
		return time.Time{}, err
	}

	return expiration, nil
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
		`INSERT INTO %s (user_id, total_storage_size) VALUES (?, ?) ON DUPLICATE KEY UPDATE total_storage_size=?`, userInfoTable)
	_, err := n.db.Exec(query, userID, totalSize, totalSize)

	return err
}

// LoadUserInfo load user storage size
func (n *SQLDB) LoadUserInfo(userID string) (*types.UserInfo, error) {
	query := fmt.Sprintf("SELECT total_storage_size, used_storage_size, total_traffic, peak_bandwidth, download_count, enable_vip FROM %s WHERE user_id=?", userInfoTable)
	info := types.UserInfo{}
	err := n.db.Get(&info, query, userID)
	return &info, err
}

// UpdateUserInfo update user info
func (n *SQLDB) UpdateUserInfo(userID string, incSize, peakSize, incCount int64) error {
	query := fmt.Sprintf(
		`UPDATE %s SET total_traffic=total_traffic+?,peak_bandwidth=?,download_count=download_count+? WHERE user_id=?`, userInfoTable)
	_, err := n.db.Exec(query, incSize, peakSize, incCount, userID)
	return err
}

// SaveUserAPIKeys save user api keys, apiKeys is map[string]UsetAPIKeysInfo encode by gob
func (n *SQLDB) SaveUserAPIKeys(userID string, apiKeys []byte) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (user_id, api_keys) VALUES (?, ?) ON DUPLICATE KEY UPDATE api_keys=?`, userInfoTable)
	_, err := n.db.Exec(query, userID, apiKeys, apiKeys)
	return err
}

// LoadUserAPIKeys load user api keys
func (n *SQLDB) LoadUserAPIKeys(userID string) ([]byte, error) {
	query := fmt.Sprintf("SELECT api_keys FROM %s WHERE user_id=?", userInfoTable)
	var apiKeys []byte
	err := n.db.Get(&apiKeys, query, userID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return apiKeys, err
}

// UpdateAssetVisitCount update asset visit count
func (n *SQLDB) UpdateAssetVisitCount(assetHash string) error {
	query := fmt.Sprintf(`INSERT INTO %s (hash, count) VALUES ( ?, ?) ON DUPLICATE KEY UPDATE count=count+?`, assetVisitCountTable)
	_, err := n.db.Exec(query, assetHash, 1, 1)
	return err
}

// GetAssetVisitCount get asset visit count
func (n *SQLDB) GetAssetVisitCount(assetHash string) (int, error) {
	query := fmt.Sprintf("SELECT count FROM %s WHERE hash=?", assetVisitCountTable)
	var count int
	err := n.db.Get(&count, query, assetHash)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return count, err
}

// UpdateUserInfo update user info
func (n *SQLDB) UpdateUserVIPAndStorageSize(userID string, enableVIP bool, storageSize int64) error {
	query := fmt.Sprintf(
		`UPDATE %s SET enable_vip=?, total_storage_size=? WHERE user_id=?`, userInfoTable)
	_, err := n.db.Exec(query, enableVIP, storageSize, userID)
	return err
}
