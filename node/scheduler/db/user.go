package db

// import (
// 	"database/sql"
// 	"fmt"
// 	"time"

// 	"github.com/Filecoin-Titan/titan/api/types"
// 	"github.com/jmoiron/sqlx"
// 	"golang.org/x/xerrors"
// )

// // SaveAssetUser save asset and user info
// func (n *SQLDB) SaveAssetUser(hash, userID, assetName, assetType string, size int64, expiration time.Time, password string, groupID int) error {
// 	tx, err := n.db.Beginx()
// 	if err != nil {
// 		return err
// 	}

// 	defer func() {
// 		err = tx.Rollback()
// 		if err != nil && err != sql.ErrTxDone {
// 			log.Errorf("SaveAssetUser Rollback err:%s", err.Error())
// 		}
// 	}()

// 	query := fmt.Sprintf(
// 		`INSERT INTO %s (hash, user_id, asset_name, total_size, asset_type, expiration, password, group_id)
// 		        VALUES (?, ?, ?, ?, ?, ?, ?, ?) `, userAssetTable)
// 	_, err = tx.Exec(query, hash, userID, assetName, size, assetType, expiration, password, groupID)
// 	if err != nil {
// 		return err
// 	}

// 	query = fmt.Sprintf(
// 		`UPDATE %s SET used_storage_size=used_storage_size+? WHERE user_id=?`, userInfoTable)
// 	_, err = tx.Exec(query, size, userID)
// 	if err != nil {
// 		return err
// 	}

// 	return tx.Commit()
// }

// // UpdateAssetShareStatus save share status
// func (n *SQLDB) UpdateAssetShareStatus(hash, userID string, status int64) error {
// 	query := fmt.Sprintf(
// 		`UPDATE %s SET share_status=? WHERE user_id=? AND hash=?`, userAssetTable)
// 	_, err := n.db.Exec(query, status, userID, hash)
// 	return err
// }

// // DeleteAssetUser delete asset and user info
// func (n *SQLDB) DeleteAssetUser(hash, userID string) error {
// 	tx, err := n.db.Beginx()
// 	if err != nil {
// 		return err
// 	}

// 	defer func() {
// 		err = tx.Rollback()
// 		if err != nil && err != sql.ErrTxDone {
// 			log.Errorf("DeleteAssetUser Rollback err:%s", err.Error())
// 		}
// 	}()

// 	var size int64
// 	query := fmt.Sprintf("SELECT total_size FROM %s WHERE hash=? AND user_id=?", userAssetTable)
// 	err = tx.Get(&size, query, hash, userID)
// 	if err != nil {
// 		return err
// 	}

// 	query = fmt.Sprintf(`DELETE FROM %s WHERE hash=? AND user_id=? `, userAssetTable)
// 	result, err := tx.Exec(query, hash, userID)
// 	if err != nil {
// 		return err
// 	}

// 	r, err := result.RowsAffected()
// 	if r < 1 {
// 		return xerrors.New("nothing to update")
// 	}

// 	query = fmt.Sprintf(
// 		`UPDATE %s SET used_storage_size=used_storage_size-? WHERE user_id=?`, userInfoTable)
// 	_, err = tx.Exec(query, size, userID)
// 	if err != nil {
// 		return err
// 	}

// 	return tx.Commit()
// }

// // ListUsersForAsset Get a list of users by asset
// func (n *SQLDB) ListUsersForAsset(hash string) ([]string, error) {
// 	var infos []string
// 	query := fmt.Sprintf("SELECT user_id FROM %s WHERE hash=?", userAssetTable)

// 	err := n.db.Select(&infos, query, hash)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return infos, nil
// }

// // AssetExistsOfUser checks if an asset exists
// func (n *SQLDB) AssetExistsOfUser(hash, userID string) (bool, error) {
// 	var total int64
// 	countSQL := fmt.Sprintf(`SELECT count(hash) FROM %s WHERE hash=? AND user_id=?`, userAssetTable)
// 	if err := n.db.Get(&total, countSQL, hash, userID); err != nil {
// 		return false, err
// 	}

// 	return total > 0, nil
// }

// // GetSizeForUserAsset Get asset size of user
// func (n *SQLDB) GetSizeForUserAsset(hash, userID string) (int64, error) {
// 	var size int64
// 	query := fmt.Sprintf("SELECT total_size FROM %s WHERE hash=? AND user_id=?", userAssetTable)

// 	err := n.db.Get(&size, query, hash, userID)
// 	if err != nil {
// 		return 0, err
// 	}

// 	return size, nil
// }

// // ListAssetsForUser Get a list of assets by user
// func (n *SQLDB) ListAssetsForUser(user string, limit, offset, groupID int) ([]*types.UserAssetDetail, error) {
// 	if limit > loadAssetRecordsDefaultLimit {
// 		limit = loadAssetRecordsDefaultLimit
// 	}

// 	var infos []*types.UserAssetDetail
// 	query := fmt.Sprintf("SELECT * FROM %s WHERE user_id=? AND group_id=? order by created_time desc LIMIT ? OFFSET ?", userAssetTable)
// 	err := n.db.Select(&infos, query, user, groupID, limit, offset)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return infos, nil
// }

// // GetUserAssetCountByGroupID Get count by group id
// func (n *SQLDB) GetUserAssetCountByGroupID(user string, groupID int) (int, error) {
// 	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE user_id=? AND group_id=? ", userAssetTable)
// 	var count int
// 	err := n.db.Get(&count, countQuery, user, groupID)
// 	if err != nil {
// 		return 0, err
// 	}

// 	return count, nil
// }

// // GetAssetCountsForUser Get asset count
// func (n *SQLDB) GetAssetCountsForUser(user string, groupID int) (int, error) {
// 	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE user_id=? AND group_id=? ", userAssetTable)
// 	var count int
// 	err := n.db.Get(&count, countQuery, user, groupID)
// 	if err != nil {
// 		return 0, err
// 	}

// 	return count, nil
// }

// // GetAssetName Get asset name of user
// func (n *SQLDB) GetAssetName(hash, userID string) (string, error) {
// 	var assetName string
// 	query := fmt.Sprintf("SELECT asset_name FROM %s WHERE hash=? AND user_id=?", userAssetTable)

// 	err := n.db.Get(&assetName, query, hash, userID)
// 	if err != nil {
// 		return "", err
// 	}

// 	return assetName, nil
// }

// func (n *SQLDB) GetAssetExpiration(hash, userID string) (time.Time, error) {
// 	var expiration time.Time
// 	query := fmt.Sprintf("SELECT expiration FROM %s WHERE hash=? AND user_id=?", userAssetTable)

// 	err := n.db.Get(&expiration, query, hash, userID)
// 	if err != nil {
// 		return time.Time{}, err
// 	}

// 	return expiration, nil
// }

// // GetUserStorageSize Get the Storage Size already used by the user
// func (n *SQLDB) GetUserStorageSize(user string) (int64, error) {
// 	var hashes []string

// 	query := fmt.Sprintf("SELECT hash FROM %s WHERE user_id=? ", userAssetTable)
// 	err := n.db.Select(&hashes, query, user)
// 	if err != nil {
// 		return 0, err
// 	}

// 	if len(hashes) > 0 {
// 		var size int64
// 		sQuery := fmt.Sprintf(`SELECT sum(total_size) FROM %s WHERE hash in (?)`, assetRecordTable)
// 		query, args, err := sqlx.In(sQuery, hashes)
// 		if err != nil {
// 			return 0, err
// 		}

// 		query = n.db.Rebind(query)
// 		err = n.db.Get(&size, query, args...)
// 		return size, err
// 	}

// 	return 0, nil
// }

// // SaveUserTotalStorageSize save user storage size
// func (n *SQLDB) SaveUserTotalStorageSize(userID string, totalSize int64) error {
// 	query := fmt.Sprintf(
// 		`INSERT INTO %s (user_id, total_storage_size) VALUES (?, ?) ON DUPLICATE KEY UPDATE total_storage_size=?`, userInfoTable)
// 	_, err := n.db.Exec(query, userID, totalSize, totalSize)

// 	return err
// }

// // LoadUserInfo load user info
// func (n *SQLDB) LoadUserInfo(userID string) (*types.UserInfo, error) {
// 	query := fmt.Sprintf("SELECT total_storage_size, used_storage_size, total_traffic, peak_bandwidth, download_count, enable_vip, update_peak_time FROM %s WHERE user_id=?", userInfoTable)
// 	info := types.UserInfo{}
// 	err := n.db.Get(&info, query, userID)
// 	return &info, err
// }

// // LoadStorageStatsOfUser load user storage stats
// func (n *SQLDB) LoadStorageStatsOfUser(userID string) (*types.StorageStats, error) {
// 	query := fmt.Sprintf("SELECT a.user_id, a.total_storage_size, a.used_storage_size, a.total_traffic, (SELECT COUNT(*) FROM %s WHERE user_id = a.user_id) AS asset_count FROM %s a WHERE user_id=?", userAssetTable, userInfoTable)
// 	info := types.StorageStats{}
// 	err := n.db.Get(&info, query, userID)
// 	return &info, err
// }

// // ListStorageStatsOfUsers load users storage stats
// func (n *SQLDB) ListStorageStatsOfUsers(limit, offset int) (*types.ListStorageStatsRsp, error) {
// 	res := new(types.ListStorageStatsRsp)
// 	var infos []*types.StorageStats
// 	query := fmt.Sprintf("SELECT a.user_id, a.total_storage_size, a.used_storage_size, a.total_traffic, (SELECT COUNT(*) FROM %s WHERE user_id = a.user_id) AS asset_count FROM %s a order by a.user_id desc LIMIT ? OFFSET ?", userAssetTable, userInfoTable)
// 	if limit > loadUserDefaultLimit {
// 		limit = loadUserDefaultLimit
// 	}

// 	err := n.db.Select(&infos, query, limit, offset)
// 	if err != nil {
// 		return nil, err
// 	}

// 	res.Storages = infos

// 	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s ", userInfoTable)
// 	var count int
// 	err = n.db.Get(&count, countQuery)
// 	if err != nil {
// 		return nil, err
// 	}

// 	res.Total = count

// 	return res, nil
// }

// // UpdateUserInfo update user info
// func (n *SQLDB) UpdateUserInfo(userID string, incSize, incCount int64) error {
// 	query := fmt.Sprintf(
// 		`UPDATE %s SET total_traffic=total_traffic+?,download_count=download_count+? WHERE user_id=?`, userInfoTable)
// 	_, err := n.db.Exec(query, incSize, incCount, userID)
// 	return err
// }

// // UpdateUserPeakSize update user peakSize
// func (n *SQLDB) UpdateUserPeakSize(userID string, peakSize int64) error {
// 	query := fmt.Sprintf(
// 		`UPDATE %s SET peak_bandwidth=?,update_peak_time=NOW() WHERE user_id=? AND peak_bandwidth<?`, userInfoTable)
// 	_, err := n.db.Exec(query, peakSize, userID, peakSize)
// 	return err
// }

// // SaveUserAPIKeys save user api keys, apiKeys is map[string]UsetAPIKeysInfo encode by gob
// func (n *SQLDB) SaveUserAPIKeys(userID string, apiKeys []byte) error {
// 	query := fmt.Sprintf(
// 		`INSERT INTO %s (user_id, api_keys) VALUES (?, ?) ON DUPLICATE KEY UPDATE api_keys=?`, userInfoTable)
// 	_, err := n.db.Exec(query, userID, apiKeys, apiKeys)
// 	return err
// }

// // LoadUserAPIKeys load user api keys
// func (n *SQLDB) LoadUserAPIKeys(userID string) ([]byte, error) {
// 	query := fmt.Sprintf("SELECT api_keys FROM %s WHERE user_id=?", userInfoTable)
// 	var apiKeys []byte
// 	err := n.db.Get(&apiKeys, query, userID)
// 	if err == sql.ErrNoRows {
// 		return nil, nil
// 	}
// 	return apiKeys, err
// }

// // UpdateAssetVisitCount update asset visit count
// func (n *SQLDB) UpdateAssetVisitCount(assetHash string) error {
// 	query := fmt.Sprintf(`INSERT INTO %s (hash, count) VALUES ( ?, ?) ON DUPLICATE KEY UPDATE count=count+?`, assetVisitCountTable)
// 	_, err := n.db.Exec(query, assetHash, 1, 1)
// 	return err
// }

// // GetAssetVisitCount get asset visit count
// func (n *SQLDB) GetAssetVisitCount(assetHash string) (int, error) {
// 	query := fmt.Sprintf("SELECT count FROM %s WHERE hash=?", assetVisitCountTable)
// 	var count int
// 	err := n.db.Get(&count, query, assetHash)
// 	if err == sql.ErrNoRows {
// 		return 0, nil
// 	}
// 	return count, err
// }

// // UpdateUserVIPAndStorageSize update user info
// func (n *SQLDB) UpdateUserVIPAndStorageSize(userID string, enableVIP bool, storageSize int64) error {
// 	query := fmt.Sprintf(
// 		`UPDATE %s SET enable_vip=?, total_storage_size=? WHERE user_id=?`, userInfoTable)
// 	_, err := n.db.Exec(query, enableVIP, storageSize, userID)
// 	return err
// }

// // CreateAssetGroup create a group
// func (n *SQLDB) CreateAssetGroup(info *types.AssetGroup) (*types.AssetGroup, error) {
// 	query := fmt.Sprintf(
// 		`INSERT INTO %s (user_id, name, parent)
// 				VALUES (:user_id, :name, :parent)`, userAssetGroupTable)

// 	result, err := n.db.NamedExec(query, info)
// 	if err != nil {
// 		return nil, err
// 	}

// 	insertedID, err := result.LastInsertId()
// 	if err != nil {
// 		return nil, err
// 	}
// 	info.ID = int(insertedID)

// 	return info, err
// }

// // AssetGroupExists is group exists
// func (n *SQLDB) AssetGroupExists(userID string, gid int) (bool, error) {
// 	var total int64
// 	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE user_id=? AND id=?`, userAssetGroupTable)
// 	if err := n.db.Get(&total, countSQL, userID, gid); err != nil {
// 		return false, err
// 	}

// 	return total > 0, nil
// }

// // GetAssetGroupParent get a group parent
// func (n *SQLDB) GetAssetGroupParent(gid int) (int, error) {
// 	var parent int

// 	query := fmt.Sprintf("SELECT parent FROM %s WHERE id=?", userAssetGroupTable)
// 	err := n.db.Get(&parent, query, gid)
// 	if err != nil {
// 		return 0, err
// 	}

// 	return parent, nil
// }

// // GetAssetGroupCount get asset group count of user
// func (n *SQLDB) GetAssetGroupCount(userID string) (int64, error) {
// 	var total int64
// 	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE user_id=? `, userAssetGroupTable)
// 	if err := n.db.Get(&total, countSQL, userID); err != nil {
// 		return 0, err
// 	}

// 	return total, nil
// }

// // UpdateAssetGroupName update user asset group name
// func (n *SQLDB) UpdateAssetGroupName(userID, rename string, groupID int) error {
// 	query := fmt.Sprintf(
// 		`UPDATE %s SET name=? WHERE user_id=? AND id=? `, userAssetGroupTable)
// 	_, err := n.db.Exec(query, rename, userID, groupID)
// 	return err
// }

// // UpdateAssetGroupParent update user asset group parent
// func (n *SQLDB) UpdateAssetGroupParent(userID string, groupID, parent int) error {
// 	query := fmt.Sprintf(
// 		`UPDATE %s SET parent=? WHERE user_id=? AND id=? `, userAssetGroupTable)
// 	_, err := n.db.Exec(query, parent, userID, groupID)
// 	return err
// }

// // UpdateAssetGroup update user asset group
// func (n *SQLDB) UpdateAssetGroup(hash, userID string, groupID int) error {
// 	query := fmt.Sprintf(
// 		`UPDATE %s SET group_id=? WHERE user_id=? AND hash=?`, userAssetTable)
// 	_, err := n.db.Exec(query, groupID, userID, hash)
// 	return err
// }

// // ListAssetGroupForUser get asset group list
// func (n *SQLDB) ListAssetGroupForUser(user string, parent, limit, offset int) (*types.ListAssetGroupRsp, error) {
// 	res := new(types.ListAssetGroupRsp)
// 	var infos []*types.AssetGroup
// 	var count int

// 	query := fmt.Sprintf("SELECT a.*, COUNT(ua.user_id) AS asset_count,	COALESCE(SUM(ua.total_size), 0) AS asset_size FROM %s a	LEFT JOIN %s ua ON ua.user_id=a.user_id AND ua.group_id=a.id WHERE a.user_id=? AND a.parent=? GROUP BY a.id	ORDER BY a.created_time DESC LIMIT ? OFFSET ?", userAssetGroupTable, userAssetTable)
// 	err := n.db.Select(&infos, query, user, parent, limit, offset)
// 	if err != nil {
// 		return nil, err
// 	}

// 	res.AssetGroups = infos

// 	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE user_id=? AND parent=?", userAssetGroupTable)

// 	err = n.db.Get(&count, countQuery, user, parent)
// 	if err != nil {
// 		return nil, err
// 	}

// 	res.Total = count

// 	return res, nil
// }

// // DeleteAssetGroup delete asset group
// func (n *SQLDB) DeleteAssetGroup(userID string, gid int) error {
// 	tx, err := n.db.Beginx()
// 	if err != nil {
// 		return err
// 	}

// 	defer func() {
// 		err = tx.Rollback()
// 		if err != nil && err != sql.ErrTxDone {
// 			log.Errorf("DeleteFileGroup Rollback err:%s", err.Error())
// 		}
// 	}()

// 	query := fmt.Sprintf(`DELETE FROM %s WHERE user_id=? AND parent=?`, userAssetGroupTable)
// 	_, err = tx.Exec(query, userID, gid)
// 	if err != nil {
// 		return err
// 	}

// 	query = fmt.Sprintf(`DELETE FROM %s WHERE user_id=? AND id=?`, userAssetGroupTable)
// 	_, err = tx.Exec(query, userID, gid)
// 	if err != nil {
// 		return err
// 	}

// 	return tx.Commit()
// }
