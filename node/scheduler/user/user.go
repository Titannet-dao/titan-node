package user

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/terrors"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/scheduler/assets"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/filecoin-project/go-jsonrpc/auth"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("user")

type User struct {
	*db.SQLDB
	ID string
	*assets.Manager
}

// AllocateStorage allocates storage space.
func (u *User) AllocateStorage(ctx context.Context, size int64) (*types.UserInfo, error) {
	userInfo, err := u.GetInfo(ctx)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	// already allocate storage
	if userInfo != nil && userInfo.TotalSize > 0 {
		return userInfo, nil
	}
	// TODO check total size of the titan
	if err := u.SaveUserTotalStorageSize(u.ID, size); err != nil {
		return nil, err
	}
	return u.GetInfo(ctx)
}

// GetInfo get user info
func (u *User) GetInfo(ctx context.Context) (*types.UserInfo, error) {
	return u.LoadUserInfo(u.ID)
}

// CreateAPIKey creates a key for the client API.
func (u *User) CreateAPIKey(ctx context.Context, keyName string, commonAPI api.Common) (string, error) {
	buf, err := u.LoadUserAPIKeys(u.ID)
	if err != nil {
		return "", err
	}

	apiKeys := make(map[string]types.UserAPIKeysInfo)
	if len(buf) > 0 {
		apiKeys, err = u.decodeAPIKeys(buf)
		if err != nil {
			return "", err
		}
	}

	if len(apiKeys) > 0 {
		return "", fmt.Errorf("only support create one api key now")
	}

	keyValue, err := generateAPIKey(u.ID, commonAPI)
	if err != nil {
		return "", err
	}
	apiKeys[keyName] = types.UserAPIKeysInfo{CreatedTime: time.Now(), APIKey: keyValue}

	buf, err = u.encodeAPIKeys(apiKeys)
	if err != nil {
		return "", err
	}

	if err = u.SaveUserAPIKeys(u.ID, buf); err != nil {
		return "", err
	}

	return keyValue, nil
}

// GetAPIKeys get all api key for user.
func (u *User) GetAPIKeys(ctx context.Context) (map[string]types.UserAPIKeysInfo, error) {
	buf, err := u.LoadUserAPIKeys(u.ID)
	if err != nil {
		return nil, err
	}

	apiKeys := make(map[string]types.UserAPIKeysInfo)
	if len(buf) > 0 {
		apiKeys, err = u.decodeAPIKeys(buf)
		if err != nil {
			return nil, err
		}
	}

	return apiKeys, nil
}

// UpdateShareStatus update status
func (u *User) SetAssetAtShareStatus(ctx context.Context, assetCID string) error {
	hash, err := cidutil.CIDToHash(assetCID)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:%s", assetCID, err.Error())
	}

	return u.UpdateAssetShareStatus(hash, u.ID, int64(types.UserAssetShareStatusShared))
}

func (u *User) DeleteAPIKey(ctx context.Context, name string) error {
	buf, err := u.LoadUserAPIKeys(u.ID)
	if err != nil {
		return err
	}

	apiKeys := make(map[string]types.UserAPIKeysInfo)
	if len(buf) > 0 {
		apiKeys, err = u.decodeAPIKeys(buf)
		if err != nil {
			return err
		}
	}

	if _, ok := apiKeys[name]; !ok {
		return fmt.Errorf("api key with name %s not exist", name)
	}

	delete(apiKeys, name)

	buf, err = u.encodeAPIKeys(apiKeys)
	if err != nil {
		return err
	}
	return u.SaveUserAPIKeys(u.ID, buf)
}

// CreateAsset creates an asset with car CID, car name, and car size.
func (u *User) CreateAsset(ctx context.Context, req *types.CreateAssetReq) (*types.CreateAssetRsp, error) {
	hash, err := cidutil.CIDToHash(req.AssetCID)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.CidToHashFiled, Message: err.Error()}
	}

	storageSize, err := u.LoadUserInfo(req.UserID)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.DatabaseErr, Message: err.Error()}
	}

	if storageSize.TotalSize-storageSize.UsedSize < req.AssetSize {
		return nil, &api.ErrWeb{Code: terrors.UserStorageSizeNotEnough}
	}

	return u.Manager.CreateAssetUploadTask(hash, req)
}

// ListAssets lists the assets of the user.
func (u *User) ListAssets(ctx context.Context, limit, offset int, maxCountOfVisitAsset int) (*types.ListAssetRecordRsp, error) {
	count, err := u.GetAssetCountsForUser(u.ID)
	if err != nil {
		log.Errorf("GetAssetCountsForUser err:%s", err.Error())
		return nil, err
	}

	userInfo, err := u.LoadUserInfo(u.ID)

	userAssets, err := u.ListAssetsForUser(u.ID, limit, offset)
	if err != nil {
		log.Errorf("ListAssetsForUser err:%s", err.Error())
		return nil, err
	}

	list := make([]*types.AssetOverview, 0)
	for _, userAsset := range userAssets {
		record, err := u.LoadAssetRecord(userAsset.Hash)
		if err != nil {
			log.Errorf("asset LoadAssetRecord err: %s", err.Error())
			continue
		}

		record.ReplicaInfos, err = u.LoadReplicasByStatus(userAsset.Hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
		if err != nil {
			log.Errorf("asset LoadReplicasByStatus err: %s", err.Error())
			continue
		}

		count, err := u.GetAssetVisitCount(userAsset.Hash)
		if err != nil {
			log.Errorf("get asset visit count err: %s", err.Error())
			continue
		}

		if !userInfo.EnableVIP && count >= maxCountOfVisitAsset {
			userAsset.ShareStatus = int64(types.UserAssetShareStatusForbid)
		}

		r := &types.AssetOverview{
			AssetRecord:      record,
			UserAssetDetail:  userAsset,
			VisitCount:       count,
			RemainVisitCount: maxCountOfVisitAsset - count,
		}

		list = append(list, r)
	}

	return &types.ListAssetRecordRsp{Total: count, AssetOverviews: list}, nil
}

// DeleteAsset deletes the assets of the user.
func (u *User) DeleteAsset(ctx context.Context, cid string) error {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return err
	}

	users, err := u.ListUsersForAsset(hash)
	if err != nil {
		return err
	}

	if len(users) == 1 && users[0] == u.ID {
		return u.Manager.RemoveAsset(hash, true)
	}

	return u.DeleteAssetUser(hash, u.ID)
}

// ShareAssets shares the assets of the user.
func (u *User) ShareAssets(ctx context.Context, assetCIDs []string, schedulerAPI api.Scheduler, domain string) (map[string]string, error) {
	// TODO　check asset if belong to user
	urls := make(map[string]string)
	for _, assetCID := range assetCIDs {
		downloadInfos, err := schedulerAPI.GetCandidateDownloadInfos(context.Background(), assetCID)
		if err != nil {
			return nil, err
		}

		if len(downloadInfos) == 0 {
			return nil, fmt.Errorf("asset %s not exist", assetCID)
		}

		tk, err := generateAccessToken(&types.AuthUserUploadDownloadAsset{UserID: u.ID, AssetCID: assetCID}, schedulerAPI.(api.Common))
		if err != nil {
			return nil, err
		}

		hash, err := cidutil.CIDToHash(assetCID)
		if err != nil {
			return nil, err
		}
		assetName, err := u.GetAssetName(hash, u.ID)
		if err != nil {
			return nil, err
		}

		url := fmt.Sprintf("http://%s/ipfs/%s?token=%s&filename=%s", downloadInfos[0].Address, assetCID, tk, assetName)
		if len(domain) > 0 {
			// remove prefix 'c_'
			// nodeID := downloadInfos[0].NodeID[2:]
			url = fmt.Sprintf("https://%s.%s/ipfs/%s?token=%s&filename=%s", assetCID, domain, assetCID, tk, assetName)
		}
		urls[assetCID] = url
	}

	return urls, nil
}

// GetAssetStatus retrieves a asset status
func (u *User) GetAssetStatus(ctx context.Context, assetCID string, config *config.SchedulerCfg) (*types.AssetStatus, error) {
	hash, err := cidutil.CIDToHash(assetCID)
	if err != nil {
		return nil, err
	}

	ret := &types.AssetStatus{}
	expiration, err := u.GetAssetExpiration(hash, u.ID)
	if err != nil {
		if err == sql.ErrNoRows {
			return ret, nil
		}
		return nil, err
	}

	ret.IsExist = true
	if expiration.Before(time.Now()) {
		ret.IsExpiration = true
		return ret, nil
	}

	userInfo, err := u.LoadUserInfo(u.ID)
	if err != nil {
		return nil, err
	}

	if userInfo.EnableVIP {
		return ret, nil
	}

	count, err := u.GetAssetVisitCount(hash)
	if err != nil {
		return nil, err
	}

	if count >= config.MaxCountOfVisitShareLink {
		ret.IsVisitOutOfLimit = true
	}

	return ret, nil
}

func (u *User) decodeAPIKeys(buf []byte) (map[string]types.UserAPIKeysInfo, error) {
	apiKeys := make(map[string]types.UserAPIKeysInfo)

	buffer := bytes.NewBuffer(buf)
	dec := gob.NewDecoder(buffer)
	err := dec.Decode(&apiKeys)
	if err != nil {
		return nil, err
	}
	return apiKeys, nil
}

func (u *User) encodeAPIKeys(apiKeys map[string]types.UserAPIKeysInfo) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(apiKeys)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func generateAPIKey(userID string, commonAPI api.Common) (string, error) {
	payload := types.JWTPayload{ID: userID, Allow: []auth.Permission{api.RoleUser}}
	tk, err := commonAPI.AuthNew(context.Background(), &payload)
	if err != nil {
		return "", err
	}

	return tk, nil
}

func generateAccessToken(auth *types.AuthUserUploadDownloadAsset, commonAPI api.Common) (string, error) {
	buf, err := json.Marshal(auth)
	if err != nil {
		return "", err
	}

	payload := types.JWTPayload{Extend: string(buf)}
	tk, err := commonAPI.AuthNew(context.Background(), &payload)
	if err != nil {
		return "", err
	}

	return tk, nil
}
