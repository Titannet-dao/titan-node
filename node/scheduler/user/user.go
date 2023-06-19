package user

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
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
func (u *User) AllocateStorage(ctx context.Context, size int64) (*types.StorageSize, error) {
	// TODO check total size of the titan
	if err := u.SaveUserTotalStorageSize(u.ID, size); err != nil {
		return nil, err
	}
	return u.GetStorageSize(ctx)
}

// GetStorage get size of user storage
func (u *User) GetStorageSize(ctx context.Context) (*types.StorageSize, error) {
	return u.LoadUserStorageSize(u.ID)
}

// CreateAPIKey creates a key for the client API.
func (u *User) CreateAPIKey(ctx context.Context, keyName string, commonAPI api.Common) (string, error) {
	buf, err := u.LoadUserAPIKeys(u.ID)
	if err != nil {
		return "", err
	}

	apiKeys := make(map[string]string)
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
	apiKeys[keyName] = keyValue

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
func (u *User) GetAPIKeys(ctx context.Context) (map[string]string, error) {
	buf, err := u.LoadUserAPIKeys(u.ID)
	if err != nil {
		return nil, err
	}

	apiKeys := make(map[string]string)
	if len(buf) > 0 {
		apiKeys, err = u.decodeAPIKeys(buf)
		if err != nil {
			return nil, err
		}
	}

	return apiKeys, nil
}

func (u *User) DeleteAPIKey(ctx context.Context, name string) error {
	buf, err := u.LoadUserAPIKeys(u.ID)
	if err != nil {
		return err
	}

	apiKeys := make(map[string]string)
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
		return nil, xerrors.Errorf("%s cid to hash err:%s", req.AssetCID, err.Error())
	}

	storageSize, err := u.LoadUserStorageSize(req.UserID)
	if err != nil {
		return nil, xerrors.Errorf("LoadUserStorageSize err:%s", err.Error())
	}

	if storageSize.TotalSize-storageSize.UsedSize < req.AssetSize {
		return nil, xerrors.New("user storage size not enough")
	}

	return u.Manager.CreateAssetUploadTask(hash, req)
}

// ListAssets lists the assets of the user.
func (u *User) ListAssets(ctx context.Context, limit, offset int) ([]*types.AssetRecord, error) {
	aRows, err := u.ListAssetsForUser(u.ID, limit, offset)
	if err != nil {
		log.Errorf("ListAssetsForUser err:%s", err.Error())
		return nil, err
	}
	defer aRows.Close()

	list := make([]*types.AssetRecord, 0)
	// loading asset records
	for aRows.Next() {
		cInfo := &types.AssetRecord{}
		err = aRows.StructScan(cInfo)
		if err != nil {
			log.Errorf("asset StructScan err: %s", err.Error())
			continue
		}

		stateInfo, err := u.LoadAssetStateInfo(cInfo.Hash, cInfo.ServerID)
		if err != nil {
			log.Errorf("asset LoadAssetState err: %s", err.Error())
			continue
		}

		cInfo.ReplicaInfos, err = u.LoadReplicasByStatus(cInfo.Hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
		if err != nil {
			log.Errorf("asset %s load replicas err: %s", cInfo.CID, err.Error())
			continue
		}

		cInfo.State = stateInfo.State

		list = append(list, cInfo)
	}

	return list, nil
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
		err = u.Manager.RemoveAsset(hash)
		if err != nil {
			return err
		}
	}

	info, err := u.LoadAssetRecord(hash)
	if err != nil {
		return err
	}

	return u.DeleteAssetUser(hash, u.ID, info.TotalSize)
}

// ShareAssets shares the assets of the user.
func (u *User) ShareAssets(ctx context.Context, assetCIDs []string, schedulerAPI api.Scheduler, domain string) (map[string]string, error) {
	// TODO　check asset if belong to user
	urls := make(map[string]string)
	if len(domain) > 0 {
		for _, assetCID := range assetCIDs {
			tk, err := generateAccessToken(&types.AuthUserDownloadAsset{UserID: u.ID, AssetCID: assetCID}, schedulerAPI.(api.Common))
			if err != nil {
				return nil, err
			}
			url := fmt.Sprintf("http://%s.%s?token=%s", assetCID, domain, tk)
			urls[assetCID] = url
		}

		return urls, nil
	}

	for _, assetCID := range assetCIDs {
		downloadInfos, err := schedulerAPI.GetCandidateDownloadInfos(context.Background(), assetCID)
		if err != nil {
			return nil, err
		}

		if len(downloadInfos) == 0 {
			return nil, fmt.Errorf("asset %s not exist", assetCID)
		}

		tk, err := generateAccessToken(&types.AuthUserDownloadAsset{UserID: u.ID, AssetCID: assetCID}, schedulerAPI.(api.Common))
		if err != nil {
			return nil, err
		}

		url := fmt.Sprintf("http://%s/ipfs/%s?token=%s", downloadInfos[0].Address, assetCID, tk)
		urls[assetCID] = url
	}

	return urls, nil
}

func (u *User) decodeAPIKeys(buf []byte) (map[string]string, error) {
	apiKeys := make(map[string]string)

	buffer := bytes.NewBuffer(buf)
	dec := gob.NewDecoder(buffer)
	err := dec.Decode(&apiKeys)
	if err != nil {
		return nil, err
	}
	return apiKeys, nil
}

func (u *User) encodeAPIKeys(apiKeys map[string]string) ([]byte, error) {
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

func generateAccessToken(auth *types.AuthUserDownloadAsset, commonAPI api.Common) (string, error) {
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
