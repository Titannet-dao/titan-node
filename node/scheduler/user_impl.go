package scheduler

import (
	"context"
	"fmt"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/node/scheduler/user"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"golang.org/x/xerrors"
)

// UserExists checks if the user exists.
func (s *Scheduler) UserAPIKeysExists(ctx context.Context, userID string) error {
	u := s.newUser(userID)
	keys, err := u.GetAPIKeys(ctx)
	if err != nil {
		return err
	}

	if len(keys) == 0 {
		return fmt.Errorf("user %s api keys not exist", userID)
	}

	return nil
}

// AllocateStorage allocates storage space.
func (s *Scheduler) AllocateStorage(ctx context.Context, userID string) (*types.UserInfo, error) {
	u := s.newUser(userID)

	info, err := u.AllocateStorage(ctx, s.SchedulerCfg.UserFreeStorageSize)
	if err != nil {
		return nil, xerrors.Errorf("AllocateStorage err:%s", err.Error())
	}

	return info, nil
}

// GetUserInfo get user info
func (s *Scheduler) GetUserInfo(ctx context.Context, userID string) (*types.UserInfo, error) {
	return s.loadUserInfo(userID)
}

func (s *Scheduler) loadUserInfo(userID string) (*types.UserInfo, error) {
	u := s.newUser(userID)
	return u.GetInfo()
}

// CreateAPIKey creates a key for the client API.
func (s *Scheduler) CreateAPIKey(ctx context.Context, userID, keyName string) (string, error) {
	u := s.newUser(userID)
	info, err := u.CreateAPIKey(ctx, keyName, s.CommonAPI)
	if err != nil {
		return "", err
	}

	return info, nil
}

// GetAPIKeys get all api key for user.
func (s *Scheduler) GetAPIKeys(ctx context.Context, userID string) (map[string]types.UserAPIKeysInfo, error) {
	u := s.newUser(userID)
	info, err := u.GetAPIKeys(ctx)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func (s *Scheduler) DeleteAPIKey(ctx context.Context, userID, name string) error {
	u := s.newUser(userID)
	return u.DeleteAPIKey(ctx, name)
}

func (s *Scheduler) UpdateShareStatus(ctx context.Context, userID, assetCID string) error {
	u := s.newUser(userID)
	return u.SetAssetAtShareStatus(ctx, assetCID)
}

func (s *Scheduler) newUser(userID string) *user.User {
	return &user.User{ID: userID, SQLDB: s.AssetManager.SQLDB, Manager: s.AssetManager}
}

// UserAssetDownloadResult download result
func (s *Scheduler) UserAssetDownloadResult(ctx context.Context, userID, cid string, totalTraffic, peakBandwidth int64) error {
	nodeID := handler.GetNodeID(ctx)

	cNode := s.NodeManager.GetNode(nodeID)
	if cNode == nil {
		return xerrors.Errorf("UserAssetDownloadResult node not found: %s", nodeID)
	}

	err := s.db.UpdateUserInfo(userID, totalTraffic, 1)
	if err != nil {
		return err
	}

	return s.db.UpdateUserPeakSize(userID, peakBandwidth)
}

func (s *Scheduler) SetUserVIP(ctx context.Context, userID string, enableVIP bool) error {
	storageSize := s.SchedulerCfg.UserFreeStorageSize
	if enableVIP {
		storageSize = s.SchedulerCfg.UserVipStorageSize
	}
	return s.db.UpdateUserVIPAndStorageSize(userID, enableVIP, storageSize)
}

func (s *Scheduler) GetUserAccessToken(ctx context.Context, userID string) (string, error) {
	_, err := s.GetUserInfo(ctx, userID)
	if err != nil {
		return "", err
	}

	payload := types.JWTPayload{ID: userID, Allow: []auth.Permission{api.RoleUser}}
	tk, err := s.AuthNew(ctx, &payload)
	if err != nil {
		return "", err
	}
	return tk, nil
}
