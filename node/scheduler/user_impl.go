package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/terrors"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/Filecoin-Titan/titan/node/scheduler/user"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"golang.org/x/xerrors"
)

const (
	userAssetGroupMaxCount = 20
	rootGroup              = 0
)

// UserAPIKeysExists checks if the user exists.
func (s *Scheduler) UserAPIKeysExists(ctx context.Context, userID string) error {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

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
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	return s.loadUserInfo(userID)
}

// GetUserInfos get user infos
func (s *Scheduler) GetUserInfos(ctx context.Context, userIDs []string) (map[string]*types.UserInfo, error) {
	out := make(map[string]*types.UserInfo, 0)
	for _, userID := range userIDs {
		info, err := s.loadUserInfo(userID)
		if err != nil {
			continue
		}

		out[userID] = info
	}

	return out, nil
}

func (s *Scheduler) loadUserInfo(userID string) (*types.UserInfo, error) {
	u := s.newUser(userID)
	return u.GetInfo()
}

// CreateAPIKey creates a key for the client API.
func (s *Scheduler) CreateAPIKey(ctx context.Context, userID, keyName string, perms []types.UserAccessControl) (string, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	u := s.newUser(userID)
	info, err := u.CreateAPIKey(ctx, keyName, perms, s.SchedulerCfg, s.CommonAPI)
	if err != nil {
		return "", err
	}

	return info, nil
}

// GetAPIKeys get all api key for user.
func (s *Scheduler) GetAPIKeys(ctx context.Context, userID string) (map[string]types.UserAPIKeysInfo, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	u := s.newUser(userID)
	info, err := u.GetAPIKeys(ctx)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func (s *Scheduler) DeleteAPIKey(ctx context.Context, userID, name string) error {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	u := s.newUser(userID)
	return u.DeleteAPIKey(ctx, name)
}

func (s *Scheduler) UpdateShareStatus(ctx context.Context, userID, assetCID string) error {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

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
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	storageSize := s.SchedulerCfg.UserFreeStorageSize
	if enableVIP {
		storageSize = s.SchedulerCfg.UserVipStorageSize
	}
	return s.db.UpdateUserVIPAndStorageSize(userID, enableVIP, storageSize)
}

func (s *Scheduler) GetUserAccessToken(ctx context.Context, userID string) (string, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	_, err := s.GetUserInfo(ctx, userID)
	if err != nil {
		return "", err
	}

	payload := types.JWTPayload{ID: userID, Allow: []auth.Permission{api.RoleUser}, AccessControlList: types.UserAccessControlAll}
	tk, err := s.AuthNew(ctx, &payload)
	if err != nil {
		return "", err
	}
	return tk, nil
}

// GetUserStorageStats get user storage info
func (s *Scheduler) GetUserStorageStats(ctx context.Context, userID string) (*types.StorageStats, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}
	return s.db.LoadStorageStatsOfUser(userID)
}

// ListUserStorageStats list storage info
func (s *Scheduler) ListUserStorageStats(ctx context.Context, limit, offset int) (*types.ListStorageStatsRsp, error) {
	return s.db.ListStorageStatsOfUsers(limit, offset)
}

// CreateAssetGroup create file group
func (s *Scheduler) CreateAssetGroup(ctx context.Context, userID, name string, parent int) (*types.AssetGroup, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	if parent != rootGroup {
		exist, err := s.db.AssetGroupExists(userID, parent)
		if err != nil {
			return nil, err
		}

		if !exist {
			return nil, &api.ErrWeb{Code: terrors.GroupNotExist.Int(), Message: fmt.Sprintf("CreateAssetGroup failed, group parent [%d] is not exist ", parent)}
		}
	}

	count, err := s.db.GetAssetGroupCount(userID)
	if err != nil {
		return nil, err
	}

	if count >= userAssetGroupMaxCount {
		return nil, &api.ErrWeb{Code: terrors.GroupLimit.Int(), Message: fmt.Sprintf("CreateAssetGroup failed, Exceed the limit %d", userAssetGroupMaxCount)}
	}

	info, err := s.db.CreateAssetGroup(&types.AssetGroup{UserID: userID, Parent: parent, Name: name})
	if err != nil {
		return nil, err
	}

	return info, nil
}

// ListAssetGroup list file group
func (s *Scheduler) ListAssetGroup(ctx context.Context, userID string, parent, limit int, offset int) (*types.ListAssetGroupRsp, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	return s.db.ListAssetGroupForUser(userID, parent, limit, offset)
}

// ListAssetSummary list file group
func (s *Scheduler) ListAssetSummary(ctx context.Context, userID string, parent, limit, offset int) (*types.ListAssetSummaryRsp, error) {
	startTime := time.Now()
	defer func() {
		log.Debugf("ListAssetSummary [userID:%s,parent:%d,limit:%d,offset:%d] request time:%s", userID, parent, limit, offset, time.Since(startTime))
	}()

	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	out := new(types.ListAssetSummaryRsp)

	// list group
	groupRsp, err := s.db.ListAssetGroupForUser(userID, parent, limit, offset)
	if err != nil {
		return nil, xerrors.Errorf("ListAssetGroupForUser err:%s", err.Error())
	}

	for _, group := range groupRsp.AssetGroups {
		i := new(types.UserAssetSummary)
		i.AssetGroup = group
		out.List = append(out.List, i)
	}

	out.Total += groupRsp.Total

	aLimit := limit - len(groupRsp.AssetGroups)
	if aLimit < 0 {
		aLimit = 0
	}

	aOffset := offset - groupRsp.Total
	if aOffset < 0 {
		aOffset = 0
	}

	u := s.newUser(userID)
	assetRsp, err := u.ListAssets(ctx, aLimit, aOffset, s.SchedulerCfg.MaxCountOfVisitShareLink, parent)
	if err != nil {
		return nil, xerrors.Errorf("ListAssets err:%s", err.Error())
	}

	for _, asset := range assetRsp.AssetOverviews {
		i := new(types.UserAssetSummary)
		i.AssetOverview = asset
		out.List = append(out.List, i)
	}

	out.Total += assetRsp.Total

	return out, nil
}

// DeleteAssetGroup delete asset group
func (s *Scheduler) DeleteAssetGroup(ctx context.Context, userID string, gid int) error {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	gCount, err := s.db.GetUserAssetCountByGroupID(userID, gid)
	if err != nil {
		return err
	}

	if gCount > 0 {
		return &api.ErrWeb{Code: terrors.GroupNotEmptyCannotBeDelete.Int(), Message: "There are assets in the group and the group cannot be deleted"}
	}

	rsp, err := s.db.ListAssetGroupForUser(userID, gid, 1, 0)
	if err != nil {
		return err
	}

	if rsp.Total > 0 {
		return &api.ErrWeb{Code: terrors.GroupNotEmptyCannotBeDelete.Int(), Message: "There are assets in the group and the group cannot be deleted"}
	}

	return s.db.DeleteAssetGroup(userID, gid)
}

// RenameAssetGroup rename group
func (s *Scheduler) RenameAssetGroup(ctx context.Context, userID, newName string, groupID int) error {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	return s.db.UpdateAssetGroupName(userID, newName, groupID)
}

// MoveAssetToGroup move a file to group
func (s *Scheduler) MoveAssetToGroup(ctx context.Context, userID, cid string, groupID int) error {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return err
	}

	return s.db.UpdateAssetGroup(hash, userID, groupID)
}

// MoveAssetGroup move a asset group
func (s *Scheduler) MoveAssetGroup(ctx context.Context, userID string, groupID, targetGroupID int) error {
	startTime := time.Now()
	defer func() {
		log.Debugf("MoveAssetGroup [userID:%s,gid:%d,targetGroupID:%d] request time:%s", userID, groupID, targetGroupID, time.Since(startTime))
	}()

	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	if groupID == rootGroup {
		return &api.ErrWeb{Code: terrors.RootGroupCannotMoved.Int(), Message: "the root group cannot be moved"}
	}

	if groupID == targetGroupID {
		return &api.ErrWeb{Code: terrors.GroupsAreSame.Int(), Message: "groups are the same"}
	}

	if targetGroupID != rootGroup {
		exist, err := s.db.AssetGroupExists(userID, targetGroupID)
		if err != nil {
			return err
		}

		if !exist {
			return &api.ErrWeb{Code: terrors.GroupNotExist.Int(), Message: fmt.Sprintf("MoveAssetGroup failed, group parent [%d] is not exist ", targetGroupID)}
		}

		// Prevent loops
		gid := targetGroupID
		for {
			gid, err = s.db.GetAssetGroupParent(gid)
			if err != nil {
				return err
			}

			if gid == groupID {
				return &api.ErrWeb{Code: terrors.CannotMoveToSubgroup.Int(), Message: "cannot move to subgroup"}
			}

			if gid == rootGroup {
				break
			}
		}
	}

	return s.db.UpdateAssetGroupParent(userID, groupID, targetGroupID)
}

// GetAPPKeyPermissions get the permission of user app key
func (s *Scheduler) GetAPPKeyPermissions(ctx context.Context, userID string, keyName string) ([]string, error) {
	keyMap, err := s.GetAPIKeys(ctx, userID)
	if err != nil {
		return nil, err
	}

	key, ok := keyMap[keyName]
	if !ok {
		return nil, &api.ErrWeb{Code: terrors.APPKeyNotFound.Int(), Message: fmt.Sprintf("the API key %s already exist", keyName)}
	}

	payload, err := s.AuthVerify(ctx, key.APIKey)
	if err != nil {
		return nil, err
	}

	permissions := make([]string, 0, len(payload.AccessControlList))
	for _, accessControl := range payload.AccessControlList {
		permissions = append(permissions, string(accessControl))
	}
	return permissions, nil
}

func (s *Scheduler) GetNodeUploadInfo(ctx context.Context, userID string) (*types.UploadInfo, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	_, nodes := s.NodeManager.GetAllCandidateNodes()

	cNodes := make([]*node.Node, 0)
	for _, node := range nodes {
		if node.IsStorageOnly {
			cNodes = append(cNodes, node)
		}
	}

	if len(cNodes) == 0 {
		return nil, &api.ErrWeb{Code: terrors.NodeOffline.Int(), Message: fmt.Sprintf("storage's nodes not found")}
	}

	index := rand.Intn(len(cNodes))
	cNode := cNodes[index]

	if cNode == nil {
		return nil, &api.ErrWeb{Code: terrors.NodeOffline.Int(), Message: fmt.Sprintf("storage's nodes not found")}
	}

	payload := &types.JWTPayload{Allow: []auth.Permission{api.RoleUser}, ID: userID}

	token, err := cNode.API.AuthNew(context.Background(), payload)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.RequestNodeErr.Int(), Message: err.Error()}
	}

	uploadURL := fmt.Sprintf("http://%s/uploadv2", cNode.RemoteAddr)
	if len(cNode.ExternalURL) > 0 {
		uploadURL = fmt.Sprintf("%s/uploadv2", cNode.ExternalURL)
	}

	return &types.UploadInfo{UploadURL: uploadURL, Token: token, NodeID: cNode.NodeID}, nil
}
