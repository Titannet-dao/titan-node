package scheduler

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"golang.org/x/xerrors"
)

// NodeRemoveAssetResult updates a node's disk usage and block count based on the resultInfo.
func (s *Scheduler) NodeRemoveAssetResult(ctx context.Context, resultInfo types.RemoveAssetResult) error {
	nodeID := handler.GetNodeID(ctx)

	// update node info
	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.DiskUsage = resultInfo.DiskUsage
	}

	return nil
}

// RePullFailedAssets retries the pull process for a list of failed assets
func (s *Scheduler) RePullFailedAssets(ctx context.Context, hashes []types.AssetHash) error {
	return s.AssetManager.RestartPullAssets(hashes)
}

// UpdateAssetExpiration resets the expiration time of an asset record based on the provided CID and new expiration time.
func (s *Scheduler) UpdateAssetExpiration(ctx context.Context, cid string, t time.Time) error {
	if time.Now().After(t) {
		return xerrors.Errorf("expiration:%s has passed", t.String())
	}

	return s.AssetManager.UpdateAssetExpiration(cid, t)
}

// GetAssetRecord retrieves an asset record by its CID.
func (s *Scheduler) GetAssetRecord(ctx context.Context, cid string) (*types.AssetRecord, error) {
	info, err := s.AssetManager.GetAssetRecordInfo(cid)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// GetAssetRecords lists asset records with optional filtering by status, limit, and offset.
func (s *Scheduler) GetAssetRecords(ctx context.Context, limit, offset int, statuses []string, serverID dtypes.ServerID) ([]*types.AssetRecord, error) {
	if serverID == "" {
		serverID = s.ServerID
	}

	rows, err := s.db.LoadAssetRecords(statuses, limit, offset, serverID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]*types.AssetRecord, 0)

	// loading assets to local
	for rows.Next() {
		cInfo := &types.AssetRecord{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("asset StructScan err: %s", err.Error())
			continue
		}

		cInfo.ReplicaInfos, err = s.db.LoadReplicasByStatus(cInfo.Hash, types.ReplicaStatusAll)
		if err != nil {
			log.Errorf("asset %s load replicas err: %s", cInfo.CID, err.Error())
			continue
		}

		list = append(list, cInfo)
	}

	return list, nil
}

// RemoveAssetRecord removes an asset record from the system by its CID.
func (s *Scheduler) RemoveAssetRecord(ctx context.Context, cid string) error {
	if cid == "" {
		return xerrors.Errorf("Cid Is Nil")
	}

	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return err
	}

	return s.AssetManager.RemoveAsset(hash) // TODO UserID
}

// RemoveAssetReplica removes an asset replica from the system by its CID and nodeID.
func (s *Scheduler) RemoveAssetReplica(ctx context.Context, cid, nodeID string) error {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return err
	}

	return s.AssetManager.RemoveReplica(cid, hash, nodeID)
}

// PullAsset pull an asset based on the provided PullAssetReq structure.
func (s *Scheduler) PullAsset(ctx context.Context, info *types.PullAssetReq) error {
	if info.CID == "" {
		return xerrors.New("Cid is Nil")
	}

	hash, err := cidutil.CIDToHash(info.CID)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:%s", info.CID, err.Error())
	}

	info.Hash = hash

	if info.Replicas < 1 {
		return xerrors.Errorf("replicas %d must greater than 1", info.Replicas)
	}

	if time.Now().After(info.Expiration) {
		return xerrors.Errorf("expiration %s less than now(%v)", info.Expiration.String(), time.Now())
	}

	return s.AssetManager.CreateAssetPullTask(info, "") // TODO UserID
}

// GetAssetListForBucket retrieves a list of asset hashes for the specified node's bucket.
func (s *Scheduler) GetAssetListForBucket(ctx context.Context, bucketID uint32) ([]string, error) {
	nodeID := handler.GetNodeID(ctx)
	id := fmt.Sprintf("%s:%d", nodeID, bucketID)
	hashBytes, err := s.NodeManager.LoadBucket(id)
	if err != nil {
		return nil, err
	}

	if len(hashBytes) == 0 {
		return make([]string, 0), nil
	}

	buffer := bytes.NewBuffer(hashBytes)
	dec := gob.NewDecoder(buffer)

	out := make([]string, 0)
	if err = dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetAssetCount retrieves a count of asset
func (s *Scheduler) GetAssetCount(ctx context.Context) (int, error) {
	return s.AssetManager.GetAssetCount()
}

// GetAssetsForNode retrieves a asset list of node
func (s *Scheduler) GetAssetsForNode(ctx context.Context, nodeID string, limit, offset int) (*types.ListNodeAssetRsp, error) {
	log.Debugf("GetAssetsForNode start time %s", time.Now().Format("2006-01-02 15:04:05"))
	defer log.Debugf("GetAssetsForNode end time %s", time.Now().Format("2006-01-02 15:04:05"))

	return s.db.LoadReplicas(nodeID, limit, offset)
}

// GetReplicaEventsForNode retrieves a replica event list of node
func (s *Scheduler) GetReplicaEventsForNode(ctx context.Context, nodeID string, limit, offset int) (*types.ListReplicaEventRsp, error) {
	log.Debugf("GetReplicaEventsForNode start time %s", time.Now().Format("2006-01-02 15:04:05"))
	defer log.Debugf("GetReplicaEventsForNode end time %s", time.Now().Format("2006-01-02 15:04:05"))

	return s.db.LoadReplicaEvents(nodeID, limit, offset)
}

// CreateUserAsset creates an user asset with car CID, car name, and car size.
func (s *Scheduler) CreateUserAsset(ctx context.Context, assetCID, assetName string, assetSize int64) (*types.CreateAssetRsp, error) {
	userID := handler.GetUserID(ctx)
	if len(userID) == 0 {
		return nil, fmt.Errorf("CreateUserAsset failed, can not get user id")
	}
	return s.CreateAsset(ctx, &types.CreateAssetReq{UserID: userID, AssetCID: assetCID, AssetName: assetName, AssetSize: assetSize})
}

func (s *Scheduler) ListUserAssets(ctx context.Context, limit, offset int) ([]*types.AssetRecord, error) {
	userID := handler.GetUserID(ctx)
	if len(userID) == 0 {
		return nil, fmt.Errorf("ListUserAssets failed, can not get user id")
	}
	return s.ListAssets(ctx, userID, limit, offset)
}

// DeleteUserAsset deletes the asset of the user.
func (s *Scheduler) DeleteUserAsset(ctx context.Context, assetCID string) error {
	userID := handler.GetUserID(ctx)
	if len(userID) == 0 {
		return fmt.Errorf("DeleteUserAsset failed, can not get user id")
	}
	return s.DeleteAsset(ctx, userID, assetCID)
}

// ShareUserAssets shares the assets of the user.
func (s *Scheduler) ShareUserAssets(ctx context.Context, assetCID []string) (map[string]string, error) {
	userID := handler.GetUserID(ctx)
	if len(userID) == 0 {
		return nil, fmt.Errorf("ShareUserAssets failed, can not get user id")
	}
	return s.ShareAssets(ctx, userID, assetCID)
}

// CreateAsset creates an asset with car CID, car name, and car size.
func (s *Scheduler) CreateAsset(ctx context.Context, req *types.CreateAssetReq) (*types.CreateAssetRsp, error) {
	u := s.newUser(req.UserID)
	return u.CreateAsset(ctx, req)
}

// ListAssets lists the assets of the user.
func (s *Scheduler) ListAssets(ctx context.Context, userID string, limit, offset int) ([]*types.AssetRecord, error) {
	u := s.newUser(userID)
	return u.ListAssets(ctx, limit, offset)
}

// DeleteAsset deletes the assets of the user.
func (s *Scheduler) DeleteAsset(ctx context.Context, userID, assetCID string) error {
	u := s.newUser(userID)
	return u.DeleteAsset(ctx, assetCID)
}

// ShareAssets shares the assets of the user.
func (s *Scheduler) ShareAssets(ctx context.Context, userID string, assetCIDs []string) (map[string]string, error) {
	u := s.newUser(userID)
	return u.ShareAssets(ctx, assetCIDs, s, s.SchedulerCfg.AssetDomain)
}
