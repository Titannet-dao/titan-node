package scheduler

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"math"
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
	s.NodeManager.UpdateNodeDiskUsage(nodeID, resultInfo.DiskUsage)
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
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, err
	}

	dInfo, err := s.db.LoadAssetRecord(hash)
	if err != nil {
		return nil, err
	}

	dInfo.ReplicaInfos, err = s.db.LoadReplicasByStatus(hash, types.ReplicaStatusAll)
	if err != nil {
		log.Errorf("GetAssetRecordInfo hash:%s, LoadAssetReplicas err:%s", hash, err.Error())
	}

	return dInfo, nil
}

// GetReplicas list asset replicas by CID.
func (s *Scheduler) GetReplicas(ctx context.Context, cid string, limit, offset int) (*types.ListReplicaRsp, error) {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, err
	}

	dInfo, err := s.db.LoadReplicasByHash(hash, limit, offset)
	if err != nil {
		return nil, err
	}

	return dInfo, nil
}

// GetAssetRecords lists asset records with optional filtering by status, limit, and offset.
func (s *Scheduler) GetAssetRecords(ctx context.Context, limit, offset int, statuses []string, serverID dtypes.ServerID) ([]*types.AssetRecord, error) {
	if serverID == "" {
		serverID = s.ServerID
	}

	rows, err := s.db.LoadAssetRecords(statuses, limit, offset, serverID)
	if err != nil {
		return nil, xerrors.Errorf("LoadAssetRecords err:%s", err.Error())
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

		// cInfo.ReplicaInfos, err = s.db.LoadReplicasByStatus(cInfo.Hash, types.ReplicaStatusAll)
		// if err != nil {
		// 	log.Errorf("asset %s load replicas err: %s", cInfo.CID, err.Error())
		// 	continue
		// }

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

	return s.AssetManager.RemoveAsset(hash, true) // TODO UserID
}

// StopAssetRecord stop an asset record from the system by its CID.
func (s *Scheduler) StopAssetRecord(ctx context.Context, cids []string) error {
	if len(cids) <= 0 {
		return xerrors.Errorf("Cid Is Nil")
	}

	hashs := make([]string, 0)
	for _, cid := range cids {
		hash, err := cidutil.CIDToHash(cid)
		if err != nil {
			continue
		}

		hashs = append(hashs, hash)
	}

	return s.AssetManager.StopAsset(hashs)
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

	return s.AssetManager.CreateAssetPullTask(info) // TODO UserID
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
	count, err := s.AssetManager.GetAssetCount()
	if err != nil {
		return 0, xerrors.Errorf("GetAssetCount err:%s", err.Error())
	}

	return count, nil
}

// GetAssetsForNode retrieves a asset list of node
func (s *Scheduler) GetAssetsForNode(ctx context.Context, nodeID string, limit, offset int) (*types.ListNodeAssetRsp, error) {
	info, err := s.db.LoadSucceedReplicasByNodeID(nodeID, limit, offset)
	if err != nil {
		return nil, xerrors.Errorf("GetAssetsForNode err:%s", err.Error())
	}

	return info, nil
}

// GetReplicasForNode retrieves a asset list of node
func (s *Scheduler) GetReplicasForNode(ctx context.Context, nodeID string, limit, offset int, statuses []types.ReplicaStatus) (*types.ListNodeReplicaRsp, error) {
	if len(statuses) == 0 {
		return nil, nil
	}

	info, err := s.db.LoadAllReplicasByNodeID(nodeID, limit, offset, statuses)
	if err != nil {
		return nil, xerrors.Errorf("GetReplicasForNode err:%s", err.Error())
	}

	return info, nil
}

// GetReplicaEventsForNode retrieves a replica event list of node
func (s *Scheduler) GetReplicaEventsForNode(ctx context.Context, nodeID string, limit, offset int) (*types.ListReplicaEventRsp, error) {
	info, err := s.db.LoadReplicaEventsOfNode(nodeID, limit, offset)
	if err != nil {
		return nil, xerrors.Errorf("LoadReplicaEvents err:%s", err.Error())
	}

	return info, nil
}

// GetReplicaEvents retrieves a replica event list
func (s *Scheduler) GetReplicaEvents(ctx context.Context, start, end time.Time, limit, offset int) (*types.ListReplicaEventRsp, error) {
	info, err := s.db.LoadReplicaEvents(start, end, limit, offset)
	if err != nil {
		return nil, xerrors.Errorf("LoadReplicaEvents err:%s", err.Error())
	}

	return info, nil
}

// CreateAsset creates an asset with car CID, car name, and car size.
func (s *Scheduler) CreateAsset(ctx context.Context, req *types.CreateAssetReq) (*types.UploadInfo, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		req.UserID = uID
	}

	u := s.newUser(req.UserID)
	return u.CreateAsset(ctx, req)
}

// ListAssets lists the assets of the user.
func (s *Scheduler) ListAssets(ctx context.Context, userID string, limit, offset, groupID int) (*types.ListAssetRecordRsp, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	u := s.newUser(userID)
	info, err := u.ListAssets(ctx, limit, offset, s.SchedulerCfg.MaxCountOfVisitShareLink, groupID)
	if err != nil {
		return nil, xerrors.Errorf("ListAssets err:%s", err.Error())
	}

	return info, nil
}

// DeleteAsset deletes the assets of the user.
func (s *Scheduler) DeleteAsset(ctx context.Context, userID, assetCID string) error {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	u := s.newUser(userID)
	return u.DeleteAsset(ctx, assetCID)
}

// ShareAssets shares the assets of the user.
func (s *Scheduler) ShareAssets(ctx context.Context, userID string, assetCIDs []string) (map[string]string, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	u := s.newUser(userID)
	info, err := u.ShareAssets(ctx, assetCIDs, s, s.NodeManager)
	if err != nil {
		return nil, xerrors.Errorf("ShareAssets err:%s", err.Error())
	}

	return info, nil
}

// GetAssetStatus retrieves a asset status
func (s *Scheduler) GetAssetStatus(ctx context.Context, userID, assetCID string) (*types.AssetStatus, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	u := s.newUser(userID)
	status, err := u.GetAssetStatus(ctx, assetCID, s.SchedulerCfg)
	if err != nil {
		return nil, err
	}

	return status, nil
}

func (s *Scheduler) MinioUploadFileEvent(ctx context.Context, event *types.MinioUploadFileEvent) error {
	// TODO limit rate or verify valid data
	if len(event.AssetCID) == 0 {
		return fmt.Errorf("AssetCID can not empty")
	}

	hash, err := cidutil.CIDToHash(event.AssetCID)
	if err != nil {
		return err
	}

	nodeID := handler.GetNodeID(ctx)

	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.DownloadTraffic += event.Size
	}

	log.Debugf("MinioUploadFileEvent nodeID:%s, assetCID:", nodeID, event.AssetCID)

	return s.db.SaveReplicaEvent(hash, event.AssetCID, nodeID, event.Size, event.Expiration, types.MinioEventAdd, int64(types.AssetSourceMinio))
}

func (s *Scheduler) AddAWSData(ctx context.Context, list []types.AWSDataInfo) error {
	return s.db.SaveAWSData(list)
}

func (s *Scheduler) LoadAWSData(ctx context.Context, limit, offset int, isDistribute bool) ([]*types.AWSDataInfo, error) {
	return s.db.ListAWSData(limit, offset, isDistribute)
}

func (s *Scheduler) SwitchFillDiskTimer(ctx context.Context, open bool) error {
	log.Infof("SwitchFillDiskTimer open:%v", open)
	if open {
		s.AssetManager.StartFillDiskTimer()
	} else {
		s.AssetManager.StopFillDiskTimer()
	}

	return nil
}

func splitSliceIntoChunks(slice []*types.ReplicaInfo, numChunks int) [][]*types.ReplicaInfo {
	chunkSize := int(math.Ceil(float64(len(slice)) / float64(numChunks)))
	chunks := make([][]*types.ReplicaInfo, 0, numChunks)

	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}

	return chunks
}

func (s *Scheduler) RemoveNodeFailedReplica(ctx context.Context) error {
	rList, err := s.db.LoadFailedReplicas()
	if err != nil {
		return err
	}

	log.Infof("remove replica len :%d", len(rList))
	chunks := splitSliceIntoChunks(rList, 10)

	for _, chunk := range chunks {
		list := chunk
		go func() {
			for _, info := range list {
				cid, err := cidutil.HashToCID(info.Hash)
				if err != nil {
					continue
				}

				node := s.NodeManager.GetNode(info.NodeID)
				if node == nil {
					log.Infof("remove replica node offline :%s", info.NodeID)
					continue
				}

				log.Infof("remove replica node :%s", info.NodeID)

				node.DeleteAsset(context.Background(), cid)
			}
		}()
	}

	return nil
}
