package scheduler

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/terrors"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/scheduler/assets"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/docker/go-units"
	"github.com/filecoin-project/go-jsonrpc/auth"
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

// ResetAssetReplicaCount resets the replica count for an asset with the specified CID
func (s *Scheduler) ResetAssetReplicaCount(ctx context.Context, cid string, count int) error {
	if count > assets.AssetEdgeReplicasLimit || count < 1 {
		return xerrors.Errorf("ResetAssetReplicaCount count %d not meeting the requirements", count)
	}

	return s.AssetManager.ResetAssetReplicaCount(cid, count)
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

	list, err := s.db.LoadReplicasByStatus(hash, []types.ReplicaStatus{types.ReplicaStatusPulling, types.ReplicaStatusWaiting, types.ReplicaStatusSucceeded})
	if err != nil {
		log.Errorf("GetAssetRecordInfo hash:%s, LoadReplicasByStatus err:%s", hash, err.Error())
	} else {
		for _, info := range list {
			if info.Status == types.ReplicaStatusSucceeded {
				dInfo.ReplicaInfos = append(dInfo.ReplicaInfos, info)
			} else {
				dInfo.PullingReplicaInfos = append(dInfo.PullingReplicaInfos, info)
			}
		}
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

// GetAssetRecordsWithCIDs retrieves a list of asset records with cid
func (s *Scheduler) GetAssetRecordsWithCIDs(ctx context.Context, cids []string) ([]*types.AssetRecord, error) {
	rows, err := s.db.LoadAssetRecordRowsWithCID(cids, s.ServerID)
	if err != nil {
		return nil, xerrors.Errorf("LoadAssetRecords err:%s", err.Error())
	}
	defer rows.Close()

	aList := make([]*types.AssetRecord, 0)

	// loading assets to local
	for rows.Next() {
		dInfo := &types.AssetRecord{}
		err = rows.StructScan(dInfo)
		if err != nil {
			log.Errorf("asset StructScan err: %s", err.Error())
			continue
		}

		list, err := s.db.LoadReplicasByStatus(dInfo.Hash, []types.ReplicaStatus{types.ReplicaStatusPulling, types.ReplicaStatusWaiting, types.ReplicaStatusSucceeded})
		if err != nil {
			log.Errorf("GetAssetRecordInfo hash:%s, LoadReplicasByStatus err:%s", dInfo.Hash, err.Error())
		} else {
			for _, info := range list {
				if info.Status == types.ReplicaStatusSucceeded {
					dInfo.ReplicaInfos = append(dInfo.ReplicaInfos, info)
				} else {
					dInfo.PullingReplicaInfos = append(dInfo.PullingReplicaInfos, info)
				}
			}
		}

		aList = append(aList, dInfo)
	}

	return aList, nil
}

// GetAssetRecords lists asset records with optional filtering by status, limit, and offset.
func (s *Scheduler) GetAssetRecords(ctx context.Context, limit, offset int, statuses []string, serverID dtypes.ServerID) ([]*types.AssetRecord, error) {
	if serverID == "" {
		serverID = s.ServerID
	}

	rows, err := s.db.LoadAssetRecordRows(statuses, limit, offset, serverID)
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

// RemoveAssetRecords removes an asset record from the system by its CID.
func (s *Scheduler) RemoveAssetRecords(ctx context.Context, cids []string) error {
	if len(cids) == 0 {
		return xerrors.Errorf("Cid Is Nil")
	}

	for _, cid := range cids {
		hash, err := cidutil.CIDToHash(cid)
		if err != nil {
			continue
		}

		err = s.AssetManager.RemoveAsset(hash, false)
	}

	return nil
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
	if len(info.CIDs) == 0 {
		return xerrors.New("Cid is Nil")
	}

	if info.Replicas < 1 {
		return xerrors.Errorf("replicas %d must greater than 1", info.Replicas)
	}

	if time.Now().After(info.Expiration) {
		return xerrors.Errorf("expiration %s less than now(%v)", info.Expiration.String(), time.Now())
	}

	if info.Replicas > assets.AssetEdgeReplicasLimit {
		return xerrors.Errorf("The number of replicas %d exceeds the limit %d", info.Replicas, assets.AssetEdgeReplicasLimit)
	}

	if info.Bandwidth > assets.AssetBandwidthLimit {
		return xerrors.Errorf("The number of bandwidthDown %d exceeds the limit %d", info.Bandwidth, assets.AssetBandwidthLimit)
	}

	list := make([]types.AssetDataInfo, 0)

	for _, cid := range info.CIDs {
		hash, err := cidutil.CIDToHash(cid)
		if err != nil {
			return xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
		}

		list = append(list, types.AssetDataInfo{Cid: cid, Replicas: info.Replicas, Owner: info.Owner, Expiration: info.Expiration, Hash: hash, Status: 0})
	}

	return s.db.SaveAssetData(list)
	// return s.AssetManager.CreateAssetPullTask(info) // TODO UserID
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

	info, err := s.db.LoadNodeReplicasByNode(nodeID, limit, offset, statuses)
	if err != nil {
		return nil, xerrors.Errorf("GetReplicasForNode err:%s", err.Error())
	}

	return info, nil
}

// GetReplicaEventsForNode retrieves a replica event list of node
func (s *Scheduler) GetReplicaEventsForNode(ctx context.Context, nodeID string, limit, offset int) (*types.ListAssetReplicaEventRsp, error) {
	info, err := s.db.LoadReplicaEventsOfNode(nodeID, limit, offset)
	if err != nil {
		return nil, xerrors.Errorf("LoadReplicaEvents err:%s", err.Error())
	}

	return info, nil
}

// GetReplicaEvents retrieves replica events within a specified time range.
func (s *Scheduler) GetReplicaEvents(ctx context.Context, start, end time.Time, limit, offset int) (*types.ListAssetReplicaEventRsp, error) {
	res := new(types.ListAssetReplicaEventRsp)
	return res, nil
}

// CreateAsset creates an asset with car CID, car name, and car size.
func (s *Scheduler) CreateAsset(ctx context.Context, req *types.CreateAssetReq) (*types.UploadInfo, error) {
	if req == nil {
		return nil, &api.ErrWeb{Code: terrors.ParametersAreWrong.Int()}
	}

	hash, err := cidutil.CIDToHash(req.AssetCID)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.CidToHashFiled.Int(), Message: err.Error()}
	}

	if hash == "1220e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		return nil, &api.ErrWeb{Code: terrors.InvalidAsset.Int(), Message: "Uploading empty files is not allowed"}
	}

	return s.AssetManager.CreateAssetUploadTask(hash, req)
}

// CreateSyncAsset Synchronizing assets from other schedulers
func (s *Scheduler) CreateSyncAsset(ctx context.Context, req *types.CreateSyncAssetReq) error {
	if req == nil {
		return &api.ErrWeb{Code: terrors.ParametersAreWrong.Int()}
	}

	hash, err := cidutil.CIDToHash(req.AssetCID)
	if err != nil {
		return &api.ErrWeb{Code: terrors.CidToHashFiled.Int(), Message: err.Error()}
	}

	return s.AssetManager.CreateSyncAssetTask(hash, req)
}

// GenerateTokenForDownloadSources Generate Token For Download Source
func (s *Scheduler) GenerateTokenForDownloadSources(ctx context.Context, cid string) ([]*types.SourceDownloadInfo, error) {
	_, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.CidToHashFiled.Int(), Message: err.Error()}
	}

	return s.AssetManager.GenerateTokenForDownloadSources(cid)
}

// GenerateTokenForDownloadSource Generate Token For Download Source
func (s *Scheduler) GenerateTokenForDownloadSource(ctx context.Context, nodeID string, cid string) (*types.SourceDownloadInfo, error) {
	_, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.CidToHashFiled.Int(), Message: err.Error()}
	}

	return s.AssetManager.GenerateTokenForDownloadSource(nodeID, cid)
}

// ShareAssetV2  shares the file
func (s *Scheduler) ShareAssetV2(ctx context.Context, info *types.ShareAssetReq) (*types.ShareAssetRsp, error) {
	assetCID := info.AssetCID
	expireTime := info.ExpireTime

	out := &types.ShareAssetRsp{}

	rsp, _, count, err := s.getDownloadInfos(assetCID, false)
	if err != nil {
		log.Errorf("ShareAssetV2 %s getDownloadInfos err:%s \n", assetCID, err.Error())
		return nil, &api.ErrWeb{Code: terrors.NotFound.Int(), Message: err.Error()}
	}
	if len(rsp.SourceList) == 0 {
		log.Errorf("ShareAssetV2 %s rsp.SourceList == 0 \n", assetCID)
		return nil, &api.ErrWeb{Code: terrors.NotFoundNode.Int()}
	}

	payload := &types.AuthUserUploadDownloadAsset{UserID: info.UserID, AssetCID: assetCID, TraceID: info.TraceID}
	if !expireTime.IsZero() {
		payload.Expiration = expireTime
	}

	tk, err := generateAccessToken(payload, info.FilePass, s)
	if err != nil {
		log.Errorf("ShareAssetV2 %s generateAccessToken err:%s \n", assetCID, err.Error())
		return nil, &api.ErrWeb{Code: terrors.GenerateAccessToken.Int()}
	}

	var ret []string

	for _, info := range rsp.SourceList {
		n := s.NodeManager.GetNode(info.NodeID)
		if n != nil {
			url := fmt.Sprintf("http://%s/ipfs/%s?token=%s", info.Address, assetCID, tk)
			if len(n.ExternalURL) > 0 {
				url = fmt.Sprintf("%s/ipfs/%s?token=%s", n.ExternalURL, assetCID, tk)
			}
			ret = append(ret, url)
		}
	}

	out.URLs = ret
	out.NodeCount = count

	return out, err
}

// ShareAssets shares the assets of the user.
// func (s *Scheduler) ShareAssets(ctx context.Context, userID string, assetCIDs []string, expireTime time.Time) (map[string][]string, error) {
// 	urls := make(map[string][]string)
// 	for _, assetCID := range assetCIDs {
// 		rsp, _, err := s.getDownloadInfos(assetCID, true)
// 		if err != nil {
// 			log.Errorf("ShareAssets %s getDownloadInfos err:%s \n", assetCID, err.Error())
// 			return nil, &api.ErrWeb{Code: terrors.NotFound.Int(), Message: err.Error()}
// 		}

// 		if len(rsp.SourceList) == 0 {
// 			log.Errorf("ShareAssets %s rsp.SourceList == 0 \n", assetCID)
// 			return nil, &api.ErrWeb{Code: terrors.NotFoundNode.Int()}
// 		}

// 		payload := &types.AuthUserUploadDownloadAsset{UserID: userID, AssetCID: assetCID}
// 		if !expireTime.IsZero() {
// 			payload.Expiration = expireTime
// 		}

// 		tk, err := generateAccessToken(payload, "", s)
// 		if err != nil {
// 			log.Errorf("ShareAssets %s generateAccessToken err:%s \n", assetCID, err.Error())
// 			return nil, &api.ErrWeb{Code: terrors.GenerateAccessToken.Int()}
// 		}

// 		for _, info := range rsp.SourceList {
// 			n := s.NodeManager.GetCandidateNode(info.NodeID)
// 			if n != nil {
// 				url := fmt.Sprintf("http://%s/ipfs/%s?token=%s", info.Address, assetCID, tk)
// 				if len(n.ExternalURL) > 0 {
// 					url = fmt.Sprintf("%s/ipfs/%s?token=%s", n.ExternalURL, assetCID, tk)
// 				}
// 				urls[assetCID] = append(urls[assetCID], url)
// 			}
// 		}
// 	}

// 	return urls, nil
// }

// ShareEncryptedAsset shares the encrypted file
// func (s *Scheduler) ShareEncryptedAsset(ctx context.Context, userID, assetCID, filePass string, expireTime time.Time) ([]string, error) {
// 	rsp, _, err := s.getDownloadInfos(assetCID, true)
// 	if err != nil {
// 		log.Errorf("ShareEncryptedAsset %s getDownloadInfos err:%s \n", assetCID, err.Error())
// 		return nil, &api.ErrWeb{Code: terrors.NotFound.Int(), Message: err.Error()}
// 	}
// 	if len(rsp.SourceList) == 0 {
// 		log.Errorf("ShareEncryptedAsset %s rsp.SourceList == 0 \n", assetCID)
// 		return nil, &api.ErrWeb{Code: terrors.NotFoundNode.Int()}
// 	}

// 	payload := &types.AuthUserUploadDownloadAsset{UserID: userID, AssetCID: assetCID}
// 	if !expireTime.IsZero() {
// 		payload.Expiration = expireTime
// 	}

// 	tk, err := generateAccessToken(payload, filePass, s)
// 	if err != nil {
// 		log.Errorf("ShareEncryptedAsset %s generateAccessToken err:%s \n", assetCID, err.Error())
// 		return nil, &api.ErrWeb{Code: terrors.GenerateAccessToken.Int()}
// 	}

// 	var ret []string

// 	for _, info := range rsp.SourceList {
// 		n := s.NodeManager.GetCandidateNode(info.NodeID)
// 		if n != nil {
// 			url := fmt.Sprintf("http://%s/ipfs/%s?token=%s", info.Address, assetCID, tk)
// 			if len(n.ExternalURL) > 0 {
// 				url = fmt.Sprintf("%s/ipfs/%s?token=%s", n.ExternalURL, assetCID, tk)
// 			}
// 			ret = append(ret, url)
// 		}
// 	}

// 	return ret, err
// }

// MinioUploadFileEvent handles the Minio file upload event.
func (s *Scheduler) MinioUploadFileEvent(ctx context.Context, event *types.MinioUploadFileEvent) error {
	if event == nil {
		return fmt.Errorf("event can not empty")
	}

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

	return s.db.SaveReplicaEvent(&types.AssetReplicaEventInfo{
		Hash: hash, Cid: event.AssetCID, NodeID: nodeID, TotalSize: event.Size, Event: types.MinioEventAdd, Source: types.AssetSourceMinio,
	}, 1, 0)
}

// AddAWSData saves the provided AWS data information to the database.
func (s *Scheduler) AddAWSData(ctx context.Context, list []types.AWSDataInfo) error {
	return s.db.SaveAWSData(list)
}

// LoadAWSData retrieves AWS data from the database with pagination and distribution options.
func (s *Scheduler) LoadAWSData(ctx context.Context, limit, offset int, isDistribute bool) ([]*types.AWSDataInfo, error) {
	return s.db.ListAWSData(limit, offset, isDistribute)
}

// SwitchFillDiskTimer toggles the fill disk timer based on the open parameter.
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

// RemoveNodeFailedReplica removes failed replicas from nodes.
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

				log.Infof("remove replica node:%s, cid:%s", info.NodeID, cid)

				node.DeleteAsset(context.Background(), cid)
			}
		}()
	}

	return nil
}

func generateAccessToken(auth *types.AuthUserUploadDownloadAsset, passNonce string, commonAPI api.Common) (string, error) {
	buf, err := json.Marshal(auth)
	if err != nil {
		return "", err
	}

	payload := types.JWTPayload{Extend: string(buf), FilePassNonce: passNonce}
	tk, err := commonAPI.AuthNew(context.Background(), &payload)
	if err != nil {
		return "", err
	}

	return tk, nil
}

// UserAssetDownloadResultV2 handles the download result for user assets.
// When a user downloads a file through a shared link, L1 will submit a report
func (s *Scheduler) UserAssetDownloadResultV2(ctx context.Context, info *types.RetrieveEvent) error {
	info.NodeID = handler.GetNodeID(ctx)
	cNode := s.NodeManager.GetNode(info.NodeID)
	if cNode == nil {
		return xerrors.Errorf("UserAssetDownloadResult node not found: %s", info.NodeID)
	}

	s.NodeManager.UpdateNodeBandwidths(info.NodeID, 0, info.Speed)

	log.Infof("UserAssetDownloadResultV2 Hash[%s] ClientID[%s] NodeID[%s] PeakBandwidth[%d] Speed[%d] Size[%d]\n", info.Hash, info.ClientID, info.NodeID, info.PeakBandwidth, info.Speed, info.Size)

	succeededCount := 0
	failedCount := 0

	if info.Status == types.EventStatusSucceed {
		succeededCount = 1

		err := s.db.SaveAssetDownloadResult(&types.AssetDownloadResult{Hash: info.Hash, NodeID: info.NodeID, TotalTraffic: info.Size, PeakBandwidth: info.PeakBandwidth, UserID: info.ClientID})
		if err != nil {
			return err
		}
	} else {
		failedCount = 1
	}

	return s.db.SaveRetrieveEventInfo(info, succeededCount, failedCount)
}

// UserAssetDownloadResult download result
func (s *Scheduler) UserAssetDownloadResult(ctx context.Context, userID, cid string, totalTraffic, peakBandwidth int64) error {
	nodeID := handler.GetNodeID(ctx)
	cNode := s.NodeManager.GetNode(nodeID)
	if cNode == nil {
		return xerrors.Errorf("UserAssetDownloadResult node not found: %s", nodeID)
	}

	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return err
	}

	log.Infof("UserAssetDownloadResult Hash[%s] ClientID[%s] NodeID[%s] PeakBandwidth[%d] Size[%d]\n", hash, userID, nodeID, peakBandwidth, totalTraffic)

	return s.db.SaveAssetDownloadResult(&types.AssetDownloadResult{Hash: hash, NodeID: nodeID, TotalTraffic: totalTraffic, PeakBandwidth: peakBandwidth, UserID: userID})
}

// GetNodeUploadInfoV2 retrieves upload information for a specific user.
func (s *Scheduler) GetNodeUploadInfoV2(ctx context.Context, info *types.GetUploadInfoReq) (*types.UploadInfo, error) {
	userID := info.UserID

	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	return s.getUploadInfo(userID, info.URLMode, info.TraceID)
}

// GetNodeUploadInfo retrieves upload information for a specific user.
func (s *Scheduler) GetNodeUploadInfo(ctx context.Context, userID string, passNonce string, urlMode bool) (*types.UploadInfo, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	return s.getUploadInfo(userID, urlMode, "")
}

func (s *Scheduler) getUploadInfo(userID string, urlMode bool, traceID string) (*types.UploadInfo, error) {
	_, nodes := s.NodeManager.GetResourceCandidateNodes()

	cNodes := make([]*node.Node, 0)
	for _, node := range nodes {
		if node.IsStorageNode && !s.ValidationMgr.IsValidator(node.NodeID) {
			cNodes = append(cNodes, node)
		}
	}

	if len(cNodes) == 0 {
		return nil, &api.ErrWeb{Code: terrors.NodeOffline.Int(), Message: fmt.Sprintf("storage's nodes not found")}
	}

	// sort.Slice(cNodes, func(i, j int) bool {
	// 	return cNodes[i].BandwidthDown > cNodes[j].BandwidthDown
	// })

	// TODO New rules Sort by remaining bandwidth
	sort.Slice(cNodes, func(i, j int) bool {
		return cNodes[i].BandwidthDownScore > cNodes[j].BandwidthDownScore
	})

	// mixup nodes
	// rand.Shuffle(len(cNodes), func(i, j int) { cNodes[i], cNodes[j] = cNodes[j], cNodes[i] })

	ret := &types.UploadInfo{
		List:          make([]*types.NodeUploadInfo, 0),
		AlreadyExists: false,
	}

	// TODO remove FilePassNonce in order to avoid L1-updates
	payload := &types.JWTPayload{Allow: []auth.Permission{api.RoleUser}, ID: userID, TraceID: traceID} //  FilePassNonce: passNonce

	var suffix string = "/uploadv2"
	if urlMode {
		suffix = "/uploadv3"
	}

	for i := 0; i < len(cNodes); i++ {
		cNode := cNodes[i]
		// if cNode.BandwidthDown < units.MiB && len(ret.List) > 2 {
		// 	break
		// }

		// TODO New rules Sort by remaining bandwidth
		if cNode.BandwidthFreeDown < units.MiB && len(ret.List) > 2 {
			break
		}

		if len(ret.List) > 5 {
			break
		}

		token, err := cNode.API.AuthNew(context.Background(), payload)
		if err != nil {
			log.Errorf("getUploadInfo node:[%s] AuthNew err:%s", cNode.NodeID, err.Error())
			// return nil, &api.ErrWeb{Code: terrors.RequestNodeErr.Int(), Message: err.Error()}
			continue
		}

		uploadURL := fmt.Sprintf("http://%s%s", cNode.RemoteAddr, suffix)
		if len(cNode.ExternalURL) > 0 {
			uploadURL = fmt.Sprintf("%s%s", cNode.ExternalURL, suffix)
		}

		// count, err := s.db.LoadPullingReplicaCountNodeID(cNode.NodeID)
		// if err != nil {
		// 	log.Errorf("LoadPullingReplicaCountNodeID %s , err :%s", cNode.NodeID, err.Error())
		// }

		ret.List = append(ret.List, &types.NodeUploadInfo{UploadURL: uploadURL, Token: token, NodeID: cNode.NodeID})

	}

	// sort.Slice(ret.List, func(i, j int) bool {
	// 	return ret.List[i].PullingCount < ret.List[j].PullingCount
	// })
	rand.Shuffle(len(ret.List), func(i, j int) { ret.List[i], ret.List[j] = ret.List[j], ret.List[i] })
	// return &types.UploadInfo{UploadURL: uploadURL, Token: token, NodeID: cNode.NodeID}, nil
	return ret, nil
}

// GetAssetDownloadResults Retrieves Asset Download Results
func (s *Scheduler) GetAssetDownloadResults(ctx context.Context, hash string, start, end time.Time) (*types.ListAssetDownloadRsp, error) {
	info, err := s.db.LoadAssetDownloadResults(hash, start, end)
	if err != nil {
		return nil, xerrors.Errorf("LoadAssetDownloadResults err:%s", err.Error())
	}

	return info, nil
}

// GetDownloadResultsFromAssets Retrieves Asset Download Results
func (s *Scheduler) GetDownloadResultsFromAssets(ctx context.Context, hashes []string, start, end time.Time) ([]*types.AssetDownloadResultRsp, error) {
	return s.db.LoadDownloadResultsFromAsset(ctx, hashes, start, end)
}

// GetActiveAssetRecords retrieves a list of asset records.
func (s *Scheduler) GetActiveAssetRecords(ctx context.Context, offset int, limit int) (*types.ListAssetRecordRsp, error) {
	info := &types.ListAssetRecordRsp{List: make([]*types.AssetRecord, 0)}

	rows, total, err := s.NodeManager.LoadActiveAssetRecords(s.ServerID, limit, offset)
	if err != nil {
		return nil, xerrors.Errorf("LoadNodeInfos err:%s", err.Error())
	}
	defer rows.Close()

	list := make([]*types.AssetRecord, 0)
	for rows.Next() {
		cInfo := &types.AssetRecord{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("NodeInfo StructScan err: %s", err.Error())
			continue
		}

		list = append(list, cInfo)
	}

	info.List = list
	info.Total = total

	return info, nil
}

// GetAssetRecordsByDateRange retrieves a list of asset records.
func (s *Scheduler) GetAssetRecordsByDateRange(ctx context.Context, offset int, limit int, start, end time.Time) (*types.ListAssetRecordRsp, error) {
	info := &types.ListAssetRecordRsp{List: make([]*types.AssetRecord, 0)}

	rows, total, err := s.NodeManager.LoadAssetRecordsByDateRange(s.ServerID, limit, offset, start, end)
	if err != nil {
		return nil, xerrors.Errorf("LoadNodeInfos err:%s", err.Error())
	}
	defer rows.Close()

	list := make([]*types.AssetRecord, 0)
	for rows.Next() {
		cInfo := &types.AssetRecord{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("NodeInfo StructScan err: %s", err.Error())
			continue
		}

		cInfo.SucceededCount, err = s.db.LoadReplicaCountByStatus(cInfo.Hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
		if err != nil {
			log.Errorf("GetAssetRecordsByDateRange hash:%s, LoadReplicaCountByStatus err:%s", cInfo.Hash, err.Error())
		}

		cInfo.FailedCount, err = s.db.LoadReplicaEventCountByStatus(cInfo.Hash, []types.ReplicaEvent{types.ReplicaEventFailed})
		if err != nil {
			log.Errorf("GetAssetRecordsByDateRange hash:%s, LoadReplicaEventCountByStatus err:%s", cInfo.Hash, err.Error())
		}

		list = append(list, cInfo)
	}

	info.List = list
	info.Total = total

	return info, nil
}

// GetSucceededReplicaByCID retrieves succeeded replicas by CID with a specified limit and offset.
func (s *Scheduler) GetSucceededReplicaByCID(ctx context.Context, cid string, limit, offset int) (*types.ListReplicaRsp, error) {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	dInfo, err := s.db.LoadReplicasByHash(hash, limit, offset)
	if err != nil {
		return nil, err
	}

	return dInfo, nil
}

// GetNodeAssetReplicasByHashes retrieves replica by CID and node.
func (s *Scheduler) GetNodeAssetReplicasByHashes(ctx context.Context, nodeID string, hashes []string) ([]*types.ReplicaInfo, error) {
	dInfo, err := s.db.LoadReplicasByHashes(hashes, nodeID)
	if err != nil {
		return nil, err
	}

	return dInfo, nil
}

// GetFailedReplicaByCID retrieves failed replicas by CID with a specified limit and offset.
func (s *Scheduler) GetFailedReplicaByCID(ctx context.Context, cid string, limit, offset int) (*types.ListAssetReplicaEventRsp, error) {
	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	info, err := s.db.LoadReplicaEventsByHash(hash, types.ReplicaEventFailed, limit, offset)
	if err != nil {
		return nil, xerrors.Errorf("LoadReplicaEvents err:%s", err.Error())
	}

	return info, nil
}

// GetSucceededReplicaByNode retrieves succeeded replicas by node ID with a specified limit and offset.
func (s *Scheduler) GetSucceededReplicaByNode(ctx context.Context, nodeID string, limit, offset int) (*types.ListReplicaRsp, error) {
	info, err := s.db.LoadSucceededReplicasByNode(nodeID, limit, offset)
	if err != nil {
		return nil, xerrors.Errorf("GetReplicasForNode err:%s", err.Error())
	}

	return info, nil
}

// GetFailedReplicaByNode retrieves failed replica events for a specific node.
func (s *Scheduler) GetFailedReplicaByNode(ctx context.Context, nodeID string, limit, offset int) (*types.ListAssetReplicaEventRsp, error) {
	info, err := s.db.LoadReplicaEventsByNode(nodeID, types.ReplicaEventFailed, limit, offset)
	if err != nil {
		return nil, xerrors.Errorf("LoadReplicaEvents err:%s", err.Error())
	}

	return info, nil
}

// Function to convert AssetRecord to ReplicaInfo
func convertToReplicaInfo(dInfo *types.AssetRecord) *types.ReplicaInfo {
	// Implement the conversion logic here
	return &types.ReplicaInfo{
		// Map fields from AssetRecord to ReplicaInfo
	}
}
