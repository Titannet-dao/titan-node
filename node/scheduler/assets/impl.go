package assets

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/terrors"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	xerrors "golang.org/x/xerrors"
)

// CreateSyncAssetTask Synchronizing assets from other schedulers
func (m *Manager) CreateSyncAssetTask(hash string, req *types.CreateSyncAssetReq) error {
	m.stateMachineWait.Wait()
	log.Infof("asset event: %s, add sync asset ", req.AssetCID)

	if req.DownloadInfo != nil {
		req.DownloadInfos = append(req.DownloadInfos, req.DownloadInfo)
	}

	if len(req.DownloadInfos) == 0 {
		return &api.ErrWeb{Code: terrors.ParametersAreWrong.Int()}
	}

	bandwidth := int64(0)

	day := req.ExpirationDay
	if day <= 0 || day > 365*5 {
		day = expirationOfStorageAsset
	}
	expiration := time.Now().Add(time.Duration(day) * 24 * time.Hour)

	replicaCount := req.ReplicaCount
	if replicaCount <= 0 || replicaCount > 1000 {
		replicaCount = defaultReplicaCount
	}

	assetRecord, err := m.LoadAssetRecord(hash)
	if err != nil && err != sql.ErrNoRows {
		return &api.ErrWeb{Code: terrors.DatabaseErr.Int(), Message: err.Error()}
	}

	if assetRecord != nil && assetRecord.State != "" && assetRecord.State != Remove.String() && assetRecord.State != SyncFailed.String() {
		m.UpdateAssetRecordExpiration(hash, expiration)

		return nil
	}

	record := &types.AssetRecord{
		Hash:                  hash,
		CID:                   req.AssetCID,
		ServerID:              m.nodeMgr.ServerID,
		NeedEdgeReplica:       replicaCount,
		NeedCandidateReplicas: int64(m.candidateReplicaCount),
		Expiration:            expiration,
		NeedBandwidth:         bandwidth,
		State:                 UploadInit.String(),
		TotalSize:             req.AssetSize,
		CreatedTime:           time.Now(),
		Source:                int64(AssetSourceStorage),
		Owner:                 req.Owner,
	}

	err = m.SaveAssetRecord(record)
	if err != nil {
		return &api.ErrWeb{Code: terrors.DatabaseErr.Int(), Message: err.Error()}
	}

	ss := []*SourceDownloadInfo{}
	for _, s := range req.DownloadInfos {
		ss = append(ss, sourceDownloadInfoFrom(s))
	}
	// create asset task
	rInfo := AssetForceState{
		State:           SeedSync,
		DownloadSources: ss,
	}
	if err := m.assetStateMachines.Send(AssetHash(hash), rInfo); err != nil {
		return &api.ErrWeb{Code: terrors.NotFound.Int(), Message: err.Error()}
	}

	return nil
}

// CreateAssetUploadTask create a new asset upload task
func (m *Manager) CreateAssetUploadTask(hash string, req *types.CreateAssetReq, cNodes []*node.Node) (*types.UploadInfo, error) {
	// Waiting for state machine initialization
	m.stateMachineWait.Wait()
	log.Infof("asset event: %s, add asset ", req.AssetCID)

	bandwidth := int64(0)

	day := req.ExpirationDay
	if day <= 0 || day > 365*5 {
		day = expirationOfStorageAsset
	}
	expiration := time.Now().Add(time.Duration(day) * 24 * time.Hour)

	replicaCount := req.ReplicaCount
	if replicaCount <= 0 || replicaCount > 1000 {
		replicaCount = defaultReplicaCount
	}

	candidateCount := m.candidateReplicaCount
	if req.Test {
		candidateCount = 1
		replicaCount = 0
	}

	assetRecord, err := m.LoadAssetRecord(hash)
	if err != nil && err != sql.ErrNoRows {
		return nil, &api.ErrWeb{Code: terrors.DatabaseErr.Int(), Message: err.Error()}
	}

	if assetRecord != nil && assetRecord.State != "" && assetRecord.State != Remove.String() && assetRecord.State != UploadFailed.String() {
		info := &types.UploadInfo{AlreadyExists: true}
		m.UpdateAssetRecordExpiration(hash, expiration)

		return info, nil
	}

	payload := &types.AuthUserUploadDownloadAsset{
		UserID:     req.UserID,
		AssetCID:   req.AssetCID,
		AssetSize:  req.AssetSize,
		Expiration: time.Now().Add(time.Hour * 24),
		TraceID:    req.TraceID,
	}

	ret := &types.UploadInfo{
		List:          make([]*types.NodeUploadInfo, 0),
		AlreadyExists: false,
	}

	seedIDs := make([]string, 0)
	for i := 0; i < len(cNodes); i++ {
		cNode := cNodes[i]

		token, err := cNode.API.CreateAsset(context.Background(), payload)
		if err != nil {
			log.Errorf("%s CreateAsset err:%s \n", cNode.NodeID, err.Error())
			continue
			// return nil, &api.ErrWeb{Code: terrors.RequestNodeErr.Int(), Message: err.Error()}
		}

		uploadURL := fmt.Sprintf("http://%s/upload", cNode.RemoteAddr)
		if len(cNode.ExternalURL) > 0 {
			uploadURL = fmt.Sprintf("%s/upload", cNode.ExternalURL)
		}

		ret.List = append(ret.List, &types.NodeUploadInfo{UploadURL: uploadURL, Token: token, NodeID: cNode.NodeID})

		seedIDs = append(seedIDs, cNode.NodeID)
	}

	if len(ret.List) == 0 {
		return nil, &api.ErrWeb{Code: terrors.NotFoundNode.Int(), Message: fmt.Sprintf("storage's nodes not found")}
	}

	record := &types.AssetRecord{
		Hash:                  hash,
		CID:                   req.AssetCID,
		ServerID:              m.nodeMgr.ServerID,
		NeedEdgeReplica:       replicaCount,
		NeedCandidateReplicas: int64(candidateCount),
		Expiration:            expiration,
		NeedBandwidth:         bandwidth,
		State:                 UploadInit.String(),
		TotalSize:             req.AssetSize,
		CreatedTime:           time.Now(),
		Source:                int64(AssetSourceStorage),
		Owner:                 req.Owner,
	}

	err = m.SaveAssetRecord(record)
	if err != nil {
		return nil, &api.ErrWeb{Code: terrors.DatabaseErr.Int(), Message: err.Error()}
	}

	// create asset task
	rInfo := AssetForceState{
		State:       UploadInit,
		SeedNodeIDs: seedIDs,
	}
	if err := m.assetStateMachines.Send(AssetHash(hash), rInfo); err != nil {
		return nil, &api.ErrWeb{Code: terrors.NotFound.Int(), Message: err.Error()}
	}

	rand.Shuffle(len(ret.List), func(i, j int) { ret.List[i], ret.List[j] = ret.List[j], ret.List[i] })

	return ret, nil
}

// CreateAssetPullTask create a new asset pull task
func (m *Manager) CreateAssetPullTask(info *types.PullAssetInfo) error {
	// Waiting for state machine initialization
	m.stateMachineWait.Wait()

	if len(m.getPullingAssetList()) >= m.assetPullTaskLimit {
		return xerrors.Errorf("The asset in the pulling exceeds the limit %d, please wait", m.assetPullTaskLimit)
	}

	log.Infof("asset event: %s, add asset replica: %d,expiration: %s", info.CID, info.Replicas, info.Expiration.String())

	assetRecord, err := m.LoadAssetRecord(info.Hash)
	if err != nil && err != sql.ErrNoRows {
		return xerrors.Errorf("LoadAssetRecord err:%s", err.Error())
	}

	if info.CandidateReplicas == 0 {
		info.CandidateReplicas = int64(m.candidateReplicaCount)
	}

	source := AssetSourceIPFS
	note := ""
	if info.Bucket != "" {
		source = AssetSourceAWS
		note = info.Bucket
	}

	if assetRecord == nil {
		record := &types.AssetRecord{
			Hash:                  info.Hash,
			CID:                   info.CID,
			ServerID:              m.nodeMgr.ServerID,
			NeedEdgeReplica:       info.Replicas,
			NeedCandidateReplicas: info.CandidateReplicas,
			Expiration:            info.Expiration,
			NeedBandwidth:         info.Bandwidth,
			State:                 SeedSelect.String(),
			CreatedTime:           time.Now(),
			Note:                  note,
			Source:                int64(source),
			Owner:                 info.Owner,
		}

		err = m.SaveAssetRecord(record)
		if err != nil {
			return xerrors.Errorf("SaveAssetRecord err:%s", err.Error())
		}

		rInfo := AssetForceState{
			State: SeedSelect,
			// Requester:  info.UserID,
			SeedNodeIDs: []string{info.SeedNodeID},
		}

		// create asset task
		return m.assetStateMachines.Send(AssetHash(info.Hash), rInfo)
	}

	if exist, _ := m.assetStateMachines.Has(AssetHash(assetRecord.Hash)); !exist {
		return xerrors.Errorf("No operation rights, the asset belongs to another scheduler %s", assetRecord.Hash)
	}

	// Check if the asset is in servicing state
	if assetRecord.State != Servicing.String() && assetRecord.State != Remove.String() {
		return xerrors.Errorf("asset state is %s , no tasks can be created in this state", assetRecord.State)
	}

	if assetRecord.State == Remove.String() {
		assetRecord.NeedEdgeReplica = 0
		assetRecord.NeedBandwidth = 0
	}

	if info.Replicas <= assetRecord.NeedEdgeReplica && info.Bandwidth <= assetRecord.NeedBandwidth {
		return xerrors.New("No increase in the number of replicas or bandwidth")
	}

	assetRecord.NeedEdgeReplica = info.Replicas
	assetRecord.Expiration = info.Expiration
	assetRecord.NeedBandwidth = info.Bandwidth
	assetRecord.NeedCandidateReplicas = info.CandidateReplicas

	return m.replenishAssetReplicas(assetRecord, 0, info.Bucket, "", SeedSelect, info.SeedNodeID)
}
