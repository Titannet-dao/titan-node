package validation

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
)

const (
	validationInterval = 30 * time.Minute // validation start-up time interval

	duration = 10 // Validation duration per node (Unit:Second)

	// Processing validation result data from 30 days ago
	vResultDay = 30 * oneDay

	trafficProfitLimit = units.GiB // GB
)

func (m *Manager) startValidationTicker() {
	nextTick := time.Now().Truncate(validationInterval)
	if nextTick.Before(time.Now()) {
		nextTick = nextTick.Add(validationInterval)
	}

	time.Sleep(time.Until(nextTick))

	ticker := time.NewTicker(validationInterval)
	defer ticker.Stop()

	doFunc := func(t time.Time) {
		hour := t.Hour()
		log.Infof("start validation ------------- %d:%d  (%d != 0)[%v] [%v] \n", hour, t.Minute(), hour%2, hour%2 != 0, t.Minute() == 0)

		if hour%2 != 0 && t.Minute() == 0 {
			m.doValidate()
		} else {
			m.cleanValidator()

			nodes := m.nodeMgr.GetResourceEdgeNodes()
			m.computeNodeProfits(nodes)
		}
	}

	doFunc(time.Now())

	for {
		t := <-ticker.C
		doFunc(t)
	}
}

func (m *Manager) computeNodeProfits(nodes []*node.Node) {
	if nodes == nil || len(nodes) == 0 {
		return
	}

	for _, node := range nodes {
		rsp, err := m.nodeMgr.LoadValidationResultInfos(node.NodeID, 20, 0)
		if err != nil || len(rsp.ValidationResultInfos) == 0 {
			log.Warnf("%s LoadValidationResultInfos err:%v", node.NodeID, err)
			continue
		}

		limit := 10
		useLen := 0
		size := 0.0

		for _, info := range rsp.ValidationResultInfos {
			if info.Status == types.ValidationStatusCancel ||
				info.Status == types.ValidationStatusCreate ||
				info.Status == types.ValidationStatusValidatorMismatch {
				continue
			}

			useLen++
			size += info.Bandwidth * float64(info.Duration)

			if useLen >= limit {
				break
			}
		}

		if useLen > 0 {
			size = size / float64(useLen)
		}

		dInfo := m.nodeMgr.GetNodeValidatableProfitDetails(node, size)
		if dInfo != nil {
			err := m.nodeMgr.AddNodeProfitDetails([]*types.ProfitDetails{dInfo})
			if err != nil {
				log.Errorf("updateResultInfo AddNodeProfit %s,%d, %.4f err:%s", dInfo.NodeID, dInfo.PType, dInfo.Profit, err.Error())
			}
		}
	}
}

func (m *Manager) doValidate() {
	m.updateTimeoutResultInfo()

	// if !m.enableValidation {
	// 	return
	// }

	if err := m.startValidate(); err != nil {
		log.Errorf("start new round: %v", err)
	}
}

func (m *Manager) stopValidation() error {
	close(m.close)
	return nil
}

// startValidate is a method of the Manager that starts a new validation round.
func (m *Manager) startValidate() error {
	m.curRoundID = uuid.NewString()

	seed, err := m.getSeedFromFilecoin()
	if err != nil {
		log.Errorf("startNewRound:%s getSeedFromFilecoin err:%s", m.curRoundID, err.Error())
	}
	m.seed = seed

	validateReqs := make(map[string]*api.ValidateReq)
	_, candidates := m.nodeMgr.GetValidCandidateNodes()

	if len(candidates) == 0 {
		return nil
	}

	// sort.Slice(candidates, func(i, j int) bool {
	// 	return candidates[i].BandwidthDown < candidates[j].BandwidthDown
	// })

	// TODO New rules Sort by remaining bandwidth
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].BandwidthDownScore < candidates[j].BandwidthDownScore
	})

	// mixup nodes
	// rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	candidates = candidates[:len(candidates)/2]

	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}

		if candidate.NATType != types.NatTypeNo.String() && candidate.NATType != types.NatTypeUnknown.String() {
			continue
		}

		m.addValidator(candidate.NodeID)

		vTCPAddr := candidate.TCPAddr()
		wURL := candidate.WsURL()

		req := &api.ValidateReq{
			RandomSeed: m.seed,
			Duration:   duration,
			TCPSrvAddr: vTCPAddr,
			WSURL:      wURL,
		}

		validateReqs[candidate.NodeID] = req
	}

	edges := m.nodeMgr.GetResourceEdgeNodes()
	sort.Slice(edges, func(i, j int) bool {
		return edges[i].LastValidateTime < edges[j].LastValidateTime
	})

	log.Infoln("start validation validateReqs:%d, edges:%d", len(validateReqs), len(edges))

	// L2s that are not randomly checked will be rewarded directly
	nodes := m.distributeEdges(edges, validateReqs)
	m.computeNodeProfits(nodes)

	return nil
}

func (m *Manager) distributeEdges(edges []*node.Node, validateReqs map[string]*api.ValidateReq) []*node.Node {
	dbInfos := make([]*types.ValidationResultInfo, 0)
	totalEdges := len(edges)
	currentEdgeIndex := 0
	edgesPerRound := 3
	duration := 20

	loops := (30 * 60) / duration
	delay := 0

outerLoop:
	for i := 0; i < loops; i++ {
		for vID, req := range validateReqs {
			if currentEdgeIndex >= totalEdges {
				continue outerLoop
			}

			for j := 0; j < edgesPerRound; j++ {
				if currentEdgeIndex >= totalEdges {
					continue outerLoop
				}
				eID := edges[currentEdgeIndex].NodeID
				currentEdgeIndex++

				dbInfo, err := m.getValidationResultInfo(eID, vID)
				dbInfos = append(dbInfos, dbInfo)

				if err != nil {
					log.Errorf("%s RandomAsset err:%s", eID, err.Error())
					continue
				}
				go m.sendValidateReqToNode(eID, req, delay)
			}
		}
		delay += duration
	}

	err := m.nodeMgr.SaveValidationResultInfos(dbInfos)
	if err != nil {
		log.Errorf("SaveValidationResultInfos err:%s", err.Error())
	}

	// No spot check of L2
	start := currentEdgeIndex
	if start < totalEdges {
		return edges[start:]
	}

	return nil
}

// sends a validation request to a node.
func (m *Manager) sendValidateReqToNode(nID string, req *api.ValidateReq, delay int) {
	time.Sleep(time.Duration(delay) * time.Second)
	log.Infof("%d sendValidateReqToNodes v:[%s] n:[%s]", delay, req.TCPSrvAddr, nID)

	status := types.ValidationStatusNodeOffline

	cNode := m.nodeMgr.GetNode(nID)
	if cNode != nil {
		cNode.LastValidateTime = time.Now().Unix()

		err := cNode.ExecuteValidation(context.Background(), req)
		if err == nil {
			return
		}
		log.Errorf("%s Validate err:%s", nID, err.Error())
		status = types.ValidationStatusNodeTimeOut
	}

	err := m.nodeMgr.UpdateValidationResultStatus(m.curRoundID, nID, status)
	if err != nil {
		log.Errorf("%s UpdateValidationResultStatus err:%s", nID, err.Error())
	}
}

func (m *Manager) getValidationResultInfo(nodeID, vID string) (*types.ValidationResultInfo, error) {
	dbInfo := &types.ValidationResultInfo{
		RoundID:     m.curRoundID,
		NodeID:      nodeID,
		ValidatorID: vID,
		Status:      types.ValidationStatusCancel,
		Cid:         "",
		StartTime:   time.Now(),
		EndTime:     time.Now(),
		// NodeCount:   m.nodeMgr.TotalNetworkEdges,
	}

	cid, err := m.RandomAsset(nodeID, m.seed)
	if err != nil {
		return dbInfo, err
	}

	dbInfo.Status = types.ValidationStatusCreate
	dbInfo.Cid = cid.String()

	return dbInfo, nil
}

// RandomAsset get node asset with random seed
func (m *Manager) RandomAsset(nodeID string, seed int64) (*cid.Cid, error) {
	r := rand.New(rand.NewSource(seed))
	bytes, err := m.nodeMgr.LoadBucketHashes(nodeID)
	if err != nil {
		return nil, err
	}

	hashes := make(map[uint32]string)
	if err := decode(bytes, &hashes); err != nil {
		return nil, err
	}

	if len(hashes) == 0 {
		return nil, sql.ErrNoRows
	}

	// TODO　save bucket hashes as array
	bucketIDs := make([]int, 0, len(hashes))
	for k := range hashes {
		bucketIDs = append(bucketIDs, int(k))
	}

	sort.Ints(bucketIDs)

	index := r.Intn(len(bucketIDs))
	bucketID := bucketIDs[index]

	id := fmt.Sprintf("%s:%d", nodeID, bucketID)
	bytes, err = m.nodeMgr.LoadBucket(id)
	if err != nil {
		return nil, err
	}

	assetHashes := make([]string, 0)
	if err = decode(bytes, &assetHashes); err != nil {
		return nil, err
	}

	if len(assetHashes) == 0 {
		return nil, sql.ErrNoRows
	}

	index = r.Intn(len(assetHashes))
	hash := assetHashes[index]

	bytes, err = hex.DecodeString(hash)
	if err != nil {
		return nil, err
	}

	c := cid.NewCidV0(bytes)
	return &c, nil
}

func decode(data []byte, out interface{}) error {
	if len(data) == 0 {
		return nil
	}

	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)
	return dec.Decode(out)
}

// getRandNum generates a random number up to a given maximum value.
func (m *Manager) getRandNum(max int, r *rand.Rand) int {
	if max > 0 {
		return r.Intn(max)
	}

	return max
}

func (m *Manager) updateTimeoutResultInfo() {
	list, err := m.nodeMgr.LoadCreateValidationResultInfos()
	if err != nil {
		log.Errorf("updateTimeoutResultInfo LoadCreateValidationResultInfos err:%s", err.Error())
		return
	}

	detailsList := make([]*types.ProfitDetails, 0)

	for _, resultInfo := range list {
		bandwidth := int64(resultInfo.Bandwidth) * resultInfo.Duration
		resultInfo.Status = types.ValidationStatusValidatorTimeOut

		node := m.nodeMgr.GetNode(resultInfo.NodeID)
		if node != nil {
			if m.curRoundID == "" {
				dInfo := m.nodeMgr.GetNodeValidatableProfitDetails(node, float64(node.BandwidthUp))
				if dInfo != nil {
					dInfo.CID = resultInfo.Cid

					resultInfo.Profit = dInfo.Profit
					detailsList = append(detailsList, dInfo)
				}
			} else {
				resultInfo.Status = types.ValidationStatusNodeTimeOut
			}

			node.UploadTraffic += bandwidth
		} else {
			resultInfo.Status = types.ValidationStatusNodeOffline
		}

		err = m.nodeMgr.UpdateValidationResultInfo(resultInfo)
		if err != nil {
			log.Errorf("%d updateTimeoutResultInfo UpdateValidationResultInfo err:%s", resultInfo.ID, err.Error())
		}
	}

	err = m.nodeMgr.AddNodeProfitDetails(detailsList)
	if err != nil {
		log.Errorf("AddNodeProfit err:%s", err.Error())
	}
}

// updateResultInfo updates the validation result information for a given node.
func (m *Manager) updateResultInfo(status types.ValidationStatus, vr *api.ValidationResult) error {
	if status == types.ValidationStatusValidatorMismatch || status == types.ValidationStatusLoadDBErr || status == types.ValidationStatusCIDToHashErr {
		return nil
	}

	size := vr.Bandwidth * float64(vr.CostTime)

	if vr.Bandwidth > trafficProfitLimit {
		vr.Bandwidth = trafficProfitLimit
	}

	profit := 0.0
	bandwidth := int64(0)
	// update node bandwidths
	node := m.nodeMgr.GetNode(vr.NodeID)
	if node != nil {
		if status == types.ValidationStatusNodeTimeOut || status == types.ValidationStatusValidateFail {
			// node.BandwidthUp = 0
		} else {
			if status != types.ValidationStatusCancel {
				bandwidth = int64(vr.Bandwidth)

				m.nodeMgr.UpdateNodeBandwidths(node.NodeID, 0, bandwidth)
			}

			dInfo := m.nodeMgr.GetNodeValidatableProfitDetails(node, size)
			if dInfo != nil {
				profit = dInfo.Profit

				err := m.nodeMgr.AddNodeProfitDetails([]*types.ProfitDetails{dInfo})
				if err != nil {
					log.Errorf("updateResultInfo AddNodeProfit %s,%d, %.4f err:%s", dInfo.NodeID, dInfo.PType, dInfo.Profit, err.Error())
				}
			}

			node.UploadTraffic += int64(size)
		}
	} else {
		status = types.ValidationStatusNodeOffline
	}

	resultInfo := &types.ValidationResultInfo{
		RoundID:     m.curRoundID,
		NodeID:      vr.NodeID,
		Status:      status,
		BlockNumber: int64(len(vr.Cids)),
		Bandwidth:   vr.Bandwidth,
		Duration:    vr.CostTime,
		Profit:      profit,
		TokenID:     vr.Token,
		ValidatorID: vr.Validator,
	}

	// m.addValidationProfit(vr.Validator, size)

	vNode := m.nodeMgr.GetNode(vr.Validator)
	if vNode != nil {
		vNode.DownloadTraffic += int64(size)

		m.nodeMgr.UpdateNodeBandwidths(vr.Validator, bandwidth, 0)
	}

	return m.nodeMgr.UpdateValidationResultInfo(resultInfo)
}

// PushResult push validation result info to queue
func (m *Manager) PushResult(vr *api.ValidationResult) {
	// TODO If the server is down, the data will be lost
	m.resultQueue <- vr
}

func (m *Manager) handleResults() {
	for i := 0; i < validationWorkers; i++ {
		go func() {
			for {
				result := <-m.resultQueue
				m.handleResult(result)
			}
		}()
	}
}

// handleResult handles the validation result for a given node.
func (m *Manager) handleResult(vr *api.ValidationResult) {
	var status types.ValidationStatus
	nodeID := vr.NodeID

	defer func() {
		err := m.updateResultInfo(status, vr)
		if err != nil {
			log.Errorf("updateResultInfo [%s] fail : %s", nodeID, err.Error())
			return
		}
	}()

	if vr.IsCancel {
		status = types.ValidationStatusCancel
		return
	}

	if vr.IsTimeout {
		status = types.ValidationStatusNodeTimeOut
		return
	}

	cidCount := len(vr.Cids)
	if cidCount < 1 {
		status = types.ValidationStatusValidateFail
		log.Errorf("handleResult round [%s] validator [%s] nodeID [%s] seed [%d] ;cidCount<1", m.curRoundID, vr.Validator, nodeID, m.seed)
		return
	}

	vInfo, err := m.nodeMgr.LoadNodeValidationInfo(m.curRoundID, nodeID)
	if err != nil || vInfo.Status != types.ValidationStatusCreate {
		status = types.ValidationStatusLoadDBErr
		log.Errorf("handleResult LoadNodeValidationCID %s , %s, %d , err:%v", m.curRoundID, nodeID, vInfo.Status, err)
		return
	}

	vr.CID = vInfo.Cid

	if vInfo.ValidatorID != vr.Validator {
		status = types.ValidationStatusValidatorMismatch
		log.Errorf("handleResult ValidationStatusValidatorMismatch %s , %s, %s, %s", m.curRoundID, nodeID, vInfo.ValidatorID, vr.Validator)
		return
	}

	hash, err := cidutil.CIDToHash(vInfo.Cid)
	if err != nil {
		status = types.ValidationStatusCIDToHashErr
		log.Errorf("handleResult CIDString2HashString %s,validator [%s] nodeID [%s] err:%s", vInfo.Cid, vr.Validator, nodeID, err.Error())
		return
	}

	cids, cNodeID, err := m.getAssetBlocksFromCandidate(hash, vInfo.Cid, nodeID, cidCount)
	if err != nil {
		status = types.ValidationStatusGetValidatorBlockErr
		log.Errorf("handleResult %s getAssetBlocksFromCandidate %s , %s !err , %s", nodeID, cNodeID, vInfo.Cid, err.Error())

		err = m.nodeMgr.SaveReplenishBackup([]string{hash})
		if err != nil {
			log.Errorf("handleResult %s SaveReplenishBackup err:%s", hash, err.Error())
		}

		return
	}

	if len(cids) <= 0 {
		status = types.ValidationStatusGetValidatorBlockErr
		log.Errorf("handleResult %s candidate map is nil , %s", nodeID, vInfo.Cid)
		return
	}

	// do validate
	for i := 0; i < cidCount; i++ {
		resultCid := vr.Cids[i]
		validatorCid := cids[i]

		// TODO Penalize the candidate if vCid error

		if !m.compareCid(resultCid, validatorCid) {
			status = types.ValidationStatusValidateFail
			log.Errorf("handleResult round [%s] validator [%s] cNodeID [%s] nodeID [%s], assetCID [%s] seed [%d] ; validator fail resultCid:%s, vCid:%s,index:%d", m.curRoundID, vr.Validator, cNodeID, nodeID, vInfo.Cid, m.seed, resultCid, validatorCid, i)
			return
		}
	}

	status = types.ValidationStatusSuccess
}

func (m *Manager) getAssetBlocksFromCandidate(hash, cid string, filterNode string, cidCount int) ([]string, string, error) {
	replicas, err := m.nodeMgr.LoadReplicasByStatus(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		log.Errorf("LoadReplicasByHash %s , err:%s", hash, err.Error())
		return nil, "", err
	}

	var cids []string

	for _, rInfo := range replicas {
		cNodeID := rInfo.NodeID
		if cNodeID == filterNode {
			continue
		}

		node := m.nodeMgr.GetCandidateNode(cNodeID)
		if node == nil {
			continue
		}

		cids, err = node.GetBlocksOfAsset(context.Background(), cid, m.seed, cidCount)
		if err != nil {
			log.Errorf("candidate %s GetBlocksOfAsset err:%s", cNodeID, err.Error())
			continue
		}

		return cids, cNodeID, nil
	}

	return nil, "", fmt.Errorf("can not find candidate node")
}

// compares two CID strings and returns true if they are equal, false otherwise
func (m *Manager) compareCid(cidStr1, cidStr2 string) bool {
	hash1, err := cidutil.CIDToHash(cidStr1)
	if err != nil {
		return false
	}

	hash2, err := cidutil.CIDToHash(cidStr2)
	if err != nil {
		return false
	}

	return hash1 == hash2
}
