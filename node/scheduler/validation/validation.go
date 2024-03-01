package validation

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

const (
	duration           = 10               // Validation duration per node (Unit:Second)
	validationInterval = 30 * time.Minute // validation start-up time interval (Unit:minute)

	// Processing validation result data from 5 days ago
	vResultDay = 5 * oneDay
	// Process 50000 pieces of validation result data at a time
	vResultLimit = 50000
)

// startValidationTicker starts the validation process.
func (m *Manager) startValidationTicker(ctx context.Context) {
	ticker := time.NewTicker(validationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if enable := m.isEnabled(); !enable {
				continue
			}

			m.profit = m.getValidationProfit()

			if err := m.startValidate(); err != nil {
				log.Errorf("start new round: %v", err)
			}
		case <-m.close:
			return
		}
	}
}

func (m *Manager) stopValidation(ctx context.Context) error {
	close(m.close)
	return nil
}

// isEnabled returns whether or not validation is currently enabled.
func (m *Manager) isEnabled() bool {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("get config err:%s", err.Error())
		return false
	}

	return cfg.EnableValidation
}

// get the profit of validation
func (m *Manager) getValidationProfit() float64 {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("get config err:%s", err.Error())
		return 0
	}

	return cfg.ValidationProfit
}

// startValidate is a method of the Manager that starts a new validation round.
func (m *Manager) startValidate() error {
	if m.curRoundID != "" {
		// Set the timeout status of the previous verification
		err := m.nodeMgr.UpdateValidationResultsTimeout(m.curRoundID)
		if err != nil {
			log.Errorf("startNewRound:%s UpdateValidationResultsTimeout err:%s", m.curRoundID, err.Error())
		}
	}

	roundID := uuid.NewString()
	m.curRoundID = roundID

	seed, err := m.getSeedFromFilecoin()
	if err != nil {
		log.Errorf("startNewRound:%s getSeedFromFilecoin err:%s", m.curRoundID, err.Error())
	}
	m.seed = seed

	vrs := m.PairValidatorsAndValidatableNodes()

	vReqs, dbInfos := m.getValidationDetails(vrs)
	if len(vReqs) == 0 {
		return xerrors.New("validation pair fail")
	}

	err = m.nodeMgr.SaveValidationResultInfos(dbInfos)
	if err != nil {
		return err
	}

	for nodeID, reqs := range vReqs {
		go m.sendValidateReqToNodes(nodeID, reqs)
	}

	return nil
}

// sends a validation request to a node.
func (m *Manager) sendValidateReqToNodes(nID string, req *api.ValidateReq) {
	cNode := m.nodeMgr.GetNode(nID)
	if cNode != nil {
		err := cNode.ExecuteValidation(context.Background(), req)
		if err != nil {
			log.Errorf("%s Validate err:%s", nID, err.Error())
		}
		return
	}

	log.Errorf("%s validatable Node not found", nID)
}

// get validation details.
func (m *Manager) getValidationDetails(vrs []*VWindow) (map[string]*api.ValidateReq, []*types.ValidationResultInfo) {
	bReqs := make(map[string]*api.ValidateReq)
	vrInfos := make([]*types.ValidationResultInfo, 0)

	for _, vr := range vrs {
		vID := vr.NodeID
		vNode := m.nodeMgr.GetCandidateNode(vID)
		if vNode == nil {
			log.Errorf("%s validator not exist", vNode)
			continue
		}

		for nodeID := range vr.ValidatableNodes {
			cid, err := m.assetMgr.RandomAsset(nodeID, m.seed)
			if err != nil {
				if err != sql.ErrNoRows {
					log.Errorf("%s RandomAsset err:%s", nodeID, err.Error())
				}
				continue
			}

			dbInfo := &types.ValidationResultInfo{
				RoundID:     m.curRoundID,
				NodeID:      nodeID,
				ValidatorID: vID,
				Status:      types.ValidationStatusCreate,
				Cid:         cid.String(),
				StartTime:   time.Now(),
				EndTime:     time.Now(),
			}
			vrInfos = append(vrInfos, dbInfo)

			req := &api.ValidateReq{
				RandomSeed: m.seed,
				Duration:   duration,
				TCPSrvAddr: vNode.TCPAddr(),
			}

			bReqs[nodeID] = req
		}
	}

	return bReqs, vrInfos
}

// getRandNum generates a random number up to a given maximum value.
func (m *Manager) getRandNum(max int, r *rand.Rand) int {
	if max > 0 {
		return r.Intn(max)
	}

	return max
}

// updateResultInfo updates the validation result information for a given node.
func (m *Manager) updateResultInfo(status types.ValidationStatus, vr *api.ValidationResult, profit float64) error {
	resultInfo := &types.ValidationResultInfo{
		RoundID:     m.curRoundID,
		NodeID:      vr.NodeID,
		Status:      status,
		BlockNumber: int64(len(vr.Cids)),
		Bandwidth:   vr.Bandwidth,
		Duration:    vr.CostTime,
		Profit:      profit,
		TokenID:     vr.Token,
	}

	// update node bandwidths
	if status == types.ValidationStatusSuccess {
		m.nodeMgr.UpdateNodeBandwidths(vr.NodeID, 0, int64(vr.Bandwidth))
	}

	return m.nodeMgr.UpdateValidationResultInfo(resultInfo)
}

// PushResult push validation result info to queue
func (m *Manager) PushResult(vr *api.ValidationResult) {
	// TODO If the server is down, the data will be lost
	m.resultQueue <- vr
}

func (m *Manager) pullResults() {
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
	profit := float64(0)

	defer func() {
		err := m.updateResultInfo(status, vr, profit)
		if err != nil {
			log.Errorf("updateResultInfo [%s] fail : %s", nodeID, err.Error())
			return
		}
	}()

	if vr.IsCancel {
		status = types.ValidationStatusCancel
		profit = m.profit
		return
	}

	if vr.IsTimeout {
		status = types.ValidationStatusNodeTimeOut
		return
	}

	cidCount := len(vr.Cids)
	if cidCount < 1 {
		status = types.ValidationStatusValidateFail
		return
	}

	vInfo, err := m.nodeMgr.LoadNodeValidationInfo(m.curRoundID, nodeID)
	if err != nil {
		status = types.ValidationStatusLoadDBErr
		log.Errorf("LoadNodeValidationCID %s , %s, err:%s", m.curRoundID, nodeID, err.Error())
		return
	}

	if vInfo.ValidatorID != vr.Validator {
		status = types.ValidationStatusValidatorMismatch
		return
	}

	hash, err := cidutil.CIDToHash(vInfo.Cid)
	if err != nil {
		status = types.ValidationStatusCIDToHashErr
		log.Errorf("CIDString2HashString %s, err:%s", vInfo.Cid, err.Error())
		return
	}

	cids, cNodeID, err := m.getAssetBlocksFromCandidate(hash, vInfo.Cid, nodeID, cidCount)
	if err != nil {
		status = types.ValidationStatusLoadDBErr
		return
	}

	if len(cids) <= 0 {
		status = types.ValidationStatusGetValidatorBlockErr
		log.Errorf("handleValidationResult candidate map is nil , %s", vr.CID)
		return
	}

	// do validate
	for i := 0; i < cidCount; i++ {
		resultCid := vr.Cids[i]
		validatorCid := cids[i]

		// TODO Penalize the candidate if vCid error

		if !m.compareCid(resultCid, validatorCid) {
			status = types.ValidationStatusValidateFail
			log.Errorf("round [%s] validator [%s] cNodeID [%s] nodeID [%s], assetCID [%s] seed [%d] ; validator fail resultCid:%s, vCid:%s,index:%d", m.curRoundID, vr.Validator, cNodeID, nodeID, vInfo.Cid, m.seed, resultCid, validatorCid, i)
			return
		}
	}

	profit = m.profit
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

func (m *Manager) startHandleResultsTimer() {
	now := time.Now()

	nextTime := time.Date(now.Year(), now.Month(), now.Day(), 3, 0, 0, 0, now.Location())
	if now.After(nextTime) {
		nextTime = nextTime.Add(oneDay)
	}

	duration := nextTime.Sub(now)

	timer := time.NewTimer(duration)
	defer timer.Stop()

	for {
		<-timer.C

		log.Debugln("start handle validation results timer...")
		m.handleValidationResults()

		timer.Reset(oneDay)
	}
}

func (m *Manager) handleValidationResults() {
	if !m.leadershipMgr.RequestAndBecomeMaster() {
		return
	}

	startTime := time.Now()
	defer func() {
		log.Debugf("handleValidationResults time:%s", time.Since(startTime))
	}()

	maxTime := time.Now().Add(-vResultDay)

	// do handle validation result
	for {
		ids, nodeProfits, err := m.loadResults(maxTime)
		if err != nil {
			log.Errorf("loadResults err:%s", err.Error())
			return
		}

		if len(ids) == 0 {
			return
		}

		err = m.nodeMgr.UpdateNodeInfosByValidationResult(ids, nodeProfits)
		if err != nil {
			log.Errorf("UpdateNodeProfitsByValidationResult err:%s", err.Error())
			return
		}

	}
}

func (m *Manager) loadResults(maxTime time.Time) ([]int, map[string]float64, error) {
	rows, err := m.nodeMgr.LoadUnCalculatedValidationResults(maxTime, vResultLimit)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// infos := make([]*types.ValidationResultInfo, 0)
	ids := make([]int, 0)
	nodeProfits := make(map[string]float64)

	for rows.Next() {
		vInfo := &types.ValidationResultInfo{}
		err = rows.StructScan(vInfo)
		if err != nil {
			log.Errorf("loadResults StructScan err: %s", err.Error())
			continue
		}

		if vInfo.Status == types.ValidationStatusCancel {
			tokenID := vInfo.TokenID
			record, err := m.nodeMgr.LoadRetrieveEvent(tokenID)
			if err != nil {
				vInfo.Profit = 0
			} else {
				// check time
				if record.CreatedTime > vInfo.EndTime.Unix() {
					vInfo.Profit = 0
				}

				if record.EndTime < vInfo.StartTime.Unix() {
					vInfo.Profit = 0
				}
			}
		}

		// infos = append(infos, vInfo)
		ids = append(ids, vInfo.ID)

		if vInfo.Profit == 0 {
			continue
		}

		nodeProfits[vInfo.NodeID] += vInfo.Profit
	}

	return ids, nodeProfits, nil
}
