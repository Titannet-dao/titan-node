package workload

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/scheduler/leadership"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("workload")

const (
	workloadInterval = 24 * time.Hour
	confirmationTime = 7 * 24 * time.Hour

	// Process 500 pieces of workload result data at a time
	vWorkloadLimit = 500

	handlerWorkers = 50
)

// Manager node workload
type Manager struct {
	config        dtypes.GetSchedulerConfigFunc
	leadershipMgr *leadership.Manager
	nodeMgr       *node.Manager
	*db.SQLDB

	resultQueue chan *WorkloadResult
}

// NewManager return new node manager instance
func NewManager(sdb *db.SQLDB, configFunc dtypes.GetSchedulerConfigFunc, lmgr *leadership.Manager, nmgr *node.Manager) *Manager {
	manager := &Manager{
		config:        configFunc,
		leadershipMgr: lmgr,
		SQLDB:         sdb,
		nodeMgr:       nmgr,
		resultQueue:   make(chan *WorkloadResult),
	}

	go manager.handleResults()

	return manager
}

type WorkloadResult struct {
	data   *types.WorkloadRecordReq
	nodeID string
}

func (m *Manager) PushResult(data *types.WorkloadRecordReq, nodeID string) error {
	m.resultQueue <- &WorkloadResult{data: data, nodeID: nodeID}

	return nil
}

func (m *Manager) handleResults() {
	// for i := 0; i < handlerWorkers; i++ {
	// 	go func() {
	for {
		result := <-m.resultQueue

		if result.nodeID == "" {
			m.handleUserWorkload(result.data)
		} else {
			m.handleNodeWorkload(result.data, result.nodeID)
		}
	}
	// 	}()
	// }
}

// handleUserWorkload handle node workload
func (m *Manager) handleUserWorkload(data *types.WorkloadRecordReq) error {
	record, err := m.LoadWorkloadRecordOfID(data.WorkloadID, types.WorkloadStatusCreate)
	if err != nil {
		log.Errorf("handleUserWorkload LoadWorkloadRecordOfID error: %s", err.Error())
		return err
	}

	if record == nil {
		log.Errorf("handleUserWorkload not found Workload: %s", data.WorkloadID)
		return nil
	}

	downloadTotalSize := int64(0)

	ws := make([]*types.Workload, 0)
	dec := gob.NewDecoder(bytes.NewBuffer(record.Workloads))
	err = dec.Decode(&ws)
	if err != nil {
		log.Errorf("handleUserWorkload decode data to []*types.Workload error: %s", err.Error())
		return err
	}

	sourceIDSet := make(map[string]struct{})
	for _, w := range ws {
		sourceIDSet[w.SourceID] = struct{}{}
	}

	downloadTotalSize = int64(0)

	for _, dw := range data.Workloads {
		downloadTotalSize += dw.DownloadSize
		if _, exists := sourceIDSet[dw.SourceID]; !exists {
			log.Errorf("handleUserWorkload source not found : %s", dw.SourceID)
			return nil
		}
	}

	// update status
	record.Status = types.WorkloadStatusSucceeded
	err = m.UpdateWorkloadRecord(record)
	if err != nil {
		log.Errorf("HandleNodeWorkload UpdateWorkloadRecord error: %s", err.Error())
		return err
	}

	eventList := make([]*types.RetrieveEvent, 0)
	detailsList := make([]*types.ProfitDetails, 0)

	limit := int64(float64(record.AssetSize) * 1.3)
	if downloadTotalSize > limit {
		downloadTotalSize = limit
	}

	for _, dw := range data.Workloads {
		if dw.SourceID == types.DownloadSourceAWS.String() || dw.SourceID == types.DownloadSourceIPFS.String() || dw.SourceID == types.DownloadSourceSDK.String() {
			continue
		}

		if dw.DownloadSize > limit {
			dw.DownloadSize = limit
		}

		retrieveEvent := &types.RetrieveEvent{
			CID:         record.AssetCID,
			TokenID:     uuid.NewString(),
			NodeID:      dw.SourceID,
			ClientID:    record.ClientID,
			Size:        dw.DownloadSize,
			CreatedTime: record.CreatedTime.Unix(),
			EndTime:     time.Now().Unix(),
		}
		eventList = append(eventList, retrieveEvent)

		node := m.nodeMgr.GetNode(dw.SourceID)
		if node != nil {
			dInfo := m.nodeMgr.GetNodeBePullProfitDetails(node, float64(dw.DownloadSize), "")
			if dInfo != nil {
				dInfo.CID = retrieveEvent.CID

				detailsList = append(detailsList, dInfo)
			}
		}

		// update node bandwidths
		speed := int64((float64(dw.DownloadSize) / float64(dw.CostTime)) * 1000)
		if speed > 0 {
			m.nodeMgr.UpdateNodeBandwidths(dw.SourceID, 0, speed)
			m.nodeMgr.UpdateNodeBandwidths(record.ClientID, speed, 0)
		}
	}

	// Retrieve Event
	if err := m.SaveRetrieveEventInfo(eventList); err != nil {
		log.Errorf("HandleNodeWorkload SaveRetrieveEventInfo token:%s ,  error %s", record.WorkloadID, err.Error())
	}

	err = m.nodeMgr.AddNodeProfits(detailsList)
	if err != nil {
		log.Errorf("HandleNodeWorkload AddNodeProfits err:%s", err.Error())
	}

	return nil
}

// handleNodeWorkload handle node workload
func (m *Manager) handleNodeWorkload(data *types.WorkloadRecordReq, nodeID string) error {
	list, err := m.LoadWorkloadRecord(&types.WorkloadRecord{AssetCID: data.AssetCID, ClientID: nodeID, Status: types.WorkloadStatusCreate})
	if err != nil {
		log.Errorf("HandleNodeWorkload LoadWorkloadRecord error: %s", err.Error())
		return err
	}

	if len(list) == 0 {
		return nil
	}

	var record *types.WorkloadRecord
	downloadTotalSize := int64(0)

outerLoop:
	for _, info := range list {

		ws := make([]*types.Workload, 0)
		dec := gob.NewDecoder(bytes.NewBuffer(info.Workloads))
		err := dec.Decode(&ws)
		if err != nil {
			log.Errorf("HandleNodeWorkload decode data to []*types.Workload error: %s", err.Error())
			continue
		}

		sourceIDSet := make(map[string]struct{})
		for _, w := range ws {
			sourceIDSet[w.SourceID] = struct{}{}
		}

		downloadTotalSize = int64(0)

		for _, dw := range data.Workloads {
			downloadTotalSize += dw.DownloadSize
			if _, exists := sourceIDSet[dw.SourceID]; !exists {
				continue outerLoop
			}
		}

		record = info
		break // Find the first matching workload, no need to continue
	}

	if record == nil {
		return nil
	}

	// update status
	record.Status = types.WorkloadStatusSucceeded
	err = m.UpdateWorkloadRecord(record)
	if err != nil {
		log.Errorf("HandleNodeWorkload UpdateWorkloadRecord error: %s", err.Error())
		return err
	}

	eventList := make([]*types.RetrieveEvent, 0)
	detailsList := make([]*types.ProfitDetails, 0)

	limit := int64(float64(record.AssetSize) * 1.3)
	if downloadTotalSize > limit {
		downloadTotalSize = limit
	}

	if record.Event == types.WorkloadEventPull {
		node := m.nodeMgr.GetNode(nodeID)
		if node != nil {
			detailsList = append(detailsList, m.nodeMgr.GetNodePullProfitDetails(node, float64(downloadTotalSize), ""))
		}
	}

	for _, dw := range data.Workloads {
		if dw.SourceID == types.DownloadSourceAWS.String() || dw.SourceID == types.DownloadSourceIPFS.String() || dw.SourceID == types.DownloadSourceSDK.String() {
			continue
		}

		if dw.DownloadSize > limit {
			dw.DownloadSize = limit
		}

		retrieveEvent := &types.RetrieveEvent{
			CID:         record.AssetCID,
			TokenID:     uuid.NewString(),
			NodeID:      dw.SourceID,
			ClientID:    record.ClientID,
			Size:        dw.DownloadSize,
			CreatedTime: record.CreatedTime.Unix(),
			EndTime:     time.Now().Unix(),
		}
		eventList = append(eventList, retrieveEvent)

		node := m.nodeMgr.GetNode(dw.SourceID)
		if node != nil {
			dInfo := m.nodeMgr.GetNodeBePullProfitDetails(node, float64(dw.DownloadSize), "")
			if dInfo != nil {
				dInfo.CID = retrieveEvent.CID

				detailsList = append(detailsList, dInfo)
			}
		}

		// update node bandwidths
		speed := int64((float64(dw.DownloadSize) / float64(dw.CostTime)) * 1000)
		if speed > 0 {
			m.nodeMgr.UpdateNodeBandwidths(dw.SourceID, 0, speed)
			m.nodeMgr.UpdateNodeBandwidths(record.ClientID, speed, 0)
		}
	}

	// Retrieve Event
	if err := m.SaveRetrieveEventInfo(eventList); err != nil {
		log.Errorf("HandleNodeWorkload SaveRetrieveEventInfo token:%s ,  error %s", record.WorkloadID, err.Error())
	}

	err = m.nodeMgr.AddNodeProfits(detailsList)
	if err != nil {
		log.Errorf("HandleNodeWorkload AddNodeProfits err:%s", err.Error())
	}

	return nil
}

// func (m *Manager) handleWorkloadReport(nodeID string, report *types.WorkloadReport, isClient bool) (*types.WorkloadRecord, error) {
// 	workloadRecord, err := m.LoadWorkloadRecord(report.TokenID)
// 	if err != nil {
// 		return nil, xerrors.Errorf("load token payload and workloads with token id %s error: %w", report.TokenID, err)
// 	}
// 	if isClient && workloadRecord.NodeID != nodeID {
// 		return nil, fmt.Errorf("token payload node id %s, but report node id is %s", workloadRecord.NodeID, report.NodeID)
// 	}

// 	if !isClient && workloadRecord.ClientID != report.ClientID {
// 		return nil, fmt.Errorf("token payload client id %s, but report client id is %s", workloadRecord.ClientID, report.ClientID)
// 	}

// 	if workloadRecord.Expiration.Before(time.Now()) {
// 		return nil, fmt.Errorf("token payload expiration %s < %s", workloadRecord.Expiration.Local().String(), time.Now().Local().String())
// 	}

// 	workloadBytes := workloadRecord.NodeWorkload
// 	if isClient {
// 		workloadBytes = workloadRecord.ClientWorkload
// 	}

// 	workload := &types.Workload{}
// 	if len(workloadBytes) > 0 {
// 		dec := gob.NewDecoder(bytes.NewBuffer(workloadBytes))
// 		err = dec.Decode(workload)
// 		if err != nil {
// 			return nil, xerrors.Errorf("decode data to []*types.Workload error: %w", err)
// 		}
// 	}

// 	workload = m.mergeWorkloads([]*types.Workload{workload, report.Workload})

// 	buffer := &bytes.Buffer{}
// 	enc := gob.NewEncoder(buffer)
// 	err = enc.Encode(workload)
// 	if err != nil {
// 		return nil, xerrors.Errorf("encode data to Buffer error: %w", err)
// 	}

// 	if isClient {
// 		workloadRecord.ClientWorkload = buffer.Bytes()
// 		workloadRecord.ClientEndTime = time.Now().Unix()
// 	} else {
// 		workloadRecord.NodeWorkload = buffer.Bytes()
// 	}

// 	return workloadRecord, m.UpdateWorkloadRecord(workloadRecord)
// }

// func (m *Manager) mergeWorkloads(workloads []*types.Workload) *types.Workload {
// 	if len(workloads) == 0 {
// 		return nil
// 	}

// 	// costTime := int64(0)
// 	downloadSize := int64(0)
// 	startTime := time.Time{}
// 	endTime := time.Time{}
// 	speedCount := int64(0)
// 	accumulateSpeed := int64(0)

// 	for _, workload := range workloads {
// 		if workload.DownloadSpeed > 0 {
// 			accumulateSpeed += workload.DownloadSpeed
// 			speedCount++
// 		}
// 		downloadSize += workload.DownloadSize

// 		if startTime.IsZero() || workload.StartTime.Before(startTime) {
// 			startTime = workload.StartTime
// 		}

// 		if workload.EndTime.After(endTime) {
// 			endTime = workload.EndTime
// 		}
// 	}

// 	downloadSpeed := int64(0)
// 	if speedCount > 0 {
// 		downloadSpeed = accumulateSpeed / speedCount
// 	}
// 	return &types.Workload{DownloadSpeed: downloadSpeed, DownloadSize: downloadSize, StartTime: startTime, EndTime: endTime}
// }
