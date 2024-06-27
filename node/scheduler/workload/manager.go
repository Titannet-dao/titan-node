package workload

import (
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/scheduler/leadership"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
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
	log.Infof("workload PushResult nodeID:[%s] , %s\n", nodeID, data.WorkloadID)

	if nodeID == "" {
		return nil
	}

	m.resultQueue <- &WorkloadResult{data: data, nodeID: nodeID}

	return nil
}

func (m *Manager) handleResults() {
	for {
		result := <-m.resultQueue

		m.handleClientWorkload(result.data, result.nodeID)
	}
}

// handleClientWorkload handle node workload
func (m *Manager) handleClientWorkload(data *types.WorkloadRecordReq, nodeID string) error {
	downloadTotalSize := int64(0)

	if data.WorkloadID == "" {
		return xerrors.New("WorkloadID is nil")
	}

	record, err := m.LoadWorkloadRecordOfID(data.WorkloadID)
	if err != nil {
		log.Errorf("handleClientWorkload LoadWorkloadRecordOfID %s error: %s", data.WorkloadID, err.Error())
		return err
	}

	if record.Status != types.WorkloadStatusCreate {
		return nil
	}

	for _, dw := range data.Workloads {
		downloadTotalSize += dw.DownloadSize
	}

	if record == nil {
		log.Errorf("handleClientWorkload record is nil : %s, %s", data.AssetCID, nodeID)
		return nil
	}

	// update status
	record.Status = types.WorkloadStatusSucceeded
	err = m.UpdateWorkloadRecord(record, types.WorkloadStatusCreate)
	if err != nil {
		log.Errorf("handleClientWorkload UpdateWorkloadRecord error: %s", err.Error())
		return err
	}

	eventList := make([]*types.RetrieveEvent, 0)
	detailsList := make([]*types.ProfitDetails, 0)

	limit := int64(float64(record.AssetSize))

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

		// Only edge can get this reward
		node := m.nodeMgr.GetEdgeNode(dw.SourceID)
		if node != nil {
			dInfo := m.nodeMgr.GetNodeBePullProfitDetails(node, float64(dw.DownloadSize), "")
			if dInfo != nil {
				dInfo.CID = retrieveEvent.CID
				dInfo.Note = fmt.Sprintf("%s,%s", dInfo.Note, record.WorkloadID)

				detailsList = append(detailsList, dInfo)
			}

			node.UploadTraffic += dw.DownloadSize
		}

		// update node bandwidths
		speed := int64((float64(dw.DownloadSize) / float64(dw.CostTime)) * 1000)
		if speed > 0 {
			// m.nodeMgr.UpdateNodeBandwidths(dw.SourceID, 0, speed)
			m.nodeMgr.UpdateNodeBandwidths(record.ClientID, speed, 0)
		}
	}

	// Retrieve Event
	for _, data := range eventList {
		if err := m.SaveRetrieveEventInfo(data); err != nil {
			log.Errorf("handleClientWorkload SaveRetrieveEventInfo token:%s ,  error %s", record.WorkloadID, err.Error())
		}
	}

	for _, data := range detailsList {
		err = m.nodeMgr.AddNodeProfit(data)
		if err != nil {
			log.Errorf("handleClientWorkload AddNodeProfit %s,%d, %.4f err:%s", data.NodeID, data.PType, data.Profit, err.Error())
		}
	}

	return nil
}
