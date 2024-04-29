package filelogger

import (
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/scheduler/leadership"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("recordfile")

const (
	// Process 1000 pieces of validation result data at a time
	vResultLimit = 1000

	oneDay = 24 * time.Hour

	BufSize = (4 << 20) / 128 * 127

	BlockGasLimit = int64(10_000_000_000)
)

// Manager is the node manager responsible for managing the online nodes
type Manager struct {
	*db.SQLDB
	dtypes.ServerID // scheduler server id
	leadershipMgr   *leadership.Manager
}

// NewManager creates a new instance of the node manager
func NewManager(sdb *db.SQLDB, serverID dtypes.ServerID, lmgr *leadership.Manager) *Manager {
	mgr := &Manager{
		SQLDB:         sdb,
		leadershipMgr: lmgr,
		ServerID:      serverID,
	}

	// go mgr.startTimer()

	return mgr
}

func (m *Manager) startTimer() {
	now := time.Now()

	nextTime := time.Date(now.Year(), now.Month(), now.Day(), 2, 0, 0, 0, time.UTC)
	if now.After(nextTime) {
		nextTime = nextTime.Add(oneDay)
	}

	duration := nextTime.Sub(now)
	log.Debugf("start save validation result to files time : %d", duration.Seconds())

	timer := time.NewTimer(duration)
	defer timer.Stop()

	for {
		<-timer.C

		log.Debugln("start save validation result to files timer...")
		m.handleValidationResultSaveToFiles()
		// Packaging

		timer.Reset(oneDay)
	}
}

func (m *Manager) handleValidationResultSaveToFiles() {
	if !m.leadershipMgr.RequestAndBecomeMaster() {
		return
	}

	// do handle validation result
	for {
		ids, ds, err := m.loadResults()
		if err != nil {
			log.Errorf("loadResults err:%s", err.Error())
			return
		}

		if len(ids) == 0 {
			return
		}

		m.saveValidationResultToFiles(ds)

		err = m.RemoveInvalidValidationResult(ids)
		if err != nil {
			log.Errorf("RemoveInvalidValidationResult err:%s", err.Error())
			return
		}
	}
}

func (m *Manager) loadResults() ([]int, map[string]string, error) {
	rows, err := m.LoadUnSavedValidationResults(vResultLimit)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	ids := make([]int, 0)
	ds := make(map[string]string)

	for rows.Next() {
		vInfo := &types.ValidationResultInfo{}
		err = rows.StructScan(vInfo)
		if err != nil {
			log.Errorf("loadResults StructScan err: %s", err.Error())
			continue
		}

		ids = append(ids, vInfo.ID)

		str := fmt.Sprintf("RoundID:%s, NodeID:%s, ValidatorID:%s, Profit:%.2f, ValidationCID:%s,EndTime:%s \n",
			vInfo.RoundID, vInfo.NodeID, vInfo.ValidatorID, vInfo.Profit, vInfo.Cid, vInfo.EndTime.String())

		filename := vInfo.EndTime.Format("20060102")
		data := ds[filename]
		ds[filename] = fmt.Sprintf("%s%s", data, str)
	}

	return ids, ds, nil
}

func (m *Manager) saveValidationResultToFiles(ds map[string]string) {
	saveFile := func(data, filename string) error {
		writer, err := NewWriter(DirectoryNameValidation, filename)
		if err != nil {
			return xerrors.Errorf("NewWriter err:%s", err.Error())
		}
		defer writer.Close()

		err = writer.WriteData(data)
		if err != nil {
			return xerrors.Errorf("WriteData err:%s", err.Error())
		}

		return nil
	}

	for filename, data := range ds {
		err := saveFile(filename, data)
		if err != nil {
			log.Errorf("filename:%s , saveFile err:%s", filename, err.Error())
		}
	}
}
