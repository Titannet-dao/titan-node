package workload

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/sqldb"
)

func TestWorkloadRecord(t *testing.T) {
	sqldb, err := sqldb.NewDB("user01:sql001@tcp(127.0.0.1:3306)/test")
	if err != nil {
		t.Errorf("NewDB error:%s", err.Error())
		return
	}

	db, err := db.NewSQLDB(sqldb)
	if err != nil {
		t.Errorf("NewSQLDB error:%s", err.Error())
		return
	}

	tkPayload := types.TokenPayload{ID: "123", NodeID: "222", AssetCID: "11111", ClientID: "3333", CreatedTime: time.Now(), Expiration: time.Now().Add(1 * time.Hour)}
	record := &types.WorkloadRecord{TokenPayload: tkPayload, Status: types.WorkloadStatusCreate}
	if err := db.SaveWorkloadRecord([]*types.WorkloadRecord{record}); err != nil {
		t.Errorf("SaveToken error:%s", err.Error())
		return
	}

	workload := &types.Workload{DownloadSpeed: 11, DownloadSize: 222, StartTime: time.Now().Unix(), EndTime: time.Now().Unix()}
	// workloads := []*types.Workload{workload}

	buffer := &bytes.Buffer{}
	enc := gob.NewEncoder(buffer)
	err = enc.Encode(workload)
	if err != nil {
		t.Errorf("encode error:%s", err.Error())
		return
	}

	record.NodeWorkload = buffer.Bytes()
	record.ClientWorkload = buffer.Bytes()
	if err := db.UpdateWorkloadRecord(record); err != nil {
		t.Errorf("UpdateWorkload error:%s", err.Error())
		return
	}

	workloadRecord, err := db.LoadWorkloadRecord(record.ID)
	if err != nil {
		t.Errorf("LoadTokenPayloadAndWorkloads error:%s", err.Error())
		return
	}
	t.Logf("record:%#v", *record)

	if len(workloadRecord.NodeWorkload) > 0 {
		newBuffer := bytes.NewBuffer(workloadRecord.NodeWorkload)
		dec := gob.NewDecoder(newBuffer)

		workload := types.Workload{}
		err = dec.Decode(&workload)
		if err != nil {
			t.Errorf("Decode node workload error:%s", err.Error())
			return
		}

		t.Logf("decode node workload:%v", workload)
	}

	if len(workloadRecord.ClientWorkload) > 0 {
		newBuffer := bytes.NewBuffer(workloadRecord.ClientWorkload)
		dec := gob.NewDecoder(newBuffer)

		workload := types.Workload{}
		err = dec.Decode(&workload)
		if err != nil {
			t.Errorf("encode client workload error:%s", err.Error())
			return
		}

		t.Logf("decode client workload:%v", workload)

	}
}

func TestLoadWorkloadRecord(t *testing.T) {
	sqldb, err := sqldb.NewDB("user01:sql001@tcp(127.0.0.1:3306)/test")
	if err != nil {
		t.Errorf("NewDB error:%s", err.Error())
		return
	}

	db, err := db.NewSQLDB(sqldb)
	if err != nil {
		t.Errorf("NewSQLDB error:%s", err.Error())
		return
	}

	id := "f8f436e5-7354-4611-a9a1-0f9286f49517"
	workloadRecord, err := db.LoadWorkloadRecord(id)
	if err != nil {
		t.Errorf("LoadTokenPayloadAndWorkloads error:%s", err.Error())
		return
	}

	if len(workloadRecord.NodeWorkload) > 0 {
		newBuffer := bytes.NewBuffer(workloadRecord.NodeWorkload)
		dec := gob.NewDecoder(newBuffer)

		workload := types.Workload{}
		err = dec.Decode(&workload)
		if err != nil {
			t.Errorf("Decode node workload error:%s", err.Error())
			return
		}

		t.Logf("decode node workload:%v", workload)
	}

	if len(workloadRecord.ClientWorkload) > 0 {
		newBuffer := bytes.NewBuffer(workloadRecord.ClientWorkload)
		dec := gob.NewDecoder(newBuffer)

		workload := types.Workload{}
		err = dec.Decode(&workload)
		if err != nil {
			t.Errorf("encode client workload error:%s", err.Error())
			return
		}

		t.Logf("decode client workload:%v", workload)

	}
}
