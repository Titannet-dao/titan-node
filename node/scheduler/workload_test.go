package scheduler

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/sqldb"
)

func TestSaveToken(t *testing.T) {
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

	tkPayload := &types.TokenPayload{ID: "123", NodeID: "222", AssetCID: "11111", ClientID: "3333", CreateTime: time.Now(), Expiration: time.Now().Add(1 * time.Hour)}
	if err := db.SaveTokenPayload([]*types.TokenPayload{tkPayload}); err != nil {
		t.Errorf("SaveToken error:%s", err.Error())
		return
	}

	workload := &types.Workload{DownloadSpeed: 11, DownloadSize: 222, StartTime: time.Now().Unix(), EndTime: time.Now().Unix()}
	workloads := []*types.Workload{workload}

	buffer := &bytes.Buffer{}
	enc := gob.NewEncoder(buffer)
	err = enc.Encode(workloads)
	if err != nil {
		t.Errorf("encode error:%s", err.Error())
		return
	}

	if err := db.UpdateWorkloadReport(tkPayload.ID, false, buffer.Bytes()); err != nil {
		t.Errorf("UpdateWorkload error:%s", err.Error())
		return
	}

	payload, data, err := db.LoadTokenPayloadAndWorkloads(tkPayload.ID, false)
	if err != nil {
		t.Errorf("LoadTokenPayloadAndWorkloads error:%s", err.Error())
		return
	}
	t.Logf("payload:%#v", *payload)

	if len(data) == 0 {
		t.Errorf("workload is empty")
		return
	}

	newBuffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(newBuffer)

	decodeWorkloads := make([]*types.Workload, 0)
	err = dec.Decode(&decodeWorkloads)
	if err != nil {
		t.Errorf("encode error:%s", err.Error())
		return
	}

	for _, workload := range decodeWorkloads {
		t.Logf("decodeWorkloads:%v", *workload)
	}
}
