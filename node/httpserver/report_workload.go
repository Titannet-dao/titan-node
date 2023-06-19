package httpserver

import (
	"bytes"
	"context"
	"crypto"
	"encoding/gob"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
)

const (
	batch          = 1000
	tickerInterval = 60 * time.Second
)

type report struct {
	TokenID  string
	ClientID string
	*types.Workload
}

type reporter struct {
	reports []*report
	lock    *sync.Mutex
	server  *HttpServer
}

func newReporter(server *HttpServer) *reporter {
	r := &reporter{
		reports: make([]*report, 0),
		lock:    &sync.Mutex{},
		server:  server,
	}
	go r.startTicker()

	return r
}

func (r *reporter) startTicker() {
	for {
		time.Sleep(tickerInterval)

		if err := r.handleReports(); err != nil {
			log.Errorf("sendReports error:%s", err.Error())
		}
	}

}

func (r *reporter) addReport(report *report) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.reports = append(r.reports, report)
}

func (r *reporter) removeReportsFromHead(n int) []*report {
	r.lock.Lock()
	defer r.lock.Unlock()

	if len(r.reports) < n {
		n = len(r.reports)
	}

	reports := r.reports[0:n]
	r.reports = r.reports[n:]
	return reports
}

func (r *reporter) handleReports() error {
	for len(r.reports) > 0 {
		reports := r.removeReportsFromHead(batch)
		rps := r.mergeReports(reports)
		err := r.doSendReports(rps)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *reporter) mergeReports(rps []*report) []*types.WorkloadReport {
	type reportStats struct {
		tokenID         string
		clientID        string
		downloadSize    int64
		speedCount      int
		accumulateSpeed int64
		startTime       int64
		endTime         int64
	}
	// reportMap := make(map[string]*types.WorkloadReport)
	reportStatsMap := make(map[string]*reportStats)
	for _, rp := range rps {
		r, ok := reportStatsMap[rp.TokenID]
		if !ok {
			r = &reportStats{tokenID: rp.TokenID, clientID: rp.ClientID}
		}

		r.downloadSize += rp.DownloadSize
		if rp.DownloadSpeed > 0 {
			r.accumulateSpeed += rp.DownloadSpeed
			r.speedCount++
		}
		if r.startTime == 0 || rp.StartTime < r.startTime {
			r.startTime = rp.StartTime
		}
		if rp.EndTime > r.endTime {
			r.endTime = rp.EndTime
		}
		reportStatsMap[rp.TokenID] = r
	}

	reports := make([]*types.WorkloadReport, 0, len(reportStatsMap))
	for _, v := range reportStatsMap {
		downloadSpeed := int64(0)
		if v.speedCount > 0 {
			downloadSpeed = v.accumulateSpeed / int64(v.speedCount)
		}
		workload := &types.Workload{DownloadSpeed: downloadSpeed, DownloadSize: v.downloadSize, StartTime: v.startTime, EndTime: v.endTime}
		workloadReport := &types.WorkloadReport{TokenID: v.tokenID, ClientID: v.clientID, Workload: workload}
		reports = append(reports, workloadReport)
	}

	return reports
}

func (r *reporter) doSendReports(rps []*types.WorkloadReport) error {

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(rps)
	if err != nil {
		return err
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	cipherText, err := titanRsa.Encrypt(buffer.Bytes(), r.server.schedulerPublicKey)
	if err != nil {
		return err
	}

	sign, err := titanRsa.Sign(r.server.privateKey, cipherText)
	if err != nil {
		return err
	}

	report := types.NodeWorkloadReport{CipherText: cipherText, Sign: sign}

	encodeBuf := &bytes.Buffer{}
	enc = gob.NewEncoder(encodeBuf)
	err = enc.Encode(report)
	if err != nil {
		return err
	}

	log.Debugf("doSendReports SubmitNodeWorkloadReport")
	return r.server.scheduler.SubmitNodeWorkloadReport(context.Background(), encodeBuf)
}
