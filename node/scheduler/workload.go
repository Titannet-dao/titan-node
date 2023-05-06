package scheduler

import (
	"bytes"
	"context"
	"crypto"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/handler"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"golang.org/x/xerrors"
)

// GetEdgeDownloadInfos finds edge download information for a given CID
func (s *Scheduler) GetEdgeDownloadInfos(ctx context.Context, cid string) (*types.EdgeDownloadInfoList, error) {
	if cid == "" {
		return nil, xerrors.New("cids is nil")
	}

	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	rows, err := s.NodeManager.LoadReplicasByHash(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	infos := make([]*types.EdgeDownloadInfo, 0)
	tkPayloads := make([]*types.TokenPayload, 0)

	for rows.Next() {
		rInfo := &types.ReplicaInfo{}
		err = rows.StructScan(rInfo)
		if err != nil {
			log.Errorf("replica StructScan err: %s", err.Error())
			continue
		}

		if rInfo.IsCandidate {
			continue
		}

		nodeID := rInfo.NodeID
		eNode := s.NodeManager.GetEdgeNode(nodeID)
		if eNode == nil {
			continue
		}

		token, tkPayload, err := eNode.Token(cid, titanRsa, s.NodeManager.PrivateKey)
		if err != nil {
			continue
		}
		tkPayloads = append(tkPayloads, tkPayload)

		info := &types.EdgeDownloadInfo{
			URL:     eNode.DownloadAddr(),
			NodeID:  nodeID,
			Tk:      token,
			NatType: eNode.NATType,
		}
		infos = append(infos, info)
	}

	if len(infos) == 0 {
		return nil, nil
	}

	if len(tkPayloads) > 0 {
		if err = s.NodeManager.SaveTokenPayload(tkPayloads); err != nil {
			return nil, err
		}
	}

	pk, err := s.GetSchedulerPublicKey(ctx)
	if err != nil {
		return nil, err
	}

	ret := &types.EdgeDownloadInfoList{
		Infos:        infos,
		SchedulerURL: s.SchedulerCfg.ExternalURL,
		SchedulerKey: pk,
	}

	return ret, nil
}

// SubmitUserWorkloadReport submits report of workload for User Asset Download
func (s *Scheduler) SubmitUserWorkloadReport(ctx context.Context, r io.Reader) error {
	cipherText, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	data, err := titanRsa.Decrypt(cipherText, s.PrivateKey)
	if err != nil {
		return xerrors.Errorf("decrypt error: %w", err)
	}

	reports := make([]*types.WorkloadReport, 0)
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err = dec.Decode(&reports)
	if err != nil {
		return xerrors.Errorf("decode data to []*types.WorkloadReport error: %w", err)
	}

	for _, rp := range reports {
		if err = s.handleWorkloadReport(rp.NodeID, rp, true); err != nil {
			log.Errorf("handler user workload report error %s, token id %s", err.Error(), rp.TokenID)
			continue
		}
	}
	return nil
}

// SubmitNodeWorkloadReport submits report of workload for node Asset Download
func (s *Scheduler) SubmitNodeWorkloadReport(ctx context.Context, r io.Reader) error {
	nodeID := handler.GetNodeID(ctx)
	node := s.NodeManager.GetNode(nodeID)
	if node == nil {
		return fmt.Errorf("node %s not exists", nodeID)
	}

	report := &types.NodeWorkloadReport{}
	dec := gob.NewDecoder(r)
	err := dec.Decode(report)
	if err != nil {
		return xerrors.Errorf("decode data to NodeWorkloadReport error: %w", err)
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	if err = titanRsa.VerifySign(node.PublicKey(), report.Sign, report.CipherText); err != nil {
		return xerrors.Errorf("verify sign error: %w", err)
	}

	data, err := titanRsa.Decrypt(report.CipherText, s.PrivateKey)
	if err != nil {
		return xerrors.Errorf("decrypt error: %w", err)
	}

	reports := make([]*types.WorkloadReport, 0)
	dec = gob.NewDecoder(bytes.NewBuffer(data))
	err = dec.Decode(&reports)
	if err != nil {
		return xerrors.Errorf("decode data to []*types.WorkloadReport error: %w", err)
	}

	for _, rp := range reports {
		if err = s.handleWorkloadReport(nodeID, rp, false); err != nil {
			log.Errorf("handler node workload report error %s", err.Error())
			continue
		}
	}

	return nil
}

func (s *Scheduler) handleWorkloadReport(nodeID string, report *types.WorkloadReport, isClient bool) error {
	tkPayload, workloadBytes, err := s.NodeManager.LoadTokenPayloadAndWorkloads(report.TokenID, isClient)
	if err != nil {
		return xerrors.Errorf("load token payload and workloads with token id %s error: %w", report.TokenID, err)
	}
	if isClient && tkPayload.NodeID != nodeID {
		return fmt.Errorf("token payload node id %s, but report node id is %s", tkPayload.NodeID, report.NodeID)
	}

	if !isClient && tkPayload.ClientID != report.ClientID {
		return fmt.Errorf("token payload client id %s, but report client id is %s", tkPayload.ClientID, report.ClientID)
	}

	if tkPayload.Expiration.Before(time.Now()) {
		return fmt.Errorf("token payload expiration %s < %s", tkPayload.Expiration.Local().String(), time.Now().Local().String())
	}

	workload := &types.Workload{}
	if len(workloadBytes) > 0 {
		dec := gob.NewDecoder(bytes.NewBuffer(workloadBytes))
		err = dec.Decode(workload)
		if err != nil {
			return xerrors.Errorf("decode data to []*types.Workload error: %w", err)
		}
	}

	workload = s.mergeWorkloads([]*types.Workload{workload, report.Workload})

	buffer := &bytes.Buffer{}
	enc := gob.NewEncoder(buffer)
	err = enc.Encode(workload)
	if err != nil {
		return xerrors.Errorf("decode data to []types.Workload error: %w", err)
	}

	return s.NodeManager.UpdateWorkloadReport(report.TokenID, isClient, buffer.Bytes())
}

func (s *Scheduler) mergeWorkloads(workloads []*types.Workload) *types.Workload {
	if len(workloads) == 0 {
		return nil
	}

	costTime := int64(0)
	downloadSize := int64(0)
	startTime := int64(0)
	endTime := int64(0)

	for _, workload := range workloads {
		if workload.DownloadSpeed > 0 {
			costTime += workload.DownloadSize / workload.DownloadSpeed
		}
		downloadSize += workload.DownloadSize

		if startTime == 0 || workload.StartTime < startTime {
			startTime = workload.StartTime
		}

		if workload.EndTime > endTime {
			endTime = workload.EndTime
		}
	}

	downloadSpeed := int64(0)
	if costTime > 0 {
		downloadSpeed = downloadSize / costTime
	}
	return &types.Workload{DownloadSpeed: downloadSpeed, DownloadSize: downloadSize, StartTime: startTime, EndTime: endTime}
}
