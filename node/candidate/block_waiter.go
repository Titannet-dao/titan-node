package candidate

import (
	"context"
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// blockWaiter holds the information necessary to wait for blocks from a node
type blockWaiter struct {
	ch       chan tcpMsg
	result   *api.ValidationResult
	duration int
	NodeValidatedResulter
}

// NodeValidatedResulter is the interface to return the validation result
type NodeValidatedResulter interface {
	NodeValidationResult(ctx context.Context, vr api.ValidationResult) error
}

// newBlockWaiter creates a new blockWaiter instance
func newBlockWaiter(nodeID string, ch chan tcpMsg, duration int, resulter NodeValidatedResulter) *blockWaiter {
	bw := &blockWaiter{ch: ch, duration: duration, result: &api.ValidationResult{NodeID: nodeID}, NodeValidatedResulter: resulter}
	go bw.wait()

	return bw
}

// wait waits for blocks from a node, and send the validation result
func (bw *blockWaiter) wait() {
	size := int64(0)
	now := time.Now()

	defer func() {
		bw.calculateBandwidth(int64(time.Since(now)), size)
		if err := bw.sendValidateResult(); err != nil {
			log.Errorf("send validate result %s", err.Error())
		}

		log.Debugf("validator %s %d block, bandwidth:%f, cost time:%d, IsTimeout:%v, duration:%d, size:%d, randCount:%d",
			bw.result.NodeID, len(bw.result.Cids), bw.result.Bandwidth, bw.result.CostTime, bw.result.IsTimeout, bw.duration, size, bw.result.RandomCount)
	}()

	for {
		tcpMsg, ok := <-bw.ch
		if !ok {
			return
		}

		switch tcpMsg.msgType {
		case api.TCPMsgTypeCancel:
			bw.result.IsCancel = true
		case api.TCPMsgTypeBlock:
			if tcpMsg.length > 0 {
				if cid, err := cidFromData(tcpMsg.msg); err == nil {
					bw.result.Cids = append(bw.result.Cids, cid)
				} else {
					log.Errorf("waitBlock, cidFromData error:%v", err)
				}
			}
			size += int64(tcpMsg.length)
			bw.result.RandomCount++
		}

	}
}

// sendValidateResult sends the validation result
func (bw *blockWaiter) sendValidateResult() error {
	return bw.NodeValidationResult(context.Background(), *bw.result)
}

// calculateBandwidth calculates the bandwidth based on the block size and duration
func (bw *blockWaiter) calculateBandwidth(costTime int64, size int64) {
	bw.result.CostTime = costTime
	if costTime < int64(bw.duration) {
		costTime = int64(bw.duration)
	}
	bw.result.Bandwidth = float64(size) / float64(costTime)
}

// cidFromData creates a CID from the given data
func cidFromData(data []byte) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("convert data to cid error: data len == 0")
	}

	pref := cid.Prefix{
		Version:  1,
		Codec:    uint64(cid.Raw),
		MhType:   mh.SHA2_256,
		MhLength: -1, // default length
	}

	c, err := pref.Sum(data)
	if err != nil {
		return "", err
	}

	return c.String(), nil
}
