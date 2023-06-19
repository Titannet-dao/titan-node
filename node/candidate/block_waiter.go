package candidate

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

// blockWaiter holds the information necessary to wait for blocks from a node
type blockWaiter struct {
	ch       chan tcpMsg
	result   *api.ValidationResult
	duration int
	NodeValidatedResulter
	privateKey *rsa.PrivateKey
}

type blockWaiterOptions struct {
	nodeID     string
	ch         chan tcpMsg
	duration   int
	resulter   NodeValidatedResulter
	privateKey *rsa.PrivateKey
}

// NodeValidatedResulter is the interface to return the validation result
type NodeValidatedResulter interface {
	NodeValidationResult(ctx context.Context, r io.Reader, sign string) error
}

// newBlockWaiter creates a new blockWaiter instance
func newBlockWaiter(opts *blockWaiterOptions) *blockWaiter {
	bw := &blockWaiter{
		ch:                    opts.ch,
		duration:              opts.duration,
		result:                &api.ValidationResult{NodeID: opts.nodeID},
		NodeValidatedResulter: opts.resulter,
		privateKey:            opts.privateKey,
	}
	go bw.wait()

	return bw
}

// wait waits for blocks from a node, and send the validation result
func (bw *blockWaiter) wait() {
	size := int64(0)
	now := time.Now()

	defer func() {
		bw.result.CostTime = int64(time.Since(now) / time.Second)
		bw.calculateBandwidth(size)
		if err := bw.sendValidateResult(); err != nil {
			log.Errorf("send validate result %s", err.Error())
		}

		log.Debugf("validate %s %d block, bandwidth:%f, cost time:%d, IsTimeout:%v, duration:%d, size:%d, randCount:%d, isCancel:%t, token:%s",
			bw.result.NodeID, len(bw.result.Cids), bw.result.Bandwidth, bw.result.CostTime, bw.result.IsTimeout, bw.duration, size, bw.result.RandomCount, bw.result.IsCancel, bw.result.Token)
	}()

	for {
		tcpMsg, ok := <-bw.ch
		if !ok {
			return
		}

		switch tcpMsg.msgType {
		case api.TCPMsgTypeCancel:
			bw.result.IsCancel = true
			bw.result.Token = string(tcpMsg.msg)
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
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(bw.result)
	if err != nil {
		return err
	}

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sign, err := titanRsa.Sign(bw.privateKey, buffer.Bytes())
	if err != nil {
		return xerrors.Errorf("sign validate result error: %w", err.Error())
	}

	return bw.NodeValidationResult(context.Background(), &buffer, hex.EncodeToString(sign))
}

// calculateBandwidth calculates the bandwidth based on the block size and duration
func (bw *blockWaiter) calculateBandwidth(size int64) {
	costTime := bw.result.CostTime
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
