package validation

import (
	"context"
	"net"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/node/device"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/time/rate"
	"golang.org/x/xerrors"
)

var log = logging.Logger("validate")

type Validation struct {
	checker       Checker
	device        *device.Device
	cancelChannel chan bool
}

type Checker interface {
	GetChecker(ctx context.Context, randomSeed int64) (Asset, error)
}

type Asset interface {
	GetBlock(ctx context.Context) (blocks.Block, error)
}

// NewValidation creates a new Validation instance
func NewValidation(c Checker, device *device.Device) *Validation {
	return &Validation{checker: c, device: device}
}

// ExecuteValidation performs the validation process
func (v *Validation) ExecuteValidation(ctx context.Context, req *api.ValidateReq) error {
	conn, err := newTCPClient(req.TCPSrvAddr)
	if err != nil {
		log.Errorf("new tcp client err:%v", err)
		return err
	}

	go v.sendBlocks(conn, req, v.device.GetBandwidthUp())

	return nil
}

// StopValidation sends a cancellation signal to stop the validation process
func (v *Validation) StopValidation() {
	if v.cancelChannel != nil {
		v.cancelChannel <- true
	}
}

// sendBlocks sends blocks over a TCP connection with rate limiting
func (v *Validation) sendBlocks(conn *net.TCPConn, req *api.ValidateReq, speedRate int64) error {
	defer func() {
		v.cancelChannel = nil
		if err := conn.Close(); err != nil {
			log.Errorf("close tcp error: %s", err.Error())
		}
	}()

	v.cancelChannel = make(chan bool)

	t := time.NewTimer(time.Duration(req.Duration) * time.Second)
	limiter := rate.NewLimiter(rate.Limit(speedRate), int(speedRate))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	asset, err := v.checker.GetChecker(ctx, req.RandomSeed)
	if err != nil {
		return xerrors.Errorf("get checker error %w", err)
	}

	nodeID, err := v.device.GetNodeID(ctx)
	if err != nil {
		return err
	}

	if err := sendNodeID(conn, nodeID, limiter); err != nil {
		return err
	}

	for {
		select {
		case <-t.C:
			return nil
		case <-v.cancelChannel:
			return sendData(conn, nil, api.TCPMsgTypeCancel, limiter)
		default:
		}

		blk, err := asset.GetBlock(ctx)
		if err != nil {
			return err
		}
		err = sendData(conn, blk.RawData(), api.TCPMsgTypeBlock, limiter)
		if err != nil {
			return err
		}
	}
}
