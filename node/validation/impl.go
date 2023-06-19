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
	checker    Checker
	device     *device.Device
	firstToken func() string
}

type Checker interface {
	GetAssetForValidation(ctx context.Context, randomSeed int64) (Asset, error)
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

	go func() {
		if err = v.sendBlocks(conn, req, v.device.GetBandwidthUp()); err != nil {
			log.Errorf("send blocks error %s", err.Error())
		}
	}()

	return nil
}

func (v *Validation) SetFunc(fun func() string) {
	v.firstToken = fun
}

// sendBlocks sends blocks over a TCP connection with rate limiting
func (v *Validation) sendBlocks(conn *net.TCPConn, req *api.ValidateReq, speedRate int64) error {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Errorf("close tcp error: %s", err.Error())
		}
	}()

	t := time.NewTimer(time.Duration(req.Duration) * time.Second)
	limiter := rate.NewLimiter(rate.Limit(speedRate), int(speedRate))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	asset, err := v.checker.GetAssetForValidation(ctx, req.RandomSeed)
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
		default:
		}

		token := v.firstToken()
		if len(token) > 0 {
			log.Debugf("user is downloading, cancel validation, token %d", token)
			return sendData(conn, []byte(token), api.TCPMsgTypeCancel, limiter)
		}

		blk, err := asset.GetBlock(ctx)
		if err != nil {
			return err
		}

		// don't send empty block
		if len(blk.RawData()) == 0 {
			continue
		}

		err = sendData(conn, blk.RawData(), api.TCPMsgTypeBlock, limiter)
		if err != nil {
			return err
		}
	}
}
