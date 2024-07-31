package main

import (
	"context"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/terrors"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/edge/clib"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"golang.org/x/xerrors"
)

type heartbeatParams struct {
	shutdownChan chan struct{}
	edgeAPI      api.Edge
	schedulerAPI api.Scheduler
	nodeID       string
	daemonSwitch *clib.DaemonSwitch
	geoInfo      *region.GeoInfo
}

func heartbeat(ctx context.Context, hbp heartbeatParams) error {
	schedulerSession, err := hbp.schedulerAPI.Session(ctx)
	if err != nil {
		err = xerrors.Errorf("getting scheduler session: %w", err)
		hbp.daemonSwitch.ErrMsg = err.Error()
		return err
	}

	token, err := hbp.edgeAPI.AuthNew(ctx, &types.JWTPayload{Allow: []auth.Permission{api.RoleAdmin}, ID: hbp.nodeID})
	if err != nil {
		err = xerrors.Errorf("generate token for scheduler error: %w", err)
		hbp.daemonSwitch.ErrMsg = err.Error()
		return err
	}

	heartbeats := time.NewTicker(HeartbeatInterval)
	defer heartbeats.Stop()

	// Set daemonSwitch on heartbeat exit
	// Avoid missing set stop at return
	defer func() {
		hbp.daemonSwitch.IsOnline = false
		hbp.daemonSwitch.IsStop = true
		quitWg.Done()
	}()

	var readyCh chan struct{}
	for {
		// TODO: we could get rid of this, but that requires tracking resources for restarted tasks correctly
		if readyCh == nil {
			log.Info("Making sure no local tasks are running")
			readyCh = waitQuietCh(hbp.edgeAPI)
		}

		for {
			// basically when ctx.Done or hbp.daemonSwitch.StopChan receive signal, break the heartbeat loop
			select {
			case <-readyCh:
				opts := &types.ConnectOptions{Token: token, GeoInfo: hbp.geoInfo}
				if err := hbp.schedulerAPI.EdgeConnect(ctx, opts); err != nil {
					log.Errorf("Registering edge failed: %s", err.Error())
					hbp.daemonSwitch.ErrMsg = err.Error()
					hbp.shutdownChan <- struct{}{}
					return err
				}
				hbp.daemonSwitch.IsOnline = true
				hbp.daemonSwitch.IsStop = false
				log.Info("Edge registered successfully, waiting for tasks")
				readyCh = nil
			case <-heartbeats.C:
			case <-ctx.Done():
				log.Warn("heartbeat stopped")
				hbp.daemonSwitch.IsOnline = false
				hbp.daemonSwitch.IsStop = true
				return nil // graceful shutdown
			case isStop := <-hbp.daemonSwitch.StopChan:
				hbp.daemonSwitch.IsOnline = !isStop
				hbp.daemonSwitch.IsStop = isStop
				if isStop {
					log.Info("stop daemon")
					return nil
				} else {
					log.Info("start daemon")
				}
			}

			if hbp.daemonSwitch.IsStop {
				return nil
			}

			curSession, err := keepalive(hbp.schedulerAPI, 10*time.Second)
			if err != nil {
				log.Errorf("heartbeat: keepalive failed: %+v", err)
				errNode, ok := err.(*api.ErrNode)
				if ok {
					if errNode.Code == int(terrors.NodeDeactivate) {
						hbp.shutdownChan <- struct{}{}
						return nil
					} else if errNode.Code == int(terrors.NodeIPInconsistent) {
						break
					} else if errNode.Code == int(terrors.NodeOffline) && readyCh == nil {
						break
					}
				} else {
					hbp.daemonSwitch.IsOnline = false
				}
			} else if curSession != schedulerSession {
				log.Warn("change session id")
				schedulerSession = curSession
				break
			} else {
				hbp.daemonSwitch.IsOnline = true
			}

			// log.Infof("cur session id %s", curSession.String())
		}

		log.Errorf("TITAN-EDGE CONNECTION LOST")
	}
}
