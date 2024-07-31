package edge

import (
	"context"
	"fmt"
	"sync"

	"github.com/Filecoin-Titan/titan/api/types"
	tunclient "github.com/Filecoin-Titan/titan/node/tunnel/client"
)

type TunManager struct {
	tunclients sync.Map
}

func NewTunManager() *TunManager {
	return &TunManager{tunclients: sync.Map{}}
}

func (tm *TunManager) AddTunclient(tc *tunclient.Tunclient) {
	tm.tunclients.Store(tc.ID, tc)

}

func (tm *TunManager) RemoveTunclient(id string) {
	tm.tunclients.Delete(id)
}

func (edge *Edge) CreateTunnel(ctx context.Context, req *types.CreateTunnelReq) error {
	service := edge.Services.Get(req.ProjectID)
	if service == nil {
		return fmt.Errorf("Project %s not exist", req.ProjectID)
	}

	nodeID, err := edge.GetNodeID(ctx)
	if err != nil {
		return err
	}

	if req.NodeID != nodeID {
		req.NodeID = nodeID
	}

	onClose := func(id string) {
		edge.TunManager.RemoveTunclient(id)
	}

	// wsURL := fmt.Sprintf("%s?uuid=%s", req.WsURL, req.TunnelID)
	tunclient, err := tunclient.NewTunclient(req, service, onClose)
	if err != nil {
		return err
	}

	go tunclient.StartService()
	return nil
}
