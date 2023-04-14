package scheduler

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	cliutil "github.com/Filecoin-Titan/titan/cli/util"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

// GetEdgeExternalServiceAddress returns the external service address of an edge node
func (s *Scheduler) GetEdgeExternalServiceAddress(ctx context.Context, nodeID, schedulerURL string) (string, error) {
	eNode := s.NodeManager.GetEdgeNode(nodeID)
	if eNode != nil {
		return eNode.ExternalServiceAddress(ctx, schedulerURL)
	}

	return "", fmt.Errorf("Node %s offline or not exist", nodeID)
}

// CheckEdgeConnectivity checks if the edge node is reachable
func (s *Scheduler) CheckEdgeConnectivity(ctx context.Context, url string) error {
	udpPacketConn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return err
	}
	defer func() {
		err = udpPacketConn.Close()
		if err != nil {
			log.Errorf("udpPacketConn Close err:%s", err.Error())
		}
	}()

	httpClient, err := cliutil.NewHTTP3Client(udpPacketConn, true, "")
	if err != nil {
		return err
	}

	edgeAPI, close, err := client.NewEdgeWithHTTPClient(context.Background(), url, nil, httpClient)
	if err != nil {
		return err
	}
	defer close()

	if _, err := edgeAPI.Version(context.Background()); err != nil {
		return err
	}
	return nil
}

// CheckNetworkConnectivity check tcp or udp network connectivity
// network is "tcp" or "udp"
func (s *Scheduler) CheckNetworkConnectivity(ctx context.Context, network, targetURL string) error {
	switch network {
	case "tcp":
		return s.verifyTCPConnectivity(targetURL)
	case "udp":
		return s.verifyUDPConnectivity(targetURL)
	}

	return fmt.Errorf("unknow network %s type", network)
}

// checks if an edge node is behind a Full Cone NAT
func (s *Scheduler) detectFullConeNAT(ctx context.Context, schedulerURL, edgeURL string) (bool, error) {
	schedulerAPI, close, err := client.NewScheduler(context.Background(), schedulerURL, nil)
	if err != nil {
		return false, err
	}
	defer close()

	if err = schedulerAPI.CheckNetworkConnectivity(context.Background(), "udp", edgeURL); err == nil {
		return true, nil
	}

	log.Debugf("check udp connectivity failed: %s", err.Error())

	return false, nil
}

// checks if an edge node is behind a Restricted NAT
func (s *Scheduler) detectRestrictedNAT(ctx context.Context, edgeURL string) (bool, error) {
	udpPacketConn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return false, err
	}
	defer func() {
		err = udpPacketConn.Close()
		if err != nil {
			log.Errorf("udpPacketConn Close err:%s", err.Error())
		}
	}()

	httpClient := &http.Client{}
	edgeAPI, close, err := client.NewEdgeWithHTTPClient(context.Background(), edgeURL, nil, httpClient)
	if err != nil {
		return false, err
	}
	defer close()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := edgeAPI.Version(ctx); err != nil {
		log.Warnf("detectRestrictedNAT,edge %s PortRestrictedNAT", edgeURL)
		return false, nil //nolint:nilerr
	}

	return true, nil
}

// determines the NAT type of an edge node
func (s *Scheduler) analyzeEdgeNodeNATType(ctx context.Context, edgeAPI *node.API, edgeAddr string) (types.NatType, error) {
	if len(s.SchedulerCfg.SchedulerServer1) == 0 {
		return types.NatTypeUnknow, nil
	}

	externalAddr, err := edgeAPI.ExternalServiceAddress(ctx, s.SchedulerCfg.SchedulerServer1)
	if err != nil {
		return types.NatTypeUnknow, err
	}

	if externalAddr != edgeAddr {
		return types.NatTypeSymmetric, nil
	}

	if err = s.CheckNetworkConnectivity(ctx, "tcp", edgeAddr); err == nil {
		return types.NatTypeNo, nil
	}

	log.Debugf("analyzeEdgeNodeNATType error: %s", err.Error())

	if len(s.SchedulerCfg.SchedulerServer2) == 0 {
		return types.NatTypeUnknow, nil
	}

	edgeURL := fmt.Sprintf("https://%s/rpc/v0", edgeAddr)
	isBehindFullConeNAT, err := s.detectFullConeNAT(ctx, s.SchedulerCfg.SchedulerServer2, edgeURL)
	if err != nil {
		return types.NatTypeUnknow, err
	}

	if isBehindFullConeNAT {
		return types.NatTypeFullCone, nil
	}

	isBehindRestrictedNAT, err := s.detectRestrictedNAT(ctx, edgeURL)
	if err != nil {
		return types.NatTypeUnknow, err
	}

	if isBehindRestrictedNAT {
		return types.NatTypeRestricted, nil
	}

	return types.NatTypePortRestricted, nil
}

// identifies the NAT type of a node
func (s *Scheduler) determineNATType(ctx context.Context, edgeAPI *node.API, edgeAddr string) types.NatType {
	natType, err := s.analyzeEdgeNodeNATType(context.Background(), edgeAPI, edgeAddr)
	if err != nil {
		log.Errorf("determineNATType, error:%s", err.Error())
		natType = types.NatTypeUnknow
	}
	return natType
}

// GetNodeNATType gets the node's NAT type
func (s *Scheduler) GetNodeNATType(ctx context.Context, nodeID string) (types.NatType, error) {
	eNode := s.NodeManager.GetEdgeNode(nodeID)
	if eNode == nil {
		return types.NatTypeUnknow, fmt.Errorf("node %s offline or not exist", nodeID)
	}

	return s.determineNATType(ctx, eNode.API, eNode.RemoteAddr()), nil
}

func (s *Scheduler) verifyTCPConnectivity(targetURL string) error {
	url, err := url.ParseRequestURI(targetURL)
	if err != nil {
		return xerrors.Errorf("parse uri error: %w, url: %s", err, targetURL)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", url.Host)
	if err != nil {
		return xerrors.Errorf("resolve tcp addr %w, host %s", err, url.Host)
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return xerrors.Errorf("dial tcp %w, addr %s", err, tcpAddr)
	}
	defer conn.Close()

	return nil
}

func (s *Scheduler) verifyUDPConnectivity(targetURL string) error {
	udpPacketConn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return xerrors.Errorf("list udp %w, url %s", err, targetURL)
	}
	defer func() {
		err = udpPacketConn.Close()
		if err != nil {
			log.Errorf("udpPacketConn Close err:%s", err.Error())
		}
	}()

	httpClient, err := cliutil.NewHTTP3Client(udpPacketConn, true, "")
	if err != nil {
		return xerrors.Errorf("new http3 client %w", err)
	}
	httpClient.Timeout = 5 * time.Second

	resp, err := httpClient.Get(targetURL)
	if err != nil {
		return xerrors.Errorf("http3 client get error: %w, url: %s", err, targetURL)
	}
	defer resp.Body.Close()

	return nil
}

// NatPunch performs NAT traversal
func (s *Scheduler) NatPunch(ctx context.Context, target *types.NatPunchReq) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	sourceURL := fmt.Sprintf("https://%s/ping", remoteAddr)

	eNode := s.NodeManager.GetEdgeNode(target.NodeID)
	if eNode == nil {
		return xerrors.Errorf("edge %s not exist", target.NodeID)
	}

	return eNode.UserNATPunch(context.Background(), sourceURL, target)
}
