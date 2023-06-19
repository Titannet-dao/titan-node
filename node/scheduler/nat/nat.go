package nat

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	cliutil "github.com/Filecoin-Titan/titan/cli/util"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"

	"golang.org/x/xerrors"
)

// checks if an edge node is behind a Full Cone NAT
func detectFullConeNAT(ctx context.Context, candidate *node.Node, edgeURL string) error {
	return candidate.API.CheckNetworkConnectivity(ctx, "udp", edgeURL)
}

// checks if an edge node is behind a Restricted NAT
func detectRestrictedNAT(ctx context.Context, edgeURL string) (bool, error) {
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

	httpClient, err := cliutil.NewHTTP3Client(udpPacketConn, true, "")
	if err != nil {
		return false, xerrors.Errorf("new http3 client %w", err)
	}
	httpClient.Timeout = 5 * time.Second

	resp, err := httpClient.Get(edgeURL)
	if err != nil {
		log.Debugf("detectRestrictedNAT failed: %s", err.Error())
		return false, nil
	}
	defer resp.Body.Close()

	return true, nil
}

// determines the NAT type of an edge node
func analyzeEdgeNodeNATType(ctx context.Context, edgeNode *node.Node, candidateNodes []*node.Node) (types.NatType, error) {
	if len(candidateNodes) < miniCandidateCount {
		return types.NatTypeUnknown, fmt.Errorf("a minimum of %d candidates is required for nat detect", miniCandidateCount)
	}

	candidate1 := candidateNodes[0]
	externalAddr, err := edgeNode.API.ExternalServiceAddress(ctx, candidate1.RPCURL())
	if err != nil {
		return types.NatTypeUnknown, err
	}

	if externalAddr != edgeNode.RemoteAddr {
		return types.NatTypeSymmetric, nil
	}

	edgeURL := fmt.Sprintf("https://%s/rpc/v0", edgeNode.RemoteAddr)

	candidate2 := candidateNodes[1]
	if err = candidate2.API.CheckNetworkConnectivity(ctx, "tcp", edgeURL); err == nil {
		return types.NatTypeNo, nil
	}

	log.Debugf("check candidate %s to edge %s tcp connectivity failed: %s", candidate2.NodeID, edgeURL, err.Error())

	if err = detectFullConeNAT(ctx, candidate2, edgeURL); err == nil {
		return types.NatTypeFullCone, nil
	}

	log.Debugf("check candidate %s to edge %s udp connectivity failed: %s", candidate2.NodeID, edgeURL, err.Error())

	if isBehindRestrictedNAT, err := detectRestrictedNAT(ctx, edgeURL); err != nil {
		return types.NatTypeUnknown, err
	} else if isBehindRestrictedNAT {
		return types.NatTypeRestricted, nil
	}

	return types.NatTypePortRestricted, nil
}

// determineNATType detect the NAT type of an edge node
func determineEdgeNATType(ctx context.Context, edgeNode *node.Node, candidateNodes []*node.Node) (types.NatType, error) {
	natType, err := analyzeEdgeNodeNATType(ctx, edgeNode, candidateNodes)
	if err != nil {
		log.Warnf("determineNATType, error: %s", err.Error())
		natType = types.NatTypeUnknown
	}
	return natType, nil
}
