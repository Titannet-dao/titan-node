package nat

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
)

// checks if an edge node is behind a Full Cone NAT
func detectFullConeNAT(ctx context.Context, candidate *node.Node, edgeURL string) (bool, error) {
	return candidate.API.CheckNetworkConnectable(ctx, "udp", edgeURL)
}

// checks if an edge node is behind a Restricted NAT
func detectRestrictedNAT(ctx context.Context, http3Client *http.Client, edgeURL string) (bool, error) {
	resp, err := http3Client.Get(edgeURL)
	if err != nil {
		log.Debugf("detectRestrictedNAT failed: %s", err.Error())
		return false, nil
	}
	defer resp.Body.Close()

	return true, nil
}

// determines the NAT type of an edge node
func analyzeEdgeNodeNATType(ctx context.Context, edgeNode *node.Node, candidateNodes []*node.Node, http3Client *http.Client) (types.NatType, error) {
	if len(candidateNodes) < miniCandidateCount {
		return types.NatTypeUnknown, fmt.Errorf("a minimum of %d candidates is required for nat detect", miniCandidateCount)
	}

	candidate1 := candidateNodes[0]
	externalAddr, err := edgeNode.API.ExternalServiceAddress(ctx, candidate1.RPCURL())
	if err != nil {
		return types.NatTypeUnknown, fmt.Errorf("ExternalServiceAddress error %s", err.Error())
	}

	if externalAddr != edgeNode.RemoteAddr {
		return types.NatTypeSymmetric, nil
	}

	edgeURL := fmt.Sprintf("https://%s/rpc/v0", edgeNode.RemoteAddr)

	candidate2 := candidateNodes[1]
	ok, err := candidate2.API.CheckNetworkConnectable(ctx, "tcp", edgeURL)
	if err != nil {
		return types.NatTypeUnknown, err
	}

	if ok {
		ok, err = candidate2.API.CheckNetworkConnectable(ctx, "udp", edgeURL)
		if err != nil {
			return types.NatTypeUnknown, err
		}

		if ok {
			return types.NatTypeNo, nil
		}
	}

	log.Debugf("check candidate %s to edge %s tcp connectivity failed", candidate2.NodeID, edgeURL)

	if ok, err := detectFullConeNAT(ctx, candidate2, edgeURL); err != nil {
		return types.NatTypeFullCone, err
	} else if ok {
		return types.NatTypeFullCone, nil
	}

	log.Debugf("check candidate %s to edge %s udp connectivity failed", candidate2.NodeID, edgeURL)

	if isBehindRestrictedNAT, err := detectRestrictedNAT(ctx, http3Client, edgeURL); err != nil {
		return types.NatTypeUnknown, err
	} else if isBehindRestrictedNAT {
		return types.NatTypeRestricted, nil
	}

	return types.NatTypePortRestricted, nil
}

// determineNATType detect the NAT type of an edge node
func determineEdgeNATType(ctx context.Context, edgeNode *node.Node, candidateNodes []*node.Node, http3Client *http.Client) (types.NatType, error) {
	natType, err := analyzeEdgeNodeNATType(ctx, edgeNode, candidateNodes, http3Client)
	if err != nil {
		log.Warnf("determineNATType, error: %s", err.Error())
		natType = types.NatTypeUnknown
	}
	return natType, nil
}
