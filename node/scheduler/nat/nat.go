package nat

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
)

// checks if an edge node is behind a Full Cone NAT
func detectFullConeNAT(ctx context.Context, candidate *node.Node, eURL string) (bool, error) {
	return candidate.API.CheckNetworkConnectable(ctx, "udp", eURL)
}

// checks if an edge node is behind a Restricted NAT
func detectRestrictedNAT(http3Client *http.Client, eURL string) (bool, error) {
	resp, err := http3Client.Get(eURL)
	if err != nil {
		log.Debugf("detectRestrictedNAT failed: %s", err.Error())
		return false, nil
	}
	defer resp.Body.Close()

	return true, nil
}

// determines the NAT type of an edge node
func analyzeNodeNATType(ctx context.Context, eNode *node.Node, candidateNodes []*node.Node, http3Client *http.Client) (types.NatType, error) {
	if candidateNodes == nil || len(candidateNodes) < miniCandidateCount {
		return types.NatTypeUnknown, fmt.Errorf("a minimum of %d candidates is required for nat detect", miniCandidateCount)
	}

	candidate1 := candidateNodes[0]
	externalAddr, err := eNode.API.ExternalServiceAddress(ctx, candidate1.RPCURL())
	if err != nil {
		return types.NatTypeUnknown, fmt.Errorf("ExternalServiceAddress error %s", err.Error())
	}

	if externalAddr != eNode.RemoteAddr {
		return types.NatTypeSymmetric, nil
	}

	eURL := fmt.Sprintf("https://%s/abc", eNode.RemoteAddr)

	candidate2 := candidateNodes[1]
	ok, err := candidate2.API.CheckNetworkConnectable(ctx, "tcp", eURL)
	if err != nil {
		return types.NatTypeUnknown, err
	}

	if ok {
		ok, err = candidate2.API.CheckNetworkConnectable(ctx, "udp", eURL)
		if err != nil {
			return types.NatTypeUnknown, err
		}

		if ok {
			return types.NatTypeNo, nil
		}
	}

	log.Debugf("check candidate %s to edge %s tcp connectivity failed", candidate2.NodeID, eURL)

	if ok, err := detectFullConeNAT(ctx, candidate2, eURL); err != nil {
		return types.NatTypeUnknown, err
	} else if ok {
		return types.NatTypeFullCone, nil
	}

	log.Debugf("check candidate %s to edge %s udp connectivity failed", candidate2.NodeID, eURL)

	if isBehindRestrictedNAT, err := detectRestrictedNAT(http3Client, eURL); err != nil {
		return types.NatTypeUnknown, err
	} else if isBehindRestrictedNAT {
		return types.NatTypeRestricted, nil
	}

	return types.NatTypePortRestricted, nil
}

// determineNATType detect the NAT type of an edge node
func determineNodeNATType(edgeNode *node.Node, candidateNodes []*node.Node, http3Client *http.Client) string {
	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	natType, err := analyzeNodeNATType(ctx, edgeNode, candidateNodes, http3Client)
	if err != nil {
		log.Warnf("determineNATType, %s error: %s", edgeNode.NodeID, err.Error())
		natType = types.NatTypeUnknown
	}
	return natType.String()
}
