package cli

import (
	"context"
	"fmt"

	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/repo"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
)

var CandidateCmds = []*cli.Command{
	nodeInfoCmd,
	cacheStatCmd,
	progressCmd,
	keyCmds,
	configCmds,
	bindCmd,
}

func allocateSchedulerForCandidate(locatorURL, code string) (string, error) {
	locator, close, err := client.NewLocator(context.Background(), locatorURL, nil, jsonrpc.WithHTTPClient(client.NewHTTP3Client()))
	if err != nil {
		return "", err
	}
	defer close()

	return locator.AllocateSchedulerForNode(context.Background(), types.NodeCandidate, code)
}

func RegisterCandidateNode(lr repo.LockedRepo, locatorURL string, code string) error {
	schedulerURL, err := allocateSchedulerForCandidate(locatorURL, code)
	if err != nil {
		return err
	}

	schedulerAPI, closer, err := client.NewScheduler(context.Background(), schedulerURL, nil, jsonrpc.WithHTTPClient(client.NewHTTP3Client()))
	if err != nil {
		return err
	}
	defer closer()

	bits := 1024
	privateKey, err := titanrsa.GeneratePrivateKey(bits)
	if err != nil {
		return err
	}

	pem := titanrsa.PublicKey2Pem(&privateKey.PublicKey)
	nodeID := fmt.Sprintf("c_%s", uuid.NewString())

	info, err := schedulerAPI.RegisterCandidateNode(context.Background(), nodeID, string(pem), code)
	if err != nil {
		return err
	}

	privatePem := titanrsa.PrivateKey2Pem(privateKey)
	if err := lr.SetPrivateKey(privatePem); err != nil {
		return err
	}

	if err := lr.SetNodeID([]byte(nodeID)); err != nil {
		return err
	}

	return saveConfigAfterRegister(lr, info, locatorURL)
}
