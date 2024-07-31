package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

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
	exitCmds,
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

var exitCmds = &cli.Command{
	Name: "exit",
	// Usage: "EXIT YOUR L1-NODE, be careful and thoughtful",
	Usage: "Exit your L1-Node",
	Subcommands: []*cli.Command{
		exitExecCmd,
		exitCalCmd,
	},
}

var exitExecCmd = &cli.Command{
	Name:  "exec",
	Usage: "exec exit your L1-Node, be careful and thoughtful",
	Action: func(cctx *cli.Context) error {
		candidateAPI, close, err := GetCandidateAPI(cctx)
		if err != nil {
			fmt.Println(err)
			return err
		}
		defer close()

		ctx := ReqContext(cctx)

		reader := bufio.NewReader(os.Stdin)

		fmt.Println("Important: this action will quit your l1-node from titan-network!\n" +
			"It means node will be wasted and will not be able to connect to titan-network.\n" +
			"If you are aware of this, please type \"CONFIRM\" to continue, any words besides it are not accepted:")

		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input != "CONFIRM" {
			return fmt.Errorf("exit action cancelled")
		}

		calResp, err := candidateAPI.CalculateExitProfit(ctx)
		if err != nil {
			fmt.Println(err)
			return fmt.Errorf("failed to calculate exit profit: %w, exit failed", err)
		}
		fmt.Println()
		fmt.Printf("Penalty info: \n Current Rewards: %f\n Penalty Rate: %f\n Rewards After Penalty: %f\n", calResp.CurrentPoint, calResp.PenaltyRate*100, calResp.RemainingPoint)
		fmt.Println()
		fmt.Println("Type \"Accept\" to accept penalty, any words aside are not accepted:")

		input, _ = reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input != "Accept" {
			fmt.Println("Operation cancelled.")
			return nil
		}

		fmt.Println("Exiting, please wait...")
		time.Sleep(3 * time.Second)
		if err := candidateAPI.DeactivateNode(ctx); err != nil {
			fmt.Println(err)
			return fmt.Errorf("failed to exit node: %w", err)
		}
		fmt.Println("Exit successfully.")

		return nil
	},
}

var exitCalCmd = &cli.Command{
	Name:  "cal",
	Usage: "calculate exit profit",
	Action: func(cctx *cli.Context) error {
		candidateAPI, close, err := GetCandidateAPI(cctx)
		if err != nil {
			fmt.Println(err)
			return err
		}
		defer close()

		ctx := ReqContext(cctx)

		calResp, err := candidateAPI.CalculateExitProfit(ctx)
		if err != nil {
			fmt.Println(err)
			return fmt.Errorf("failed to calculate exit profit: %w, exit failed", err)
		}
		fmt.Printf("Penalty info: \n Current Rewards: %f\n Penalty Rate: %f\n Rewards After Penalty: %f\n", calResp.CurrentPoint, calResp.PenaltyRate*100, calResp.RemainingPoint)
		return nil
	},
}
