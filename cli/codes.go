package cli

import (
	"fmt"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/urfave/cli/v2"
)

var codesCmds = &cli.Command{
	Name:  "codes",
	Usage: "Manage candidate codes",
	Subcommands: []*cli.Command{
		generateCandidateCodeCmd,
		loadCandidateCodeCmd,
		removeCandidateCodeCmd,
		resetCandidateCodeCmd,
	},
}

var removeCandidateCodeCmd = &cli.Command{
	Name:  "rm",
	Usage: "Remove code info",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "code",
			Usage: "Code ID (required)",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		code := cctx.String("code")
		if code == "" {
			return fmt.Errorf("the --code flag is required")
		}

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return fmt.Errorf("failed to get scheduler API: %w", err)
		}
		defer closer()

		if err := schedulerAPI.RemoveCandidateCode(ctx, code); err != nil {
			return fmt.Errorf("failed to remove candidate code: %w", err)
		}

		fmt.Println("Candidate code removed successfully")
		return nil
	},
}

var resetCandidateCodeCmd = &cli.Command{
	Name:  "reset",
	Usage: "Reset code info",
	Flags: []cli.Flag{
		nodeIDFlag,
		&cli.StringFlag{
			Name:  "code",
			Usage: "Code ID (required)",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		code := cctx.String("code")

		if nodeID == "" {
			return fmt.Errorf("the --node-id flag is required")
		}
		if code == "" {
			return fmt.Errorf("the --code flag is required")
		}

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return fmt.Errorf("failed to get scheduler API: %w", err)
		}
		defer closer()

		if err := schedulerAPI.ResetCandidateCode(ctx, nodeID, code); err != nil {
			return fmt.Errorf("failed to reset candidate code: %w", err)
		}

		fmt.Println("Candidate code reset successfully")
		return nil
	},
}

var generateCandidateCodeCmd = &cli.Command{
	Name:  "new",
	Usage: "Generate codes",
	Flags: []cli.Flag{
		&cli.Int64Flag{
			Name:  "count",
			Usage: "Number of codes to generate (must be greater than 0)",
			Value: 0,
		},
		&cli.BoolFlag{
			Name:  "test",
			Usage: "Generate test codes",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		count := cctx.Int("count")
		isTest := cctx.Bool("test")

		if count <= 0 {
			return fmt.Errorf("the --count flag must be greater than 0")
		}

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return fmt.Errorf("failed to get scheduler API: %w", err)
		}
		defer closer()

		list, err := schedulerAPI.GenerateCandidateCodes(ctx, count, types.NodeCandidate, isTest)
		if err != nil {
			return fmt.Errorf("failed to generate candidate codes: %w", err)
		}

		fmt.Println("Generated codes:")
		for _, code := range list {
			fmt.Println(code)
		}

		return nil
	},
}

var loadCandidateCodeCmd = &cli.Command{
	Name:  "get",
	Usage: "Load candidate code info",
	Flags: []cli.Flag{
		nodeIDFlag,
		&cli.StringFlag{
			Name:  "code",
			Usage: "Code ID (optional)",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		code := cctx.String("code")

		if nodeID == "" {
			return fmt.Errorf("the --node-id flag is required")
		}

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return fmt.Errorf("failed to get scheduler API: %w", err)
		}
		defer closer()

		infos, err := schedulerAPI.GetCandidateCodeInfos(ctx, nodeID, code)
		if err != nil {
			return fmt.Errorf("failed to load candidate code info: %w", err)
		}

		fmt.Println("Candidate code infos:")
		for _, info := range infos {
			fmt.Printf("Code: %s, Node: %s, Type: %s\n", info.Code, info.NodeID, info.NodeType.String())
		}

		return nil
	},
}
