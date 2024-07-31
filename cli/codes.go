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
	Usage: "remove code info",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "code",
			Usage: "code id",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		code := cctx.String("code")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.RemoveCandidateCode(ctx, code)
	},
}

var resetCandidateCodeCmd = &cli.Command{
	Name:  "reset",
	Usage: "reset code info",
	Flags: []cli.Flag{
		nodeIDFlag,
		&cli.StringFlag{
			Name:  "code",
			Usage: "code id",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		code := cctx.String("code")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.ResetCandidateCode(ctx, nodeID, code)
	},
}

var generateCandidateCodeCmd = &cli.Command{
	Name:  "new",
	Usage: "generate code",
	Flags: []cli.Flag{
		// nodeTypeFlag,
		&cli.Int64Flag{
			Name:  "count",
			Usage: "code count",
			Value: 0,
		},
		&cli.BoolFlag{
			Name:  "test",
			Usage: "is test",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		// nodeType := cctx.Int("node-type")
		count := cctx.Int("count")
		isTest := cctx.Bool("test")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		// if nodeType != int(types.NodeCandidate) && nodeType != int(types.NodeValidator) {
		// 	return nil
		// }

		if count <= 0 {
			return nil
		}

		list, err := schedulerAPI.GenerateCandidateCodes(ctx, count, types.NodeCandidate, isTest)
		if err != nil {
			return err
		}

		for _, code := range list {
			fmt.Println(code)
		}

		return nil
	},
}

var loadCandidateCodeCmd = &cli.Command{
	Name:  "get",
	Usage: "load candidate code info",
	Flags: []cli.Flag{
		nodeIDFlag,
		&cli.StringFlag{
			Name:  "code",
			Usage: "code id",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		code := cctx.String("code")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		infos, err := schedulerAPI.GetCandidateCodeInfos(ctx, nodeID, code)
		if err != nil {
			return err
		}

		for _, info := range infos {
			fmt.Printf("code:%s node:%s type:%s\n", info.Code, info.NodeID, info.NodeType.String())
		}

		return nil
	},
}
