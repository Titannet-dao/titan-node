package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/urfave/cli/v2"
)

// SchedulerCMDs Scheduler cmd
var SchedulerCMDs = []*cli.Command{
	WithCategory("node", nodeCmd),
	WithCategory("asset", assetCmd),
	startElectionCmd,
	// other
	edgeUpdaterCmd,
}

var (
	nodeIDFlag = &cli.StringFlag{
		Name:  "node-id",
		Usage: "node id",
		Value: "",
	}

	cidFlag = &cli.StringFlag{
		Name:  "cid",
		Usage: "specify the cid of a asset",
		Value: "",
	}

	replicaCountFlag = &cli.IntFlag{
		Name:        "replica-count",
		Usage:       "Number of replica pull to nodes",
		Value:       2,
		DefaultText: "2",
	}

	nodeTypeFlag = &cli.IntFlag{
		Name:  "node-type",
		Usage: "node type 0:ALL 1:Edge 2:Candidate 3:Validator 4:Scheduler",
		Value: 0,
	}

	limitFlag = &cli.IntFlag{
		Name:        "limit",
		Usage:       "the numbering of limit",
		Value:       50,
		DefaultText: "50",
	}

	offsetFlag = &cli.IntFlag{
		Name:        "offset",
		Usage:       "the numbering of offset",
		Value:       0,
		DefaultText: "0",
	}

	expirationDateFlag = &cli.StringFlag{
		Name:        "expiration-date",
		Usage:       "Set the asset expiration, format with '2006-1-2 15:04:05' layout.",
		Value:       "",
		DefaultText: "now + 7 days",
	}

	dateFlag = &cli.StringFlag{
		Name:  "date-time",
		Usage: "date time (2006-1-2 15:04:05)",
		Value: "",
	}

	portFlag = &cli.StringFlag{
		Name:  "port",
		Usage: "port",
		Value: "",
	}
)

var setNodePortCmd = &cli.Command{
	Name:  "set-node-port",
	Usage: "set the node port",
	Flags: []cli.Flag{
		nodeIDFlag,
		portFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		port := cctx.String("port")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.UpdateNodePort(ctx, nodeID, port)
	},
}

var startElectionCmd = &cli.Command{
	Name:  "start-election",
	Usage: "Start election validator",

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}

		defer closer()

		return schedulerAPI.TriggerElection(ctx)
	},
}

var edgeUpdaterCmd = &cli.Command{
	Name:  "edge-updater",
	Usage: "get edge update info or set edge update info",
	Subcommands: []*cli.Command{
		edgeUpdateInfoCmd,
		setEdgeUpdateInfoCmd,
		deleteEdgeUpdateInfoCmd,
	},
}

var deleteEdgeUpdateInfoCmd = &cli.Command{
	Name:  "delete",
	Usage: "delete edge update info",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "node-type",
			Usage: "node type: edge 1, update 6",
			Value: 1,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		nodeType := cctx.Int("node-type")
		err = schedulerAPI.DeleteEdgeUpdateConfig(ctx, nodeType)
		if err != nil {
			return err
		}
		return nil
	},
}

var edgeUpdateInfoCmd = &cli.Command{
	Name:  "get",
	Usage: "get edge update info",
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		updateInfos, err := schedulerAPI.GetEdgeUpdateConfigs(ctx)
		if err != nil {
			return err
		}

		if len(updateInfos) == 0 {
			fmt.Printf("No update info exist\n")
			return nil
		}

		for k, updateInfo := range updateInfos {
			fmt.Printf("AppName:%s\n", updateInfo.AppName)
			fmt.Printf("NodeType:%d\n", k)
			fmt.Printf("Hash:%s\n", updateInfo.Hash)
			fmt.Printf("Version:%s\n", updateInfo.Version)
			fmt.Printf("DownloadURL:%s\n", updateInfo.DownloadURL)
			fmt.Println()
		}

		return nil
	},
}

var setEdgeUpdateInfoCmd = &cli.Command{
	Name:  "set",
	Usage: "set edge update info",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "version",
			Usage: "latest node app version",
			Value: "0.0.1",
		},
		&cli.StringFlag{
			Name:  "hash",
			Usage: "latest node app hash",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "download-url",
			Usage: "latest node app download url",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "app-name",
			Usage: "app name",
			Value: "",
		},
		&cli.IntFlag{
			Name:  "node-type",
			Usage: "node type: 1 is edge, 6 is update",
			Value: 1,
		},
	},

	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		versionStr := cctx.String("version")
		hash := cctx.String("hash")
		downloadURL := cctx.String("download-url")
		nodeType := cctx.Int("node-type")
		appName := cctx.String("app-name")

		version, err := newVersion(versionStr)
		if err != nil {
			return err
		}

		updateInfo := &api.EdgeUpdateConfig{AppName: appName, NodeType: nodeType, Version: version, Hash: hash, DownloadURL: downloadURL}
		err = schedulerAPI.SetEdgeUpdateConfig(ctx, updateInfo)
		if err != nil {
			return err
		}
		return nil
	},
}

func newVersion(version string) (api.Version, error) {
	stringSplit := strings.Split(version, ".")
	if len(stringSplit) != 3 {
		return api.Version(0), fmt.Errorf("parse version error")
	}

	// major, minor, patch
	major, err := strconv.Atoi(stringSplit[0])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version major error:%s", err)
	}

	minor, err := strconv.Atoi(stringSplit[1])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version minor error:%s", err)
	}

	patch, err := strconv.Atoi(stringSplit[2])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version patch error:%s", err)
	}

	return api.Version(uint32(major)<<16 | uint32(minor)<<8 | uint32(patch)), nil
}
