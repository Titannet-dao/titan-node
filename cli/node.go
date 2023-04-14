package cli

import (
	"fmt"
	"os"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/docker/go-units"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var nodeCmd = &cli.Command{
	Name:  "node",
	Usage: "Manage node",
	Subcommands: []*cli.Command{
		onlineNodeCountCmd,
		registerNodeCmd,
		showNodeInfoCmd,
		nodeQuitCmd,
		setNodePortCmd,
		edgeExternalAddrCmd,
	},
}

var onlineNodeCountCmd = &cli.Command{
	Name:  "online-count",
	Usage: "online node count",
	Flags: []cli.Flag{
		nodeTypeFlag,
	},
	Action: func(cctx *cli.Context) error {
		t := cctx.Int("node-type")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		nodes, err := schedulerAPI.GetOnlineNodeCount(ctx, types.NodeType(t))

		fmt.Println("Online nodes count:", nodes)
		return err
	},
}

var registerNodeCmd = &cli.Command{
	Name:  "register",
	Usage: "Register nodeID and public key ",
	Flags: []cli.Flag{
		nodeTypeFlag,
		&cli.StringFlag{
			Name:  "public-key-path",
			Usage: "node public key path",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		t := cctx.Int("node-type")
		publicKeyPath := cctx.String("public-key-path")

		if t != int(types.NodeEdge) && t != int(types.NodeCandidate) {
			return xerrors.Errorf("node-type err:%d", t)
		}

		if publicKeyPath == "" {
			return xerrors.New("public-key-path is nil")
		}

		pem, err := os.ReadFile(publicKeyPath)
		if err != nil {
			return err
		}

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		nodeID, err := schedulerAPI.RegisterNode(ctx, string(pem), types.NodeType(t))
		fmt.Printf("nodeID is : %s", nodeID)

		return err
	},
}

var showNodeInfoCmd = &cli.Command{
	Name:  "info",
	Usage: "Show node info",
	Flags: []cli.Flag{
		nodeIDFlag,
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		if nodeID == "" {
			return xerrors.New("node-id is nil")
		}

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		info, err := schedulerAPI.GetNodeInfo(ctx, nodeID)
		if err != nil {
			return err
		}

		natType, _ := schedulerAPI.GetNodeNATType(ctx, nodeID)

		fmt.Printf("node id: %s \n", info.NodeID)
		fmt.Printf("online: %v \n", info.IsOnline)
		fmt.Printf("name: %s \n", info.NodeName)
		fmt.Printf("external_ip: %s \n", info.ExternalIP)
		fmt.Printf("internal_ip: %s \n", info.InternalIP)
		fmt.Printf("system version: %s \n", info.SystemVersion)
		fmt.Printf("disk usage: %.2f %s\n", info.DiskUsage, "%")
		fmt.Printf("disk space: %s \n", units.BytesSize(info.DiskSpace))
		fmt.Printf("fsType: %s \n", info.IoSystem)
		fmt.Printf("mac: %s \n", info.MacLocation)
		fmt.Printf("download bandwidth: %s \n", units.BytesSize(info.BandwidthDown))
		fmt.Printf("upload bandwidth: %s \n", units.BytesSize(info.BandwidthUp))
		fmt.Printf("cpu percent: %.2f %s \n", info.CPUUsage, "%")
		//
		fmt.Printf("DownloadCount: %d \n", info.DownloadBlocks)
		fmt.Printf("NatType: %s \n", natType.String())

		return nil
	},
}

var nodeQuitCmd = &cli.Command{
	Name:  "quit",
	Usage: "Node quit the titan",
	Flags: []cli.Flag{
		nodeIDFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		if nodeID == "" {
			return xerrors.New("node-id is nil")
		}

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		err = schedulerAPI.UnregisterNode(ctx, nodeID)
		if err != nil {
			return err
		}

		return nil
	},
}

var edgeExternalAddrCmd = &cli.Command{
	Name:  "external-addr",
	Usage: "get edge external addr",
	Flags: []cli.Flag{
		nodeIDFlag,
		&cli.StringFlag{
			Name:  "scheduler-url",
			Usage: "scheduler url",
			Value: "http://localhost:3456/rpc/v0",
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		schedulerURL := cctx.String("scheduler-url")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		addr, err := schedulerAPI.GetEdgeExternalServiceAddress(ctx, nodeID, schedulerURL)
		if err != nil {
			return err
		}

		fmt.Printf("edge external addr:%s\n", addr)
		return nil
	},
}
