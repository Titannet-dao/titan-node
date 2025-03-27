package cli

import (
	"fmt"
	"os"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/tablewriter"
	"github.com/docker/go-units"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var nodeCmds = &cli.Command{
	Name:  "node",
	Usage: "Manage node",
	Subcommands: []*cli.Command{
		onlineNodeCountCmd,
		showNodeInfoCmd,
		edgeExternalAddrCmd,
		listNodeCmd,
		deactivateCmd,
		forceNodeOfflineCmd,
		unDeactivateCmd,
		listNodeOfIPCmd,
		listReplicaCmd,
		nodeCleanReplicasCmd,
		nodeFromGeoCmd,
		getRegionInfosCmd,
		recompenseProfitCmd,
	},
}

var recompenseProfitCmd = &cli.Command{
	Name:  "profit",
	Usage: "recompense profit",
	Flags: []cli.Flag{
		nodeIDFlag,
		&cli.StringFlag{
			Name:  "note",
			Usage: "note",
			Value: "",
		},
		&cli.Float64Flag{
			Name:  "profit",
			Usage: "profit",
			Value: 0.0,
		},
	},
	Action: func(cctx *cli.Context) error {
		note := cctx.String("note")
		nodeID := cctx.String("node-id")
		profit := cctx.Float64("profit")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.RecompenseNodeProfit(ctx, nodeID, note, profit)
	},
}

var deactivateCmd = &cli.Command{
	Name:  "deactivate",
	Usage: "node deactivate (40% of the points will be retained)",
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

		return schedulerAPI.DeactivateNode(ctx, nodeID, 24*7)
	},
}

var forceNodeOfflineCmd = &cli.Command{
	Name:  "force-offline",
	Usage: "force node offline (Points will not be retained)",
	Flags: []cli.Flag{
		nodeIDFlag,
		&cli.BoolFlag{
			Name:  "value",
			Usage: "",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		if nodeID == "" {
			return xerrors.New("node-id is nil")
		}

		value := cctx.Bool("value")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.ForceNodeOffline(ctx, nodeID, value)
	},
}

var unDeactivateCmd = &cli.Command{
	Name:  "un-deactivate",
	Usage: "node undo deactivate",
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

		return schedulerAPI.UndoNodeDeactivation(ctx, nodeID)
	},
}

var onlineNodeCountCmd = &cli.Command{
	Name:  "online",
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

var listNodeCmd = &cli.Command{
	Name:  "list",
	Usage: "list node",
	Flags: []cli.Flag{
		limitFlag,
		offsetFlag,
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		limit := cctx.Int("limit")
		offset := cctx.Int("offset")

		r, err := schedulerAPI.GetNodeList(ctx, offset, limit)
		if err != nil {
			return err
		}

		tw := tablewriter.New(
			tablewriter.Col("NodeID"),
			tablewriter.Col("Status"),
			tablewriter.Col("Nat"),
			tablewriter.Col("IP"),
			tablewriter.Col("Profit"),
			tablewriter.Col("OnlineDuration"),
			tablewriter.Col("OfflineDuration"),
			// tablewriter.Col("Geo"),
			tablewriter.Col("Type"),
			// tablewriter.Col("Ver"),
		)

		for w := 0; w < len(r.Data); w++ {
			info := r.Data[w]

			m := map[string]interface{}{
				"NodeID":          info.NodeID,
				"Status":          colorOnline(info.Status),
				"Nat":             info.NATType,
				"IP":              info.ExternalIP,
				"Addr":            info.RemoteAddr,
				"Profit":          fmt.Sprintf("%.4f", info.Profit),
				"OnlineDuration":  fmt.Sprintf("%d", info.OnlineDuration),
				"OfflineDuration": fmt.Sprintf("%d", info.OfflineDuration),
				// "Geo":             fmt.Sprintf("%s", info.AreaID),
				"Type": info.Type.String(),
				// "Test":            info.IsTestNode,
				// "Ver":             info.SystemVersion,
			}

			tw.Write(m)
		}
		err = tw.Flush(os.Stdout)

		fmt.Printf(color.YellowString("\n Total:%d ", r.Total))

		return err
	},
}

var listReplicaCmd = &cli.Command{
	Name:  "list-replica",
	Usage: "list node replica",
	Flags: []cli.Flag{
		nodeIDFlag,
		limitFlag,
		offsetFlag,
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		nodeID := cctx.String("node-id")
		limit := cctx.Int("limit")
		offset := cctx.Int("offset")

		list, err := schedulerAPI.GetReplicasForNode(ctx, nodeID, limit, offset, types.ReplicaStatusAll)
		if err != nil {
			return err
		}

		for _, info := range list.NodeReplicaInfos {
			fmt.Printf("cid:%s %s %d/%d , %s \n", info.Cid, colorReplicaState(info.Status), info.DoneSize, info.TotalSize, info.StartTime.String())
		}

		return err
	},
}

var listNodeOfIPCmd = &cli.Command{
	Name:  "list-ip",
	Usage: "list node of ip",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "ip",
			Usage: "node ip",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		ip := cctx.String("ip")

		list, err := schedulerAPI.GetNodeOfIP(ctx, ip)
		if err != nil {
			return err
		}

		for _, nodeID := range list {
			fmt.Println(nodeID)
		}

		return err
	},
}

func colorReplicaState(state types.ReplicaStatus) string {
	if state == types.ReplicaStatusSucceeded {
		return color.GreenString(state.String())
	} else if state == types.ReplicaStatusFailed {
		return color.RedString(state.String())
	} else {
		return color.YellowString(state.String())
	}
}

func colorOnline(status types.NodeStatus) string {
	if status == types.NodeServicing {
		return color.GreenString(status.String())
	}

	if status == types.NodeOffline {
		return color.RedString(status.String())
	}

	return color.YellowString(status.String())
}

var nodeFromGeoCmd = &cli.Command{
	Name:  "list-from-geo",
	Usage: "get nodes from geo",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "area",
			Usage: "area id like 'Asia-China-Guangdong-Shenzhen' or 'Asia-HongKong'",
			Value: "",
		},
		&cli.BoolFlag{
			Name:  "show-node",
			Usage: "is list nodes",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		areaID := cctx.String("area")
		show := cctx.Bool("show-node")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		list, err := schedulerAPI.GetNodesFromRegion(ctx, areaID)
		if err != nil {
			return err
		}

		if show {
			for _, nodeInfo := range list {
				fmt.Println(nodeInfo.NodeID)
			}
		}

		fmt.Println("size:", len(list))

		return nil
	},
}

var getRegionInfosCmd = &cli.Command{
	Name:  "regions",
	Usage: "get region infos",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "area",
			Usage: "area id like 'Asia-China-Guangdong-Shenzhen' or 'Asia-HongKong'",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		areaID := cctx.String("area")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		list, err := schedulerAPI.GetCurrentRegionInfos(ctx, areaID)
		if err != nil {
			return err
		}

		for geoKey, num := range list {
			fmt.Println(geoKey, ":", num)
		}

		fmt.Println("size:", len(list))

		return nil
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

		fmt.Printf("node id: %s \n", info.NodeID)
		fmt.Printf("status: %v \n", info.Status.String())
		fmt.Printf("name: %s \n", info.NodeName)
		fmt.Printf("external_ip: %s \n", info.ExternalIP)
		fmt.Printf("addr: %s \n", info.RemoteAddr)
		fmt.Printf("system version: %s \n", info.SystemVersion)
		fmt.Printf("disk usage: %.4f %s\n", info.DiskUsage, "%")
		fmt.Printf("disk space: %s \n", units.BytesSize(info.DiskSpace))
		fmt.Printf("titan disk usage: %s\n", units.BytesSize(info.TitanDiskUsage))
		fmt.Printf("titan disk space: %s\n", units.BytesSize(info.AvailableDiskSpace))
		fmt.Printf("fsType: %s \n", info.IoSystem)
		fmt.Printf("mac: %s \n", info.MacLocation)
		fmt.Printf("download bandwidth: %s \n", units.BytesSize(float64(info.BandwidthDown)))
		fmt.Printf("upload bandwidth: %s \n", units.BytesSize(float64(info.BandwidthUp)))
		fmt.Printf("cpu percent: %.2f %s \n", info.CPUUsage, "%")
		fmt.Printf("NatType: %s \n", info.NATType)
		fmt.Printf("OnlineDuration: %d \n", info.OnlineDuration)
		fmt.Printf("Geo: %s \n", info.AreaID)
		fmt.Printf("netflow upload: %s \n", units.BytesSize(float64(info.NetFlowUp)))
		fmt.Printf("netflow download: %s \n", units.BytesSize(float64(info.NetFlowDown)))
		fmt.Printf("AssetCount: %d \n", info.AssetCount)
		fmt.Printf("RetrieveCount: %d \n", info.RetrieveCount)

		return nil
	},
}

var nodeCleanReplicasCmd = &cli.Command{
	Name:  "clean-replica",
	Usage: "clean nodes failed replica",
	Flags: []cli.Flag{},

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

		return schedulerAPI.RemoveNodeFailedReplica(ctx)
	},
}

var edgeExternalAddrCmd = &cli.Command{
	Name:  "external-addr",
	Usage: "get edge external addr",
	Flags: []cli.Flag{
		nodeIDFlag,
		&cli.StringFlag{
			Name:  "candidate-url",
			Usage: "candidate url",
			Value: "http://localhost:3456/rpc/v0",
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		candidateURL := cctx.String("candidate-url")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		addr, err := schedulerAPI.GetEdgeExternalServiceAddress(ctx, nodeID, candidateURL)
		if err != nil {
			return err
		}

		fmt.Printf("edge external addr:%s\n", addr)
		return nil
	},
}
