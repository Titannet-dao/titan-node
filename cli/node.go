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
		requestActivationCodesCmd,
		showNodeInfoCmd,
		nodeQuitCmd,
		setNodePortCmd,
		edgeExternalAddrCmd,
		listNodeCmd,
		deactivateCmd,
		unDeactivateCmd,
		listNodeOfIPCmd,
		listReplicaCmd,
		nodeCleanReplicasCmd,
		listValidationResultsCmd,
		listProfitDetailsCmd,
		freeUpDiskSpaceCmd,
		updateNodeDynamicInfoCmd,
		generateCandidateCodeCmd,
		nodeFromGeoCmd,
		getRegionInfosCmd,
		setTunserverURLCmd,
		reimburseProfitCmd,
	},
}

var reimburseProfitCmd = &cli.Command{
	Name:  "rp",
	Usage: "reimburse profit",
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

		return schedulerAPI.ReimburseNodeProfit(ctx, nodeID, note, profit)
	},
}

var setTunserverURLCmd = &cli.Command{
	Name:  "sts",
	Usage: "set node tun server url",
	Flags: []cli.Flag{
		nodeIDFlag,
		&cli.StringFlag{
			Name:  "ws-node",
			Usage: "ws node id",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		wsNodeID := cctx.String("ws-node")
		nodeID := cctx.String("node-id")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.SetTunserverURL(ctx, nodeID, wsNodeID)
	},
}

var updateNodeDynamicInfoCmd = &cli.Command{
	Name:  "update-info",
	Usage: "update node info",
	Flags: []cli.Flag{
		nodeIDFlag,
		&cli.Int64Flag{
			Name:  "dt",
			Usage: "Download Traffic",
			Value: 0,
		},
		&cli.Int64Flag{
			Name:  "ut",
			Usage: "Upload Traffic",
			Value: 0,
		},
	},
	Action: func(cctx *cli.Context) error {
		dt := cctx.Int64("dt")
		ut := cctx.Int64("ut")
		nodeID := cctx.String("node-id")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.UpdateNodeDynamicInfo(ctx, &types.NodeDynamicInfo{NodeID: nodeID, DownloadTraffic: dt, UploadTraffic: ut})
	},
}

var freeUpDiskSpaceCmd = &cli.Command{
	Name:  "fuds",
	Usage: "free up disk space",
	Flags: []cli.Flag{
		nodeIDFlag,
		&cli.Int64Flag{
			Name:  "size",
			Usage: "free up size",
			Value: 0,
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		size := cctx.Int64("size")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		_, err = schedulerAPI.FreeUpDiskSpace(ctx, nodeID, size)
		return err
	},
}

var deactivateCmd = &cli.Command{
	Name:  "deactivate",
	Usage: "node deactivate",
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
			tablewriter.Col("Geo"),
			// tablewriter.Col("Test"),
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
				"Geo":             fmt.Sprintf("%s", info.AreaID),
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

var listValidationResultsCmd = &cli.Command{
	Name:  "lv",
	Usage: "list node validation results",
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

		list, err := schedulerAPI.GetValidationResults(ctx, nodeID, limit, offset)
		if err != nil {
			return err
		}

		for _, info := range list.ValidationResultInfos {
			fmt.Printf("cid:%s %d , %s \n", info.Cid, info.Status, info.StartTime.String())
		}

		return err
	},
}

var listReplicaCmd = &cli.Command{
	Name:  "lr",
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
	Name:  "lip",
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

var requestActivationCodesCmd = &cli.Command{
	Name:  "activation-code",
	Usage: "request node activation codes ",
	Flags: []cli.Flag{
		nodeTypeFlag,
	},
	Action: func(cctx *cli.Context) error {
		t := cctx.Int("node-type")

		if t != int(types.NodeEdge) && t != int(types.NodeCandidate) {
			return xerrors.Errorf("node-type err:%d", t)
		}

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		list, err := schedulerAPI.RequestActivationCodes(ctx, types.NodeType(t), 1)
		if err != nil {
			return err
		}

		for _, code := range list {
			fmt.Println("node:", code.NodeID)
			fmt.Println("code:", code.ActivationCode)
			fmt.Println("")
		}

		return err
	},
}

var nodeFromGeoCmd = &cli.Command{
	Name:  "nfg",
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

		err = schedulerAPI.DeactivateNode(ctx, nodeID, 10)
		if err != nil {
			return err
		}

		return nil
	},
}

var nodeCleanReplicasCmd = &cli.Command{
	Name:  "cr",
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

var listProfitDetailsCmd = &cli.Command{
	Name:  "lpd",
	Usage: "List Profit Details",
	Flags: []cli.Flag{
		limitFlag,
		offsetFlag,
		nodeIDFlag,
		&cli.IntSliceFlag{
			Name:  "type",
			Usage: "profit type",
		},
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
		nID := cctx.String("node-id")
		types := cctx.IntSlice("type")

		tw := tablewriter.New(
			tablewriter.Col("NodeID"),
			tablewriter.Col("PType"),
			tablewriter.Col("Size"),
			tablewriter.Col("Profit"),
			tablewriter.Col("CreatedTime"),
			tablewriter.Col("Rate"),
			tablewriter.Col("Note"),
			// tablewriter.NewLineCol("Processes"),
		)

		info, err := schedulerAPI.GetProfitDetailsForNode(ctx, nID, limit, offset, types)
		if err != nil {
			return err
		}

		fmt.Println("info : ", info.Total)

		for w := 0; w < len(info.Infos); w++ {
			info := info.Infos[w]

			m := map[string]interface{}{
				"NodeID":      info.NodeID,
				"PType":       info.PType,
				"Size":        units.BytesSize(float64(info.Size)),
				"Profit":      info.Profit,
				"CreatedTime": info.CreatedTime.Format(defaultDateTimeLayout),
				"Rate":        info.Rate,
				"Note":        info.Note,
			}

			tw.Write(m)
		}

		tw.Flush(os.Stdout)
		return nil
	},
}
