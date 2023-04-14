package cli

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/lib/tablewriter"
	"github.com/Filecoin-Titan/titan/node/scheduler/assets"
	"github.com/docker/go-units"
	"github.com/fatih/color"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var (
	defaultExpireDays     = 7
	defaultExpiration     = time.Duration(defaultExpireDays) * time.Hour * 24
	defaultDateTimeLayout = "2006-01-02 15:04:05"
)

var assetCmd = &cli.Command{
	Name:  "asset",
	Usage: "Manage asset record",
	Subcommands: []*cli.Command{
		listAssetRecordCmd,
		pullAssetCmd,
		showAssetInfoCmd,
		removeAssetRecordCmd,
		removeAssetReplicaCmd,
		resetExpirationCmd,
	},
}

var resetExpirationCmd = &cli.Command{
	Name:  "reset-expiration",
	Usage: "Reset the asset record expiration",
	Flags: []cli.Flag{
		cidFlag,
		dateFlag,
	},
	Action: func(cctx *cli.Context) error {
		cid := cctx.String("cid")
		dateTime := cctx.String("date-time")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		time, err := time.ParseInLocation("2006-1-2 15:04:05", dateTime, time.Local)
		if err != nil {
			return xerrors.Errorf("date time err:%s", err.Error())
		}

		err = schedulerAPI.UpdateAssetExpiration(ctx, cid, time)
		if err != nil {
			return err
		}

		return nil
	},
}

var removeAssetRecordCmd = &cli.Command{
	Name:  "remove",
	Usage: "Remove the asset record",
	Flags: []cli.Flag{
		cidFlag,
	},
	Action: func(cctx *cli.Context) error {
		cid := cctx.String("cid")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.RemoveAssetRecord(ctx, cid)
	},
}

var removeAssetReplicaCmd = &cli.Command{
	Name:  "remove-replica",
	Usage: "Remove a asset replica",
	Flags: []cli.Flag{
		cidFlag,
		nodeIDFlag,
	},
	Action: func(cctx *cli.Context) error {
		cid := cctx.String("cid")
		nodeID := cctx.String("node-id")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.RemoveAssetReplica(ctx, cid, nodeID)
	},
}

var showAssetInfoCmd = &cli.Command{
	Name:  "info",
	Usage: "Show the asset record info",
	Flags: []cli.Flag{
		cidFlag,
	},
	Action: func(cctx *cli.Context) error {
		cid := cctx.String("cid")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		info, err := schedulerAPI.GetAssetRecord(ctx, cid)
		if err != nil {
			return err
		}

		fmt.Printf("CID:\t%s\n", info.CID)
		fmt.Printf("Hash:\t%s\n", info.Hash)
		fmt.Printf("State:\t%s\n", colorState(info.State))
		fmt.Printf("Blocks:\t%d\n", info.TotalBlocks)
		fmt.Printf("Size:\t%s\n", units.BytesSize(float64(info.TotalSize)))
		fmt.Printf("NeedEdgeReplica:\t%d\n", info.NeedEdgeReplica)
		fmt.Printf("Expiration:\t%v\n", info.Expiration.Format(defaultDateTimeLayout))

		fmt.Printf("--------\nProcesses:\n")
		for _, cache := range info.ReplicaInfos {
			fmt.Printf("%s(%s): %s\t%s/%s\n", cache.NodeID, edgeOrCandidate(cache.IsCandidate), colorState(cache.Status.String()),
				units.BytesSize(float64(cache.DoneSize)), units.BytesSize(float64(info.TotalSize)))
		}

		return nil
	},
}

var pullAssetCmd = &cli.Command{
	Name:  "cache",
	Usage: "publish cache tasks to nodes",
	Flags: []cli.Flag{
		cidFlag,
		replicaCountFlag,
		expirationDateFlag,
	},
	Action: func(cctx *cli.Context) error {
		cid := cctx.String("cid")
		replicaCount := cctx.Int64("replica-count")
		date := cctx.String("expiration-date")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		if cid == "" {
			return xerrors.New("cid is nil")
		}

		info := &types.PullAssetReq{CID: cid}

		if date == "" {
			date = time.Now().Add(defaultExpiration).Format(defaultDateTimeLayout)
		}

		eTime, err := time.ParseInLocation(defaultDateTimeLayout, date, time.Local)
		if err != nil {
			return xerrors.Errorf("parse expiration err:%s", err.Error())
		}

		info.Expiration = eTime
		info.Replicas = replicaCount

		err = schedulerAPI.PullAsset(ctx, info)
		if err != nil {
			return err
		}

		return nil
	},
}

var listAssetRecordCmd = &cli.Command{
	Name:  "list",
	Usage: "List asset record",
	Flags: []cli.Flag{
		limitFlag,
		offsetFlag,
		&cli.BoolFlag{
			Name:  "pulling",
			Usage: "only show the pulling assets",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "processes",
			Usage: "show the assets processes",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "failed",
			Usage: "only show the failed state assets",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "restart",
			Usage: "restart the failed assets, only apply for failed asset state",
			Value: false,
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

		states := append([]string{assets.Servicing.String()}, append(assets.FailedStates, assets.PullingStates...)...)

		if cctx.Bool("pulling") {
			states = assets.PullingStates
		}
		if cctx.Bool("failed") {
			states = assets.FailedStates
		}

		restart := cctx.Bool("restart")
		if restart && !cctx.Bool("failed") {
			log.Error("only --failed can be restarted")
			return nil
		}

		tw := tablewriter.New(
			tablewriter.Col("CID"),
			tablewriter.Col("State"),
			tablewriter.Col("Blocks"),
			tablewriter.Col("Size"),
			tablewriter.Col("CreateTime"),
			tablewriter.Col("Expiration"),
			tablewriter.NewLineCol("Processes"),
		)

		list, err := schedulerAPI.GetAssetRecords(ctx, limit, offset, states)
		if err != nil {
			return err
		}

		for w := 0; w < len(list); w++ {
			info := list[w]
			m := map[string]interface{}{
				"CID":        info.CID,
				"State":      colorState(info.State),
				"Blocks":     info.TotalBlocks,
				"Size":       units.BytesSize(float64(info.TotalSize)),
				"CreateTime": info.CreateTime.Format(defaultDateTimeLayout),
				"Expiration": info.Expiration.Format(defaultDateTimeLayout),
			}

			sort.Slice(info.ReplicaInfos, func(i, j int) bool {
				return info.ReplicaInfos[i].NodeID < info.ReplicaInfos[j].NodeID
			})

			if cctx.Bool("processes") {
				processes := "\n"
				for j := 0; j < len(info.ReplicaInfos); j++ {
					cache := info.ReplicaInfos[j]
					status := colorState(cache.Status.String())
					processes += fmt.Sprintf("\t%s(%s): %s\t%s/%s\n", cache.NodeID, edgeOrCandidate(cache.IsCandidate), status, units.BytesSize(float64(cache.DoneSize)), units.BytesSize(float64(info.TotalSize)))
				}
				m["Processes"] = processes
			}

			tw.Write(m)
		}

		if !restart {
			tw.Flush(os.Stdout)
			return nil
		}

		var hashes []types.AssetHash
		for _, info := range list {
			if strings.Contains(info.State, "Failed") {
				hashes = append(hashes, types.AssetHash(info.Hash))
			}
		}
		return schedulerAPI.RePullFailedAssets(ctx, hashes)
	},
}

func edgeOrCandidate(isCandidate bool) string {
	if isCandidate {
		return "candidate"
	}
	return "edge"
}

func colorState(state string) string {
	if strings.Contains(state, "Failed") {
		return color.RedString(state)
	} else if strings.Contains(state, "Servicing") || strings.Contains(state, "Succeeded") {
		return color.GreenString(state)
	} else {
		return color.YellowString(state)
	}
}
