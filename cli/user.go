package cli

import (
	"fmt"
	"os"
	"sort"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/tablewriter"
	"github.com/docker/go-units"
	"github.com/urfave/cli/v2"
)

var userCmds = &cli.Command{
	Name:  "user",
	Usage: "Manage user",
	Subcommands: []*cli.Command{
		userAPIKeyCmds,
		userStorageCmds,
		userAssetCmds,
		changeVIP,
	},
}

var userAPIKeyCmds = &cli.Command{
	Name:  "api-key",
	Usage: "Manage user api keys",
	Subcommands: []*cli.Command{
		createUserAPIKey,
		listUserAPIKeys,
		deleteUserAPIKey,
	},
}

var userStorageCmds = &cli.Command{
	Name:  "storage",
	Usage: "Manage user storage",
	Subcommands: []*cli.Command{
		allocateStorage,
		getStorageSize,
	},
}

var userAssetCmds = &cli.Command{
	Name:  "asset",
	Usage: "Manage user asset",
	Subcommands: []*cli.Command{
		listAssets,
		removeAsset,
		shareLink,
	},
}

var createUserAPIKey = &cli.Command{
	Name:  "create",
	Usage: "create api key for user",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "user",
			Usage:    "Specify the user id",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "key-name",
			Usage:    "special a name for key",
			Required: true,
		},
		&cli.StringSliceFlag{
			Name:  "perms",
			Usage: "special user access control for key",
			Value: cli.NewStringSlice("readFile", "createFile", "deleteFile", "readFolder", "createFolder", "deleteFolder"),
		},
	},

	Action: func(cctx *cli.Context) error {
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		userID := cctx.String("user")
		keyName := cctx.String("key-name")
		perms := cctx.StringSlice("perms")

		acl := make([]types.UserAccessControl, 0, len(perms))
		for _, perm := range perms {
			acl = append(acl, types.UserAccessControl(perm))
		}

		ctx := ReqContext(cctx)
		key, err := schedulerAPI.CreateAPIKey(ctx, userID, keyName, acl)
		if err != nil {
			return err
		}

		fmt.Printf("%s %s", keyName, key)
		return nil
	},
}

var listUserAPIKeys = &cli.Command{
	Name:  "list",
	Usage: "list api keys for user",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "user",
			Usage:    "Specify the user id",
			Required: true,
		},
	},

	Action: func(cctx *cli.Context) error {
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		userID := cctx.String("user")

		ctx := ReqContext(cctx)
		keys, err := schedulerAPI.GetAPIKeys(ctx, userID)
		if err != nil {
			return err
		}

		for k, v := range keys {
			fmt.Printf("%s %s\n", k, v)
		}
		return nil
	},
}

var deleteUserAPIKey = &cli.Command{
	Name:  "delete",
	Usage: "delete a api key for user",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "user",
			Usage:    "Specify the user id",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "key-name",
			Usage:    "special a key name",
			Required: true,
		},
	},

	Action: func(cctx *cli.Context) error {
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		userID := cctx.String("user")
		keyName := cctx.String("key-name")

		ctx := ReqContext(cctx)
		return schedulerAPI.DeleteAPIKey(ctx, userID, keyName)
	},
}

var allocateStorage = &cli.Command{
	Name:  "allocate",
	Usage: "allocate storage for user",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "user",
			Usage:    "Specify the user id",
			Required: true,
		},
	},

	Action: func(cctx *cli.Context) error {
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		userID := cctx.String("user")

		ctx := ReqContext(cctx)
		storageSize, err := schedulerAPI.AllocateStorage(ctx, userID)
		if err != nil {
			return err
		}

		fmt.Printf("storage total size: %d, used size: %d", storageSize.TotalSize, storageSize.UsedSize)
		return nil
	},
}

var getStorageSize = &cli.Command{
	Name:  "get",
	Usage: "get storage size for user",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "user",
			Usage:    "Specify the user id",
			Required: true,
		},
	},

	Action: func(cctx *cli.Context) error {
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		userID := cctx.String("user")

		ctx := ReqContext(cctx)
		userInfo, err := schedulerAPI.GetUserInfo(ctx, userID)
		if err != nil {
			return err
		}

		fmt.Printf("storage total size: %d, used size: %d", userInfo.TotalSize, userInfo.UsedSize)
		return nil
	},
}

var listAssets = &cli.Command{
	Name:  "list",
	Usage: "list assets of user",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "user",
			Usage:    "Specify the user id",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "limit",
			Usage: "count of list",
			Value: 50,
		},
		&cli.IntFlag{
			Name:  "offset",
			Usage: "offset of list",
			Value: 0,
		},
	},

	Action: func(cctx *cli.Context) error {
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		userID := cctx.String("user")
		limit := cctx.Int("limit")
		offset := cctx.Int("offset")

		ctx := ReqContext(cctx)
		info, err := schedulerAPI.ListAssets(ctx, userID, limit, offset, 0)
		if err != nil {
			return err
		}

		tw := tablewriter.New(
			tablewriter.Col("CID"),
			tablewriter.Col("State"),
			tablewriter.Col("Blocks"),
			tablewriter.Col("Size"),
			tablewriter.Col("CreatedTime"),
			tablewriter.Col("Expiration"),
			tablewriter.NewLineCol("Processes"),
		)

		for w := 0; w < len(info.AssetOverviews); w++ {
			view := info.AssetOverviews[w]
			m := map[string]interface{}{
				"CID":         view.AssetRecord.CID,
				"State":       colorState(view.AssetRecord.State),
				"Blocks":      view.AssetRecord.TotalBlocks,
				"Size":        units.BytesSize(float64(view.AssetRecord.TotalSize)),
				"CreatedTime": view.AssetRecord.CreatedTime.Format(defaultDateTimeLayout),
				"Expiration":  view.AssetRecord.Expiration.Format(defaultDateTimeLayout),
			}

			sort.Slice(view.AssetRecord.ReplicaInfos, func(i, j int) bool {
				return view.AssetRecord.ReplicaInfos[i].NodeID < view.AssetRecord.ReplicaInfos[j].NodeID
			})

			if cctx.Bool("processes") {
				processes := "\n"
				for j := 0; j < len(view.AssetRecord.ReplicaInfos); j++ {
					replica := view.AssetRecord.ReplicaInfos[j]
					status := colorState(replica.Status.String())
					processes += fmt.Sprintf("\t%s(%s): %s\t%s/%s\n", replica.NodeID, edgeOrCandidate(replica.IsCandidate), status, units.BytesSize(float64(replica.DoneSize)), units.BytesSize(float64(view.AssetRecord.TotalSize)))
				}
				m["Processes"] = processes
			}

			tw.Write(m)
		}

		return tw.Flush(os.Stdout)
	},
}

var removeAsset = &cli.Command{
	Name:  "remove",
	Usage: "remove assets of user",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "user",
			Usage:    "Specify the user id",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "cid",
			Usage:    "Specify the user id",
			Required: true,
		},
	},

	Action: func(cctx *cli.Context) error {
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		userID := cctx.String("user")
		assetCID := cctx.String("cid")

		ctx := ReqContext(cctx)
		return schedulerAPI.DeleteAsset(ctx, userID, assetCID)
	},
}

var shareLink = &cli.Command{
	Name:  "share",
	Usage: "remove assets of user",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "user",
			Usage:    "Specify the user id",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "cid",
			Usage:    "special a id for asset",
			Required: true,
		},
	},

	Action: func(cctx *cli.Context) error {
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		userID := cctx.String("user")
		assetCID := cctx.String("cid")

		ctx := ReqContext(cctx)
		links, err := schedulerAPI.ShareAssets(ctx, userID, []string{assetCID})
		if err != nil {
			return err
		}

		if len(links) == 0 {
			fmt.Printf("User %s not exist asset %s\n", userID, assetCID)
			return nil
		}

		for _, v := range links {
			fmt.Println(v)
		}

		return nil
	},
}

var changeVIP = &cli.Command{
	Name:  "vip",
	Usage: "change user vip",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "user",
			Usage:    "Specify the user id",
			Required: true,
		},
		&cli.BoolFlag{
			Name:  "enable",
			Usage: "set vip state",
		},
	},

	Action: func(cctx *cli.Context) error {
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		userID := cctx.String("user")
		isVIP := cctx.Bool("enable")
		ctx := ReqContext(cctx)
		return schedulerAPI.SetUserVIP(ctx, userID, isVIP)
	},
}
