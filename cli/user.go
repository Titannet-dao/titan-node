package cli

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

var userCmds = &cli.Command{
	Name:  "user",
	Usage: "Manage user",
	Subcommands: []*cli.Command{
		userAPIKeyCmds,
		userStorageCmds,
	},
}

var userAPIKeyCmds = &cli.Command{
	Name:  "api-key",
	Usage: "Manage user api keys",
	Subcommands: []*cli.Command{
		createUserAPIKey,
		getUserAPIKeys,
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

var createUserAPIKey = &cli.Command{
	Name:  "create",
	Usage: "create api key for user",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "user-id",
			Usage:    "special a id user",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "key-name",
			Usage:    "special a name for key",
			Required: true,
		},
	},

	Action: func(cctx *cli.Context) error {
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		userID := cctx.String("user-id")
		keyName := cctx.String("key-name")

		ctx := ReqContext(cctx)
		key, err := schedulerAPI.CreateAPIKey(ctx, userID, keyName)
		if err != nil {
			return err
		}

		fmt.Printf("%s %s", keyName, key)
		return nil
	},
}

var getUserAPIKeys = &cli.Command{
	Name:  "get",
	Usage: "get api keys for user",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "user-id",
			Usage:    "special a id user",
			Required: true,
		},
	},

	Action: func(cctx *cli.Context) error {
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		userID := cctx.String("user-id")

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
			Name:     "user-id",
			Usage:    "special a id user",
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

		userID := cctx.String("user-id")
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
			Name:     "user-id",
			Usage:    "special a id user",
			Required: true,
		},
	},

	Action: func(cctx *cli.Context) error {
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		userID := cctx.String("user-id")

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
			Name:     "user-id",
			Usage:    "special a id user",
			Required: true,
		},
	},

	Action: func(cctx *cli.Context) error {
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		userID := cctx.String("user-id")

		ctx := ReqContext(cctx)
		storageSize, err := schedulerAPI.GetStorageSize(ctx, userID)
		if err != nil {
			return err
		}

		fmt.Printf("storage total size: %d, used size: %d", storageSize.TotalSize, storageSize.UsedSize)
		return nil
	},
}
