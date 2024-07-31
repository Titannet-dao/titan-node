package cli

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"strings"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/urfave/cli/v2"
)

// SchedulerCMDs Scheduler cmd
var SchedulerCMDs = []*cli.Command{
	WithCategory("node", nodeCmds),
	WithCategory("asset", assetCmds),
	WithCategory("config", sConfigCmds),
	WithCategory("user", userCmds),
	WithCategory("project", projectCmds),
	WithCategory("codes", codesCmds),
	WithCategory("provider", providerCmds),
	WithCategory("deployment", deploymentCmds),
	startElectionCmd,
	// other
	edgeUpdaterCmd,
	loadWorkloadCmd,
	reNatCmd,
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
		Value:       500,
		DefaultText: "500",
	}

	nodeTypeFlag = &cli.IntFlag{
		Name:  "node-type",
		Usage: "node type 0:ALL 1:Edge 2:Candidate 3:Validator 4:Scheduler",
		Value: 0,
	}

	limitFlag = &cli.IntFlag{
		Name:        "limit",
		Usage:       "the numbering of limit",
		Value:       500,
		DefaultText: "500",
	}

	offsetFlag = &cli.IntFlag{
		Name:        "offset",
		Usage:       "the numbering of offset",
		Value:       0,
		DefaultText: "0",
	}

	expirationDateFlag = &cli.StringFlag{
		Name:        "expiration-date",
		Usage:       "Set the asset expiration, format with '2006-01-02 15:04:05' layout.",
		Value:       "",
		DefaultText: "now + 360 days",
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

	bandwidthFlag = &cli.Float64Flag{
		Name:  "bandwidth",
		Usage: "bandwidth (unit:MiB/s)",
		Value: 0,
	}
)

var loadWorkloadCmd = &cli.Command{
	Name:  "workload",
	Usage: "load work load",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "id",
			Usage: "specify the cid of a asset",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		id := cctx.String("id")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		info, err := schedulerAPI.GetWorkloadRecord(ctx, id)
		if err != nil {
			return err
		}

		fmt.Println("find---------- ", info.AssetCID)

		ws := make([]*types.Workload, 0)
		dec := gob.NewDecoder(bytes.NewBuffer(info.Workloads))
		err = dec.Decode(&ws)
		if err != nil {
			log.Errorf("decode data to []*types.Workload error: %s", err.Error())
			return err
		}

		for _, w := range ws {
			fmt.Println(w.SourceID)
		}

		return nil
	},
}

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

var reNatCmd = &cli.Command{
	Name:  "rn",
	Usage: "re determine node nat type",
	Flags: []cli.Flag{
		nodeIDFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.ReDetermineNodeNATType(ctx, nodeID)
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

var sConfigCmds = &cli.Command{
	Name:  "config",
	Usage: "set or show config",
	Subcommands: []*cli.Command{
		sConfigSetCmd,
		sConfigShowCmd,
	},
}

var sConfigShowCmd = &cli.Command{
	Name:  "show",
	Usage: "show scheduler config",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		_, lr, err := openRepoAndLock(cctx)
		if err != nil {
			return err
		}
		defer lr.Close() //nolint:errcheck  // ignore error

		cfg, err := lr.Config()
		if err != nil {
			return err
		}

		fmt.Printf("%#v\n", cfg)
		return nil
	},
}

var sConfigSetCmd = &cli.Command{
	Name:  "set",
	Usage: "set scheduler config",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "external-url",
			Usage: "scheduler's external ip. example: --external-url=https://server_ip:port/rpc/v0",
			Value: "https://localhost:3456/rpc/v0",
		},
		&cli.StringFlag{
			Name:  "listen-address",
			Usage: "local listen address, example: --listen-address=local_server_ip:port",
			Value: "0.0.0.0:1234",
		},
		&cli.BoolFlag{
			Name:  "insecure-skip-verify",
			Usage: "skip tls verify, example: --insecure-skip-verify=true",
			Value: true,
		},
		&cli.StringFlag{
			Name:  "certificate-path",
			Usage: "used for http3 server, be used if InsecureSkipVerify is true , example: --certificate-path=your_certificate_path",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "private-key-path",
			Usage: "used for http3 server, be used if InsecureSkipVerify is true , example: --private-key-path=your_private_key_path",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "ca-certificate-path",
			Usage: "self sign certificate, use for client , example: --ca-certificate-path=your_ca_certificate_path",
			Value: "",
		},
		&cli.BoolFlag{
			Name:  "enable-validation",
			Usage: "config to enabled node validation, example: --enable-validation=true",
			Value: true,
		},
		&cli.StringSliceFlag{
			Name:  "etcd-addresses",
			Usage: "etcd cluster address example: --etcd-addresses=etcd-address1,etcd-address2",
			Value: &cli.StringSlice{},
		},
		&cli.StringFlag{
			Name:  "area-id",
			Usage: "example: --area-id=your_area_id",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "database-address",
			Usage: "mysql cluster address metadata path, example: --database-address=mysql_user:mysql_password@tcp(mysql_ip:mysql_port)/mysql_database",
			Value: "",
		},
		&cli.IntFlag{
			Name:  "candidate-replicas",
			Usage: "number of candidate node replicas (does not contain 'seed'), example: --candidate-replicas=candidate_replicas_count",
			Value: 0,
		},
		&cli.Float64Flag{
			Name:  "validator-ratio",
			Usage: "proportion of validator in candidate nodes (0 ~ 1), example: --validator-ratio=validator_ratio",
			Value: 1,
		},
		&cli.IntFlag{
			Name:  "validator-base-bandwidth",
			Usage: "the base downstream bandwidth per validator window (unit : MiB), example: --validator-base-bandwidth=bandwidth",
			Value: 100,
		},
		&cli.Float64Flag{
			Name:  "validation-profit",
			Usage: "increased profit after node validation passes",
			Value: 1,
		},
		&cli.Float64Flag{
			Name:  "workload-profit",
			Usage: "increased profit after node workload passes",
			Value: 5,
		},
		&cli.Float64Flag{
			Name:  "election-cycle",
			Usage: "lectionCycle cycle (Unit:day)",
			Value: 5,
		},
	},

	Action: func(cctx *cli.Context) error {
		_, lr, err := openRepoAndLock(cctx)
		if err != nil {
			return err
		}
		defer lr.Close() //nolint:errcheck  // ignore error

		lr.SetConfig(func(raw interface{}) {
			cfg, ok := raw.(*config.SchedulerCfg)
			if !ok {
				log.Errorf("can not convert interface to SchedulerCfg")
				return
			}

			if cctx.IsSet("listen-address") {
				cfg.ListenAddress = cctx.String("listen-address")
			}
			if cctx.IsSet("external-url") {
				cfg.ExternalURL = cctx.String("external-url")
			}
			if cctx.IsSet("database-address") {
				cfg.DatabaseAddress = cctx.String("database-address")
			}
			if cctx.IsSet("area-id") {
				cfg.AreaID = cctx.String("area-id")
			}
			if cctx.IsSet("etcd-addresses") {
				cfg.EtcdAddresses = cctx.StringSlice("etcd-addresses")
			}
			if cctx.IsSet("insecure-skip-verify") {
				cfg.InsecureSkipVerify = cctx.Bool("insecure-skip-verify")
			}
			if cctx.IsSet("certificate-path") {
				cfg.CertificatePath = cctx.String("certificate-path")
			}
			if cctx.IsSet("private-key-path") {
				cfg.PrivateKeyPath = cctx.String("private-key-path")
			}
			if cctx.IsSet("ca-certificate-path") {
				cfg.CaCertificatePath = cctx.String("ca-certificate-path")
			}
			if cctx.IsSet("enable-validation") {
				cfg.EnableValidation = cctx.Bool("enable-validation")
			}
			if cctx.IsSet("candidate-replicas") {
				cfg.CandidateReplicas = cctx.Int("candidate-replicas")
			}
			if cctx.IsSet("validator-ratio") {
				cfg.ValidatorRatio = cctx.Float64("validator-ratio")
			}
			if cctx.IsSet("validator-base-bandwidth") {
				cfg.ValidatorBaseBwDn = cctx.Int("validator-base-bandwidth")
			}
			if cctx.IsSet("validation-profit") {
				cfg.ValidationProfit = cctx.Float64("validation-profit")
			}
			if cctx.IsSet("workload-profit") {
				cfg.WorkloadProfit = cctx.Float64("workload-profit")
			}
			if cctx.IsSet("election-cycle") {
				cfg.ElectionCycle = cctx.Int("election-cycle")
			}
		})

		return nil
	},
}
