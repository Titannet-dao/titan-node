package cli

import (
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/tablewriter"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	"os"
)

var DomainCmds = &cli.Command{
	Name:  "domain",
	Usage: "Manager deployment domains",
	Subcommands: []*cli.Command{
		GetDomainsCmd,
		AddDomainCmd,
		DeleteDomainCmd,
	},
}

var GetDomainsCmd = &cli.Command{
	Name:  "list",
	Usage: "display deployment domains",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		if cctx.NArg() != 1 {
			return IncorrectNumArgs(cctx)
		}

		api, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		deploymentID := types.DeploymentID(cctx.Args().First())
		if deploymentID == "" {
			return errors.Errorf("deploymentID empty")
		}

		domains, err := api.GetDeploymentDomains(ctx, deploymentID)
		if err != nil {
			return err
		}

		tw := tablewriter.New(
			tablewriter.Col("ID"),
			tablewriter.Col("HostURI"),
			tablewriter.Col("State"),
		)

		for index, domain := range domains {
			m := map[string]interface{}{
				"ID":      index + 1,
				"HostURI": domain.Name,
				"State":   domain.State,
			}
			tw.Write(m)
		}

		return tw.Flush(os.Stdout)
	},
}

var AddDomainCmd = &cli.Command{
	Name:  "add",
	Usage: "add new deployment domain configuration",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "deployment-id",
			Usage:    "the deployment id",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "key",
			Usage: "Path to private key associated with given certificate",
		},
		&cli.StringFlag{
			Name:  "cert",
			Usage: "Path to PEM encoded public key certificate.",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.NArg() != 1 {
			return IncorrectNumArgs(cctx)
		}

		api, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		deploymentID := types.DeploymentID(cctx.String("deployment-id"))
		if deploymentID == "" {
			return errors.Errorf("deploymentID empty")
		}

		hostname := cctx.Args().First()
		if deploymentID == "" {
			return errors.Errorf("hostname empty")
		}

		cert := &types.Certificate{
			Hostname: hostname,
		}

		if cctx.String("cert") != "" {
			certFile, err := os.ReadFile(cctx.String("cert"))
			if err != nil {
				return err
			}
			cert.Certificate = certFile
		}

		if cctx.String("key") != "" {
			certKeyFile, err := os.ReadFile(cctx.String("key"))
			if err != nil {
				return err
			}
			cert.PrivateKey = certKeyFile
		}

		err = api.AddDeploymentDomain(ctx, deploymentID, cert)
		if err != nil {
			return errors.Errorf("add new hostname: %v", err)
		}

		return nil
	},
}

var DeleteDomainCmd = &cli.Command{
	Name:  "delete",
	Usage: "delete a deployment domain configuration",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "deployment-id",
			Usage:    "the deployment id",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.NArg() != 1 {
			return IncorrectNumArgs(cctx)
		}

		api, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		deploymentID := types.DeploymentID(cctx.String("deployment-id"))
		if deploymentID == "" {
			return errors.Errorf("deploymentID empty")
		}

		domain := cctx.Args().First()
		err = api.DeleteDeploymentDomain(ctx, deploymentID, domain)
		if err != nil {
			return errors.Errorf("delete domain %s: %v", domain, err)
		}

		return nil
	},
}
