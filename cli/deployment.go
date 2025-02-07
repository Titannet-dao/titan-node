package cli

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/tablewriter"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/docker/go-units"
	dockerterm "github.com/moby/term"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	"io/ioutil"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/kubectl/pkg/util/term"
	"path"
	"sigs.k8s.io/yaml"
)

var deploymentCmds = &cli.Command{
	Name:  "deployment",
	Usage: "Manager deployment",
	Subcommands: []*cli.Command{
		CreateDeployment,
		DeploymentList,
		DeleteDeployment,
		StatusDeployment,
		UpdateDeployment,
		ExecuteCmd,
		DomainCmds,
		CopyFileCmd,
	},
}

var CreateDeployment = &cli.Command{
	Name:  "create",
	Usage: "create new deployment",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "provider-id",
			Usage:    "the provider id",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "owner",
			Usage: "owner address",
		},
		&cli.StringFlag{
			Name:  "template",
			Usage: "from the template file",
		},
		&cli.StringFlag{
			Name:  "name",
			Usage: "deployment name",
		},
		&cli.IntFlag{
			Name:  "port",
			Usage: "deployment internal server port",
		},
		&cli.BoolFlag{
			Name:  "auth",
			Usage: "deploy from authority",
		},
		&cli.StringFlag{
			Name:  "image",
			Usage: "deployment image",
		},
		&cli.Float64Flag{
			Name:  "cpu",
			Usage: "cpu cores",
		},
		&cli.Float64Flag{
			Name:  "gpu",
			Usage: "gpu devices",
		},
		&cli.Int64Flag{
			Name:  "mem",
			Usage: "memory",
		},
		&cli.Int64Flag{
			Name:  "storage",
			Usage: "storage",
		},
		&cli.StringFlag{
			Name:  "env",
			Usage: "set the deployment running environment",
		},
		&cli.StringSliceFlag{
			Name:  "args",
			Usage: "set the deployment running arguments",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		providerID := cctx.String("provider-id")

		if cctx.String("template") != "" {
			return createDeploymentFromTemplate(ctx, api, providerID, cctx.String("template"))
		}

		if cctx.String("image") == "" {
			return errors.Errorf("Required flags image not set")
		}

		var env types.Env
		if cctx.String("env") != "" {
			err := json.Unmarshal([]byte(cctx.String("env")), &env)
			if err != nil {
				return err
			}
		}

		deployment := &types.Deployment{
			ProviderID: providerID,
			Name:       cctx.String("name"),
			Authority:  cctx.Bool("auth"),
			Services: []*types.Service{
				{
					Image: cctx.String("image"),
					Ports: []types.Port{
						{
							Port: cctx.Int("port"),
						},
					},
					ComputeResources: types.ComputeResources{
						CPU:     cctx.Float64("cpu"),
						GPU:     cctx.Float64("gpu"),
						Memory:  cctx.Int64("mem"),
						Storage: []*types.Storage{{Quantity: cctx.Int64("storage")}},
					},
					Env:       env,
					Arguments: cctx.StringSlice("args"),
				},
			},
		}

		return api.CreateDeployment(ctx, deployment)
	},
}

func createDeploymentFromTemplate(ctx context.Context, api api.Scheduler, providerID string, path string) error {
	yamlFiles, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var deployment types.Deployment
	err = yaml.Unmarshal(yamlFiles, &deployment)
	if err != nil {
		return err
	}

	x, _ := json.Marshal(deployment)
	fmt.Println(string(x))

	deployment.ProviderID = providerID
	return api.CreateDeployment(ctx, &deployment)
}

var DeploymentList = &cli.Command{
	Name:  "list",
	Usage: "List deployments",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "owner",
			Usage: "owner address",
		},
		&cli.StringFlag{
			Name:  "id",
			Usage: "the deployment id",
		},
		&cli.StringFlag{
			Name:  "provider-id",
			Usage: "the provider id",
		},
		&cli.BoolFlag{
			Name:  "show-all",
			Usage: "show deleted and inactive deployments",
		},
		&cli.IntFlag{
			Name:  "page",
			Usage: "the page number",
			Value: 1,
		},
		&cli.IntFlag{
			Name:  "size",
			Usage: "the page size",
			Value: 10,
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		tw := tablewriter.New(
			tablewriter.Col("ID"),
			tablewriter.Col("Name"),
			tablewriter.Col("Image"),
			tablewriter.Col("State"),
			tablewriter.Col("Authority"),
			tablewriter.Col("Total"),
			tablewriter.Col("Ready"),
			tablewriter.Col("Available"),
			tablewriter.Col("CPU"),
			tablewriter.Col("GPU"),
			tablewriter.Col("Memory"),
			tablewriter.Col("Storage"),
			tablewriter.Col("Provider"),
			tablewriter.Col("Port"),
			tablewriter.Col("CreatedTime"),
		)

		opts := &types.GetDeploymentOption{
			Owner:        cctx.String("owner"),
			State:        []types.DeploymentState{types.DeploymentStateActive},
			DeploymentID: types.DeploymentID(cctx.String("id")),
			ProviderID:   cctx.String("provider-id"),
			Page:         cctx.Int("page"),
			Size:         cctx.Int("size"),
		}

		if cctx.Bool("show-all") {
			opts.State = types.AllDeploymentStates
		}

		resp, err := api.GetDeploymentList(ctx, opts)
		if err != nil {
			return err
		}

		for _, deployment := range resp.Deployments {
			for _, service := range deployment.Services {
				state := types.DeploymentStateInActive
				if service.Status.TotalReplicas == service.Status.ReadyReplicas {
					state = types.DeploymentStateActive
				}

				var exposePorts []string
				for _, port := range service.Ports {
					exposePorts = append(exposePorts, fmt.Sprintf("%d->%d", port.Port, port.ExposePort))
				}

				var storageSize int64
				for _, storage := range service.Storage {
					storageSize += storage.Quantity
				}

				m := map[string]interface{}{
					"ID":          deployment.ID,
					"Name":        deployment.Name,
					"Image":       service.Image,
					"State":       types.DeploymentStateString(state),
					"Authority":   deployment.Authority,
					"Total":       service.Status.TotalReplicas,
					"Ready":       service.Status.ReadyReplicas,
					"Available":   service.Status.AvailableReplicas,
					"CPU":         service.CPU,
					"GPU":         service.GPU,
					"Memory":      units.BytesSize(float64(service.Memory * units.MiB)),
					"Storage":     units.BytesSize(float64(storageSize * units.MiB)),
					"Provider":    deployment.ProviderExposeIP,
					"Port":        strings.Join(exposePorts, " "),
					"CreatedTime": deployment.CreatedAt.Format(defaultDateTimeLayout),
				}
				tw.Write(m)
			}
		}

		tw.Flush(os.Stdout)
		return nil
	},
}

var DeleteDeployment = &cli.Command{
	Name:  "delete",
	Usage: "delete deployment",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "f",
			Usage: "Force delete",
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
		deploymentID := types.DeploymentID(cctx.Args().First())

		resp, err := api.GetDeploymentList(ctx, &types.GetDeploymentOption{
			DeploymentID: deploymentID,
		})
		if err != nil {
			return err
		}

		if len(resp.Deployments) == 0 {
			return errors.New("deployment not found")
		}

		for _, deployment := range resp.Deployments {
			err = api.CloseDeployment(ctx, deployment, cctx.Bool("f"))
			if err != nil {
				log.Errorf("delete deployment failed: %v", err)
			}
		}

		return nil
	},
}

var StatusDeployment = &cli.Command{
	Name:  "status",
	Usage: "show deployment status",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "log",
			Usage: "show deployment log",
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
		deploymentID := types.DeploymentID(cctx.Args().First())

		resp, err := api.GetDeploymentList(ctx, &types.GetDeploymentOption{
			DeploymentID: deploymentID,
		})
		if err != nil {
			return err
		}

		var deployment *types.Deployment
		for _, d := range resp.Deployments {
			if d.ID == deploymentID {
				deployment = d
				continue
			}
		}

		if deployment == nil {
			return errors.New("deployment not found")
		}

		fmt.Printf("DeploymentID:\t%s\n", deployment.ID)
		fmt.Printf("State:\t\t%s\n", types.DeploymentStateString(deployment.State))
		fmt.Printf("CreadTime:\t%v\n", deployment.CreatedAt)
		fmt.Printf("--------\nEvents:\n")

		serviceEvents, err := api.GetEvents(ctx, deployment)
		if err != nil {
			return err
		}

		for _, sv := range serviceEvents {
			for i, event := range sv.Events {
				fmt.Printf("%d.\t[%s]\t%s\n", i, sv.ServiceName, event)
			}
		}

		if cctx.Bool("log") {
			fmt.Printf("--------\nLogs:\n")

			serviceLogs, err := api.GetLogs(ctx, deployment)
			if err != nil {
				return err
			}

			for _, sl := range serviceLogs {
				for _, l := range sl.Logs {
					fmt.Printf("%s\n", l)
				}
			}
		}

		return nil
	},
}

var UpdateDeployment = &cli.Command{
	Name:  "update",
	Usage: "update deployment",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "deployment-id",
			Usage:    "the deployment id",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "template",
			Usage: "from the template file",
		},
	},
	Action: func(cctx *cli.Context) error {
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

		if cctx.String("template") == "" {
			return errors.Errorf("template empty")
		}

		yamlFiles, err := os.ReadFile(cctx.String("template"))
		if err != nil {
			return err
		}

		var deployment types.Deployment
		err = yaml.Unmarshal(yamlFiles, &deployment)
		if err != nil {
			return err
		}

		deployment.ID = deploymentID
		return api.UpdateDeployment(ctx, &deployment)
	},
}

func encode(esc *types.Token) (*bytes.Buffer, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(esc)
	if err != nil {
		return nil, err
	}

	return &buffer, nil
}

var ExecuteCmd = &cli.Command{
	Name:  "exec",
	Usage: "deployment executor",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "pod",
			Usage: "pod name",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
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

		resp, err := api.GetDeploymentList(ctx, &types.GetDeploymentOption{
			DeploymentID: deploymentID,
		})
		if err != nil {
			return err
		}

		var deployment *types.Deployment
		for _, d := range resp.Deployments {
			if d.ID == deploymentID {
				deployment = d
				continue
			}
		}

		if deployment == nil {
			return errors.New("deployment not found")
		}

		podName := cctx.String("pod")
		if podName == "" && len(deployment.Services) > 0 {
			podName = deployment.Services[0].Name
		}

		commands := cctx.Args().Tail()
		if len(commands) == 0 {
			commands = []string{"sh"}
		}

		leaseEndpoint, err := api.GetLeaseShellEndpoint(ctx, deploymentID)
		if err != nil {
			return err
		}

		scheme := "ws://"
		if leaseEndpoint.Scheme == "https" {
			scheme = "wss://"
		}

		endpoint, err := url.Parse(scheme + leaseEndpoint.Host + leaseEndpoint.ShellPath)
		if err != nil {
			return err
		}

		fmt.Println(endpoint.String())
		fmt.Println(leaseEndpoint.Token)

		header := http.Header{}
		header.Add("Authorization", "Bearer "+leaseEndpoint.Token)

		var stdin io.ReadCloser
		var stdout io.Writer
		var stderr io.Writer
		stdout = os.Stdout
		stderr = os.Stderr
		stdin = os.Stdin

		query := url.Values{}
		query.Set("pod", podName)
		for index, cmd := range commands {
			query.Set(fmt.Sprintf("cmd%d", index), cmd)
		}

		query.Set("tty", "1")
		stdinVal := "0"
		if stdin != nil {
			stdinVal = "1"
		}
		query.Set("stdin", stdinVal)
		endpoint.RawQuery = query.Encode()

		fmt.Println("creating connection to", endpoint.String())

		var tty term.TTY
		var tsq remotecommand.TerminalSizeQueue

		tty = term.TTY{
			Parent: nil,
			Out:    os.Stdout,
			In:     stdin,
		}

		if !tty.IsTerminalIn() {
			return errors.Errorf("errTerminalNotATty: %v", err)
		}

		dockerStdin, dockerStdout, _ := dockerterm.StdStreams()
		tty.In = dockerStdin
		tty.Out = dockerStdout

		stdin = dockerStdin
		stdout = dockerStdout
		tsq = tty.MonitorSize(tty.GetSize())
		tty.Raw = true

		var terminalResizes chan remotecommand.TerminalSize
		wg := &sync.WaitGroup{}
		ctx, cancel := context.WithCancel(cctx.Context)

		if tsq != nil {
			terminalResizes = make(chan remotecommand.TerminalSize, 1)
			go func() {
				for {
					// this blocks waiting for a resize event, the docs suggest
					// that this isn't the case but there is not a code path that ever does that
					// so this goroutine is just left running until the process exits
					size := tsq.Next()
					if size == nil {
						return
					}
					terminalResizes <- *size

				}
			}()
		}

		signals := make(chan os.Signal, 1)
		signalsToCatch := []os.Signal{syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP}

		signal.Notify(signals, signalsToCatch...)
		wasHalted := make(chan os.Signal, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sig := <-signals:
				cancel()
				wasHalted <- sig
			case <-ctx.Done():
			}
		}()

		shellFn := func() error {
			return doLeaseShell(ctx, endpoint.String(), header, stdin, stdout, stderr, true, terminalResizes)
		}

		err = tty.Safe(shellFn)

		// Check if a signal halted things
		select {
		case haltSignal := <-wasHalted:
			fmt.Println(fmt.Sprintf("\nhalted by signal: %v\n", haltSignal))
			err = nil // Don't show this error, as it is always something complaining about use of a closed connection
		default:
			cancel()
		}
		wg.Wait()

		if err != nil {
			return showErrorToUser(err)
		}

		return nil
	},
}

var CopyFileCmd = &cli.Command{
	Name:      "cp",
	Usage:     "Copy files to container",
	UsageText: "manager deployment cp <deployment-id> <src> <dest> [options]",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "pod",
			Usage: "pod name",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
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

		resp, err := api.GetDeploymentList(ctx, &types.GetDeploymentOption{
			DeploymentID: deploymentID,
		})
		if err != nil {
			return err
		}

		var deployment *types.Deployment
		for _, d := range resp.Deployments {
			if d.ID == deploymentID {
				deployment = d
				continue
			}
		}

		if deployment == nil {
			return errors.New("deployment not found")
		}

		podName := cctx.String("pod")
		if podName == "" && len(deployment.Services) > 0 {
			podName = deployment.Services[0].Name
		}

		if len(cctx.Args().Tail()) != 2 {
			return errors.Errorf("The source path and destination path of the file cannot be empty")
		}

		args := cctx.Args().Tail()
		srcPath := args[0]
		destPath := args[1]

		leaseEndpoint, err := api.GetLeaseShellEndpoint(ctx, deploymentID)
		if err != nil {
			return err
		}

		scheme := "ws://"
		if leaseEndpoint.Scheme == "https" {
			scheme = "wss://"
		}

		endpoint, err := url.Parse(scheme + leaseEndpoint.Host + leaseEndpoint.ShellPath)
		if err != nil {
			return err
		}

		header := http.Header{}
		header.Add("Authorization", "Bearer "+leaseEndpoint.Token)

		err = checkDestinationIsDir(cctx, endpoint.String(), header, podName, destPath)
		if err == nil {
			destPath = filepath.Join(destPath, filepath.Base(srcPath))
		}

		reader, writer := io.Pipe()
		go func(src, dest string, writer io.WriteCloser) {
			defer writer.Close()
			err = makeTar(src, dest, writer)
		}(srcPath, destPath, writer)

		var commands []string
		commands = append(commands, []string{"tar", "-xvf", "-"}...)
		//commands = append(commands, []string{"sh", "-c", "cat > " + destPath + ".tar.gz"}...)
		destFileDir := path.Dir(destPath)
		if len(destFileDir) > 0 {
			commands = append(commands, "-C", destFileDir)
		}

		stdout := os.Stdout
		stderr := os.Stderr

		query := url.Values{}
		query.Set("pod", podName)
		for index, cmd := range commands {
			query.Set(fmt.Sprintf("cmd%d", index), cmd)
		}
		query.Set("stdin", "1")
		endpoint.RawQuery = query.Encode()

		var terminalResizes chan remotecommand.TerminalSize

		ctx, cancel := context.WithCancel(cctx.Context)
		defer cancel()

		err = doLeaseShell(ctx, endpoint.String(), header, reader, stdout, stderr, false, terminalResizes)
		if err != nil {
			fmt.Fprintf(os.Stderr, "provider error message:\n%v\n", err.Error())
		}

		return nil
	},
}

func checkDestinationIsDir(cctx *cli.Context, wsUrl string, header http.Header, podName, destPath string) error {
	ctx, cancel := context.WithCancel(cctx.Context)
	defer cancel()

	var commands []string
	commands = append(commands, []string{"test", "-d", destPath}...)

	reader, writer := io.Pipe()
	defer writer.Close()

	stdout := bytes.NewBuffer([]byte{})
	stderr := bytes.NewBuffer([]byte{})

	endpoint, err := url.Parse(wsUrl)
	if err != nil {
		return err
	}

	query := url.Values{}
	query.Set("pod", podName)
	for index, cmd := range commands {
		query.Set(fmt.Sprintf("cmd%d", index), cmd)
	}
	query.Set("stdin", "1")
	endpoint.RawQuery = query.Encode()

	return doLeaseShell(ctx, endpoint.String(), header, reader, stdout, stderr, false, nil)
}

func makeTar(srcPath, destPath string, writer io.Writer) error {
	// TODO: use compression here?
	tarWriter := tar.NewWriter(writer)
	defer tarWriter.Close()

	srcPath = path.Clean(srcPath)
	destPath = path.Clean(destPath)
	return recursiveTar(path.Dir(srcPath), path.Base(srcPath), path.Dir(destPath), path.Base(destPath), tarWriter)
}

func recursiveTar(srcBase, srcFile, destBase, destFile string, tw *tar.Writer) error {
	filepath := path.Join(srcBase, srcFile)
	stat, err := os.Lstat(filepath)
	if err != nil {
		return err
	}
	if stat.IsDir() {
		files, err := ioutil.ReadDir(filepath)
		if err != nil {
			return err
		}
		if len(files) == 0 {
			//case empty directory
			hdr, _ := tar.FileInfoHeader(stat, filepath)
			hdr.Name = destFile
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
		}
		for _, f := range files {
			if err := recursiveTar(srcBase, path.Join(srcFile, f.Name()), destBase, path.Join(destFile, f.Name()), tw); err != nil {
				return err
			}
		}
		return nil
	} else if stat.Mode()&os.ModeSymlink != 0 {
		//case soft link
		hdr, _ := tar.FileInfoHeader(stat, filepath)
		target, err := os.Readlink(filepath)
		if err != nil {
			return err
		}

		hdr.Linkname = target
		hdr.Name = destFile
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
	} else {
		//case regular file or other file type like pipe
		hdr, err := tar.FileInfoHeader(stat, filepath)
		if err != nil {
			return err
		}
		hdr.Name = destFile

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		f, err := os.Open(filepath)
		if err != nil {
			return err
		}
		defer f.Close()

		if _, err := io.Copy(tw, f); err != nil {
			return err
		}
		return f.Close()
	}
	return nil
}
