package cliutil

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/node/repo"
)

const (
	metadataTraceContext = "traceContext"
)

// GetAPIInfo returns the API endpoint to use for the specified kind of repo.
//
// The order of precedence is as follows:
//
//  1. *-api-url command line flags.
//  2. *_API_INFO environment variables
//  3. deprecated *_API_INFO environment variables
//  4. *-repo command line flags.
func GetAPIInfo(ctx *cli.Context, t repo.RepoType) (APIInfo, error) {
	// Check if there was a flag passed with the listen address of the API
	// server (only used by the tests)
	for _, f := range t.APIFlags() {
		if !ctx.IsSet(f) {
			continue
		}
		strma := ctx.String(f)
		strma = strings.TrimSpace(strma)

		return APIInfo{Addr: strma}, nil
	}

	//
	// Note: it is not correct/intuitive to prefer environment variables over
	// CLI flags (repo flags below).
	//
	primaryEnv, fallbacksEnvs, deprecatedEnvs := t.APIInfoEnvVars()
	env, ok := os.LookupEnv(primaryEnv)
	if ok {
		return ParseAPIInfo(env), nil
	}

	for _, env := range deprecatedEnvs {
		env, ok := os.LookupEnv(env)
		if ok {
			log.Warnf("Using deprecated env(%s) value, please use env(%s) instead.", env, primaryEnv)
			return ParseAPIInfo(env), nil
		}
	}

	for _, f := range t.RepoFlags() {
		// cannot use ctx.IsSet because it ignores default values
		path := ctx.String(f)
		if path == "" {
			continue
		}

		p, err := homedir.Expand(path)
		if err != nil {
			return APIInfo{}, xerrors.Errorf("could not expand home dir (%s): %w", f, err)
		}

		r, err := repo.NewFS(p)
		if err != nil {
			return APIInfo{}, xerrors.Errorf("could not open repo at path: %s; %w", p, err)
		}

		exists, err := r.Exists()
		if err != nil {
			return APIInfo{}, xerrors.Errorf("repo.Exists returned an error: %w", err)
		}

		if !exists {
			return APIInfo{}, errors.New("repo directory does not exist. Make sure your configuration is correct")
		}

		ma, err := r.APIEndpoint()
		if err != nil {
			return APIInfo{}, xerrors.Errorf("could not get api endpoint: %w", err)
		}

		token, err := r.APIToken()
		if err != nil {
			log.Warnf("Couldn't load CLI token, capabilities may be limited: %v", err)
		}

		return APIInfo{
			Addr:  ma.String(),
			Token: token,
		}, nil
	}

	for _, env := range fallbacksEnvs {
		env, ok := os.LookupEnv(env)
		if ok {
			return ParseAPIInfo(env), nil
		}
	}

	return APIInfo{}, fmt.Errorf("could not determine API endpoint for node type: %v", t.Type())
}

func GetRawAPI(ctx *cli.Context, t repo.RepoType, version string) (string, http.Header, error) {
	ainfo, err := GetAPIInfo(ctx, t)
	if err != nil {
		return "", nil, xerrors.Errorf("could not get API info for %s: %w", t.Type(), err)
	}

	addr, err := ainfo.DialArgs(version)
	if err != nil {
		return "", nil, xerrors.Errorf("could not get DialArgs: %w", err)
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintf(ctx.App.Writer, "using raw API %s endpoint: %s\n", version, addr)
	}

	return addr, ainfo.AuthHeader(), nil
}

func GetCommonAPI(ctx *cli.Context) (api.Common, jsonrpc.ClientCloser, error) {
	ti, ok := ctx.App.Metadata["repoType"]
	if !ok {
		log.Errorf("unknown repo type, are you sure you want to use GetCommonAPI?")
		ti = repo.Scheduler
	}
	t, ok := ti.(repo.RepoType)
	if !ok {
		log.Errorf("repoType type does not match the type of repo.RepoType")
	}

	addr, headers, err := GetRawAPI(ctx, t, "v0")
	if err != nil {
		return nil, nil, err
	}

	return client.NewCommonRPCV0(ctx.Context, addr, headers)
}

func GetSchedulerAPI(ctx *cli.Context, nodeID string) (api.Scheduler, jsonrpc.ClientCloser, error) {
	addr, headers, err := GetRawAPI(ctx, repo.Scheduler, "v0")
	if err != nil {
		return nil, nil, err
	}

	if headers != nil && len(nodeID) > 0 {
		headers.Add("Node-ID", nodeID)
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintln(ctx.App.Writer, "using full node API v0 endpoint:", addr)
	}

	a, c, e := client.NewScheduler(ctx.Context, addr, headers)
	v, err := a.Version(ctx.Context)
	if err != nil {
		return nil, nil, err
	}

	if !v.APIVersion.EqMajorMinor(api.SchedulerAPIVersion0) {
		return nil, nil, xerrors.Errorf("Remote API version didn't match (expected %s, remote %s)", api.SchedulerAPIVersion0, v.APIVersion)
	}

	return a, c, e
}

func GetCandidateAPI(ctx *cli.Context) (api.Candidate, jsonrpc.ClientCloser, error) {
	addr, headers, err := GetRawAPI(ctx, repo.Candidate, "v0")
	if err != nil {
		return nil, nil, err
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintln(ctx.App.Writer, "using candidate node API v0 endpoint:", addr)
	}

	a, c, e := client.NewCandidate(ctx.Context, addr, headers)
	v, err := a.Version(ctx.Context)
	if err != nil {
		return nil, nil, err
	}

	if !v.APIVersion.EqMajorMinor(api.CandidateAPIVersion0) {
		return nil, nil, xerrors.Errorf("Remote API version didn't match (expected %s, remote %s)", api.SchedulerAPIVersion0, v.APIVersion)
	}

	return a, c, e
}

func GetEdgeAPI(ctx *cli.Context) (api.Edge, jsonrpc.ClientCloser, error) {
	addr, headers, err := GetRawAPI(ctx, repo.Edge, "v0")
	if err != nil {
		return nil, nil, err
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintln(ctx.App.Writer, "using edge node API v0 endpoint:", addr)
	}

	a, c, e := client.NewEdge(ctx.Context, addr, headers)
	v, err := a.Version(ctx.Context)
	if err != nil {
		return nil, nil, err
	}

	if !v.APIVersion.EqMajorMinor(api.EdgeAPIVersion0) {
		return nil, nil, xerrors.Errorf("Remote API version didn't match (expected %s, remote %s)", api.SchedulerAPIVersion0, v.APIVersion)
	}

	return a, c, e
}

func GetLocatorAPI(ctx *cli.Context) (api.Locator, jsonrpc.ClientCloser, error) {
	addr, headers, err := GetRawAPI(ctx, repo.Locator, "v0")
	if err != nil {
		return nil, nil, err
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintln(ctx.App.Writer, "using full node API v0 endpoint:", addr)
	}

	a, c, e := client.NewLocator(ctx.Context, addr, headers)
	v, err := a.Version(ctx.Context)
	if err != nil {
		return nil, nil, err
	}

	if !v.APIVersion.EqMajorMinor(api.LocationAPIVersion0) {
		return nil, nil, xerrors.Errorf("Remote API version didn't match (expected %s, remote %s)", api.LocationAPIVersion0, v.APIVersion)
	}

	return a, c, e
}

func DaemonContext(cctx *cli.Context) context.Context {
	if mtCtx, ok := cctx.App.Metadata[metadataTraceContext]; ok {
		return mtCtx.(context.Context)
	}

	return context.Background()
}

// ReqContext returns context for cli execution. Calling it for the first time
// installs SIGTERM handler that will close returned context.
// Not safe for concurrent execution.
func ReqContext(cctx *cli.Context) context.Context {
	tCtx := DaemonContext(cctx)

	ctx, done := context.WithCancel(tCtx)
	sigChan := make(chan os.Signal, 2)
	go func() {
		<-sigChan
		done()
	}()
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	return ctx
}
