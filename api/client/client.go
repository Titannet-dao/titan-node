package client

import (
	"context"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/Filecoin-Titan/titan/api"

	"github.com/filecoin-project/go-jsonrpc"

	"github.com/Filecoin-Titan/titan/lib/rpcenc"
)

// NewScheduler creates a new http jsonrpc client.
func NewScheduler(ctx context.Context, addr string, requestHeader http.Header) (api.Scheduler, jsonrpc.ClientCloser, error) {
	var res api.SchedulerStruct

	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res), requestHeader)

	return &res, closer, err
}

func getPushURL(addr string) (string, error) {
	pushURL, err := url.Parse(addr)
	if err != nil {
		return "", err
	}
	switch pushURL.Scheme {
	case "ws":
		pushURL.Scheme = "http"
	case "wss":
		pushURL.Scheme = "https"
	}
	///rpc/v0 -> /rpc/streams/v0/push

	pushURL.Path = path.Join(pushURL.Path, "../streams/v0/push")
	return pushURL.String(), nil
}

// NewCandidate creates a new http jsonrpc client for candidate
func NewCandidate(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.Candidate, jsonrpc.ClientCloser, error) {
	pushURL, err := getPushURL(addr)
	if err != nil {
		return nil, nil, err
	}

	var res api.CandidateStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res), requestHeader,
		append([]jsonrpc.Option{
			rpcenc.ReaderParamEncoder(pushURL),
		}, opts...)...)

	return &res, closer, err
}

func NewEdge(ctx context.Context, addr string, requestHeader http.Header) (api.Edge, jsonrpc.ClientCloser, error) {
	pushURL, err := getPushURL(addr)
	if err != nil {
		return nil, nil, err
	}

	var res api.EdgeStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcenc.ReaderParamEncoder(pushURL),
		jsonrpc.WithNoReconnect(),
		jsonrpc.WithTimeout(30*time.Second),
	)

	return &res, closer, err
}

// custom http client for edge client
func NewEdgeWithHTTPClient(ctx context.Context, addr string, requestHeader http.Header, httpClient *http.Client) (api.Edge, jsonrpc.ClientCloser, error) {
	pushURL, err := getPushURL(addr)
	if err != nil {
		return nil, nil, err
	}

	var res api.EdgeStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcenc.ReaderParamEncoder(pushURL),
		jsonrpc.WithNoReconnect(),
		jsonrpc.WithTimeout(30*time.Second),
		jsonrpc.WithHTTPClient(httpClient),
	)

	return &res, closer, err
}

// NewCommonRPCV0 creates a new http jsonrpc client.
func NewCommonRPCV0(ctx context.Context, addr string, requestHeader http.Header) (api.Common, jsonrpc.ClientCloser, error) {
	var res api.CommonStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res), requestHeader)

	return &res, closer, err
}

func NewLocator(ctx context.Context, addr string, requestHeader http.Header) (api.Locator, jsonrpc.ClientCloser, error) {
	pushURL, err := getPushURL(addr)
	if err != nil {
		return nil, nil, err
	}

	var res api.LocatorStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcenc.ReaderParamEncoder(pushURL),
		jsonrpc.WithNoReconnect(),
		jsonrpc.WithTimeout(30*time.Second),
	)

	return &res, closer, err
}
