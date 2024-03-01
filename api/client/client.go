package client

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"github.com/filecoin-project/go-jsonrpc"

	"github.com/Filecoin-Titan/titan/lib/rpcenc"
)

// NewScheduler creates a new http jsonrpc client.
func NewScheduler(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.Scheduler, jsonrpc.ClientCloser, error) {
	pushURL, err := getPushURL(addr)
	if err != nil {
		return nil, nil, err
	}

	// TODO server not support https now
	pushURL = strings.Replace(pushURL, "https", "http", 1)

	rpcOpts := []jsonrpc.Option{rpcenc.ReaderParamEncoder(pushURL), jsonrpc.WithErrors(api.RPCErrors)}
	if len(opts) > 0 {
		rpcOpts = append(rpcOpts, opts...)
	}

	var res api.SchedulerStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcOpts...,
	)

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

	rpcOpts := []jsonrpc.Option{rpcenc.ReaderParamEncoder(pushURL), jsonrpc.WithErrors(api.RPCErrors)}
	if len(opts) > 0 {
		rpcOpts = append(rpcOpts, opts...)
	}

	var res api.CandidateStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcOpts...,
	)

	return &res, closer, err
}

func NewEdge(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.Edge, jsonrpc.ClientCloser, error) {
	pushURL, err := getPushURL(addr)
	if err != nil {
		return nil, nil, err
	}

	rpcOpts := []jsonrpc.Option{
		rpcenc.ReaderParamEncoder(pushURL),
		jsonrpc.WithErrors(api.RPCErrors),
		jsonrpc.WithNoReconnect(),
		jsonrpc.WithTimeout(30 * time.Second),
	}

	if len(opts) > 0 {
		rpcOpts = append(rpcOpts, opts...)
	}

	var res api.EdgeStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcOpts...,
	)

	return &res, closer, err
}

// NewCommonRPCV0 creates a new http jsonrpc client.
func NewCommonRPCV0(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.Common, jsonrpc.ClientCloser, error) {
	var res api.CommonStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res), requestHeader, opts...)

	return &res, closer, err
}

func NewLocator(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.Locator, jsonrpc.ClientCloser, error) {
	pushURL, err := getPushURL(addr)
	if err != nil {
		return nil, nil, err
	}

	rpcOpts := []jsonrpc.Option{rpcenc.ReaderParamEncoder(pushURL), jsonrpc.WithNoReconnect(), jsonrpc.WithTimeout(30 * time.Second)}
	if len(opts) > 0 {
		rpcOpts = append(rpcOpts, opts...)
	}

	var res api.LocatorStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcOpts...,
	)

	return &res, closer, err
}

func NewHTTP3Client() *http.Client {
	return &http.Client{
		Transport: &http3.RoundTripper{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
}

// NewHTTP3ClientWithPacketConn new http3 client for nat trave
func NewHTTP3ClientWithPacketConn(tansport *quic.Transport) (*http.Client, error) {
	dial := func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (quic.EarlyConnection, error) {
		remoteAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return nil, err
		}

		return tansport.DialEarly(ctx, remoteAddr, tlsCfg, cfg)
	}

	roundTripper := &http3.RoundTripper{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		QuicConfig: &quic.Config{},
		Dial:       dial,
	}

	return &http.Client{Transport: roundTripper, Timeout: 30 * time.Second}, nil
}
