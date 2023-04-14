package main

import (
	"context"
	"net/http"

	"github.com/Filecoin-Titan/titan/lib/rpcenc"
	"github.com/Filecoin-Titan/titan/node/handler"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/metrics/proxy"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/gorilla/mux"
)

func CandidateHandler(authv func(ctx context.Context, token string) ([]auth.Permission, error), a api.Candidate, permissioned bool) http.Handler {
	mux := mux.NewRouter()
	readerHandler, readerServerOpt := rpcenc.ReaderParamDecoder()
	rpcServer := jsonrpc.NewServer(readerServerOpt)

	wapi := proxy.MetricedCandidateAPI(a)
	if permissioned {
		wapi = api.PermissionedCandidateAPI(wapi)
	}

	rpcServer.Register("titan", wapi)

	mux.Handle("/rpc/v0", rpcServer)
	mux.Handle("/rpc/streams/v0/push/{uuid}", readerHandler)
	mux.PathPrefix("/").Handler(http.DefaultServeMux) // pprof

	if !permissioned {
		return mux
	}

	ah := &auth.Handler{
		Verify: authv,
		Next:   mux.ServeHTTP,
	}

	return handler.New(ah)
}
