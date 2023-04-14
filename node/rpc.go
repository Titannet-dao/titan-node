package node

import (
	"context"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/lib/rpcenc"
	"github.com/Filecoin-Titan/titan/metrics"
	"github.com/Filecoin-Titan/titan/metrics/proxy"
	mhandler "github.com/Filecoin-Titan/titan/node/handler"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"
)

var rpclog = logging.Logger("rpc")

// ServeRPC serves an HTTP handler over the supplied listen multiaddr.
//
// This function spawns a goroutine to run the server, and returns immediately.
// It returns the stop function to be called to terminate the endpoint.
//
// The supplied ID is used in tracing, by inserting a tag in the context.
func ServeRPC(h http.Handler, id string, addr string) (StopFunc, error) {
	// Start listening to the addr; if invalid or occupied, we will fail early.
	lst, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, xerrors.Errorf("could not listen: %w", err)
	}

	// Instantiate the server and start listening.
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 30 * time.Second,
		BaseContext: func(listener net.Listener) context.Context {
			ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, id))
			return ctx
		},
	}

	go func() {
		err = srv.Serve(lst)
		if err != http.ErrServerClosed {
			rpclog.Warnf("rpc server failed: %s", err)
		}
	}()

	return srv.Shutdown, err
}

// SchedulerHandler returns a scheduler handler, to be mounted as-is on the server.
func SchedulerHandler(a api.Scheduler, permission bool, opts ...jsonrpc.ServerOption) (http.Handler, error) {
	m := mux.NewRouter()

	serveRPC := func(path string, hnd interface{}) {
		rpcServer := jsonrpc.NewServer(append(opts, jsonrpc.WithServerErrors(api.RPCErrors))...)
		rpcServer.Register("titan", hnd)

		var handler http.Handler = rpcServer
		if permission {
			handler = mhandler.New(&auth.Handler{Verify: a.VerifyNodeAuthToken, Next: rpcServer.ServeHTTP})
		}

		m.Handle(path, handler)
	}

	fnapi := proxy.MetricedSchedulerAPI(a)
	if permission {
		fnapi = api.PermissionedSchedulerAPI(fnapi)
	}

	serveRPC("/rpc/v0", fnapi)

	// debugging
	m.Handle("/debug/metrics", metrics.Exporter())
	m.Handle("/debug/pprof-set/mutex", handleFractionOpt("MutexProfileFraction", func(x int) {
		runtime.SetMutexProfileFraction(x)
	}))

	m.PathPrefix("/").Handler(http.DefaultServeMux) // pprof

	return m, nil
}

// LocatorHandler returns a locator handler, to be mounted as-is on the server.
func LocatorHandler(a api.Locator, permission bool) (http.Handler, error) {
	mapi := proxy.MetricedLocatorAPI(a)
	if permission {
		mapi = api.PermissionedLocationAPI(mapi)
	}

	readerHandler, readerServerOpt := rpcenc.ReaderParamDecoder()
	rpcServer := jsonrpc.NewServer(jsonrpc.WithServerErrors(api.RPCErrors), readerServerOpt)
	rpcServer.Register("titan", mapi)

	rootMux := mux.NewRouter()

	// local APIs
	{
		m := mux.NewRouter()
		m.Handle("/rpc/v0", rpcServer)
		m.Handle("/rpc/streams/v0/push/{uuid}", readerHandler)
		// debugging
		m.Handle("/debug/metrics", metrics.Exporter())
		m.PathPrefix("/").Handler(http.DefaultServeMux) // pprof

		var hnd http.Handler = m
		if permission {
			hnd = mhandler.New(&auth.Handler{
				Verify: a.AuthVerify,
				Next:   m.ServeHTTP,
			})
		}

		rootMux.PathPrefix("/").Handler(hnd)
	}

	return rootMux, nil
}

func handleFractionOpt(name string, setter func(int)) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(rw, "only POST allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}

		asfr := r.Form.Get("x")
		if len(asfr) == 0 {
			http.Error(rw, "parameter 'x' must be set", http.StatusBadRequest)
			return
		}

		fr, err := strconv.Atoi(asfr)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
		rpclog.Infof("setting %s to %d", name, fr)
		setter(fr)
	}
}
