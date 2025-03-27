package httpserver

import (
	"context"
	"crypto/rsa"
	"fmt"
	gopath "path"
	"sync"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	bsfetcher "github.com/ipfs/go-fetcher/impl/blockservice"
	"github.com/ipfs/go-libipfs/files"
	dag "github.com/ipfs/go-merkledag"
	ipfspath "github.com/ipfs/go-path"
	"github.com/ipfs/go-path/resolver"
	unixfile "github.com/ipfs/go-unixfs/file"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/interface-go-ipfs-core/path"
	dagpb "github.com/ipld/go-codec-dagpb"
	ipldprime "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
)

type Validation interface {
	SetFunc(func() string)
}

type HttpServer struct {
	asset               Asset
	scheduler           api.Scheduler
	privateKey          *rsa.PrivateKey
	schedulerPublicKey  *rsa.PublicKey
	validation          Validation
	tokens              *sync.Map
	apiSecret           *jwt.HMACSHA
	maxSizeOfUploadFile int64
	webRedirect         string
	rateLimiter         *types.RateLimiter
	v3Progress          *sync.Map
	monitor             *Monitor
}

type HttpServerOptions struct {
	Asset               Asset
	Scheduler           api.Scheduler
	PrivateKey          *rsa.PrivateKey
	Validation          Validation
	APISecret           *jwt.HMACSHA
	MaxSizeOfUploadFile int64
	WebRedirect         string
	RateLimiter         *types.RateLimiter
}

// NewHttpServer creates a new HttpServer with the given Asset, Scheduler, and RSA private key.
func NewHttpServer(opts *HttpServerOptions) *HttpServer {
	hs := &HttpServer{
		asset:               opts.Asset,
		scheduler:           opts.Scheduler,
		privateKey:          opts.PrivateKey,
		validation:          opts.Validation,
		apiSecret:           opts.APISecret,
		tokens:              &sync.Map{},
		maxSizeOfUploadFile: opts.MaxSizeOfUploadFile,
		webRedirect:         opts.WebRedirect,
		rateLimiter:         opts.RateLimiter,
		v3Progress:          &sync.Map{},
		monitor:             NewMonitor(),
	}

	if hs.validation != nil {
		hs.validation.SetFunc(hs.FirstToken)
	}

	if err := hs.updateSchedulerPublicKey(); err != nil {
		log.Errorf("updateSchedulerPublicKey error %s", err.Error())
	}

	// monitoredHandler := hs.monitor.Middleware(mainHandler)

	// // 初始化其他处理函数，确保它们是 http.HandlerFunc
	// hs.handler = http.HandlerFunc(hs.handler)
	// hs.uploadHandler = http.HandlerFunc(hs.uploadHandler)
	// hs.uploadv2Handler = http.HandlerFunc(hs.uploadv2Handler)
	// hs.uploadv3Handler = http.HandlerFunc(hs.uploadv3Handler)
	// hs.uploadv3StatusHandler = http.HandlerFunc(hs.uploadv3StatusHandler)
	// hs.uploadv4Handler = http.HandlerFunc(hs.uploadv4Handler)

	hs.monitor.Start()

	return hs
}

// TODO: need to update public key on reconnect
// updateSchedulerPublicKey update the public key of the scheduler.
func (hs *HttpServer) updateSchedulerPublicKey() error {
	pem, err := hs.scheduler.GetSchedulerPublicKey(context.Background())
	if err != nil {
		return err
	}

	publicKey, err := titanrsa.Pem2PublicKey([]byte(pem))
	if err != nil {
		return err
	}

	hs.schedulerPublicKey = publicKey
	return nil
}

// GetDownloadThreadCount get download thread count of httpserver
func (hs *HttpServer) FirstToken() string {
	token := ""
	hs.tokens.Range(func(key, value interface{}) bool {
		token = key.(string)
		return false
	})
	return token
}

func (hs *HttpServer) Stats() *types.KeepaliveReq {
	// peekd, peeku := hs.monitor.Peak0.D, hs.monitor.Peak0.U

	// 默认peak0, 如果peak0小于20%, 任务量小于5, 使用peak1, 如果peak1小于20%, 任务量小于5, 使用peak2

	// ret := &types.KeepaliveReq{}

	// peak0, peak1, peak2 := hs.monitor.Peak0, hs.monitor.Peak1, hs.monitor.Peak2
	// var pu, pd int64 = peak0.U.Load(), peak0.D.Load()

	// // degrade to peak1
	// if pu < 0.2*RequiredBandDown && pu < peak1.D.Load() {
	// 	pu = peak1.U.Load()
	// }

	// if pd < 0.2*RequiredBandUp && pd < peak1.U.Load() {
	// 	pd = peak1.D.Load()
	// }

	// // degrade to peak2
	// if pd < 0.2*RequiredBandUp && hs.monitor.Routes.TaskRunningCount() < Peak1LowTask {
	// 	pd = peak2.D.Load()
	// }

	// if pu < 0.2*RequiredBandDown && hs.monitor.Routes.TaskRunningCount() < Peak1LowTask {
	// 	pu = peak2.U.Load()
	// }

	// loads
	// var lu, ld int64 = hs.monitor.Loads

	stats := hs.monitor.GetStats()
	return &types.KeepaliveReq{
		Free:      types.FlowUnit{U: stats.Free.U, D: stats.Free.D},
		Peak:      types.FlowUnit{U: stats.Peak.U, D: stats.Peak.D},
		TaskCount: stats.TaskCount,
	}
}

// resolvePath resolves an IPFS path to a ResolvedPath, given the asset CID.
func (hs *HttpServer) resolvePath(ctx context.Context, p path.Path, asset cid.Cid) (path.Resolved, error) {
	if _, ok := p.(path.Resolved); ok {
		return p.(path.Resolved), nil
	}
	if err := p.IsValid(); err != nil {
		return nil, err
	}

	ipath := ipfspath.Path(p.String())
	if ipath.Segments()[0] != "ipfs" {
		return nil, fmt.Errorf("unsupported path namespace: %s", p.Namespace())
	}

	fetcherFactory := bsfetcher.NewFetcherConfig(blockservice.New(&readOnlyBlockStore{hs, asset}, nil))
	fetcherFactory.PrototypeChooser = dagpb.AddSupportToChooser(func(lnk ipldprime.Link, lnkCtx ipldprime.LinkContext) (ipldprime.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})
	fetcherFactory.NodeReifier = unixfsnode.Reify

	resolver := resolver.NewBasicResolver(fetcherFactory)
	node, rest, err := resolver.ResolveToLastNode(ctx, ipath)
	if err != nil {
		return nil, fmt.Errorf("ResolveToLastNode error %w", err)
	}

	root, err := cid.Parse(ipath.Segments()[1])
	if err != nil {
		return nil, err
	}

	return path.NewResolvedPath(ipath, node, root, gopath.Join(rest...)), nil
}

// getUnixFsNode returns a read-only handle to a UnixFS node referenced by a ResolvedPath and root CID.
func (hs *HttpServer) getUnixFsNode(ctx context.Context, p path.Resolved, root cid.Cid) (files.Node, error) {
	ng := &nodeGetter{hs, root}
	node, err := ng.Get(ctx, p.Cid())
	if err != nil {
		return nil, err
	}

	dagService := dag.NewReadOnlyDagService(&nodeGetter{hs, root})
	return unixfile.NewUnixfsFile(ctx, dagService, node)
}

func (hs *HttpServer) lsUnixFsDir(ctx context.Context, p path.Resolved, root cid.Cid) (uio.Directory, error) {
	ng := &nodeGetter{hs, root}
	node, err := ng.Get(ctx, p.Cid())
	if err != nil {
		return nil, err
	}

	dagService := dag.NewReadOnlyDagService(&nodeGetter{hs, root})

	return uio.NewDirectoryFromNode(dagService, node)

}
