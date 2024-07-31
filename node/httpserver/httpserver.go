package httpserver

import (
	"context"
	"crypto/rsa"
	"fmt"
	"github.com/Filecoin-Titan/titan/node/container"
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
	maxSizeOfUploadFile int
	webRedirect         string
	rateLimiter         *types.RateLimiter
	client              *container.Client
}

type HttpServerOptions struct {
	Asset               Asset
	Scheduler           api.Scheduler
	PrivateKey          *rsa.PrivateKey
	Validation          Validation
	APISecret           *jwt.HMACSHA
	MaxSizeOfUploadFile int
	WebRedirect         string
	RateLimiter         *types.RateLimiter
	Client              *container.Client
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
		client:              opts.Client,
	}

	if hs.validation != nil {
		hs.validation.SetFunc(hs.FirstToken)
	}

	if err := hs.updateSchedulerPublicKey(); err != nil {
		log.Errorf("updateSchedulerPublicKey error %s", err.Error())
	}

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
