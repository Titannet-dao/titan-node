package node

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
)

// Node represents an Edge or Candidate node
type Node struct {
	NodeID string

	*API
	jsonrpc.ClientCloser

	token string

	lastRequestTime time.Time // Node last keepalive time

	selectWeights []int // The select weights assigned by the scheduler to each online node

	// node info
	PublicKey  *rsa.PublicKey
	RemoteAddr string
	TCPPort    int

	NATType     types.NatType
	CPUUsage    float64
	MemoryUsage float64
	DiskUsage   float64
	ExternalIP  string

	OnlineDuration int
	Type           types.NodeType
	PortMapping    string
	BandwidthDown  int64
	BandwidthUp    int64
	DiskSpace      float64

	DeactivateTime int64

	IsPrivateMinioOnly bool
}

// API represents the node API
type API struct {
	// common api
	api.Common
	api.Device
	api.Validation
	api.DataSync
	api.Asset
	WaitQuiet func(ctx context.Context) error
	// edge api
	ExternalServiceAddress func(ctx context.Context, candidateURL string) (string, error)
	UserNATPunch           func(ctx context.Context, sourceURL string, req *types.NatPunchReq) error
	// candidate api
	GetBlocksOfAsset         func(ctx context.Context, assetCID string, randomSeed int64, randomCount int) ([]string, error)
	CheckNetworkConnectivity func(ctx context.Context, network, targetURL string) error
	GetMinioConfig           func(ctx context.Context) (*types.MinioConfig, error)
}

// New creates a new node
func New() *Node {
	node := &Node{}

	return node
}

// APIFromEdge creates a new API from an Edge API
func APIFromEdge(api api.Edge) *API {
	a := &API{
		Common:                 api,
		Device:                 api,
		Validation:             api,
		DataSync:               api,
		Asset:                  api,
		WaitQuiet:              api.WaitQuiet,
		ExternalServiceAddress: api.ExternalServiceAddress,
		UserNATPunch:           api.UserNATPunch,
	}
	return a
}

// APIFromCandidate creates a new API from a Candidate API
func APIFromCandidate(api api.Candidate) *API {
	a := &API{
		Common:                   api,
		Device:                   api,
		Validation:               api,
		DataSync:                 api,
		Asset:                    api,
		WaitQuiet:                api.WaitQuiet,
		GetBlocksOfAsset:         api.GetBlocksWithAssetCID,
		CheckNetworkConnectivity: api.CheckNetworkConnectivity,
		GetMinioConfig:           api.GetMinioConfig,
	}
	return a
}

// ConnectRPC connects to the node RPC
func (n *Node) ConnectRPC(addr string, nodeType types.NodeType) error {
	rpcURL := fmt.Sprintf("https://%s/rpc/v0", addr)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+n.token)

	if nodeType == types.NodeEdge {
		// Connect to node
		edgeAPI, closer, err := client.NewEdge(context.Background(), rpcURL, headers)
		if err != nil {
			return xerrors.Errorf("NewEdge err:%s,url:%s", err.Error(), rpcURL)
		}

		n.API = APIFromEdge(edgeAPI)
		n.ClientCloser = closer
		return nil
	}

	if nodeType == types.NodeCandidate {
		// Connect to node
		candidateAPI, closer, err := client.NewCandidate(context.Background(), rpcURL, headers)
		if err != nil {
			return xerrors.Errorf("NewCandidate err:%s,url:%s", err.Error(), rpcURL)
		}

		n.API = APIFromCandidate(candidateAPI)
		n.ClientCloser = closer
		return nil
	}

	return xerrors.Errorf("node %s type %d not wrongful", n.NodeID, n.Type)
}

// IsAbnormal is node abnormal
func (n *Node) IsAbnormal() bool {
	if n.DeactivateTime > 0 {
		return true
	}

	return false
}

// SelectWeights get node select weights
func (n *Node) SelectWeights() []int {
	return n.selectWeights
}

// SetToken sets the token of the node
func (n *Node) SetToken(t string) {
	n.token = t
}

// TCPAddr returns the tcp address of the node
func (n *Node) TCPAddr() string {
	index := strings.Index(n.RemoteAddr, ":")
	ip := n.RemoteAddr[:index+1]
	return fmt.Sprintf("%s%d", ip, n.TCPPort)
}

// RPCURL returns the rpc url of the node
func (n *Node) RPCURL() string {
	return fmt.Sprintf("https://%s/rpc/v0", n.RemoteAddr)
}

// DownloadAddr returns the download address of the node
func (n *Node) DownloadAddr() string {
	addr := n.RemoteAddr
	if n.PortMapping != "" {
		index := strings.Index(n.RemoteAddr, ":")
		ip := n.RemoteAddr[:index+1]
		addr = ip + n.PortMapping
	}

	return addr
}

// LastRequestTime returns the last request time of the node
func (n *Node) LastRequestTime() time.Time {
	return n.lastRequestTime
}

// SetLastRequestTime sets the last request time of the node
func (n *Node) SetLastRequestTime(t time.Time) {
	n.lastRequestTime = t
}

// Token returns the token of the node
func (n *Node) Token(cid, clientID string, titanRsa *titanrsa.Rsa, privateKey *rsa.PrivateKey) (*types.Token, *types.TokenPayload, error) {
	tkPayload := &types.TokenPayload{
		ID:          uuid.NewString(),
		NodeID:      n.NodeID,
		AssetCID:    cid,
		ClientID:    clientID, // TODO auth client and allocate id
		CreatedTime: time.Now(),
		Expiration:  time.Now().Add(10 * time.Hour),
	}

	b, err := n.encryptTokenPayload(tkPayload, n.PublicKey, titanRsa)
	if err != nil {
		return nil, nil, xerrors.Errorf("%s encryptTokenPayload err:%s", n.NodeID, err.Error())
	}

	sign, err := titanRsa.Sign(privateKey, b)
	if err != nil {
		return nil, nil, xerrors.Errorf("%s Sign err:%s", n.NodeID, err.Error())
	}

	return &types.Token{ID: tkPayload.ID, CipherText: hex.EncodeToString(b), Sign: hex.EncodeToString(sign)}, tkPayload, nil
}

// encryptTokenPayload encrypts a token payload object using the given public key and RSA instance.
func (n *Node) encryptTokenPayload(tkPayload *types.TokenPayload, publicKey *rsa.PublicKey, rsa *titanrsa.Rsa) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(tkPayload)
	if err != nil {
		return nil, err
	}

	return rsa.Encrypt(buffer.Bytes(), publicKey)
}
