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
	*API
	jsonrpc.ClientCloser

	token string

	*types.NodeInfo
	publicKey  *rsa.PublicKey
	remoteAddr string
	tcpPort    int

	lastRequestTime time.Time // Node last keepalive time
	pullingCount    int       // The number of asset waiting and pulling in progress

	nodeNum int // The node number assigned by the scheduler to each online node
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
	ExternalServiceAddress func(ctx context.Context, schedulerURL string) (string, error)
	UserNATPunch           func(ctx context.Context, sourceURL string, req *types.NatPunchReq) error
	// candidate api
	GetBlocksOfAsset func(ctx context.Context, assetCID string, randomSeed int64, randomCount int) (map[int]string, error)
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
		Common:           api,
		Device:           api,
		Validation:       api,
		DataSync:         api,
		Asset:            api,
		WaitQuiet:        api.WaitQuiet,
		GetBlocksOfAsset: api.GetBlocksWithAssetCID,
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

// SetToken sets the token of the node
func (n *Node) SetToken(t string) {
	n.token = t
}

// PublicKey  returns the publicKey of the node
func (n *Node) PublicKey() *rsa.PublicKey {
	return n.publicKey
}

// SetPublicKey sets the publicKey of the node
func (n *Node) SetPublicKey(pKey *rsa.PublicKey) {
	n.publicKey = pKey
}

// RemoteAddr returns the rpc address of the node
func (n *Node) RemoteAddr() string {
	return n.remoteAddr
}

// SetRemoteAddr sets the remoteAddr of the node
func (n *Node) SetRemoteAddr(addr string) {
	n.remoteAddr = addr
}

// SetTCPPort sets the tcpPort of the node
func (n *Node) SetTCPPort(port int) {
	n.tcpPort = port
}

// TCPAddr returns the tcp address of the node
func (n *Node) TCPAddr() string {
	index := strings.Index(n.remoteAddr, ":")
	ip := n.remoteAddr[:index+1]
	return fmt.Sprintf("%s%d", ip, n.tcpPort)
}

// RPCURL returns the rpc url of the node
func (n *Node) RPCURL() string {
	return fmt.Sprintf("https://%s/rpc/v0", n.remoteAddr)
}

// DownloadAddr returns the download address of the node
func (n *Node) DownloadAddr() string {
	addr := n.remoteAddr
	if n.PortMapping != "" {
		index := strings.Index(n.remoteAddr, ":")
		ip := n.remoteAddr[:index+1]
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

// SetCurPullingCount sets the number of assets being pulled by the node
func (n *Node) SetCurPullingCount(t int) {
	n.pullingCount = t
}

// IncrCurPullingCount  increments the number of assets being pulled by the node
func (n *Node) IncrCurPullingCount(v int) {
	n.pullingCount += v
}

// CurPullingCount returns the number of assets being pulled by the node
func (n *Node) CurPullingCount() int {
	return n.pullingCount
}

// UpdateNodePort updates the node port
func (n *Node) UpdateNodePort(port string) {
	n.PortMapping = port
}

// Credentials returns the credentials of the node
func (n *Node) Credentials(cid string, titanRsa *titanrsa.Rsa, privateKey *rsa.PrivateKey) (*types.GatewayCredentials, error) {
	svc := &types.Credentials{
		ID:        uuid.NewString(),
		NodeID:    n.NodeID,
		AssetCID:  cid,
		ValidTime: time.Now().Add(10 * time.Hour).Unix(),
	}

	b, err := n.encryptCredentials(svc, n.publicKey, titanRsa)
	if err != nil {
		return nil, xerrors.Errorf("%s encryptCredentials err:%s", n.NodeID, err.Error())
	}

	sign, err := titanRsa.Sign(privateKey, b)
	if err != nil {
		return nil, xerrors.Errorf("%s Sign err:%s", n.NodeID, err.Error())
	}

	return &types.GatewayCredentials{Ciphertext: hex.EncodeToString(b), Sign: hex.EncodeToString(sign)}, nil
}

// encryptCredentials encrypts a Credentials object using the given public key and RSA instance.
func (n *Node) encryptCredentials(at *types.Credentials, publicKey *rsa.PublicKey, rsa *titanrsa.Rsa) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(at)
	if err != nil {
		return nil, err
	}

	return rsa.Encrypt(buffer.Bytes(), publicKey)
}
