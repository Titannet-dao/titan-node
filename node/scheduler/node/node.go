package node

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/google/uuid"
	"github.com/quic-go/quic-go"
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
	PublicKey   *rsa.PublicKey
	RemoteAddr  string
	TCPPort     int
	ExternalURL string

	NATType        types.NatType
	CPUUsage       float64
	MemoryUsage    float64
	DiskUsage      float64
	TitanDiskUsage float64

	OnlineDuration int
	Type           types.NodeType
	PortMapping    string
	BandwidthDown  int64
	BandwidthUp    int64

	DeactivateTime int64

	IsPrivateMinioOnly bool

	ExternalIP         string
	IncomeIncr         float64
	DiskSpace          float64
	AvailableDiskSpace float64
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
func (n *Node) ConnectRPC(transport *quic.Transport, addr string, nodeType types.NodeType) error {
	httpClient, err := client.NewHTTP3ClientWithPacketConn(transport)
	if err != nil {
		return err
	}

	rpcURL := fmt.Sprintf("https://%s/rpc/v0", addr)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+n.token)

	if nodeType == types.NodeEdge {
		// Connect to node
		edgeAPI, closer, err := client.NewEdge(context.Background(), rpcURL, headers, jsonrpc.WithHTTPClient(httpClient))
		if err != nil {
			return xerrors.Errorf("NewEdge err:%s,url:%s", err.Error(), rpcURL)
		}

		n.API = APIFromEdge(edgeAPI)
		n.ClientCloser = closer
		return nil
	}

	if nodeType == types.NodeCandidate {
		// Connect to node
		candidateAPI, closer, err := client.NewCandidate(context.Background(), rpcURL, headers, jsonrpc.WithHTTPClient(httpClient))
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
	// waiting for deactivate
	if n.DeactivateTime > 0 {
		return true
	}

	// is minio node
	if n.IsPrivateMinioOnly {
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

// GetToken get the token of the node
func (n *Node) GetToken() string {
	return n.token
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

// CalculateIncome Calculate income of the node
func (n *Node) CalculateIncome(nodeCount int) float64 {
	mb := n.calculateMb()
	mn := n.calculateMN()
	mx := weighting(nodeCount)
	mbn := mb * mn * mx

	ds := float64(n.TitanDiskUsage)
	s := bToGB(ds * 12.5)

	ms := mx * min(s, 2000) * (0.1 + float64(1/max(min(s, 2000), 10)))

	poa := mbn + ms
	poa = math.Round(poa*1000000) / 1000000
	log.Debugf("calculatePoints [%s] BandwidthUp:[%d] NAT:[%d:%.2f] DiskSpace:[%.2f*12.5=%.2f GB] poa:[%.4f] mbn:[%.4f] ms:[%.4f] mx:[%.1f]", n.NodeID, n.BandwidthUp, n.NATType, mn, n.TitanDiskUsage, s, poa, mbn, ms, mx)

	return poa
}

func bToGB(b float64) float64 {
	return b / 1024 / 1024 / 1024
}

func bToMB(b float64) float64 {
	return b / 1024 / 1024
}

func (n *Node) calculateMb() float64 {
	mb := 0.0
	b := bToMB(float64(n.BandwidthUp))
	if b <= 5 {
		mb = 0.05 * b
	} else if b <= 50 {
		mb = 0.25 + 0.8*(b-5)
	} else if b <= 200 {
		mb = 36.25 + 0.2*(b-50)
	} else {
		mb = 66.25
	}

	return mb
}

func (n *Node) calculateMN() float64 {
	switch n.NATType {
	case types.NatTypeNo:
		return 1.5
	case types.NatTypeFullCone:
		return 1.4
	case types.NatTypeRestricted:
		return 1.2
	case types.NatTypePortRestricted:
		return 1.1
	case types.NatTypeSymmetric:
		return 1
	}

	return 0
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}

	return b
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}

	return b
}

func weighting(num int) float64 {
	if num <= 2000 {
		return 1.7
	} else if num <= 5000 {
		return 1.6
	} else if num <= 10000 {
		return 1.5
	} else if num <= 15000 {
		return 1.4
	} else if num <= 25000 {
		return 1.3
	} else if num <= 35000 {
		return 1.2
	} else if num <= 50000 {
		return 1.1
	} else {
		return 1
	}
}

// Increase every 5 seconds
func (n *Node) CalculateMCx(count int) float64 {
	return 0.00289 * weighting(count)
	// mMbw := bToGB(float64(n.BandwidthUp))
	// mMsw := bToGB(n.DiskUsage * n.Info.DiskSpace)
	// if mMsw > 1 {
	// 	mMsw = 1
	// }

	// mMcx := min(mMbw, mMsw) * 0.004
	// mMcx = math.Round(mMcx*1000000) / 1000000

	// log.Debugf("CalculateMCx [%s] bandwidth:[%d] DiskUsage:[%v] DiskSpace:[%v] mMbw:[%v] mMsw:[%v] mMcx:[%v] ", n.NodeID, n.BandwidthUp, n.DiskUsage, n.Info.DiskSpace, mMbw, mMsw, mMcx)

	// return mMcx
}

func (n *Node) DiskEnough(size float64) bool {
	residual := ((100 - n.DiskUsage) / 100) * n.DiskSpace
	if residual <= size {
		log.Debugf("node %s disk residual n.DiskUsage:%.2f, n.DiskSpace:%.2f", n.NodeID, n.DiskUsage, n.DiskSpace)
		return false
	}

	residual = n.AvailableDiskSpace - n.TitanDiskUsage
	if residual <= size {
		log.Debugf("node %s disk residual AvailableDiskSpace:%.2f, TitanDiskUsage:%.2f", n.NodeID, n.AvailableDiskSpace, n.TitanDiskUsage)
		return false
	}

	return true
}
