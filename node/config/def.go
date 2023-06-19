package config

import (
	"encoding"
	"os"
	"strconv"
	"time"
)

const (
	// RetrievalPricingDefault configures the node to use the default retrieval pricing policy.
	RetrievalPricingDefaultMode = "default"
	// RetrievalPricingExternal configures the node to use the external retrieval pricing script
	// configured by the user.
	RetrievalPricingExternalMode = "external"
)

// MaxTraversalLinks configures the maximum number of links to traverse in a DAG while calculating
// CommP and traversing a DAG with graphsync; invokes a budget on DAG depth and density.
var MaxTraversalLinks uint64 = 32 * (1 << 20)

func init() {
	if envMaxTraversal, err := strconv.ParseUint(os.Getenv("TITAN_MAX_TRAVERSAL_LINKS"), 10, 64); err == nil {
		MaxTraversalLinks = envMaxTraversal
	}
}

// DefaultEdgeCfg returns the default edge config
func DefaultEdgeCfg() *EdgeCfg {
	return &EdgeCfg{
		ListenAddress: "0.0.0.0:1234",
		Timeout:       "30s",
		MetadataPath:  "",
		AssetsPaths:   []string{},
		BandwidthUp:   104857600,
		BandwidthDown: 1073741824,
		Locator:       true,

		CertificatePath:    "",
		PrivateKeyPath:     "",
		CaCertificatePath:  "",
		InsecureSkipVerify: true,

		PullBlockTimeout:  15,
		PullBlockRetry:    1,
		PullBlockParallel: 5,
	}
}

// DefaultCandidateCfg returns the default candidate config
func DefaultCandidateCfg() *CandidateCfg {
	edgeCfg := EdgeCfg{
		ListenAddress: "0.0.0.0:2345",
		Timeout:       "30s",
		MetadataPath:  "",
		AssetsPaths:   []string{},
		BandwidthUp:   1073741824,
		BandwidthDown: 1073741824,
		Locator:       true,

		InsecureSkipVerify: true,
		CertificatePath:    "",
		PrivateKeyPath:     "",
		CaCertificatePath:  "",

		PullBlockTimeout:  15,
		PullBlockRetry:    1,
		PullBlockParallel: 5,
		TCPSrvAddr:        "0.0.0.0:9000",
		IPFSAPIURL:        "http://127.0.0.1:5001",
		ValidateDuration:  10,
	}
	return &CandidateCfg{
		EdgeCfg: edgeCfg,
	}
}

// DefaultLocatorCfg returns the default locator config
func DefaultLocatorCfg() *LocatorCfg {
	return &LocatorCfg{
		ListenAddress:      "0.0.0.0:5000",
		Timeout:            "3s",
		GeoDBPath:          "./city.mmdb",
		InsecureSkipVerify: true,
		CertificatePath:    "",
		PrivateKeyPath:     "",
		CaCertificatePath:  "",
		EtcdAddresses:      []string{"127.0.0.1:2379"},
		DefaultAreaID:      "Asia-China-Guangdong-Shenzhen",
		DNSServerAddress:   "0.0.0.0:5555",
	}
}

// DefaultSchedulerCfg returns the default scheduler config
func DefaultSchedulerCfg() *SchedulerCfg {
	return &SchedulerCfg{
		ExternalURL:             "https://localhost:3456/rpc/v0",
		ListenAddress:           "0.0.0.0:3456",
		InsecureSkipVerify:      true,
		CertificatePath:         "",
		PrivateKeyPath:          "",
		CaCertificatePath:       "",
		AreaID:                  "Asia-China-Guangdong-Shenzhen",
		DatabaseAddress:         "mysql_user:mysql_password@tcp(127.0.0.1:3306)/titan",
		EnableValidation:        true,
		EtcdAddresses:           []string{},
		CandidateReplicas:       0,
		ValidatorRatio:          1,
		ValidatorBaseBwDn:       100,
		ValidationProfit:        1,
		WorkloadProfit:          5,
		ElectionCycle:           5,
		LotusRPCAddress:         "http://api.node.glif.io/rpc/v0",
		LotusToken:              "",
		EdgeDownloadRatio:       0.7,
		AssetPullTaskLimit:      10,
		UploadAssetReplicaCount: 50,
		UploadAssetExpiration:   150,
		NodeScoreLevel: map[string][]int{
			"A": {90, 100},
			"B": {50, 89},
			"C": {0, 49},
		},
		LevelSelectWeight: map[string]int{
			"A": 3,
			"B": 2,
			"C": 1,
		},
		NatDetectConcurrency: 5,
		// allocate 200M for user
		UserFreeStorageSize: 209715200,
	}
}

var (
	_ encoding.TextMarshaler   = (*Duration)(nil)
	_ encoding.TextUnmarshaler = (*Duration)(nil)
)

// Duration is a wrapper type for time.Duration
// for decoding and encoding from/to TOML
type Duration time.Duration

// UnmarshalText implements interface for TOML decoding
func (dur *Duration) UnmarshalText(text []byte) error {
	d, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*dur = Duration(d)
	return err
}

// MarshalText implements interface for TOML encoding
func (dur Duration) MarshalText() ([]byte, error) {
	d := time.Duration(dur)
	return []byte(d.String()), nil
}
