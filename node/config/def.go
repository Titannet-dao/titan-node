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
		Network: Network{
			ListenAddress: "0.0.0.0:1234",
			Timeout:       "30s",
			LocatorURL:    "https://localhost:5000/rpc/v0",
		},

		CertificatePath:    "",
		PrivateKeyPath:     "",
		CaCertificatePath:  "",
		InsecureSkipVerify: true,

		Puller: Puller{
			PullBlockTimeout:  180,
			PullBlockRetry:    5,
			PullBlockParallel: 5,
		},

		Storage: Storage{
			StorageGB: 2,
		},
		Memory: Memory{
			MemoryGB: 1,
		},
		Bandwidth: Bandwidth{
			// unlimited
			BandwidthMB: 0,
			// unlimited
			BandwidthUp: 0,
			// unlimited
			BandwidthDown: 0,
		},
		Netflow: Netflow{
			// unlimited
			NetflowUp: 0,
			// unlimited
			NetflowDown: 0,
		},
		CPU: CPU{
			Cores: 1,
		},
	}
}

// DefaultCandidateCfg returns the default candidate config
func DefaultCandidateCfg() *CandidateCfg {
	edgeCfg := EdgeCfg{
		Network: Network{
			ListenAddress: "0.0.0.0:2345",
			Timeout:       "30s",
			LocatorURL:    "https://localhost:5000/rpc/v0",
		},

		InsecureSkipVerify: true,
		CertificatePath:    "",
		PrivateKeyPath:     "",
		CaCertificatePath:  "",

		TCPSrvAddr:          "0.0.0.0:9000",
		IPFSAPIURL:          "http://127.0.0.1:5001",
		ValidateDuration:    10,
		MaxSizeOfUploadFile: 104857600, // 100 MB

		Puller: Puller{
			PullBlockTimeout:  180,
			PullBlockRetry:    5,
			PullBlockParallel: 5,
		},

		Storage: Storage{
			StorageGB: 64,
			Path:      "./",
		},
		Memory: Memory{
			// 1GB
			MemoryGB: 1,
		},
		Bandwidth: Bandwidth{
			// unlimited
			BandwidthMB: 0,
			// unlimited
			BandwidthUp: 0,
			// unlimited
			BandwidthDown: 0,
		},
		Netflow: Netflow{
			// unlimited
			NetflowUp: 0,
			// unlimited
			NetflowDown: 0,
		},
		CPU: CPU{
			Cores: 1,
		},
	}
	return &CandidateCfg{
		EdgeCfg:      edgeCfg,
		MetadataPath: "",
		AssetsPaths:  []string{},
		WebRedirect:  "https://storage.titannet.io/#/redirect",
		IsPrivate:    false,
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
		DNSServerAddress:   "0.0.0.0:53",
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
		GeoDBPath:               "./city.mmdb",
		AreaID:                  "Asia-China-Guangdong-Shenzhen",
		DatabaseAddress:         "mysql_user:mysql_password@tcp(127.0.0.1:3306)/titan",
		EnableValidation:        true,
		EtcdAddresses:           []string{},
		CandidateReplicas:       1,
		ValidatorRatio:          1,
		ValidatorBaseBwDn:       100,
		ValidationProfit:        0,
		WorkloadProfit:          0,
		ElectionCycle:           1,
		LotusRPCAddress:         "http://api.node.glif.io/rpc/v0",
		LotusToken:              "",
		EdgeDownloadRatio:       0.7,
		AssetPullTaskLimit:      10,
		UploadAssetReplicaCount: 20,
		UploadAssetExpiration:   150,
		IPLimit:                 5,
		FillAssetEdgeCount:      500,
		L2ValidatorCount:        0,
		StorageCandidates:       []string{},
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
		// allocate 100M for user
		UserFreeStorageSize:      104857600,
		UserVipStorageSize:       5368709120,
		MaxCountOfVisitShareLink: 10,
		Weight:                   100,
		MaxAPIKey:                5,
		// Maximum number of node registrations for the same IP on the same day
		MaxNumberOfRegistrations: 50,
		AndroidSymbol:            "+android-",
		IOSSymbol:                "+ios-",
		WindowsSymbol:            "+windows-",
		MacosSymbol:              "+macos-",
	}
}

var (
	_ encoding.TextMarshaler   = (*Duration)(nil)
	_ encoding.TextUnmarshaler = (*Duration)(nil)
)

// Duration is a cgo type for time.Duration
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
