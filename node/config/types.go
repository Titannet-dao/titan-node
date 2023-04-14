package config

// // NOTE: ONLY PUT STRUCT DEFINITIONS IN THIS FILE
// //
// // After making edits here, run 'make cfgdoc-gen' (or 'make gen')

// EdgeCfg edge node config
type EdgeCfg struct {
	// host address and port the edge node api will listen on
	ListenAddress string
	// used when 'ListenAddress' is unspecified. must be a valid duration recognized by golang's time.ParseDuration function
	Timeout string
	// node id
	NodeID string
	// area id
	AreaID string
	// used auth when connect to scheduler
	Secret string
	// metadata path
	MetadataPath string
	// assets path
	AssetsPaths []string
	// upload file bandwidth, unit is B/s
	BandwidthUp int64
	// download file bandwidth, unit is B/s
	BandwidthDown int64
	// if true, get scheduler url from locator
	Locator bool
	// InsecureSkipVerify skip tls verify
	InsecureSkipVerify bool
	// used for http3 server
	// be used if InsecureSkipVerify is true
	CertificatePath string
	// used for http3 server
	// be used if InsecureSkipVerify is true
	PrivateKeyPath string
	// self sign certificate, use for client
	CaCertificatePath string
	// FetchTimeout get block timeout
	FetchBlockTimeout int
	// FetchBlockRetry retry when get block failed
	FetchBlockRetry int
	// FetchBatch the number of goroutine to fetch block
	FetchBatch int
}

// CandidateCfg candidate node config
type CandidateCfg struct {
	EdgeCfg
	TCPSrvAddr string
	IpfsAPIURL string
	// seconds
	ValidateDuration int
}

// LocatorCfg locator config
type LocatorCfg struct {
	// host address and port the edge node api will listen on
	ListenAddress string
	// used when 'ListenAddress' is unspecified. must be a valid duration recognized by golang's time.ParseDuration function
	Timeout string
	// geodb path
	GeoDBPath string
	// InsecureSkipVerify skip tls verify
	InsecureSkipVerify bool
	// used for http3 server
	// be used if InsecureSkipVerify is false
	CertificatePath string
	// used for http3 server
	// be used if InsecureSkipVerify is false
	PrivateKeyPath string
	// self sign certificate, use for client
	CaCertificatePath string
	// etcd server addresses
	EtcdAddresses []string
}

// SchedulerCfg scheduler config
type SchedulerCfg struct {
	// host external address and port
	ExternalURL string
	// host address and port the edge node api will listen on
	ListenAddress string
	// database address
	DatabaseAddress string
	// area id
	AreaID string
	// InsecureSkipVerify skip tls verify
	InsecureSkipVerify bool
	// used for http3 server
	// be used if InsecureSkipVerify is true
	CertificatePath string
	// used for http3 server
	// be used if InsecureSkipVerify is true
	PrivateKeyPath string
	// self sign certificate, use for client
	CaCertificatePath string
	// test nat type
	SchedulerServer1 string
	// test nat type
	SchedulerServer2 string
	// config to enabled node validation, default: true
	EnableValidation bool
	// etcd server addresses
	EtcdAddresses []string
	// Number of candidate node replicas (does not contain 'seed')
	CandidateReplicas int
	// Proportion of validator in candidate nodes (0 ~ 1)
	ValidatorRatio float64
	// The base downstream bandwidth per validator window (unit : MiB)
	ValidatorBaseBwDn int
}
