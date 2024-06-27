package config

// // NOTE: ONLY PUT STRUCT DEFINITIONS IN THIS FILE
// //
// // After making edits here, run 'make cfgdoc-gen' (or 'make gen')

type Network struct {
	// host address and port the edge node api will listen on
	ListenAddress string
	// used when 'ListenAddress' is unspecified. must be a valid duration recognized by golang's time.ParseDuration function
	Timeout string
	// the url of locator
	LocatorURL string
}

type Storage struct {
	StorageGB int64
	Path      string
}

type Bandwidth struct {
	// unit is MiB/s, 0 means no limit
	BandwidthMB int64
	// upload file bandwidth, unit is MiB/s, 0 means no limit
	BandwidthUp int64
	// download file bandwidth, unit is MiB/s, 0 means no limit
	BandwidthDown int64
}

type Netflow struct {
	// upload network flow limit, unit is GB, 0 means no limit
	NetflowUp int64
	// download network flow limit, unit is GB, 0 means no limit
	NetflowDown int64
}

type Memory struct {
	MemoryGB int64
}

type CPU struct {
	Cores int
}

type Basic struct {
	Token string
}

type Puller struct {
	// PullBlockTimeout get block timeout
	PullBlockTimeout int
	// PullBlockRetry retry when get block failed
	PullBlockRetry int
	// PullBlockParallel the number of goroutine to pull block
	PullBlockParallel int
}

// EdgeCfg edge node config
type EdgeCfg struct {
	Network Network
	// area id
	AreaID string
	// used auth when connect to scheduler
	Secret string
	// InsecureSkipVerify http3 client skip tls verify
	InsecureSkipVerify bool
	// used for http3 server
	// be used if InsecureSkipVerify is true
	CertificatePath string
	// used for http3 server
	// be used if InsecureSkipVerify is true
	PrivateKeyPath string
	// self sign certificate, use for client
	CaCertificatePath string
	TCPSrvAddr        string
	IPFSAPIURL        string
	// seconds
	ValidateDuration    int
	MaxSizeOfUploadFile int

	Puller Puller

	Bandwidth Bandwidth
	Netflow   Netflow
	Storage   Storage
	Memory    Memory
	CPU       CPU
	Basic     Basic
}

type MinioConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
}

// CandidateCfg candidate node config
type CandidateCfg struct {
	EdgeCfg
	// metadata path
	MetadataPath string
	// assets path
	AssetsPaths []string
	MinioConfig
	WebRedirect string
	ExternalURL string
	// Let the scheduler know that this node does not do tasks
	IsPrivate bool
}

// LocatorCfg locator config
type LocatorCfg struct {
	// host address and port the edge node api will listen on
	ListenAddress string
	// used when 'ListenAddress' is unspecified. must be a valid duration recognized by golang's time.ParseDuration function
	Timeout string
	// geodb path
	GeoDBPath string
	// InsecureSkipVerify http3 client skip tls verify
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
	EtcdAddresses    []string
	DNSServerAddress string
	DNSRecords       map[string]string
	DefaultAreas     []string
}

// SchedulerCfg scheduler config
type SchedulerCfg struct {
	// host external address and port
	ExternalURL string
	// host address and port the edge node api will listen on
	ListenAddress string
	// database address
	DatabaseAddress string
	// geodb path
	GeoDBPath string
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
	// Increased profit after node validation passes
	ValidationProfit float64
	// Increased profit after node workload passes
	WorkloadProfit float64
	// ElectionCycle cycle (Unit:Day)
	ElectionCycle int
	// Node score level scale
	// The key of map is the rank name, and the value of map is a int array containing two elements,
	// the first element of which is the minimum value of score,
	// and the second element is the maximum value of score. (scores out of 100)
	NodeScoreLevel map[string][]int
	// Node level weight
	// The key of the map is the name of the level, and the value of the map is an int,
	// indicating how many select weight this level can get (the more select weight, the greater the probability of the node being selected)
	LevelSelectWeight map[string]int

	UserFreeStorageSize int64
	UserVipStorageSize  int64

	LotusRPCAddress string
	LotusToken      string

	// The ratio of edge nodes returned to the user for download
	EdgeDownloadRatio float64
	// Maximum number of concurrent asset pulls
	AssetPullTaskLimit int

	NatDetectConcurrency int
	// Default number of backups for user uploaded files
	UploadAssetReplicaCount int
	// Default expiration time for user uploaded files
	UploadAssetExpiration int // (Unit:day)
	// Non vip user
	MaxCountOfVisitShareLink int
	// if the area has several scheduler, node will connect to the scheduler which weight is bigger
	Weight                   int
	MaxAPIKey                int
	IPWhitelist              []string
	MaxNumberOfRegistrations int

	IPLimit            int
	FillAssetEdgeCount int64

	StorageCandidates []string

	L2ValidatorCount int

	AndroidSymbol string
	IOSSymbol     string
	WindowsSymbol string
	MacosSymbol   string
}
