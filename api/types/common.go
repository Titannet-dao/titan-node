package types

import (
	"time"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
)

type OpenRPCDocument map[string]interface{}

type Base struct {
	ID        uint      `gorm:"primarykey"`
	CreatedAt time.Time `json:"created_at" gorm:"comment:'创建时间';type:timestamp;"`
	UpdatedAt time.Time `json:"updated_at" gorm:"comment:'更新时间';type:timestamp;"`
}

// NodeInfo contains information about a node.
type NodeInfo struct {
	Base
	NodeID       string   `json:"node_id" form:"nodeId" gorm:"column:node_id;comment:;" db:"node_id"`
	SerialNumber string   `json:"serial_number" form:"serialNumber" gorm:"column:serial_number;comment:;"`
	Type         NodeType `json:"type"`
	ExternalIP   string   `json:"external_ip" form:"externalIp" gorm:"column:external_ip;comment:;"`
	InternalIP   string   `json:"internal_ip" form:"internalIp" gorm:"column:internal_ip;comment:;"`
	IPLocation   string   `json:"ip_location" form:"ipLocation" gorm:"column:ip_location;comment:;"`
	PkgLossRatio float64  `json:"pkg_loss_ratio" form:"pkgLossRatio" gorm:"column:pkg_loss_ratio;comment:;"`
	Latency      float64  `json:"latency" form:"latency" gorm:"column:latency;comment:;"`
	CPUUsage     float64  `json:"cpu_usage" form:"cpuUsage" gorm:"column:cpu_usage;comment:;"`
	MemoryUsage  float64  `json:"memory_usage" form:"memoryUsage" gorm:"column:memory_usage;comment:;"`
	IsOnline     bool     `json:"is_online" form:"isOnline" gorm:"column:is_online;comment:;"`

	DiskUsage       float64         `json:"disk_usage" form:"diskUsage" gorm:"column:disk_usage;comment:;" db:"disk_usage"`
	Blocks          int             `json:"blocks" form:"blockCount" gorm:"column:blocks;comment:;" db:"blocks"`
	BandwidthUp     float64         `json:"bandwidth_up" db:"bandwidth_up"`
	BandwidthDown   float64         `json:"bandwidth_down" db:"bandwidth_down"`
	NATType         string          `json:"nat_type" form:"natType" gorm:"column:nat_type;comment:;" db:"nat_type"`
	DiskSpace       float64         `json:"disk_space" form:"diskSpace" gorm:"column:disk_space;comment:;" db:"disk_space"`
	SystemVersion   string          `json:"system_version" form:"systemVersion" gorm:"column:system_version;comment:;" db:"system_version"`
	DiskType        string          `json:"disk_type" form:"diskType" gorm:"column:disk_type;comment:;" db:"disk_type"`
	IoSystem        string          `json:"io_system" form:"ioSystem" gorm:"column:io_system;comment:;" db:"io_system"`
	Latitude        float64         `json:"latitude" db:"latitude"`
	Longitude       float64         `json:"longitude" db:"longitude"`
	NodeName        string          `json:"node_name" form:"nodeName" gorm:"column:node_name;comment:;" db:"node_name"`
	Memory          float64         `json:"memory" form:"memory" gorm:"column:memory;comment:;" db:"memory"`
	CPUCores        int             `json:"cpu_cores" form:"cpuCores" gorm:"column:cpu_cores;comment:;" db:"cpu_cores"`
	ProductType     string          `json:"product_type" form:"productType" gorm:"column:product_type;comment:;" db:"product_type"`
	MacLocation     string          `json:"mac_location" form:"macLocation" gorm:"column:mac_location;comment:;" db:"mac_location"`
	OnlineDuration  int             `json:"online_duration" form:"onlineDuration" db:"online_duration"`
	Profit          float64         `json:"profit" db:"profit"`
	DownloadTraffic float64         `json:"download_traffic" db:"download_traffic"`
	UploadTraffic   float64         `json:"upload_traffic" db:"upload_traffic"`
	DownloadBlocks  int             `json:"download_blocks" form:"downloadCount" gorm:"column:download_blocks;comment:;" db:"download_blocks"`
	PortMapping     string          `db:"port_mapping"`
	LastSeen        time.Time       `db:"last_seen"`
	IsQuitted       bool            `db:"quitted"`
	SchedulerID     dtypes.ServerID `db:"scheduler_sid"`
}

// NodeType node type
type NodeType int

const (
	NodeUnknown NodeType = iota

	NodeEdge
	NodeCandidate
	NodeValidator
	NodeScheduler
	NodeLocator
	NodeUpdater
)

func (n NodeType) String() string {
	switch n {
	case NodeEdge:
		return "edge"
	case NodeCandidate:
		return "candidate"
	case NodeScheduler:
		return "scheduler"
	case NodeValidator:
		return "validator"
	case NodeLocator:
		return "locator"
	}

	return ""
}

// RunningNodeType represents the type of the running node.
var RunningNodeType NodeType

// EventTopics represents topics for pub/sub events
type EventTopics string

const (
	// EventNodeOnline node online event
	EventNodeOnline EventTopics = "node_online"
	// EventNodeOffline node offline event
	EventNodeOffline EventTopics = "node_offline"
)

func (t EventTopics) String() string {
	return string(t)
}
