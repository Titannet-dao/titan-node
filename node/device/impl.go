package device

import (
	"context"
	"net"
	"strings"

	"github.com/shirou/gopsutil/v3/cpu"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/build"
	"github.com/Filecoin-Titan/titan/node/config"
	logging "github.com/ipfs/go-log/v2"
	"github.com/shirou/gopsutil/v3/mem"
)

var log = logging.Logger("device")

const (
	// 1MB/s
	bandwidthUnit = 1024 * 1024
	// 1GB/s
	storageUnit = 1024 * 1024 * 1024
)

// Device represents a device and its properties
type Device struct {
	nodeID     string
	publicIP   string
	internalIP string
	resources  *Resources
	storage    Storage
}

type Resources struct {
	// CPU Limit cpu usage
	CPU *config.CPU
	// Memory Limit memory usage
	Memory *config.Memory
	// Storage Limit storage usage
	Storage *config.Storage
	// Bandwidth Limit bandwidth usage
	Bandwidth *config.Bandwidth
}

// Storage represents a storage system and its properties.
type Storage interface {
	GetDiskUsageStat() (totalSpace, usage float64)
	GetFileSystemType() string
}

// NewDevice creates a new Device instance with the specified properties.
func NewDevice(nodeID, internalIP string, res *Resources, storage Storage) *Device {
	device := &Device{
		nodeID:     nodeID,
		internalIP: internalIP,
		resources:  res,
		storage:    storage,
	}

	if _, err := cpu.Percent(0, false); err != nil {
		log.Errorf("stat cpu percent error: %s", err.Error())
	}

	return device
}

// GetNodeInfo returns information about the device as a NodeInfo struct.
func (device *Device) GetNodeInfo(ctx context.Context) (types.NodeInfo, error) {
	info := types.NodeInfo{}

	v, err := api.VersionForType(types.RunningNodeType)
	if err != nil {
		return info, err
	}

	version := api.APIVersion{
		Version:    build.UserVersion(),
		APIVersion: v,
	}

	info.NodeID = device.nodeID

	name := device.nodeID
	if len(name) > 10 {
		info.NodeName = name[0:10]
	}
	info.ExternalIP = device.publicIP
	info.SystemVersion = version.String()
	info.InternalIP = device.internalIP

	if device.resources.Bandwidth != nil {
		info.BandwidthDown = device.resources.Bandwidth.BandwidthDown * bandwidthUnit
		info.BandwidthUp = device.resources.Bandwidth.BandwidthUp * bandwidthUnit
	}

	if device.resources.Storage != nil {
		info.AvailableDiskSpace = float64(device.resources.Storage.StorageGB) * storageUnit
	}

	mac, err := getMacAddr(info.InternalIP)
	if err != nil {
		log.Errorf("NodeInfo getMacAddr err:%s", err.Error())
		return types.NodeInfo{}, err
	}

	info.MacLocation = mac

	vmStat, err := mem.VirtualMemory()
	if err != nil {
		log.Errorf("getMemory: %s", err.Error())
	}

	if vmStat != nil {
		info.MemoryUsage = vmStat.UsedPercent
		info.Memory = float64(vmStat.Total)
	}

	if cpuInfos, err := cpu.Info(); err != nil {
		log.Errorf("get cpu info error %s", err.Error())
	} else if len(cpuInfos) > 0 {
		info.CPUInfo = cpuInfos[0].ModelName
	}

	cpuPercent, err := cpu.Percent(0, false)
	if err != nil {
		log.Errorf("getCpuInfo: %s", err.Error())
	}

	if len(cpuPercent) > 0 {
		info.CPUUsage = cpuPercent[0]
	} else {
		log.Warn("can not get cpu percent")
	}
	info.CPUCores, _ = cpu.Counts(false)
	info.DiskSpace, info.DiskUsage = device.storage.GetDiskUsageStat()
	info.IoSystem = device.storage.GetFileSystemType()
	return info, nil
}

// getMacAddr returns the MAC address associated with the specified IP address.
func getMacAddr(ip string) (string, error) {
	ifas, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, ifa := range ifas {
		addrs, err := ifa.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			localAddr := addr.(*net.IPNet)
			localIP := strings.Split(localAddr.IP.String(), ":")[0]

			if localIP == ip {
				return ifa.HardwareAddr.String(), nil
			}
		}
	}
	return "", nil
}

// GetBandwidthUp returns the bandwidth upload limit for the device.
func (device *Device) GetBandwidthUp() int64 {
	return device.resources.Bandwidth.BandwidthUp * bandwidthUnit
}

// GetBandwidthDown returns the bandwidth download limit for the device.
func (device *Device) GetBandwidthDown() int64 {
	return device.resources.Bandwidth.BandwidthDown * bandwidthUnit
}

// GetInternalIP returns the internal IP address for the device.
func (device *Device) GetInternalIP() string {
	return device.internalIP
}

// GetNodeID returns the ID of the device.
func (device *Device) GetNodeID(ctx context.Context) (string, error) {
	return device.nodeID, nil
}
