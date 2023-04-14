package device

import (
	"context"
	"net"
	"strings"

	"github.com/shirou/gopsutil/v3/cpu"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/build"
	logging "github.com/ipfs/go-log/v2"
	"github.com/shirou/gopsutil/v3/mem"
)

var log = logging.Logger("device")

// Device represents a device and its properties
type Device struct {
	nodeID        string
	publicIP      string
	internalIP    string
	bandwidthUp   int64
	bandwidthDown int64
	storage       Storage
}

// Storage represents a storage system and its properties.
type Storage interface {
	GetDiskUsageStat() (totalSpace, usage float64)
	GetFileSystemType() string
}

// NewDevice creates a new Device instance with the specified properties.
func NewDevice(nodeID, internalIP string, bandwidthUp, bandwidthDown int64, storage Storage) *Device {
	device := &Device{
		nodeID:        nodeID,
		internalIP:    internalIP,
		bandwidthUp:   bandwidthUp,
		bandwidthDown: bandwidthDown,
		storage:       storage,
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
	info.BandwidthDown = float64(device.bandwidthDown)
	info.BandwidthUp = float64(device.bandwidthUp)

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

	cpuPercent, err := cpu.Percent(0, false)
	if err != nil {
		log.Errorf("getCpuInfo: %s", err.Error())
	}

	info.CPUUsage = cpuPercent[0]
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

// SetBandwidthUp sets the bandwidth upload limit for the device.
func (device *Device) SetBandwidthUp(bandwidthUp int64) {
	device.bandwidthUp = bandwidthUp
}

// GetBandwidthUp returns the bandwidth upload limit for the device.
func (device *Device) GetBandwidthUp() int64 {
	return device.bandwidthUp
}

// GetBandwidthDown returns the bandwidth download limit for the device.
func (device *Device) GetBandwidthDown() int64 {
	return device.bandwidthDown
}

// GetInternalIP returns the internal IP address for the device.
func (device *Device) GetInternalIP() string {
	return device.internalIP
}

// GetNodeID returns the ID of the device.
func (device *Device) GetNodeID(ctx context.Context) (string, error) {
	return device.nodeID, nil
}
