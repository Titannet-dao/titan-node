package device

import (
	"context"
	"errors"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/build"
	"github.com/Filecoin-Titan/titan/node/config"
	logging "github.com/ipfs/go-log/v2"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

var log = logging.Logger("device")

const (
	// 1MiB/s
	BandwidthUnit = 1024 * 1024
	// 1GB
	StorageUnit = 1024 * 1024 * 1024
	// 1GB
	NetFlowUnit = 1024 * 1024 * 1024

	// NicSampleInterval is the interval to sample network interface card and process bandwidth usage.
	NicSampleInterval = 10 * time.Second
)

// Device represents a device and its properties
type Device struct {
	nodeID      string
	publicIP    string
	internalIP  string
	resources   *Resources
	storage     Storage
	runningStat *types.DeviceRunningStat
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
	// Netflow Limit network-flow usage
	Netflow *config.Netflow
}

// Storage represents a storage system and its properties.
type Storage interface {
	GetDiskUsageStat() (totalSpace, usage float64)
	GetFileSystemType() string
}

// NewDevice creates a new Device instance with the specified properties.
func NewDevice(nodeID, internalIP string, res *Resources, storage Storage) *Device {
	device := &Device{
		nodeID:      nodeID,
		internalIP:  internalIP,
		resources:   res,
		storage:     storage,
		runningStat: &types.DeviceRunningStat{},
	}

	if _, err := cpu.Percent(0, false); err != nil {
		log.Errorf("stat cpu percent error: %s", err.Error())
	}

	go setRunningStat(context.TODO(), device.runningStat)

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
		info.BandwidthDown = device.resources.Bandwidth.BandwidthDown * BandwidthUnit
		info.BandwidthUp = device.resources.Bandwidth.BandwidthUp * BandwidthUnit
	}

	if device.resources.Storage != nil {
		info.AvailableDiskSpace = float64(device.resources.Storage.StorageGB) * StorageUnit
	}

	if device.resources.Netflow != nil {
		// info.NetflowTotal = device.resources.Netflow.Total
		info.NetFlowUp = device.resources.Netflow.NetflowUp * NetFlowUnit
		info.NetFlowDown = device.resources.Netflow.NetflowDown * NetFlowUnit
	}

	// mac, err := getMacAddr(info.InternalIP)
	// if err != nil {
	// 	log.Errorf("NodeInfo getMacAddr err:%s", err.Error())
	// 	return types.NodeInfo{}, err
	// }

	// info.MacLocation = mac

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
	info.IoSystem = runtime.GOOS
	info.DiskType = device.storage.GetFileSystemType()

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
	return device.resources.Bandwidth.BandwidthUp * BandwidthUnit
}

// GetBandwidthDown returns the bandwidth download limit for the device.
func (device *Device) GetBandwidthDown() int64 {
	return device.resources.Bandwidth.BandwidthDown * BandwidthUnit
}

// GetInternalIP returns the internal IP address for the device.
func (device *Device) GetInternalIP() string {
	return device.internalIP
}

// GetNodeID returns the ID of the device.
func (device *Device) GetNodeID(ctx context.Context) (string, error) {
	return device.nodeID, nil
}

func (device *Device) GetDeviceRunningStat(ctx context.Context) (*types.DeviceRunningStat, error) {
	return device.runningStat, nil
}

func setRunningStat(ctx context.Context, t *types.DeviceRunningStat) {
	// Create a ticker for periodic sampling
	ticker := time.NewTicker(NicSampleInterval)
	defer ticker.Stop()

	if runtime.GOOS != "linux" {
		log.Infof("Running Stat Unsupported OS: %s", runtime.GOOS)
		return
	}

	prevTxrxStas := &txrxStas{}
	for {
		select {
		case <-ctx.Done():
			log.Errorln("Bandwidth sampling stopped.")
			return
		case <-ticker.C:
			t.LastSampleTime = time.Now()
			setNicAndProcessBandwidth(ctx, t, prevTxrxStas)
			setMemoryUsage(ctx, t)
			setCPUUsage(ctx, t)
		}
	}
}

type txrxStas struct {
	// prevProcessTx int64
	// prevProcessRx int64
	prevNicTx int64
	prevNicRx int64
}

func setNicAndProcessBandwidth(ctx context.Context, t *types.DeviceRunningStat, tr *txrxStas) {
	// Get current process bandwidth
	// TODO current process bandwidth is unreachable in linux
	// processTx, processRx, err := getBandwidth(fmt.Sprintf("/proc/%d/net/dev", os.Getpid()))
	// if err != nil && errors.Is(err, os.ErrNotExist) {
	// 	log.Errorln("os not support get process bandwidth.")
	// 	return
	// }
	// if err != nil {
	// 	log.Errorln("Error fetching process bandwidth:", err)
	// }

	// Get current NIC bandwidth
	nicTx, nicRx, err := getBandwidth("/proc/net/dev")
	if err != nil && errors.Is(err, os.ErrNotExist) {
		log.Errorln("os not support get nic bandwidth.")
		return
	}
	if err != nil {
		log.Errorln("Error fetching NIC bandwidth:", err)
	}

	// Calculate deltas if we have previous samples
	if !t.LastSampleTime.IsZero() {
		// t.ProcessBandWidthUp = (processTx - tr.prevProcessTx) / int64(NicSampleInterval.Seconds())
		// t.ProcessBandWidthDown = (processRx - tr.prevProcessRx) / int64(NicSampleInterval.Seconds())
		t.NicBandWidthUp = (nicTx - tr.prevNicTx) / int64(NicSampleInterval.Seconds())
		t.NicBandWidthDown = (nicRx - tr.prevNicRx) / int64(NicSampleInterval.Seconds())
	}

	// Update the last sampled values
	// tr.prevProcessTx = processTx
	// tr.prevProcessRx = processRx
	tr.prevNicTx = nicTx
	tr.prevNicRx = nicRx

	// Log or update stats as needed
	// fmt.Printf("ptx: %d, prx: %d, ntx: %d, nrx: %d\n", processTx, processRx, nicTx, nicRx)
}

// getBandwidth reads network I/O stats from the given file
func getBandwidth(filePath string) (txBytes, rxBytes int64, err error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, 0, err
	}

	lines := strings.Split(string(data), "\n")

	for _, line := range lines[2:] {
		fields := strings.Fields(line)
		if len(fields) < 17 {
			continue
		}

		// Parse receive (rx) and transmit (tx) bytes
		rx, err1 := strconv.ParseInt(fields[1], 10, 64)
		tx, err2 := strconv.ParseInt(fields[9], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}

		rxBytes += rx
		txBytes += tx
	}

	return txBytes, rxBytes, nil
}

// setMemoryUsage calculates and updates memory usage
func setMemoryUsage(ctx context.Context, t *types.DeviceRunningStat) {
	vMem, err := mem.VirtualMemory()
	if err != nil {
		log.Errorln("Error fetching memory info:", err)
		return
	}

	t.Memory = vMem.Total
	t.MemoryUsed = vMem.Used
	t.MemoryUsage = vMem.UsedPercent

	log.Infof("Updated Memory Usage: %.2f\n", t.MemoryUsage)
}

// setCPUUsage calculates and updates CPU usage
func setCPUUsage(ctx context.Context, t *types.DeviceRunningStat) {
	if cpuInfos, err := cpu.Info(); err != nil {
		log.Errorf("get cpu info error %s", err.Error())
	} else if len(cpuInfos) > 0 {
		t.CPUInfo = cpuInfos[0].ModelName
	}

	// Get CPU usage for each core over an interval
	cpuUsage, err := cpu.Percent(NicSampleInterval, true) // Per-core usage
	if err != nil {
		log.Errorln("Error fetching CPU usage:", err)
		return
	}
	t.CPUCores = len(cpuUsage)
	t.CPUUsage = cpuUsage

	// Calculate total CPU usage
	totalUsage := 0.0
	for _, usage := range cpuUsage {
		totalUsage += usage
	}
	t.CPUTotalUsage = totalUsage / float64(len(cpuUsage))

	log.Infof("Updated CPU Usage: %.2f \n", t.CPUTotalUsage)
}
