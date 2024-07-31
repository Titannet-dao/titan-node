package types

import "time"

type ProviderState int

const (
	ProviderStateOnline ProviderState = iota + 1
	ProviderStateOffline
	ProviderStateAbnormal
)

func ProviderStateString(state ProviderState) string {
	switch state {
	case ProviderStateOnline:
		return "Online"
	case ProviderStateOffline:
		return "Offline"
	case ProviderStateAbnormal:
		return "Abnormal"
	default:
		return "Unknown"
	}
}

type Provider struct {
	ID         string        `db:"id"`
	Owner      string        `db:"owner"`
	RemoteAddr string        `db:"remote_addr"`
	IP         string        `db:"ip"`
	State      ProviderState `db:"state"`
	Scheme     string        `db:"-"`
	CreatedAt  time.Time     `db:"created_at"`
	UpdatedAt  time.Time     `db:"updated_at"`
}

type GetProviderOption struct {
	Owner string
	ID    string
	State []ProviderState
	Page  int
	Size  int
}

type ResourcesStatistics struct {
	Memory   Memory
	CPUCores CPUCores
	Storage  StorageStat
}

type Memory struct {
	MaxMemory uint64
	Available uint64
	Active    uint64
	Pending   uint64
}

type CPUCores struct {
	MaxCPUCores float64
	Available   float64
	Active      float64
	Pending     float64
}

type StorageStat struct {
	MaxStorage uint64
	Available  uint64
	Active     uint64
	Pending    uint64
}

type SufficientResourceNode struct {
	Name string
	ResourcesStatistics
}
