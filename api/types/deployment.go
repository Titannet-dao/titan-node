package types

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type DeploymentID string

type DeploymentState int

const (
	DeploymentStateActive DeploymentState = iota + 1
	DeploymentStateInActive
	DeploymentStateClose
)

func DeploymentStateString(state DeploymentState) string {
	switch state {
	case DeploymentStateActive:
		return "Active"
	case DeploymentStateInActive:
		return "InActive"
	case DeploymentStateClose:
		return "Deleted"
	default:
		return "Unknown"
	}
}

var AllDeploymentStates = []DeploymentState{DeploymentStateActive, DeploymentStateInActive, DeploymentStateClose}

type DeploymentType int

const (
	DeploymentTypeWeb DeploymentType = iota + 1
)

type OSType string

const (
	OSTypeWindows OSType = "windows"
	OSTypeLinux   OSType = "linux"
)

type Deployment struct {
	ID        DeploymentID    `db:"id"`
	Name      string          `db:"name"`
	Owner     string          `db:"owner"`
	State     DeploymentState `db:"state"`
	Version   string          `db:"version"`
	Authority bool            `db:"authority"`
	Services  []*Service

	// Internal
	Type             DeploymentType `db:"type"`
	Balance          float64        `db:"balance"`
	Cost             float64        `db:"cost"`
	ProviderID       string         `db:"provider_id"`
	Expiration       time.Time      `db:"expiration"`
	CreatedAt        time.Time      `db:"created_at"`
	UpdatedAt        time.Time      `db:"updated_at"`
	ProviderExposeIP string         `db:"provider_expose_ip"`
}

type GetDeploymentListResp struct {
	Deployments []*Deployment
	Total       int64
}

type ReplicasStatus struct {
	TotalReplicas     int
	ReadyReplicas     int
	AvailableReplicas int
}

type Service struct {
	Image        string         `db:"image"`
	Name         string         `db:"name"`
	Ports        Ports          `db:"ports"`
	Env          Env            `db:"env"`
	Status       ReplicasStatus `db:"status"`
	ErrorMessage string         `db:"error_message"`
	Arguments    Arguments      `db:"arguments"`
	ComputeResources
	OSType   OSType `db:"os_type"`
	Replicas int32  `db:"replicas"`

	// Internal
	ID           int64        `db:"id"`
	DeploymentID DeploymentID `db:"deployment_id"`
	CreatedAt    time.Time    `db:"created_at"`
	UpdatedAt    time.Time    `db:"updated_at"`
}

type Env map[string]string

func (e Env) Value() (driver.Value, error) {
	x := make(map[string]string)
	for k, v := range e {
		x[k] = v
	}
	return json.Marshal(x)
}

func (e Env) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	if err := json.Unmarshal(b, &e); err != nil {
		return err
	}
	return nil
}

type Arguments []string

func (a Arguments) Value() (driver.Value, error) {
	return strings.Join(a, ","), nil
}

func (a Arguments) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	copy(a, strings.Split(string(b), ","))
	return nil
}

type Protocol string

const (
	TCP = Protocol("TCP")
	UDP = Protocol("UDP")
)

type Port struct {
	Protocol   Protocol `db:"protocol"`
	Port       int      `db:"port"`
	ExposePort int      `db:"expose_port"`
}

type Ports []Port

func (a Ports) Value() (driver.Value, error) {
	x := make([]Port, 0, len(a))
	for _, i := range a {
		x = append(x, i)

	}
	return json.Marshal(x)
}

func (a Ports) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	return nil
}

type GetDeploymentOption struct {
	Owner        string
	DeploymentID DeploymentID
	ProviderID   string
	State        []DeploymentState
	Page         int
	Size         int
}

type ComputeResources struct {
	CPU     float64  `db:"cpu"`
	GPU     float64  `db:"gpu"`
	Memory  int64    `db:"memory"`
	Storage Storages `db:"storage"`
}

type Storage struct {
	Name       string `db:"name"`
	Quantity   int64  `db:"quantity"`
	Persistent bool   `db:"persistent"`
	Mount      string `db:"mount"`
}

type Storages []*Storage

func (s Storages) Value() (driver.Value, error) {
	return json.Marshal(s)
}

func (s Storages) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return nil
}

type AppType int

const (
	AppTypeL1 AppType = iota + 1
)

type Properties struct {
	ProviderID string  `db:"provider_id"`
	AppID      string  `db:"app_id"`
	AppType    AppType `db:"app_type"`

	// internal
	ID        int       `db:"id"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

type DeploymentDomain struct {
	ID           int       `db:"id"`
	Name         string    `db:"name"`
	State        string    `db:"state"`
	DeploymentID string    `db:"deployment_id"`
	ProviderID   string    `db:"provider_id"`
	CreatedAt    time.Time `db:"created_at"`
	UpdatedAt    time.Time `db:"updated_at"`
}

type LeaseEndpoint struct {
	Scheme    string
	Host      string
	ShellPath string
	Token     string
}

const (
	ShellCodeStdout         = 100
	ShellCodeStderr         = 101
	ShellCodeResult         = 102
	ShellCodeFailure        = 103
	ShellCodeStdin          = 104
	ShellCodeTerminalResize = 105
	ShellCodeEOF            = 106
)

type ExecResult struct {
	Code int
}

type ShellResponse struct {
	ExitCode int    `json:"exit_code"`
	Message  string `json:"message,omitempty"`
}

type Certificate struct {
	Hostname    string
	PrivateKey  []byte
	Certificate []byte
}

type Ingress struct {
	Annotations map[string]string
}

type AuthUserDeployment struct {
	UserID       string
	DeploymentID string
	Expiration   time.Time
}
