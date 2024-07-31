package builder

import (
	"fmt"

	"golang.org/x/xerrors"
	corev1 "k8s.io/api/core/v1"
)

// Settings configures k8s object generation such that it is customized to the
// cluster environment that is being used.
// For instance, GCP requires a different service type than minikube.
type Settings struct {
	// gcp:    NodePort
	// others: ClusterIP
	DeploymentServiceType corev1.ServiceType

	// gcp:    false
	// others: true
	DeploymentIngressStaticHosts bool
	// Ingress domain to map deployments to
	DeploymentIngressDomain string

	// Return load balancer host in lease status command ?
	// gcp:    true
	// others: optional
	DeploymentIngressExposeLBHosts bool

	// Global hostname for arbitrary ports
	ClusterPublicHostname string

	// NetworkPoliciesEnabled determines if NetworkPolicies should be installed.
	NetworkPoliciesEnabled bool

	CPUCommitLevel     float64
	GPUCommitLevel     float64
	MemoryCommitLevel  float64
	StorageCommitLevel float64

	DeploymentRuntimeClass string

	// Name of the image pull secret to use in pod spec
	DockerImagePullSecretsName string
}

var ErrSettingsValidation = xerrors.New("settings validation")

func ValidateSettings(settings Settings) error {
	if settings.DeploymentIngressStaticHosts {
		if settings.DeploymentIngressDomain == "" {
			return fmt.Errorf("%w: empty ingress domain", ErrSettingsValidation)
		}

		if isDomainName(settings.DeploymentIngressDomain) {
			return fmt.Errorf("%w: invalid domain name %q", ErrSettingsValidation, settings.DeploymentIngressDomain)
		}
	}

	return nil
}

func NewDefaultSettings() Settings {
	return Settings{
		DeploymentServiceType:          corev1.ServiceTypeClusterIP,
		DeploymentIngressStaticHosts:   false,
		DeploymentIngressExposeLBHosts: false,
		NetworkPoliciesEnabled:         false,
	}
}

type ContextKey string

const SettingsKey = ContextKey("kube-client-settings")

// isDomainName checks if a string is a presentation-format domain name
// (currently restricted to hostname-compatible "preferred name" LDH labels and
// SRV-like "underscore labels"; see golang.org/issue/12421).
func isDomainName(s string) bool {
	// See RFC 1035, RFC 3696.
	// Presentation format has dots before every label except the first, and the
	// terminal empty label is optional here because we assume fully-qualified
	// (absolute) input. We must therefore reserve space for the first and last
	// labels' length octets in wire format, where they are necessary and the
	// maximum total length is 255.
	// So our _effective_ maximum is 253, but 254 is not rejected if the last
	// character is a dot.
	l := len(s)
	if l == 0 || l > 254 || l == 254 && s[l-1] != '.' {
		return false
	}

	last := byte('.')
	nonNumeric := false // true once we've seen a letter or hyphen
	partlen := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		default:
			return false
		case 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || c == '_':
			nonNumeric = true
			partlen++
		case '0' <= c && c <= '9':
			// fine
			partlen++
		case c == '-':
			// Byte before dash cannot be dot.
			if last == '.' {
				return false
			}
			partlen++
			nonNumeric = true
		case c == '.':
			// Byte before dot cannot be dot, dash.
			if last == '.' || last == '-' {
				return false
			}
			if partlen > 63 || partlen == 0 {
				return false
			}
			partlen = 0
		}
		last = c
	}
	if last == '-' || partlen > 63 {
		return false
	}

	return nonNumeric
}
