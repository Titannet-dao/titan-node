package builder

import (
	"github.com/Filecoin-Titan/titan/node/container/kube/manifest"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	TitanManagedLabelName         = "titan.provider"
	TitanManifestServiceLabelName = "titan.provider/manifest-service"

	// AkashNetworkStorageClasses    = "akash.network/storageclasses"
	// AkashServiceTarget            = "akash.network/service-target"
	// AkashServiceCapabilityGPU     = "akash.network/capabilities.gpu"
	// AkashMetalLB                  = "metal-lb"
	titanDeploymentPolicyName = "titan-deployment-restrictions"

	titanNetworkNamespace = "titan.provider/namespace"

	titanNodeSelector = "kubernetes.io/os"
	osTypeLinux       = "linux"
	osTypeWindows     = "windows"
)

var (
	dnsPort     = intstr.FromInt(53)
	udpProtocol = corev1.Protocol("UDP")
	tcpProtocol = corev1.Protocol("TCP")
)

type IClusterDeployment interface {
	DeploymentID() manifest.DeploymentID
	ManifestGroup() *manifest.Group
	ClusterParams() ClusterSettings
}

type ClusterDeployment struct {
	Did     manifest.DeploymentID
	Group   *manifest.Group
	Sparams ClusterSettings
}

var _ IClusterDeployment = (*ClusterDeployment)(nil)

func (d *ClusterDeployment) DeploymentID() manifest.DeploymentID {
	return d.Did
}

func (d *ClusterDeployment) ManifestGroup() *manifest.Group {
	return d.Group
}
func (d *ClusterDeployment) ClusterParams() ClusterSettings {
	return d.Sparams
}

type builderBase interface {
	NS() string
	Name() string
	Validate() error
}

type builder struct {
	settings   Settings
	deployment IClusterDeployment
}

var _ builderBase = (*builder)(nil)

func (b *builder) NS() string {
	return DidNS(b.deployment.DeploymentID())
}
func (b *builder) Name() string {
	return b.NS()
}
func (b *builder) Validate() error {
	return nil
}

func (b *builder) labels() map[string]string {
	return map[string]string{
		TitanManagedLabelName: "true",
		titanNetworkNamespace: b.NS(),
	}
}

func DidNS(did manifest.DeploymentID) string {
	if len(did.ID) == 0 {
		return ""
	}
	// DNS-1123 label must consist of lower case alphanumeric characters or '-',
	// and must start and end with an alphanumeric character
	// (e.g. 'my-name',  or '123-abc', regex used for validation
	// is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')
	// path := fmt.Sprintf("%s/%s", did.Owner, did.ID)
	// sha := sha256.Sum224([]byte(path))
	// return strings.ToLower(base32.HexEncoding.WithPadding(base32.NoPadding).EncodeToString(sha[:]))
	return did.ID
}

func ShouldBeIngress(expose *manifest.ServiceExpose) bool {
	return expose.Proto == manifest.TCP && expose.Global && (exposeExternalPort(expose) == 80 || exposeExternalPort(expose) == 443)
}

func exposeExternalPort(expose *manifest.ServiceExpose) int32 {
	if expose.ExternalPort == 0 {
		return int32(expose.Port)
	}
	return int32(expose.ExternalPort)
}
