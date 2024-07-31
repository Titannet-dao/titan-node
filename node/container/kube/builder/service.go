package builder

import (
	"fmt"
	"github.com/Filecoin-Titan/titan/node/container/kube/manifest"

	"golang.org/x/xerrors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type Service interface {
	workloadBase
	Create() (*corev1.Service, error)
	Update(obj *corev1.Service) (*corev1.Service, error)
	Any() bool
}

type service struct {
	Workload
	requireNodePort bool
}

var _ Service = (*service)(nil)

func BuildService(workload Workload, requireNodePort bool) Service {
	s := &service{
		Workload:        workload,
		requireNodePort: requireNodePort,
	}

	return s
}

func (b *service) Name() string {
	basename := b.Workload.Name()
	if b.requireNodePort {
		return makeGlobalServiceNameFromBasename(basename)
	}
	return basename
}

const SuffixForNodePortServiceName = "-np"

func makeGlobalServiceNameFromBasename(basename string) string {
	return fmt.Sprintf("%s%s", basename, SuffixForNodePortServiceName)
}

func (b *service) workloadServiceType() corev1.ServiceType {
	if b.requireNodePort {
		return corev1.ServiceTypeNodePort
	}
	return corev1.ServiceTypeClusterIP
}

func (b *service) Create() (*corev1.Service, error) { // nolint:golint,unparam
	ports, err := b.ports()
	if err != nil {
		return nil, err
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   b.Name(),
			Labels: b.labels(),
		},
		Spec: corev1.ServiceSpec{
			Type:        b.workloadServiceType(),
			Selector:    b.labels(),
			Ports:       ports,
			ExternalIPs: b.externalIPs(),
		},
	}

	return svc, nil
}

func (b *service) Update(obj *corev1.Service) (*corev1.Service, error) { // nolint:golint,unparam
	obj.Labels = b.labels()
	obj.Spec.Selector = b.labels()
	ports, err := b.ports()
	if err != nil {
		return nil, err
	}

	// retain provisioned NodePort values
	if b.requireNodePort {

		// for each newly-calculated port
		for i, port := range ports {

			// if there is a current (in-kube) port defined
			// with the same specified values
			for _, curport := range obj.Spec.Ports {
				if curport.Name == port.Name &&
					curport.Port == port.Port &&
					curport.TargetPort.IntValue() == port.TargetPort.IntValue() &&
					curport.Protocol == port.Protocol {

					// re-use current port
					ports[i] = curport
				}
			}
		}
	}

	obj.Spec.Ports = ports
	return obj, nil
}

func (b *service) Any() bool {
	service := &b.deployment.ManifestGroup().Services[b.serviceIdx]

	for _, expose := range service.Expose {
		exposeIsIngress := ShouldBeIngress(expose)
		if b.requireNodePort && exposeIsIngress {
			continue
		}

		if !b.requireNodePort && exposeIsIngress {
			return true
		}

		if expose.Global == b.requireNodePort {
			return true
		}
	}
	return false
}

var errUnsupportedProtocol = xerrors.New("Unsupported protocol for service")
var errInvalidServiceBuilder = xerrors.New("service builder invalid")

func (b *service) ports() ([]corev1.ServicePort, error) {
	service := &b.deployment.ManifestGroup().Services[b.serviceIdx]

	ports := make([]corev1.ServicePort, 0, len(service.Expose))
	portsAdded := make(map[string]struct{})
	for i, expose := range service.Expose {
		shouldBeIngress := ShouldBeIngress(expose)
		if expose.Global == b.requireNodePort || (!b.requireNodePort && shouldBeIngress) {
			if b.requireNodePort && shouldBeIngress {
				continue
			}

			var exposeProtocol corev1.Protocol
			switch expose.Proto {
			case manifest.TCP:
				exposeProtocol = corev1.ProtocolTCP
			case manifest.UDP:
				exposeProtocol = corev1.ProtocolUDP
			default:
				return nil, errUnsupportedProtocol
			}
			externalPort := exposeExternalPort(service.Expose[i])
			key := fmt.Sprintf("%d:%s", externalPort, exposeProtocol)
			_, added := portsAdded[key]
			if !added {
				portsAdded[key] = struct{}{}
				ports = append(ports, corev1.ServicePort{
					Name:       fmt.Sprintf("%d-%d", i, int(externalPort)),
					Port:       externalPort,
					TargetPort: intstr.FromInt(int(expose.Port)),
					Protocol:   exposeProtocol,
				})
			}
		}
	}

	if len(ports) == 0 {
		return nil, errInvalidServiceBuilder
	}

	return ports, nil
}

func (b *service) externalIPs() []string {
	ips := make([]string, 0)

	service := &b.deployment.ManifestGroup().Services[b.serviceIdx]
	for _, expose := range service.Expose {
		if ShouldBeIngress(expose) {
			continue
		}

		if len(expose.IP) > 0 {
			ips = append(ips, expose.IP)
		}
	}

	return ips
}
