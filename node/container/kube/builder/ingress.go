package builder

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Ingress interface {
	workloadBase
	Create() (*netv1.Ingress, error)
	Update(obj *netv1.Ingress) (*netv1.Ingress, error)
}

type ingress struct {
	Workload
	directive *HostnameDirective
	tls       []netv1.IngressTLS
}

var _ Ingress = (*ingress)(nil)

func BuildIngress(workload Workload, directive *HostnameDirective, tls []netv1.IngressTLS) Ingress {
	return &ingress{
		Workload:  workload,
		directive: directive,
		tls:       tls,
	}
}

func (b *ingress) Create() (*netv1.Ingress, error) {
	directive := b.directive

	ingressClassName := directive.IngressName
	obj := &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        directive.Hostname,
			Labels:      b.labels(),
			Annotations: kubeNginxIngressAnnotations(directive),
		},
		Spec: netv1.IngressSpec{
			Rules: ingressRules([]string{directive.Hostname}, directive.ServiceName, directive.ServicePort),
		},
	}

	if len(ingressClassName) > 0 {
		obj.Spec.IngressClassName = &ingressClassName
	}

	if len(b.tls) > 0 {
		obj.Spec.TLS = b.tls
	}

	return obj, nil
}

func (b *ingress) Update(obj *netv1.Ingress) (*netv1.Ingress, error) {
	directive := b.directive

	var hostnames []string
	for _, rule := range obj.Spec.Rules {
		hostnames = append(hostnames, rule.Host)
	}

	rules := ingressRules(hostnames, directive.ServiceName, directive.ServicePort)

	obj.ObjectMeta.Labels = b.labels()

	obj.Spec.Rules = rules
	return obj, nil
}

func (b *ingress) Name() string {
	return b.directive.Hostname
}

func kubeNginxIngressAnnotations(directive *HostnameDirective) map[string]string {
	// For kubernetes/ingress-nginx
	// https://github.com/kubernetes/ingress-nginx
	const root = "nginx.ingress.kubernetes.io"

	readTimeout := math.Ceil(float64(directive.ReadTimeout) / 1000.0)
	sendTimeout := math.Ceil(float64(directive.SendTimeout) / 1000.0)
	result := map[string]string{
		fmt.Sprintf("%s/proxy-read-timeout", root): fmt.Sprintf("%d", int(readTimeout)),
		fmt.Sprintf("%s/proxy-send-timeout", root): fmt.Sprintf("%d", int(sendTimeout)),

		fmt.Sprintf("%s/proxy-next-upstream-tries", root): strconv.Itoa(int(directive.NextTries)),
		fmt.Sprintf("%s/proxy-body-size", root):           strconv.Itoa(int(directive.MaxBodySize)),
	}

	nextTimeoutKey := fmt.Sprintf("%s/proxy-next-upstream-timeout", root)
	nextTimeout := 0 // default magic value for disable
	if directive.NextTimeout > 0 {
		nextTimeout = int(math.Ceil(float64(directive.NextTimeout) / 1000.0))
	}

	result[nextTimeoutKey] = fmt.Sprintf("%d", nextTimeout)

	strBuilder := strings.Builder{}

	for i, v := range directive.NextCases {
		first := string(v[0])
		isHTTPCode := strings.ContainsAny(first, "12345")

		if isHTTPCode {
			strBuilder.WriteString("http_")
		}
		strBuilder.WriteString(v)

		if i != len(directive.NextCases)-1 {
			// The actual separator is the space character for kubernetes/ingress-nginx
			strBuilder.WriteRune(' ')
		}
	}

	// append cors
	result[fmt.Sprintf("%s/cors-allow-credentials", root)] = "true"
	result[fmt.Sprintf("%s/cors-allow-headers", root)] = "Authorization,Content-Type,Accept,Origin,User-Agent,DNT,Cache-Control,Token"
	result[fmt.Sprintf("%s/cors-allow-methods", root)] = "GET, POST, OPTIONS, PUT, DELETE"
	result[fmt.Sprintf("%s/cors-allow-origin", root)] = "*"
	result[fmt.Sprintf("%s/cors-max-age", root)] = "3600"
	result[fmt.Sprintf("%s/enable-cors", root)] = "true"

	result[fmt.Sprintf("%s/proxy-next-upstream", root)] = strBuilder.String()
	return result
}

func ingressRules(hostnames []string, kubeServiceName string, kubeServicePort int32) []netv1.IngressRule {
	// for some reason we need to pass a pointer to this
	var out []netv1.IngressRule
	for _, hostname := range hostnames {
		pathTypeForAll := netv1.PathTypePrefix
		ruleValue := netv1.HTTPIngressRuleValue{
			Paths: []netv1.HTTPIngressPath{{
				Path:     "/",
				PathType: &pathTypeForAll,
				Backend: netv1.IngressBackend{
					Service: &netv1.IngressServiceBackend{
						Name: kubeServiceName,
						Port: netv1.ServiceBackendPort{
							Number: kubeServicePort,
						},
					},
				},
			}},
		}
		out = append(out, netv1.IngressRule{
			Host:             hostname,
			IngressRuleValue: netv1.IngressRuleValue{HTTP: &ruleValue},
		})
	}

	return out
}
