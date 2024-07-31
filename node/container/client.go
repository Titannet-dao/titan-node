package container

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/container/kube"
	"github.com/Filecoin-Titan/titan/node/container/kube/builder"
	"github.com/Filecoin-Titan/titan/node/container/kube/manifest"
	"github.com/gbrlsnchs/jwt/v3"
	logging "github.com/ipfs/go-log/v2"
	"io"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/remotecommand"
	"time"
)

var log = logging.Logger("provider")

type Client struct {
	kc          kube.Client
	providerCfg *config.ProviderConfig
	apiSecret   *jwt.HMACSHA
}

var _ api.ProviderAPI = &Client{}

func NewClient(cfg *config.CandidateCfg, secret *jwt.HMACSHA) (*Client, error) {
	kubeClient, err := kube.NewClient(cfg.KubeConfigPath, &cfg.ProviderConfig)
	if err != nil {
		return nil, err
	}

	return &Client{kc: kubeClient, providerCfg: &cfg.ProviderConfig, apiSecret: secret}, nil
}

func (c *Client) GetLeaseAccessToken(ctx context.Context, id types.DeploymentID, userId string) (string, error) {
	payload := &types.AuthUserDeployment{
		DeploymentID: string(id),
		UserID:       userId,
		Expiration:   time.Now().Add(2 * time.Hour),
	}

	token, err := jwt.Sign(payload, c.apiSecret)
	if err != nil {
		return "", err
	}

	return string(token), nil
}

func (c *Client) GetStatistics(ctx context.Context) (*types.ResourcesStatistics, error) {
	nodeResources, err := c.kc.FetchAllNodeResources(ctx)
	if err != nil {
		return nil, err
	}

	if nodeResources == nil {
		return nil, fmt.Errorf("nodes resources do not exist")
	}

	statistics := &types.ResourcesStatistics{}
	for _, node := range nodeResources {
		statistics.CPUCores.MaxCPUCores += node.CPU.Capacity.AsApproximateFloat64()
		statistics.CPUCores.Available += (node.CPU.Allocatable.AsApproximateFloat64() - node.CPU.Allocated.AsApproximateFloat64())
		statistics.CPUCores.Active += node.CPU.Allocated.AsApproximateFloat64()

		statistics.Memory.MaxMemory += uint64(node.Memory.Capacity.AsApproximateFloat64())
		statistics.Memory.Available += uint64(node.Memory.Allocatable.AsApproximateFloat64() - node.Memory.Allocated.AsApproximateFloat64())
		statistics.Memory.Active += uint64(node.Memory.Allocated.AsApproximateFloat64())

		statistics.Storage.MaxStorage += uint64(node.EphemeralStorage.Capacity.AsApproximateFloat64())
		statistics.Storage.Available += uint64(node.EphemeralStorage.Allocatable.AsApproximateFloat64() - node.EphemeralStorage.Allocated.AsApproximateFloat64())
		statistics.Storage.Active += uint64(node.EphemeralStorage.Allocated.AsApproximateFloat64())
	}

	return statistics, nil
}

func (c *Client) GetNodeResources(ctx context.Context, nodeName string) (*types.ResourcesStatistics, error) {
	node, err := c.kc.FetchNodeResource(ctx, nodeName)
	if err != nil {
		return nil, err
	}

	statistics := &types.ResourcesStatistics{}
	statistics.CPUCores.MaxCPUCores += node.CPU.Capacity.AsApproximateFloat64()
	statistics.CPUCores.Available += (node.CPU.Allocatable.AsApproximateFloat64() - node.CPU.Allocated.AsApproximateFloat64())
	statistics.CPUCores.Active += node.CPU.Allocated.AsApproximateFloat64()

	statistics.Memory.MaxMemory += uint64(node.Memory.Capacity.AsApproximateFloat64())
	statistics.Memory.Available += uint64(node.Memory.Allocatable.AsApproximateFloat64() - node.Memory.Allocated.AsApproximateFloat64())
	statistics.Memory.Active += uint64(node.Memory.Allocated.AsApproximateFloat64())

	statistics.Storage.MaxStorage += uint64(node.EphemeralStorage.Capacity.AsApproximateFloat64())
	statistics.Storage.Available += uint64(node.EphemeralStorage.Allocatable.AsApproximateFloat64() - node.EphemeralStorage.Allocated.AsApproximateFloat64())
	statistics.Storage.Active += uint64(node.EphemeralStorage.Allocated.AsApproximateFloat64())
	return statistics, nil
}

func (c *Client) CreateDeployment(ctx context.Context, deployment *types.Deployment) error {
	cDeployment, err := ClusterDeploymentFromDeployment(deployment)
	if err != nil {
		log.Errorf("CreateDeployment %s", err.Error())
		return err
	}

	did := cDeployment.DeploymentID()
	ns := builder.DidNS(did)

	if isExist, err := c.isExistDeploymentOrStatefulSet(ctx, ns); err != nil {
		return err
	} else if isExist {
		return fmt.Errorf("deployment %s already exist", deployment.ID)
	}

	ctx = context.WithValue(ctx, builder.SettingsKey, builder.NewDefaultSettings())
	return c.kc.Deploy(ctx, cDeployment)
}

func (c *Client) UpdateDeployment(ctx context.Context, deployment *types.Deployment) error {
	k8sDeployment, err := ClusterDeploymentFromDeployment(deployment)
	if err != nil {
		log.Errorf("UpdateDeployment %s", err.Error())
		return err
	}

	did := k8sDeployment.DeploymentID()
	ns := builder.DidNS(did)

	if isExist, err := c.isExistDeploymentOrStatefulSet(ctx, ns); err != nil {
		return err
	} else if !isExist {
		return fmt.Errorf("deployment %s do not exist", deployment.ID)
	}

	group := k8sDeployment.ManifestGroup()
	for _, service := range group.Services {
		// check service if exist
		if c.isPersistent(&service) {
			statefulSet, err := c.kc.GetStatefulSet(ctx, ns, service.Name)
			if err != nil {
				return err
			}

			if !c.checkStatefulSetStorage(service.Resources.Storage, statefulSet.Spec.VolumeClaimTemplates) {
				return fmt.Errorf("can not change storage size in persistent status")
			}

		} else {
			_, err := c.kc.GetDeployment(ctx, ns, service.Name)
			if err != nil {
				return err
			}
		}
	}

	ctx = context.WithValue(ctx, builder.SettingsKey, builder.NewDefaultSettings())
	return c.kc.Deploy(ctx, k8sDeployment)
}

func (c *Client) CloseDeployment(ctx context.Context, deployment *types.Deployment) error {
	k8sDeployment, err := ClusterDeploymentFromDeployment(deployment)
	if err != nil {
		log.Errorf("CloseDeployment %s", err.Error())
		return err
	}

	did := k8sDeployment.DeploymentID()
	ns := builder.DidNS(did)
	if len(ns) == 0 {
		return fmt.Errorf("can not get ns from deployment id %s and owner %s", deployment.ID, deployment.Owner)
	}

	return c.kc.DeleteNS(ctx, ns)
}

func (c *Client) GetDeployment(ctx context.Context, id types.DeploymentID) (*types.Deployment, error) {
	deploymentID := manifest.DeploymentID{ID: string(id)}
	ns := builder.DidNS(deploymentID)

	services, err := c.getTitanServices(ctx, ns)
	if err != nil {
		return nil, err
	}

	serviceList, err := c.kc.ListServices(ctx, ns)
	if err != nil {
		return nil, err
	}

	portMap, err := k8sServiceToPortMap(serviceList)
	if err != nil {
		return nil, err
	}

	for i := range services {
		name := services[i].Name
		if ports, ok := portMap[name]; ok {
			services[i].Ports = ports
		}
	}

	return &types.Deployment{ID: id, Services: services}, nil
}

func (c *Client) GetLogs(ctx context.Context, id types.DeploymentID) ([]*types.ServiceLog, error) {
	deploymentID := manifest.DeploymentID{ID: string(id)}
	ns := builder.DidNS(deploymentID)

	pods, err := c.getPods(ctx, ns)
	if err != nil {
		return nil, err
	}

	logMap := make(map[string][]types.Log)

	for podName, serviceName := range pods {
		buf, err := c.getPodLogs(ctx, ns, podName)
		if err != nil {
			return nil, err
		}
		log := string(buf)

		logs, ok := logMap[serviceName]
		if !ok {
			logs = make([]types.Log, 0)
		}
		logs = append(logs, types.Log(log))
		logMap[serviceName] = logs
	}

	serviceLogs := make([]*types.ServiceLog, 0, len(logMap))
	for serviceName, logs := range logMap {
		serviceLog := &types.ServiceLog{ServiceName: serviceName, Logs: logs}
		serviceLogs = append(serviceLogs, serviceLog)
	}

	return serviceLogs, nil
}

func (c *Client) GetEvents(ctx context.Context, id types.DeploymentID) ([]*types.ServiceEvent, error) {
	deploymentID := manifest.DeploymentID{ID: string(id)}
	ns := builder.DidNS(deploymentID)

	pods, err := c.getPods(ctx, ns)
	if err != nil {
		return nil, err
	}

	podEventMap, err := c.getEvents(ctx, ns)
	if err != nil {
		return nil, err
	}

	serviceEventMap := make(map[string][]types.Event)
	for podName, serviceName := range pods {
		es, ok := serviceEventMap[serviceName]
		if !ok {
			es = make([]types.Event, 0)
		}

		if podEvents, ok := podEventMap[podName]; ok {
			es = append(es, podEvents...)
		}
		serviceEventMap[serviceName] = es
	}

	serviceEvents := make([]*types.ServiceEvent, 0, len(serviceEventMap))
	for serviceName, events := range serviceEventMap {
		serviceEvent := &types.ServiceEvent{ServiceName: serviceName, Events: events}
		serviceEvents = append(serviceEvents, serviceEvent)
	}

	return serviceEvents, nil
}

func (c *Client) getPods(ctx context.Context, ns string) (map[string]string, error) {
	pods := make(map[string]string)
	podList, err := c.kc.ListPods(context.Background(), ns, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	if podList == nil {
		return nil, nil
	}

	for _, pod := range podList.Items {
		pods[pod.Name] = pod.ObjectMeta.Labels[builder.TitanManifestServiceLabelName]
	}

	return pods, nil
}

func (c *Client) getPodLogs(ctx context.Context, ns string, podName string) ([]byte, error) {
	reader, err := c.kc.PodLogs(ctx, ns, podName)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	buf := bytes.Buffer{}
	_, err = buf.ReadFrom(reader)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *Client) getEvents(ctx context.Context, ns string) (map[string][]types.Event, error) {
	eventList, err := c.kc.Events(ctx, ns, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	if eventList == nil {
		return nil, nil
	}

	eventMap := make(map[string][]types.Event)
	for _, event := range eventList.Items {
		podName := event.InvolvedObject.Name
		events, ok := eventMap[podName]
		if !ok {
			events = make([]types.Event, 0)
		}

		events = append(events, types.Event(event.Message))
		eventMap[podName] = events
	}

	return eventMap, nil
}

// this is titan services not k8s services
func (c *Client) getTitanServices(ctx context.Context, ns string) ([]*types.Service, error) {
	deploymentList, err := c.kc.ListDeployments(ctx, ns)
	if err != nil {
		return nil, err
	}

	if deploymentList != nil && len(deploymentList.Items) > 0 {
		return k8sDeploymentsToServices(deploymentList)
	}

	statefulSets, err := c.kc.ListStatefulSets(ctx, ns)
	if err != nil {
		return nil, err
	}
	return k8sStatefulSetsToServices(statefulSets)
}

func (c *Client) isPersistent(service *manifest.Service) bool {
	for i := range service.Resources.Storage {
		attrVal := service.Resources.Storage[i].Attributes.Find(builder.StorageAttributePersistent)
		if persistent, _ := attrVal.AsBool(); persistent {
			return true
		}
	}

	return false
}

func (c *Client) checkStatefulSetStorage(storages []*manifest.Storage, pvcs []corev1.PersistentVolumeClaim) bool {
	for i := 0; i < len(storages); i++ {
		storage := storages[i]
		pvc := pvcs[i]
		quantity := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if storage.Quantity.Val.Int64() != quantity.Value() {
			return false
		}
	}

	return true
}

func (c *Client) isExistDeploymentOrStatefulSet(ctx context.Context, ns string) (bool, error) {
	deploymentList, err := c.kc.ListDeployments(ctx, ns)
	if err != nil {
		return false, err
	}

	if deploymentList != nil && len(deploymentList.Items) > 0 {
		return true, nil
	}

	statefulSets, err := c.kc.ListStatefulSets(ctx, ns)
	if err != nil {
		return false, err
	}
	if statefulSets != nil && len(statefulSets.Items) > 0 {
		return true, nil
	}

	return false, nil
}

func (c *Client) GetDomains(ctx context.Context, id types.DeploymentID) ([]*types.DeploymentDomain, error) {
	deploymentID := manifest.DeploymentID{ID: string(id)}
	ns := builder.DidNS(deploymentID)

	var out []*types.DeploymentDomain
	ingress, err := c.kc.GetIngress(ctx, ns, c.providerCfg.IngressHostName)
	if err != nil {
		return nil, err
	}

	for _, rule := range ingress.Spec.Rules {
		out = append(out, &types.DeploymentDomain{
			Name: rule.Host,
		})
	}

	return out, nil
}

func (c *Client) AddDomain(ctx context.Context, id types.DeploymentID, cert *types.Certificate) error {
	deploymentID := manifest.DeploymentID{ID: string(id)}
	ns := builder.DidNS(deploymentID)

	if len(cert.Certificate) > 0 && len(cert.PrivateKey) > 0 {
		data := map[string][]byte{
			corev1.TLSPrivateKeyKey: cert.PrivateKey,
			corev1.TLSCertKey:       cert.Certificate,
		}

		var secret *corev1.Secret
		_, err := c.kc.GetSecret(ctx, ns, cert.Hostname)
		if err == nil {
			secret, err = c.kc.UpdateSecret(ctx, corev1.SecretTypeTLS, ns, cert.Hostname, data)
		} else {
			secret, err = c.kc.CreateSecret(ctx, corev1.SecretTypeTLS, ns, cert.Hostname, data)
		}

		if err != nil {
			return err
		}

		return c.updateDomain(ctx, id, cert.Hostname, secret.Name)
	}

	ingress, err := c.kc.GetIngress(ctx, ns, c.providerCfg.IngressHostName)
	if err != nil {
		return err
	}

	newRule := netv1.IngressRule{
		Host: cert.Hostname,
	}

	if len(ingress.Spec.Rules) == 0 {
		return errors.New("please config hostname and expose port first")
	}

	newRule.IngressRuleValue = ingress.Spec.Rules[0].IngressRuleValue
	ingress.Spec.Rules = append(ingress.Spec.Rules, newRule)

	secret, err := c.kc.GetSecret(ctx, ns, c.providerCfg.IngressHostName)
	if secret != nil {
		ingress.Spec.TLS = append(ingress.Spec.TLS, netv1.IngressTLS{
			Hosts:      []string{cert.Hostname},
			SecretName: secret.Name,
		})
	}

	_, err = c.kc.UpdateIngress(ctx, ns, ingress)
	return nil
}

func (c *Client) DeleteDomain(ctx context.Context, id types.DeploymentID, hostname string) error {
	deploymentID := manifest.DeploymentID{ID: string(id)}
	ns := builder.DidNS(deploymentID)

	ingress, err := c.kc.GetIngress(ctx, ns, c.providerCfg.IngressHostName)
	if err != nil {
		return err
	}

	var newRules []netv1.IngressRule
	for _, rule := range ingress.Spec.Rules {
		if rule.Host == hostname {
			continue
		}
		newRules = append(newRules, rule)
	}

	ingress.Spec.Rules = newRules

	var newTLS []netv1.IngressTLS
	for _, tls := range ingress.Spec.TLS {
		for _, host := range tls.Hosts {
			if host != hostname {
				newTLS = append(newTLS, tls)
				continue
			}

			err = c.kc.DeleteSecret(ctx, ns, host)
			if err != nil {
				log.Errorf("delete secret: %v", err)
			}
		}
	}

	ingress.Spec.TLS = newTLS

	_, err = c.kc.UpdateIngress(ctx, ns, ingress)
	return nil
}

func (c *Client) updateDomain(ctx context.Context, id types.DeploymentID, hostname, secretName string) error {
	deploymentID := manifest.DeploymentID{ID: string(id)}
	ns := builder.DidNS(deploymentID)

	ingress, err := c.kc.GetIngress(ctx, ns, c.providerCfg.IngressHostName)
	if err != nil {
		return err
	}

	newRule := netv1.IngressRule{
		Host: hostname,
	}

	if len(ingress.Spec.Rules) == 0 {
		return errors.New("please config hostname and expose port first")
	}

	newRule.IngressRuleValue = ingress.Spec.Rules[0].IngressRuleValue
	ingress.Spec.Rules = append(ingress.Spec.Rules, newRule)
	ingress.Spec.TLS = append(ingress.Spec.TLS, netv1.IngressTLS{
		Hosts:      []string{hostname},
		SecretName: secretName,
	})

	_, err = c.kc.UpdateIngress(ctx, ns, ingress)
	return err
}

func (c *Client) getPodNameByService(ctx context.Context, ns string, serviceName string) (string, error) {
	podService, err := c.getPods(ctx, ns)
	if err != nil {
		return "", err
	}

	for podName, srvName := range podService {
		if srvName == serviceName {
			return podName, nil
		}
	}

	return "", nil
}

func (c *Client) Exec(ctx context.Context, id types.DeploymentID, serviceName string, stdin io.Reader, stdout, stderr io.Writer, cmd []string, tty bool, terminalSizeQueue remotecommand.TerminalSizeQueue) (types.ExecResult, error) {
	deploymentID := manifest.DeploymentID{ID: string(id)}
	ns := builder.DidNS(deploymentID)

	podName, err := c.getPodNameByService(ctx, ns, serviceName)
	if err != nil {
		return types.ExecResult{Code: 0}, errors.New("no pod found")
	}

	pod, err := c.kc.GetPod(ctx, ns, podName)
	if err != nil {
		return types.ExecResult{Code: 0}, err
	}

	// this should never happen, but just in case
	if len(pod.Spec.Containers) == 0 {
		return types.ExecResult{Code: 0}, fmt.Errorf("pod %s/%s does not have any containers", pod.Namespace, pod.Name)
	}

	// pick the first container as per existing behavior
	containerName := pod.Spec.Containers[0].Name

	result, err := c.kc.Exec(ctx, c.providerCfg.KubeConfigPath, ns, containerName, pod.Name, stdin, stdout, stderr, cmd, tty, terminalSizeQueue)

	return types.ExecResult{Code: result.ExitCode()}, err

}

func (c *Client) GetSufficientResourceNodes(ctx context.Context, reqResources *types.ComputeResources) ([]*types.SufficientResourceNode, error) {
	nodeResources, err := c.kc.FetchAllNodeResources(ctx)
	if err != nil {
		return nil, err
	}

	if nodeResources == nil {
		return nil, fmt.Errorf("nodes resources do not exist")
	}

	nodes := make([]*types.SufficientResourceNode, 0)
	for name, node := range nodeResources {
		usableCPU := node.CPU.Allocatable.AsApproximateFloat64() - node.CPU.Allocated.AsApproximateFloat64()
		if usableCPU < reqResources.CPU {
			continue
		}

		usableMemory := node.Memory.Allocatable.AsApproximateFloat64() - node.Memory.Allocated.AsApproximateFloat64()
		if int64(usableMemory) < reqResources.Memory {
			continue
		}

		usableGPU := node.GPU.Allocatable.AsApproximateFloat64() - node.CPU.Allocated.AsApproximateFloat64()
		if usableGPU < reqResources.GPU {
			continue
		}

		usableEphemeralStorage := node.EphemeralStorage.Allocatable.AsApproximateFloat64() - node.EphemeralStorage.Allocated.AsApproximateFloat64()
		ephemeralStorage, _ := countStorage(reqResources.Storage)
		if usableEphemeralStorage < float64(ephemeralStorage) {
			continue
		}

		// TODO: check volumesStorage size
		var statistics types.ResourcesStatistics
		statistics.CPUCores.MaxCPUCores = node.CPU.Capacity.AsApproximateFloat64()
		statistics.CPUCores.Available = node.CPU.Allocatable.AsApproximateFloat64() - node.CPU.Allocated.AsApproximateFloat64()
		statistics.CPUCores.Active = node.CPU.Allocated.AsApproximateFloat64()

		statistics.Memory.MaxMemory = uint64(node.Memory.Capacity.AsApproximateFloat64())
		statistics.Memory.Available = uint64(node.Memory.Allocatable.AsApproximateFloat64() - node.Memory.Allocated.AsApproximateFloat64())
		statistics.Memory.Active = uint64(node.Memory.Allocated.AsApproximateFloat64())

		statistics.Storage.MaxStorage = uint64(node.EphemeralStorage.Capacity.AsApproximateFloat64())
		statistics.Storage.Available = uint64(node.EphemeralStorage.Allocatable.AsApproximateFloat64() - node.EphemeralStorage.Allocated.AsApproximateFloat64())
		statistics.Storage.Active = uint64(node.EphemeralStorage.Allocated.AsApproximateFloat64())

		srNode := &types.SufficientResourceNode{Name: name, ResourcesStatistics: statistics}
		nodes = append(nodes, srNode)

	}
	return nodes, nil
}

func countStorage(storages []*types.Storage) (ephemeralStorage int64, volumesStorage int64) {
	for _, storage := range storages {
		if storage.Persistent {
			volumesStorage += storage.Quantity
		} else {
			ephemeralStorage += storage.Quantity
		}
	}
	return
}

func (c *Client) GetIngress(ctx context.Context, id types.DeploymentID) (*types.Ingress, error) {
	deploymentID := manifest.DeploymentID{ID: string(id)}
	ns := builder.DidNS(deploymentID)

	ingress, err := c.kc.GetIngress(ctx, ns, c.providerCfg.IngressHostName)
	if err != nil {
		return nil, err
	}

	out := &types.Ingress{
		Annotations: ingress.Annotations,
	}

	return out, nil
}

func (c *Client) UpdateIngress(ctx context.Context, id types.DeploymentID, annotations map[string]string) error {
	deploymentID := manifest.DeploymentID{ID: string(id)}
	ns := builder.DidNS(deploymentID)

	ingress, err := c.kc.GetIngress(ctx, ns, c.providerCfg.IngressHostName)
	if err != nil {
		return err
	}

	ingress.Annotations = annotations

	_, err = c.kc.UpdateIngress(ctx, ns, ingress)
	return err
}
