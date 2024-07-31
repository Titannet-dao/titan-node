package container

import (
	"fmt"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/container/kube/builder"
	"github.com/Filecoin-Titan/titan/node/container/kube/manifest"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	podReplicas = 1
	// 1M
	unitOfMemory = 1000000
	// 1m
	unitOfStorage = 1000000
	unitOfCPU     = 1000
)

func ClusterDeploymentFromDeployment(deployment *types.Deployment) (builder.IClusterDeployment, error) {
	if len(deployment.ID) == 0 {
		return nil, fmt.Errorf("deployment ID can not empty")
	}

	deploymentID := manifest.DeploymentID{ID: string(deployment.ID), Owner: deployment.Owner}
	group, err := deploymentToManifestGroup(deployment)
	if err != nil {
		return nil, err
	}

	settings := builder.ClusterSettings{
		SchedulerParams: make([]*builder.SchedulerParams, len(group.Services)),
	}

	return &builder.ClusterDeployment{
		Did:     deploymentID,
		Group:   group,
		Sparams: settings,
	}, nil
}

func deploymentToManifestGroup(deployment *types.Deployment) (*manifest.Group, error) {
	if len(deployment.Name) == 0 {
		return nil, fmt.Errorf("Deployment.Name can not empty")
	}

	if len(deployment.Services) == 0 {
		return nil, fmt.Errorf("deployment service can not empty")
	}

	services := make([]manifest.Service, 0, len(deployment.Services))
	for index, service := range deployment.Services {
		if len(service.Name) == 0 {
			service.Name = fmt.Sprintf("%s-%d", deployment.Name, index)
		}

		s, err := serviceToManifestService(service, deployment.ProviderExposeIP)
		if err != nil {
			return nil, err
		}
		services = append(services, s)
	}

	return &manifest.Group{Services: services}, nil
}

func serviceToManifestService(service *types.Service, exposeIP string) (manifest.Service, error) {
	if len(service.Image) == 0 {
		return manifest.Service{}, fmt.Errorf("service image can not empty")
	}

	resource := resourceToManifestResource(&service.ComputeResources)
	exposes, err := exposesFromIPAndPorts(exposeIP, service.Ports)
	if err != nil {
		return manifest.Service{}, err
	}

	if service.Replicas <= 0 {
		service.Replicas = podReplicas
	}

	s := manifest.Service{
		Name:      service.Name,
		Image:     service.Image,
		Args:      service.Arguments,
		Env:       envToManifestEnv(service.Env),
		Resources: &resource,
		Expose:    make([]*manifest.ServiceExpose, 0),
		Count:     service.Replicas,
		Params:    storageToServiceParams(service.ComputeResources.Storage),
		OSType:    string(service.OSType),
	}

	if len(exposes) > 0 {
		s.Expose = append(s.Expose, exposes...)
	}

	return s, nil
}

func storageToServiceParams(storages []*types.Storage) *manifest.ServiceParams {
	var out []manifest.StorageParams
	for _, storage := range storages {
		if !storage.Persistent {
			continue
		}
		out = append(out, manifest.StorageParams{Name: storage.Name, Mount: storage.Mount})
	}
	return &manifest.ServiceParams{Storage: out}
}

func envToManifestEnv(serviceEnv types.Env) []string {
	envs := make([]string, 0, len(serviceEnv))
	for k, v := range serviceEnv {
		env := fmt.Sprintf("%s=%s", k, v)
		envs = append(envs, env)
	}
	return envs
}

func resourceToManifestResource(resource *types.ComputeResources) manifest.ResourceUnits {
	var storages []*manifest.Storage
	for _, storage := range resource.Storage {
		storages = append(storages, manifest.NewStorage(storage.Name, uint64(storage.Quantity*unitOfStorage), storage.Persistent, storage.Mount))
	}
	return *manifest.NewResourceUnits(uint64(resource.CPU*unitOfCPU), uint64(resource.GPU), uint64(resource.Memory*unitOfMemory), storages)
}

func toServiceProto(protocol types.Protocol) (manifest.ServiceProtocol, error) {
	if len(protocol) == 0 {
		return manifest.TCP, nil
	}

	proto := strings.ToUpper(string(protocol))
	serviceProto := manifest.ServiceProtocol(proto)
	if serviceProto != manifest.TCP && serviceProto != manifest.UDP {
		return "", fmt.Errorf("it's neither tcp nor udp")
	}
	return serviceProto, nil
}

func exposesFromIPAndPorts(exposeIP string, ports types.Ports) ([]*manifest.ServiceExpose, error) {
	if len(ports) == 0 {
		return nil, nil
	}

	serviceExposes := make([]*manifest.ServiceExpose, 0, len(ports))
	for _, port := range ports {
		proto, err := toServiceProto(port.Protocol)
		if err != nil {
			return nil, err
		}

		externalPort := uint32(port.Port)
		if port.ExposePort != 0 {
			externalPort = uint32(port.ExposePort)
		}

		serviceExpose := &manifest.ServiceExpose{Port: uint32(port.Port), ExternalPort: externalPort, Proto: proto, Global: true}
		if len(exposeIP) > 0 {
			serviceExpose.IP = exposeIP
		}
		serviceExposes = append(serviceExposes, serviceExpose)
	}
	return serviceExposes, nil
}

func k8sDeploymentsToServices(deploymentList *appsv1.DeploymentList) ([]*types.Service, error) {
	services := make([]*types.Service, 0, len(deploymentList.Items))

	for _, deployment := range deploymentList.Items {
		s, err := k8sDeploymentToService(&deployment)
		if err != nil {
			return nil, err
		}
		services = append(services, s)
	}

	return services, nil
}

func k8sDeploymentToService(deployment *appsv1.Deployment) (*types.Service, error) {
	if len(deployment.Spec.Template.Spec.Containers) == 0 {
		return nil, fmt.Errorf("deployment container can not empty")
	}

	container := deployment.Spec.Template.Spec.Containers[0]
	service := &types.Service{
		Image:     container.Image,
		Name:      deployment.Name,
		CreatedAt: deployment.CreationTimestamp.Time,
		Env:       make(map[string]string),
		Replicas:  deployment.Status.Replicas,
	}

	service.CPU = container.Resources.Limits.Cpu().AsApproximateFloat64()
	service.Memory = container.Resources.Limits.Memory().Value() / unitOfMemory

	gpu := container.Resources.Limits.Name(builder.ResourceGPUNvidia, resource.DecimalSI).AsApproximateFloat64()
	if gpu == 0 {
		gpu = container.Resources.Limits.Name(builder.ResourceGPUAMD, resource.DecimalSI).AsApproximateFloat64()
	}
	service.GPU = gpu

	storage := int64(container.Resources.Limits.StorageEphemeral().AsApproximateFloat64()) / unitOfStorage
	service.Storage = []*types.Storage{{Quantity: storage}}

	for _, containerPort := range container.Ports {
		service.Ports = append(service.Ports, types.Port{
			Protocol:   types.Protocol(containerPort.Protocol),
			Port:       int(containerPort.ContainerPort),
			ExposePort: int(containerPort.HostPort),
		})
	}

	for _, envVar := range container.Env {
		service.Env[envVar.Name] = envVar.Value
	}

	status := types.ReplicasStatus{
		TotalReplicas:     int(deployment.Status.Replicas),
		ReadyReplicas:     int(deployment.Status.ReadyReplicas),
		AvailableReplicas: int(deployment.Status.AvailableReplicas),
	}

	service.Status = status

	if len(deployment.Status.Conditions) > 0 {
		service.ErrorMessage = deployment.Status.Conditions[len(deployment.Status.Conditions)-1].Reason
	}

	return service, nil
}

func k8sStatefulSetsToServices(statefulSets *appsv1.StatefulSetList) ([]*types.Service, error) {
	services := make([]*types.Service, 0, len(statefulSets.Items))

	for _, statefulSet := range statefulSets.Items {
		s, err := k8sStatefulSetToService(&statefulSet)
		if err != nil {
			return nil, err
		}
		services = append(services, s)
	}

	return services, nil
}

func k8sStatefulSetToService(statefulSet *appsv1.StatefulSet) (*types.Service, error) {
	if len(statefulSet.Spec.Template.Spec.Containers) == 0 {
		return nil, fmt.Errorf("deployment container can not empty")
	}

	container := statefulSet.Spec.Template.Spec.Containers[0]
	service := &types.Service{
		Image:     container.Image,
		Name:      container.Name,
		CreatedAt: statefulSet.CreationTimestamp.Time,
		Env:       make(map[string]string),
		Replicas:  statefulSet.Status.Replicas,
	}

	service.CPU = container.Resources.Limits.Cpu().AsApproximateFloat64()
	service.Memory = container.Resources.Limits.Memory().Value() / unitOfMemory
	gpu := container.Resources.Limits.Name(builder.ResourceGPUNvidia, resource.DecimalSI).AsApproximateFloat64()
	if gpu == 0 {
		gpu = container.Resources.Limits.Name(builder.ResourceGPUAMD, resource.DecimalSI).AsApproximateFloat64()
	}
	service.GPU = gpu

	service.Storage = []*types.Storage{}
	for _, template := range statefulSet.Spec.VolumeClaimTemplates {
		service.Storage = append(service.Storage, &types.Storage{
			Persistent: true,
			Quantity:   int64(template.Spec.Resources.Requests.Storage().AsApproximateFloat64()) / unitOfStorage},
		)
	}

	for _, containerPort := range container.Ports {
		service.Ports = append(service.Ports, types.Port{
			Protocol:   types.Protocol(containerPort.Protocol),
			Port:       int(containerPort.ContainerPort),
			ExposePort: int(containerPort.HostPort),
		})
	}

	for _, envVar := range container.Env {
		service.Env[envVar.Name] = envVar.Value
	}

	status := types.ReplicasStatus{
		TotalReplicas:     int(statefulSet.Status.Replicas),
		ReadyReplicas:     int(statefulSet.Status.ReadyReplicas),
		AvailableReplicas: int(statefulSet.Status.AvailableReplicas),
	}

	service.Status = status

	//if len(statefulSet.Status.Conditions) > 0 {
	//	service.ErrorMessage = statefulSet.Status.Conditions[len(statefulSet.Status.Conditions)-1].Reason
	//}

	return service, nil
}

func k8sServiceToPortMap(serviceList *corev1.ServiceList) (map[string]types.Ports, error) {
	portMap := make(map[string]types.Ports)
	for _, service := range serviceList.Items {
		serviceName := strings.TrimSuffix(service.Name, builder.SuffixForNodePortServiceName)

		ports := servicePortsToPortPairs(service.Spec.Ports)
		portMap[serviceName] = ports
	}
	return portMap, nil
}

func servicePortsToPortPairs(servicePorts []corev1.ServicePort) types.Ports {
	ports := make([]types.Port, 0, len(servicePorts))
	for _, servicePort := range servicePorts {
		port := types.Port{
			Port:       int(servicePort.TargetPort.IntVal),
			Protocol:   types.Protocol(servicePort.Protocol),
			ExposePort: int(servicePort.Port),
		}

		if servicePort.NodePort != 0 {
			port.ExposePort = int(servicePort.NodePort)
		}
		ports = append(ports, port)
	}
	return types.Ports(ports)
}
