package kube

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/pager"
)

func (c *client) FetchAllNodeResources(ctx context.Context) (map[string]*nodeResource, error) {
	nodes, err := c.kc.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	podsClient := c.kc.CoreV1().Pods(metav1.NamespaceAll)
	podsPager := pager.New(func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
		// opts.FieldSelector = "spec.nodeName=" + nodeName
		return podsClient.List(ctx, opts)
	})

	nodeResources := make(map[string]*nodeResource)
	for _, node := range nodes.Items {
		if !c.nodeIsActive(node) {
			continue
		}

		nodeResources[node.Name] = newNodeResource(&node.Status)
	}

	// Go over each pod and sum the resources for it into the value for the pod it lives on
	err = c.foreachPodsResource(ctx, podsPager, nodeResources)
	if err != nil {
		return nil, err
	}

	return nodeResources, nil
}

func (c *client) FetchNodeResource(ctx context.Context, nodeName string) (*nodeResource, error) {
	node, err := c.kc.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	podsClient := c.kc.CoreV1().Pods(metav1.NamespaceAll)
	podsPager := pager.New(func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
		opts.FieldSelector = "spec.nodeName=" + nodeName
		return podsClient.List(ctx, opts)
	})

	nodeResources := make(map[string]*nodeResource)
	nodeResources[node.Name] = newNodeResource(&node.Status)

	// Go over each pod and sum the resources for it into the value for the pod it lives on
	err = c.foreachPodsResource(ctx, podsPager, nodeResources)
	if err != nil {
		return nil, err
	}

	return nodeResources[node.Name], nil
}

func (c *client) foreachPodsResource(ctx context.Context, podsPage *pager.ListPager, nodeResources map[string]*nodeResource) error {
	// Go over each pod and sum the resources for it into the value for the pod it lives on
	return podsPage.EachListItem(ctx, metav1.ListOptions{}, func(obj runtime.Object) error {
		pod := obj.(*corev1.Pod)
		nodeName := pod.Spec.NodeName

		entry, validNode := nodeResources[nodeName]
		if !validNode {
			return nil
		}

		for _, container := range pod.Spec.Containers {
			entry.addAllocatedResources(container.Resources.Requests)
		}

		// Add overhead for running a pod to the sum of requests
		// https://kubernetes.io/docs/concepts/scheduling-eviction/pod-overhead/
		entry.addAllocatedResources(pod.Spec.Overhead)

		nodeResources[nodeName] = entry // Map is by value, so store the copy back into the map

		return nil
	})
}

func (c *client) nodeIsActive(node corev1.Node) bool {
	ready := false
	issues := 0

	for _, cond := range node.Status.Conditions {
		switch cond.Type {
		case corev1.NodeReady:
			if cond.Status == corev1.ConditionTrue {
				ready = true
			}
		case corev1.NodeMemoryPressure:
			fallthrough
		case corev1.NodeDiskPressure:
			fallthrough
		case corev1.NodePIDPressure:
			fallthrough
		case corev1.NodeNetworkUnavailable:
			if cond.Status != corev1.ConditionFalse {
				c.log.Warnf("node in poor condition, node=%s, condition=%s, status=%s", node.Name, cond.Type, cond.Status)

				issues++
			}
		}
	}

	// If the node has been tainted, don't consider it active.
	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectNoSchedule || taint.Effect == corev1.TaintEffectNoExecute {
			issues++
		}
	}

	return ready && issues == 0
}
