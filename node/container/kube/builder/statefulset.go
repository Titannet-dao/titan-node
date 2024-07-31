package builder

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type StatefulSet interface {
	workloadBase
	Create() (*appsv1.StatefulSet, error)
	Update(obj *appsv1.StatefulSet) (*appsv1.StatefulSet, error)
}

type statefulSet struct {
	Workload
}

var _ StatefulSet = (*statefulSet)(nil)

func BuildStatefulSet(workload Workload) StatefulSet {
	s := &statefulSet{
		Workload: workload,
	}
	return s
}

func (b *statefulSet) Create() (*appsv1.StatefulSet, error) { // nolint:golint,unparam
	falseValue := false

	kdeployment := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:   b.Name(),
			Labels: b.labels(),
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: b.labels(),
			},
			Replicas: b.replicas(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: b.labels(),
				},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &falseValue,
					},
					AutomountServiceAccountToken: &falseValue,
					Containers:                   []corev1.Container{b.container()},
					ImagePullSecrets:             b.imagePullSecrets(),
					NodeSelector:                 map[string]string{titanNodeSelector: b.osType()},
					Tolerations:                  b.tolerations(),
				},
			},
			VolumeClaimTemplates: b.persistentVolumeClaims(),
		},
	}

	return kdeployment, nil
}

func (b *statefulSet) Update(obj *appsv1.StatefulSet) (*appsv1.StatefulSet, error) { // nolint:golint,unparam
	obj.Labels = b.labels()
	obj.Spec.Selector.MatchLabels = b.labels()
	obj.Spec.Replicas = b.replicas()
	obj.Spec.Template.Labels = b.labels()
	obj.Spec.Template.Spec.Containers = []corev1.Container{b.container()}
	obj.Spec.Template.Spec.ImagePullSecrets = b.imagePullSecrets()
	// obj.Spec.VolumeClaimTemplates = b.persistentVolumeClaims()
	obj.Spec.Template.Spec.NodeSelector = map[string]string{titanNodeSelector: b.osType()}

	return obj, nil
}
