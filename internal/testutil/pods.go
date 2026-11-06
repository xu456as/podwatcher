package testutil

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// DriverPod builds a Spark driver pod with the standard labels.
func DriverPod(name, appID string, phase corev1.PodPhase, created time.Time) *corev1.Pod {
	labels := map[string]string{
		"spark-role":         "driver",
		"spark-app-name":     "spark-pi-yunikorn",
		"spark-app-selector": appID,
	}
	if appID != "" {
		labels["applicationId"] = appID
		labels["appSparkID"] = appID
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              name,
			Labels:            labels,
			CreationTimestamp: metav1.NewTime(created),
		},
		Spec:   corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// ExecutorPod builds a Spark executor pod on node-2.
func ExecutorPod(name, appID string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      name,
			Labels: map[string]string{
				"spark-role":         "executor",
				"appSparkID":         appID,
				"applicationId":      appID,
				"spark-app-selector": appID,
				"spark-exec-id":      name,
			},
		},
		Spec:   corev1.PodSpec{NodeName: "node-2"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// PlainPod builds a non-Spark pod.
func PlainPod(name string, labels map[string]string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, Labels: labels},
		Status:     corev1.PodStatus{Phase: phase},
	}
}

// WithUID assigns a UID to the pod and returns it.
func WithUID(pod *corev1.Pod, uid string) *corev1.Pod {
	pod.UID = types.UID(uid)
	return pod
}
