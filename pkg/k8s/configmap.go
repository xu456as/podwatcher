package k8s

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ConfigMapValue reads a single string key from a ConfigMap. A missing key
// or blank value is an error.
func ConfigMapValue(ctx context.Context, clientset kubernetes.Interface, namespace, name, key string) (string, error) {
	cm, err := clientset.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get configmap %s/%s: %w", namespace, name, err)
	}
	value, ok := cm.Data[key]
	if !ok {
		return "", fmt.Errorf("configmap %s/%s missing key %q", namespace, name, key)
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("configmap %s/%s key %q is empty", namespace, name, key)
	}
	return value, nil
}
