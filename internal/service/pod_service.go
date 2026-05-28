package service

import (
	"context"
	"fmt"
	"time"

	"podwatcher/internal/handler"
	"podwatcher/pkg/k8s"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type PodService struct {
	k8sClient    *k8s.Client
	eventHandler *handler.PodEventHandler
}

func NewPodService(k8sClient *k8s.Client, eventHandler *handler.PodEventHandler) *PodService {
	return &PodService{
		k8sClient:    k8sClient,
		eventHandler: eventHandler,
	}
}

func (s *PodService) ListPods(ctx context.Context, namespace string, labelSelector string) ([]corev1.Pod, error) {
	opts := metav1.ListOptions{}

	if labelSelector != "" {
		opts.LabelSelector = labelSelector
	}

	pods, err := s.k8sClient.Clientset.CoreV1().Pods(namespace).List(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	return pods.Items, nil
}

func (s *PodService) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	pod, err := s.k8sClient.Clientset.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get pod: %w", err)
	}

	return pod, nil
}

func (s *PodService) GetEvents(limit int) []handler.PodEvent {
	return s.eventHandler.GetEvents(limit)
}

func (s *PodService) GetStats() map[string]int {
	return s.eventHandler.GetStats()
}

func (s *PodService) GetPodCountByPhase(ctx context.Context, namespace string) (map[string]int, error) {
	pods, err := s.ListPods(ctx, namespace, "")
	if err != nil {
		return nil, err
	}

	counts := make(map[string]int)
	for _, pod := range pods {
		counts[string(pod.Status.Phase)]++
	}

	return counts, nil
}

func (s *PodService) GetSparkApplicationsByTimeRange(startTime, endTime string) ([]handler.SparkApplication, error) {
	var start, end time.Time
	var err error

	if startTime != "" {
		start, err = time.Parse(time.RFC3339, startTime)
		if err != nil {
			return nil, fmt.Errorf("invalid startTime format, use RFC3339: %w", err)
		}
	} else {
		start = time.Now().Add(-24 * time.Hour)
	}

	if endTime != "" {
		end, err = time.Parse(time.RFC3339, endTime)
		if err != nil {
			return nil, fmt.Errorf("invalid endTime format, use RFC3339: %w", err)
		}
	} else {
		end = time.Now()
	}

	return s.eventHandler.GetSparkApps(start, end), nil
}
