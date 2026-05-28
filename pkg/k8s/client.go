package k8s

import (
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/klog/v2"
)

type Client struct {
	Clientset *kubernetes.Clientset
	Config    *rest.Config
}

func NewClient(kubeconfigPath string) (*Client, error) {
	var config *rest.Config
	var err error

	if kubeconfigPath != "" {
		klog.Infof("Using kubeconfig: %s", kubeconfigPath)
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, err
		}
	} else {
		// 优先使用 in-cluster config（Pod 内运行时）
		config, err = rest.InClusterConfig()
		if err != nil {
			// in-cluster 失败，回退到本地 kubeconfig
			if home := homedir.HomeDir(); home != "" {
				kubeconfigPath = filepath.Join(home, ".kube", "config")
			}
			if kubeconfigPath != "" {
				klog.Infof("In-cluster config failed, using kubeconfig: %s", kubeconfigPath)
				config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		} else {
			klog.Info("Using in-cluster config")
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &Client{
		Clientset: clientset,
		Config:    config,
	}, nil
}
