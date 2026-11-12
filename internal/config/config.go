package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// LogConfig controls klog log output.
type LogConfig struct {
	File    string `yaml:"file"`    // log file path
	MaxSize int    `yaml:"maxSize"` // max size of each log file in MB
	MaxNum  int    `yaml:"maxNum"`  // number of old log files to keep
}

// PodsConfig controls the pod informer and the application/pod record stores.
type PodsConfig struct {
	ResumeFile          string `yaml:"resumeFile"`             // file to persist the pod watch resourceVersion
	MaxApplications     int    `yaml:"maxApplications"`        // max application records kept (oldest deleted evicted)
	StateFile           string `yaml:"stateFile"`              // file to persist applications and driver/executor pod coordinates
	MaxPodRecords       int    `yaml:"maxPodRecords"`          // max pod records kept (oldest deleted evicted)
	CheckpointURL       string `yaml:"checkpointUrl"`          // downstream checkpoint API base URL; when set, resourceVersion is checkpointed over HTTP instead of resumeFile
	CheckpointFlushSecs int    `yaml:"checkpointFlushSeconds"` // resourceVersion checkpoint flush interval in seconds
}

// ClusterConfig selects an environment variable or legacy ConfigMap for the
// cluster identity stamped into application, pod and Elasticsearch records.
type ClusterConfig struct {
	NameEnv            string `yaml:"nameEnv"`            // environment variable holding the cluster name
	ConfigMapName      string `yaml:"configMapName"`      // configmap holding the cluster identity
	ConfigMapNamespace string `yaml:"configMapNamespace"` // namespace of the configmap; empty uses the pod namespace, then default
	ConfigMapKey       string `yaml:"configMapKey"`       // key whose value is the cluster identity
}

// ElasticsearchConfig configures lifecycle event indexing. An empty address disables it.
type ElasticsearchConfig struct {
	IsDebug            bool   `yaml:"isDebug"`
	TimeoutSeconds     int    `yaml:"timeoutSeconds"`
	InsecureSkipVerify bool   `yaml:"insecureSkipVerify"`
	Address            string `yaml:"address"`
	Username           string `yaml:"username"`
	PasswordEnv        string `yaml:"passwordEnv"`
	Index              string `yaml:"index"`
}

// PrometheusConfig configures the Prometheus gateway. Address comes from YAML
// or PODWATCHER_PROMETHEUS_ADDRESS; the bearer token comes from
// PODWATCHER_PROMETHEUS_TOKEN.
type PrometheusConfig struct {
	TimeoutSeconds     int    `yaml:"timeoutSeconds"`
	InsecureSkipVerify bool   `yaml:"insecureSkipVerify"`
	Address            string `yaml:"address"`
	Token              string `yaml:"token"`
}

// PodMetricsQuery is one metric export: a PromQL range-query template plus an
// optional post-processing flag.
type PodMetricsQuery struct {
	// Expr is a PromQL template; {namespace} and {pod} are substituted per
	// deleted pod.
	Expr string `yaml:"expr"`
	// Accumulate replaces the per-step values with their running time
	// integral (value x stepSeconds, summed in query order), turning a gauge
	// such as working-set MB into a cumulative MB-seconds curve. Prometheus
	// has no cumulative operator, so gauge-to-counter derivation happens in
	// the exporter.
	Accumulate bool `yaml:"accumulate"`
}

// PodMetricsExportConfig tunes the asynchronous exporter writing deleted
// Spark pods' Prometheus metrics to Elasticsearch. Invalid values fall back
// to the exporter's defaults.
type PodMetricsExportConfig struct {
	// Enabled gates the exporter. Defaults to false so the feature is opt-in.
	// Requires elasticsearch.address and prometheus.address to also be set
	// in order to actually run; main.go checks all three before wiring up.
	Enabled bool `yaml:"enabled"`
	// Index receives the POD_METRICS documents. Keeping them in a dedicated
	// index isolates their label-heavy mapping from the lifecycle event
	// mapping; an empty value falls back to elasticsearch.index.
	Index       string `yaml:"index"`
	QueueSize   int    `yaml:"queueSize"` // pending pod exports buffered before new ones are dropped
	WorkerCount int    `yaml:"workers"`   // concurrent export workers
	StepSeconds int    `yaml:"stepSeconds"`
	// ShortWindowMinutes is the lifecycle-window threshold, in minutes. Pods
	// whose window is shorter are sampled with a fixed 30s step so short-lived
	// executors still yield several points; longer windows use StepSeconds to
	// bound the number of samples per query. Non-positive values fall back to
	// the exporter default.
	ShortWindowMinutes int `yaml:"shortWindowMinutes"`
	// Queries maps an export name to its query. Entries merge with the
	// built-in defaults, so one added entry exports one more metric.
	Queries map[string]PodMetricsQuery `yaml:"queries"`
}

// TracingConfig configures OpenTelemetry trace export over OTLP/HTTP.
// An empty endpoint disables tracing.
type TracingConfig struct {
	Endpoint string `yaml:"endpoint"` // OTLP/HTTP collector endpoint, e.g. http://opentelemetry-collector-collector.otel.svc.cluster.local:4318
}

// Config is the application configuration.
type Config struct {
	Log              LogConfig              `yaml:"log"`
	Pods             PodsConfig             `yaml:"pods"`
	Cluster          ClusterConfig          `yaml:"cluster"`
	Elasticsearch    ElasticsearchConfig    `yaml:"elasticsearch"`
	Prometheus       PrometheusConfig       `yaml:"prometheus"`
	PodMetricsExport PodMetricsExportConfig `yaml:"podMetricsExport"`
	Tracing          TracingConfig          `yaml:"tracing"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		Log: LogConfig{
			File:    "/var/log/podwatcher/podwatcher.log",
			MaxSize: 100,
			MaxNum:  3,
		},
		Pods: PodsConfig{
			ResumeFile:          "/var/log/podwatcher/pods-resume.json",
			StateFile:           "/var/log/podwatcher/state.json",
			MaxApplications:     5000,
			MaxPodRecords:       10000,
			CheckpointFlushSecs: 10,
		},
		Cluster: ClusterConfig{
			ConfigMapName: "cluster-cm",
			ConfigMapKey:  "currentCluster",
		},
		Elasticsearch: ElasticsearchConfig{PasswordEnv: "ES_PASSWORD", Index: "podwatcher-events", TimeoutSeconds: 20},
		Prometheus:    PrometheusConfig{TimeoutSeconds: 20},
		PodMetricsExport: PodMetricsExportConfig{
			Index:       "podwatcher-pod-metrics",
			QueueSize:   256,
			WorkerCount: 2,
			StepSeconds: 60,
			// Windows under five minutes get the exporter's fixed 30s step.
			ShortWindowMinutes: 5,
			Queries: map[string]PodMetricsQuery{
				// The Spark main container is always named "main", so exact
				// container matching yields one series per query — no summing,
				// no cAdvisor pseudo-series (POD/empty) to exclude. Pods whose
				// main container has another name produce no data and can be
				// covered by overriding these templates.
				// CPU cores in use by the main container (counter rate).
				"cpuUsage": PodMetricsQuery{Expr: `rate(container_cpu_usage_seconds_total{namespace="{namespace}",pod="{pod}",container="main"}[5m])`},
				// Cumulative CPU core-seconds; the single series' last point is
				// the pod's lifetime vcoreSeconds total (resets on container
				// restart — sum positive deltas to be exact).
				"cpuVcoreSeconds": PodMetricsQuery{Expr: `container_cpu_usage_seconds_total{namespace="{namespace}",pod="{pod}",container="main"}`},
				// Working set: the memory the kernel counts for reclaim/OOM.
				"memoryWorkingSet": PodMetricsQuery{Expr: `container_memory_working_set_bytes{namespace="{namespace}",pod="{pod}",container="main"}`},
				// Cumulative memory MB-seconds, the counterpart of
				// cpuVcoreSeconds: each step's working-set MB is integrated
				// (x stepSeconds, running sum) by the exporter.
				"memoryMbSeconds": PodMetricsQuery{
					Expr:       `container_memory_working_set_bytes{namespace="{namespace}",pod="{pod}",container="main"} / 1024 / 1024`,
					Accumulate: true,
				},
				// Writable-layer filesystem usage.
				"fsUsage": PodMetricsQuery{Expr: `container_fs_usage_bytes{namespace="{namespace}",pod="{pod}",container="main"}`},
				// kube-state-metrics gauges, one series per resource.
				"resourceRequests": PodMetricsQuery{Expr: `kube_pod_container_resource_requests{namespace="{namespace}",pod="{pod}",container="main"}`},
				"resourceLimits":   PodMetricsQuery{Expr: `kube_pod_container_resource_limits{namespace="{namespace}",pod="{pod}",container="main"}`},
			},
		},
		Tracing: TracingConfig{
			Endpoint: "http://opentelemetry-collector-collector.otel.svc.cluster.local:4318",
		},
	}
}

// Load reads configuration from file (optional) and applies environment
// variable overrides: env > config file > defaults.
func Load(configPath string) (*Config, error) {
	cfg := DefaultConfig()

	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("read config file %s: %w", configPath, err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config file: %w", err)
		}
	}

	if v := os.Getenv("PODWATCHER_LOG_FILE"); v != "" {
		cfg.Log.File = v
	}
	if v := os.Getenv("PODWATCHER_LOG_MAX_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Log.MaxSize = n
		}
	}
	if v := os.Getenv("PODWATCHER_LOG_MAX_NUM"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Log.MaxNum = n
		}
	}
	if v := os.Getenv("PODWATCHER_PODS_RESUME_FILE"); v != "" {
		cfg.Pods.ResumeFile = v
	}
	if v := os.Getenv("PODWATCHER_APPLICATIONS_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Pods.MaxApplications = n
		}
	}
	if v := os.Getenv("PODWATCHER_STATE_FILE"); v != "" {
		cfg.Pods.StateFile = v
	}
	if v := os.Getenv("PODWATCHER_POD_RECORDS_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Pods.MaxPodRecords = n
		}
	}

	if v := os.Getenv("PODWATCHER_CHECKPOINT_URL"); v != "" {
		cfg.Pods.CheckpointURL = v
	}
	if v := os.Getenv("PODWATCHER_CHECKPOINT_FLUSH_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Pods.CheckpointFlushSecs = n
		}
	}

	if v := os.Getenv("PODWATCHER_CLUSTER_CM_NAME"); v != "" {
		cfg.Cluster.ConfigMapName = v
	}
	if v := os.Getenv("PODWATCHER_CLUSTER_CM_NAMESPACE"); v != "" {
		cfg.Cluster.ConfigMapNamespace = v
	}
	if v := os.Getenv("PODWATCHER_CLUSTER_CM_KEY"); v != "" {
		cfg.Cluster.ConfigMapKey = v
	}

	if v := os.Getenv("PODWATCHER_OTEL_ENDPOINT"); v != "" {
		cfg.Tracing.Endpoint = v
	}

	return cfg, nil
}
