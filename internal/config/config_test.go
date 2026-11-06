package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadClusterAndElasticsearch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := "cluster:\n  nameEnv: CUSTOM_CLUSTER\nelasticsearch:\n  address: https://es.example:9200\n  username: writer\n  passwordEnv: CUSTOM_PASSWORD\n  index: custom-events\n  timeoutSeconds: 30\n  insecureSkipVerify: true\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster.NameEnv != "CUSTOM_CLUSTER" || cfg.Elasticsearch.Address != "https://es.example:9200" || cfg.Elasticsearch.Username != "writer" || cfg.Elasticsearch.PasswordEnv != "CUSTOM_PASSWORD" || cfg.Elasticsearch.Index != "custom-events" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.Elasticsearch.TimeoutSeconds != 30 || !cfg.Elasticsearch.InsecureSkipVerify {
		t.Fatal("timeout/TLS configuration not loaded")
	}
}

func TestElasticsearchDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Elasticsearch.TimeoutSeconds != 20 || cfg.Elasticsearch.InsecureSkipVerify || cfg.Elasticsearch.IsDebug {
		t.Fatal("expected 20s timeout and certificate verification by default")
	}
}

func TestPodMetricsExportDefaults(t *testing.T) {
	cfg := DefaultConfig()
	e := cfg.PodMetricsExport
	if e.QueueSize != 256 || e.WorkerCount != 2 || e.StepSeconds != 60 {
		t.Fatalf("unexpected exporter defaults: %+v", e)
	}
	if e.Enabled {
		t.Fatal("podMetricsExport.enabled must default to false (opt-in)")
	}
	if e.Index != "podwatcher-pod-metrics" {
		t.Fatalf("podMetricsExport.index must default to a dedicated index, got %q", e.Index)
	}
	// The confirmed default selection must include all metrics with a
	// pod/namespace selector template.
	for _, name := range []string{
		"cpuUsage", "cpuVcoreSeconds",
		"memoryWorkingSet", "memoryMbSeconds",
		"fsUsage", "resourceRequests", "resourceLimits",
	} {
		q, ok := e.Queries[name]
		if !ok {
			t.Fatalf("default query %q missing", name)
		}
		if !strings.Contains(q.Expr, "{namespace}") || !strings.Contains(q.Expr, "{pod}") {
			t.Fatalf("default query %q must use both placeholders: %s", name, q.Expr)
		}
	}
	// The memory total is gauge-derived, so it must request accumulation.
	if !e.Queries["memoryMbSeconds"].Accumulate {
		t.Fatal("memoryMbSeconds must be configured with accumulate=true")
	}
}

func TestLoadElasticsearchDebugPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("elasticsearch:\n  isDebug: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Elasticsearch.IsDebug {
		t.Fatal("isDebug must load from YAML")
	}
}
