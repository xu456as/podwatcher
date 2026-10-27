package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"podwatcher/internal/config"
	"podwatcher/internal/controller"
	"podwatcher/internal/elasticsearch"
	"podwatcher/internal/handler"
	"podwatcher/internal/informer"
	"podwatcher/internal/prometheus"
	"podwatcher/internal/service"
	"podwatcher/internal/store"
	"podwatcher/internal/telemetry"
	"podwatcher/pkg/k8s"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"k8s.io/klog/v2"
)

var (
	kubeconfig  = flag.String("kubeconfig", "", "Path to kubeconfig file")
	port        = flag.String("port", "8080", "HTTP server port")
	configFile  = flag.String("config", "", "Path to config file (override by PODWATCHER_CONFIG env)")
	showVersion = flag.Bool("version", false, "Show version info")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	if *showVersion {
		klog.Info("podwatcher v1.0.0")
		return
	}

	configPath := os.Getenv("PODWATCHER_CONFIG")
	if configPath == "" {
		configPath = *configFile
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		klog.Fatalf("Failed to load config: %v", err)
	}

	setupKlog(cfg)
	if configPath == "" {
		klog.Info("Configuration loaded from defaults and environment overrides; no config file specified")
	} else {
		klog.Infof("Configuration loaded from %s", configPath)
	}

	defer klog.Flush()

	shutdownTracer, err := telemetry.InitTracer(context.Background(), cfg.Tracing.Endpoint, "podwatcher")
	if err != nil {
		klog.Fatalf("Failed to initialize tracer: %v", err)
	}
	shutdownMeter, err := telemetry.InitMeter(context.Background(), cfg.Tracing.Endpoint, "podwatcher")
	if err != nil {
		klog.Fatalf("Failed to initialize meter provider: %v", err)
	}
	if cfg.Tracing.Endpoint == "" {
		klog.Info("OpenTelemetry tracing/metrics disabled: tracing.endpoint is empty")
	} else {
		klog.Infof("OpenTelemetry tracing and metrics enabled (endpoint=%s)", cfg.Tracing.Endpoint)
	}

	k8sClient, err := k8s.NewClient(*kubeconfig)
	if err != nil {
		klog.Fatalf("Failed to create k8s client: %v", err)
	}
	klog.Info("Kubernetes client created successfully")

	clusterName := loadClusterIdentity(k8sClient, cfg.Cluster)

	// Prometheus config overrides depend on cluster identity:
	// PROMETHEUS_ADDRESS_<cluster> / PROMETHEUS_TOKEN_<cluster> take precedence,
	// falling back to the unprefixed PROMETHEUS_ADDRESS / PROMETHEUS_TOKEN.
	cfg.Prometheus.Address = envByCluster("PROMETHEUS_ADDRESS", clusterName, cfg.Prometheus.Address)
	cfg.Prometheus.Token = envByCluster("PROMETHEUS_TOKEN", clusterName, cfg.Prometheus.Token)

	stateStore := store.NewStore(cfg.Pods.StateFile, cfg.Pods.MaxApplications, cfg.Pods.MaxPodRecords, store.WithCluster(clusterName))
	// One-time migration from the pre-single-file layout: only when no state
	// snapshot exists yet, read the legacy sibling files if present.
	if _, statErr := os.Stat(cfg.Pods.StateFile); errors.Is(statErr, os.ErrNotExist) {
		stateDir := filepath.Dir(cfg.Pods.StateFile)
		stateStore.ImportLegacy(
			filepath.Join(stateDir, "applications.json"),
			filepath.Join(stateDir, "pod-records.json"),
		)
	}
	var handlerOptions []handler.Option
	if cfg.Elasticsearch.Address != "" {
		publisher, err := elasticsearch.NewClient(cfg.Elasticsearch, clusterName)
		if err != nil {
			klog.Fatalf("Failed to configure Elasticsearch: %v", err)
		}
		handlerOptions = append(handlerOptions, handler.WithPublisher(publisher), handler.WithDebug(cfg.Elasticsearch.IsDebug))
		klog.Infof("Elasticsearch publishing enabled: index=%s timeoutSeconds=%d isDebug=%t insecureSkipVerify=%t", cfg.Elasticsearch.Index, cfg.Elasticsearch.TimeoutSeconds, cfg.Elasticsearch.IsDebug, cfg.Elasticsearch.InsecureSkipVerify)
	} else {
		klog.Warning("Elasticsearch publishing disabled: elasticsearch.address is empty; check the loaded config file")
	}
	eventHandler := handler.NewPodEventHandler(stateStore, handlerOptions...)

	resumeStore := buildResumeStore(cfg.Pods)
	flushInterval := time.Duration(cfg.Pods.CheckpointFlushSecs) * time.Second
	podInformerManager := informer.NewPodInformerManager(k8sClient, eventHandler, resumeStore, informer.WithFlushInterval(flushInterval))

	if err := podInformerManager.Start(); err != nil {
		klog.Fatalf("Failed to start informer: %v", err)
	}

	podService := service.NewPodService(k8sClient, eventHandler)
	podController := controller.NewPodController(podService)

	// The Prometheus gateway is always registered; an incomplete configuration
	// is reported to gateway requests instead of failing startup.
	promGateway := controller.NewPrometheusGateway(prometheus.NewClient(prometheus.Config{
		Address:            cfg.Prometheus.Address,
		Token:              cfg.Prometheus.Token,
		TimeoutSeconds:     cfg.Prometheus.TimeoutSeconds,
		InsecureSkipVerify: cfg.Prometheus.InsecureSkipVerify,
	}))
	if cfg.Prometheus.Address == "" {
		klog.Warning("Prometheus gateway is not configured (prometheus.address is empty); /api/v1/prometheus/query_range requests will fail")
	} else {
		klog.Infof("Prometheus gateway enabled (address=%s)", cfg.Prometheus.Address)
	}

	router := setupRouter(podController, promGateway)

	srv := &http.Server{
		Addr:    ":" + *port,
		Handler: router,
	}

	go func() {
		klog.Infof("Starting HTTP server on port %s", *port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Fatalf("Failed to start server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	klog.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	podInformerManager.Stop()
	stateStore.Stop()

	if err := srv.Shutdown(ctx); err != nil {
		klog.Errorf("Server forced to shutdown: %v", err)
	}

	if err := shutdownTracer(ctx); err != nil {
		klog.Errorf("Failed to flush tracer: %v", err)
	}

	if err := shutdownMeter(ctx); err != nil {
		klog.Errorf("Failed to flush metrics: %v", err)
	}

	klog.Info("Server exited")
}

// buildResumeStore selects the resourceVersion checkpoint backend: an HTTP
// checkpoint API when configured, a local file for dev, or no persistence.
// The manager watches all namespaces, so the checkpoint key is scoped to
// _all; a namespaced deployment should use its namespace in the key.
func buildResumeStore(cfg config.PodsConfig) store.ResumeStore {
	switch {
	case cfg.CheckpointURL != "":
		key := "podwatcher:resume:_all"
		klog.Infof("Checkpointing watch resourceVersion via %s (key=%s)", cfg.CheckpointURL, key)
		return store.NewHTTPResumeStore(cfg.CheckpointURL, key)
	case cfg.ResumeFile != "":
		return store.NewFileResumeStore(cfg.ResumeFile)
	default:
		return store.NewNopResumeStore()
	}
}

// loadClusterIdentity reads the configured environment variable when set,
// otherwise the legacy ConfigMap once at startup. Namespace resolution: explicit config ->
// POD_NAMESPACE (pod namespace via downward API) -> default. A failure is
// non-fatal: records are stored without cluster and a restart picks it up.
func loadClusterIdentity(k8sClient *k8s.Client, cfg config.ClusterConfig) string {
	if cfg.NameEnv != "" {
		cluster := strings.TrimSpace(os.Getenv(cfg.NameEnv))
		if cluster == "" {
			klog.Warningf("Cluster environment variable %q is empty", cfg.NameEnv)
		}
		return cluster
	}
	namespace := cfg.ConfigMapNamespace
	if namespace == "" {
		namespace = os.Getenv("POD_NAMESPACE")
	}
	if namespace == "" {
		namespace = "default"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cluster, err := k8s.ConfigMapValue(ctx, k8sClient.Clientset, namespace, cfg.ConfigMapName, cfg.ConfigMapKey)
	if err != nil {
		klog.Warningf("Failed to load cluster identity from configmap %s/%s[%s]; records will be stored without cluster: %v",
			namespace, cfg.ConfigMapName, cfg.ConfigMapKey, err)
		return ""
	}
	klog.Infof("Cluster identity loaded: %s", cluster)
	return cluster
}

func setupRouter(podController *controller.PodController, promGateway *controller.PrometheusGateway) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	router := gin.Default()

	router.Use(gin.Recovery())
	router.Use(gin.Logger())
	router.Use(otelgin.Middleware("podwatcher"))

	api := router.Group("/api/v1")
	{
		api.GET("/health", podController.HealthCheck)

		api.GET("/pods", podController.ListPods)
		api.GET("/pods/:namespace/:name", podController.GetPod)

		api.GET("/events", podController.GetEvents)

		api.GET("/stats/pods", podController.GetPodStats)

		api.GET("/applications", podController.ListApplications)
		api.GET("/applications/:applicationId", podController.GetApplication)

		api.GET("/pod-records", podController.ListPodRecords)

		api.GET("/prometheus/query_range", promGateway.QueryRange)
	}

	router.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Gin K8s Informer Server",
			"version": "1.0.0",
			"endpoints": []string{
				"GET  /api/v1/health",
				"GET  /api/v1/pods?namespace=&labelSelector=",
				"GET  /api/v1/pods/:namespace/:name",
				"GET  /api/v1/events?limit=",
				"GET  /api/v1/stats/pods?namespace=",
				"GET  /api/v1/applications?since=<RFC3339>&limit=&applicationId=&status=<csv>",
				"GET  /api/v1/applications/:applicationId",
				"GET  /api/v1/pod-records?applicationId=&role=&node=&status=&startTime=&endTime=&limit=",
				"GET  /api/v1/prometheus/query_range?query=&start=&end=&step=",
			},
		})
	})

	return router
}

func setupKlog(cfg *config.Config) {
	logCfg := cfg.Log

	if logCfg.MaxNum <= 0 {
		logCfg.MaxNum = 3
	}

	logDir := filepath.Dir(logCfg.File)
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		klog.Fatalf("Failed to create log directory %s: %v", logDir, err)
	}

	if err := flag.Set("log_file", logCfg.File); err != nil {
		klog.Fatalf("Failed to set log_file: %v", err)
	}
	if err := flag.Set("log_file_max_size", strconv.Itoa(logCfg.MaxSize)); err != nil {
		klog.Fatalf("Failed to set log_file_max_size: %v", err)
	}
	if err := flag.Set("logtostderr", "false"); err != nil {
		klog.Fatalf("Failed to set logtostderr: %v", err)
	}
	if err := flag.Set("alsologtostderr", "true"); err != nil {
		klog.Fatalf("Failed to set alsologtostderr: %v", err)
	}

	klog.InfoS("Klog configured", "logFile", logCfg.File, "maxSizeMB", logCfg.MaxSize)
}

// envByCluster resolves a value from environment variables with cluster-aware
// key selection. It first tries <base>_<upper-cluster> (e.g. PROMETHEUS_ADDRESS_UAT1),
// then falls back to <base> (e.g. PROMETHEUS_ADDRESS), then the provided fallback.
// An empty cluster name skips the cluster-specific lookup.
func envByCluster(base, cluster, fallback string) string {
	if cluster != "" {
		key := base + "_" + strings.ToUpper(cluster)
		if v := os.Getenv(key); v != "" {
			return v
		}
	}
	if v := os.Getenv(base); v != "" {
		return v
	}
	return fallback
}
