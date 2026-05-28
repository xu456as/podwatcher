package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"podwatcher/internal/config"
	"podwatcher/internal/controller"
	"podwatcher/internal/handler"
	"podwatcher/internal/informer"
	"podwatcher/internal/service"
	"podwatcher/pkg/k8s"

	"github.com/gin-gonic/gin"
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

	if *showVersion {
		klog.Info("podwatcher v1.0.0")
		return
	}

	// 加载配置：环境变量 > 配置文件 > 默认值
	configPath := os.Getenv("PODWATCHER_CONFIG")
	if configPath == "" {
		configPath = *configFile
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		klog.Fatalf("Failed to load config: %v", err)
	}

	// 配置 klog 输出到文件
	setupKlog(cfg)

	// 在 setupKlog 之后解析 flag，确保 klog flag 设置生效
	flag.Parse()

	defer klog.Flush()

	k8sClient, err := k8s.NewClient(*kubeconfig)
	if err != nil {
		klog.Fatalf("Failed to create k8s client: %v", err)
	}
	klog.Info("Kubernetes client created successfully")

	eventHandler := handler.NewPodEventHandler()

	podInformerManager := informer.NewPodInformerManager(k8sClient, eventHandler)

	if err := podInformerManager.Start(); err != nil {
		klog.Fatalf("Failed to start informer: %v", err)
	}

	podService := service.NewPodService(k8sClient, eventHandler)
	podController := controller.NewPodController(podService)

	router := setupRouter(podController)

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

	if err := srv.Shutdown(ctx); err != nil {
		klog.Errorf("Server forced to shutdown: %v", err)
	}

	klog.Info("Server exited")
}

func setupRouter(podController *controller.PodController) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	router := gin.Default()

	router.Use(gin.Recovery())
	router.Use(gin.Logger())

	api := router.Group("/api/v1")
	{
		api.GET("/health", podController.HealthCheck)

		api.GET("/pods", podController.ListPods)
		api.GET("/pods/:namespace/:name", podController.GetPod)

		api.GET("/events", podController.GetEvents)

		api.GET("/stats/pods", podController.GetPodStats)

		api.GET("/spark-applications", podController.GetSparkApplications)
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
				"GET  /api/v1/spark-applications?startTime=&endTime=",
			},
		})
	})

	return router
}

func setupKlog(cfg *config.Config) {
	logCfg := cfg.Log

	// 确保 maxNum 有合理默认值
	if logCfg.MaxNum <= 0 {
		logCfg.MaxNum = 3
	}

	// 确保日志目录存在
	logDir := filepath.Dir(logCfg.File)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		klog.Fatalf("Failed to create log directory %s: %v", logDir, err)
	}

	// 设置 klog flag
	if err := flag.Set("log_file", logCfg.File); err != nil {
		klog.Fatalf("Failed to set log_file: %v", err)
	}
	if err := flag.Set("log_file_max_size", strconv.Itoa(logCfg.MaxSize)); err != nil {
		klog.Fatalf("Failed to set log_file_max_size: %v", err)
	}
	// log_file_max_num not supported by klog v2.140.0
	// 日志轮转数量通过 logrotate 或外部工具管理
	if err := flag.Set("logtostderr", "false"); err != nil {
		klog.Fatalf("Failed to set logtostderr: %v", err)
	}
	if err := flag.Set("alsologtostderr", "true"); err != nil {
		klog.Fatalf("Failed to set alsologtostderr: %v", err)
	}

	klog.InfoS("Klog configured", "logFile", logCfg.File, "maxSizeMB", logCfg.MaxSize)
}
