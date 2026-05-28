package controller

import (
	"net/http"
	"strconv"

	"podwatcher/internal/service"

	"github.com/gin-gonic/gin"
)

type PodController struct {
	podService *service.PodService
}

func NewPodController(podService *service.PodService) *PodController {
	return &PodController{
		podService: podService,
	}
}

func (c *PodController) ListPods(ctx *gin.Context) {
	namespace := ctx.DefaultQuery("namespace", "default")
	labelSelector := ctx.Query("labelSelector")

	pods, err := c.podService.ListPods(ctx, namespace, labelSelector)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"pods":  pods,
		"count": len(pods),
	})
}

func (c *PodController) GetPod(ctx *gin.Context) {
	namespace := ctx.Param("namespace")
	name := ctx.Param("name")

	pod, err := c.podService.GetPod(ctx, namespace, name)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{
			"error": err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"pod": pod,
	})
}

func (c *PodController) GetEvents(ctx *gin.Context) {
	limit, _ := strconv.Atoi(ctx.DefaultQuery("limit", "50"))
	if limit > 200 {
		limit = 200
	}

	events := c.podService.GetEvents(limit)
	stats := c.podService.GetStats()

	ctx.JSON(http.StatusOK, gin.H{
		"events": events,
		"stats":  stats,
		"total":  len(events),
	})
}

func (c *PodController) GetPodStats(ctx *gin.Context) {
	namespace := ctx.DefaultQuery("namespace", "default")

	counts, err := c.podService.GetPodCountByPhase(ctx, namespace)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"namespace": namespace,
		"counts":    counts,
	})
}

func (c *PodController) GetSparkApplications(ctx *gin.Context) {
	startTime := ctx.Query("startTime")
	endTime := ctx.Query("endTime")

	apps, err := c.podService.GetSparkApplicationsByTimeRange(startTime, endTime)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"applications": apps,
		"totalCount":   len(apps),
	})
}

func (c *PodController) HealthCheck(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{
		"status":  "healthy",
		"service": "podwatcher",
	})
}
