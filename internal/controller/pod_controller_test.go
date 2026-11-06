package controller

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"podwatcher/internal/handler"
	"podwatcher/internal/service"
	"podwatcher/internal/store"
	"podwatcher/internal/testutil"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
)

func newTestApplicationsRouter(t *testing.T) *gin.Engine {
	t.Helper()
	eventHandler := handler.NewPodEventHandler(store.NewStore("", 0, 0))
	eventHandler.OnAdd(testutil.DriverPod("app-a-driver", "app-a", corev1.PodRunning, time.Now()), false)
	eventHandler.OnAdd(testutil.DriverPod("app-b-driver", "app-b", corev1.PodSucceeded, time.Now()), false)
	ctrl := NewPodController(service.NewPodService(nil, eventHandler))

	gin.SetMode(gin.TestMode)
	r := gin.New()
	api := r.Group("/api/v1")
	api.GET("/applications", ctrl.ListApplications)
	api.GET("/applications/:applicationId", ctrl.GetApplication)
	return r
}

func TestGetApplicationByPathID(t *testing.T) {
	r := newTestApplicationsRouter(t)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/applications/app-a", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Application store.ApplicationRecord `json:"application"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Application.ApplicationID != "app-a" {
		t.Fatalf("expected single application app-a, got %+v", resp.Application)
	}
}

func TestGetApplicationUnknownPathID(t *testing.T) {
	r := newTestApplicationsRouter(t)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/applications/app-missing", nil))

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown application, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListApplicationsApplicationIDFilter(t *testing.T) {
	r := newTestApplicationsRouter(t)

	tests := []struct {
		name      string
		path      string
		wantTotal int
		wantID    string
	}{
		{name: "filter by applicationId query", path: "/api/v1/applications?applicationId=app-b", wantTotal: 1, wantID: "app-b"},
		{name: "unknown applicationId matches nothing", path: "/api/v1/applications?applicationId=app-missing", wantTotal: 0},
		{name: "multiple statuses match both", path: "/api/v1/applications?status=running,succeeded", wantTotal: 2},
		{name: "multiple statuses match one", path: "/api/v1/applications?status=running,deleted", wantTotal: 1, wantID: "app-a"},
		{name: "multiple statuses with whitespace", path: "/api/v1/applications?status=running%2C%20succeeded", wantTotal: 2},
		{name: "unknown status matches nothing", path: "/api/v1/applications?status=missing", wantTotal: 0},
		{name: "no filter returns all", path: "/api/v1/applications", wantTotal: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tt.path, nil))

			if w.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
			}
			var resp struct {
				Applications []store.ApplicationRecord `json:"applications"`
				Count        int                       `json:"count"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if resp.Count != tt.wantTotal || len(resp.Applications) != tt.wantTotal {
				t.Fatalf("expected %d records, got total=%d len=%d body=%s",
					tt.wantTotal, resp.Count, len(resp.Applications), w.Body.String())
			}
			if tt.wantID != "" && resp.Applications[0].ApplicationID != tt.wantID {
				t.Fatalf("expected only %s, got %+v", tt.wantID, resp.Applications)
			}
		})
	}
}

func TestListApplicationsRejectsInvalidSince(t *testing.T) {
	r := newTestApplicationsRouter(t)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/applications?since=not-a-timestamp", nil))

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid since, got %d: %s", w.Code, w.Body.String())
	}
}
