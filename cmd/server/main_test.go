package main

import (
	"testing"

	"podwatcher/internal/config"
)

func TestClusterNameFromConfiguredEnvironment(t *testing.T) {
	t.Setenv("TEST_CLUSTER_NAME", " cluster-a ")
	if got := loadClusterIdentity(nil, config.ClusterConfig{NameEnv: "TEST_CLUSTER_NAME"}); got != "cluster-a" {
		t.Fatalf("cluster=%q", got)
	}
	t.Setenv("TEST_CLUSTER_NAME", "")
	if got := loadClusterIdentity(nil, config.ClusterConfig{NameEnv: "TEST_CLUSTER_NAME"}); got != "" {
		t.Fatalf("cluster=%q", got)
	}
}
