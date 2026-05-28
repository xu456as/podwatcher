#!/bin/bash
set -e

echo "Removing old image from k3s containerd..."
ctr -a /run/k3s/containerd/containerd.sock -n k8s.io images rm docker.io/library/sonic_lw:latest 2>/dev/null || true

echo "Importing new image into k3s containerd..."
docker save sonic_lw:latest | ctr -a /run/k3s/containerd/containerd.sock -n k8s.io images import -

echo "Done."
