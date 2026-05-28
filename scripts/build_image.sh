#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

IMAGE_NAME="ianwoolf/sonic_lw"
TAG=""
PUSH=false

usage() {
    echo "Usage: $0 -t <tag> [-p]"
    echo "  -t <tag>   Image tag (e.g. 0.0.1, latest)"
    echo "  -p         Push to Docker Hub after build"
    echo "Example:"
    echo "  $0 -t 0.0.1"
    echo "  $0 -t latest -p"
    exit 1
}

while getopts "t:ph" opt; do
    case $opt in
        t) TAG="$OPTARG" ;;
        p) PUSH=true ;;
        h) usage ;;
        *) usage ;;
    esac
done

if [ -z "$TAG" ]; then
    usage
fi

FULL_IMAGE="${IMAGE_NAME}:${TAG}"

echo "Building ${FULL_IMAGE}..."
docker build -t "$FULL_IMAGE" -f "$PROJECT_DIR/docker/Dockerfile" "$PROJECT_DIR"

echo "Built: ${FULL_IMAGE}"

if [ "$PUSH" = true ]; then
    echo "Pushing ${FULL_IMAGE}..."
    docker push "$FULL_IMAGE"
    echo "Pushed: ${FULL_IMAGE}"
fi
