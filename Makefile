APP_NAME = podwatcher
BUILD_DIR = bin
GO_FILES = $(shell find . -name '*.go' -type f)

LDFLAGS = -ldflags="-s -w"

CHART = ./charts/podwatcher
OVERLAYS = ./kustomize/overlays

.PHONY: all build run clean test mod docker-build docker-run render render-dev render-uat render-prod deploy-dev deploy-uat deploy-prod diff-dev diff-uat diff-prod help

all: clean build

build:
	@echo "Building $(APP_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(APP_NAME) ./cmd/server
	@echo "Build complete: $(BUILD_DIR)/$(APP_NAME)"

run:
	@echo "Running $(APP_NAME)..."
	go run ./cmd/server $(ARGS)

dev:
	@echo "Running in dev mode..."
	go run ./cmd/server -kubeconfig=$(HOME)/.kube/config

test:
	go test -v ./...

clean:
	@echo "Cleaning..."
	rm -rf $(BUILD_DIR)/
	go clean
	@echo "Clean complete"

mod:
	go mod download
	go mod tidy

fmt:
	go fmt ./...

lint:
	golangci-lint run

docker-build:
	docker build -t $(APP_NAME):latest .

docker-run:
	docker run -v $(HOME)/.kube/config:/root/.kube/config -p 8080:8080 $(APP_NAME):latest

render: render-dev render-uat render-prod

render-dev:
	helm template $(APP_NAME) $(CHART) -f $(OVERLAYS)/dev/values.yaml > $(OVERLAYS)/dev/rendered.yaml

render-uat:
	helm template $(APP_NAME) $(CHART) -f $(OVERLAYS)/uat/values.yaml > $(OVERLAYS)/uat/rendered.yaml

render-prod:
	helm template $(APP_NAME) $(CHART) -f $(OVERLAYS)/prod/values.yaml > $(OVERLAYS)/prod/rendered.yaml

deploy-dev: render-dev
	kubectl apply -k $(OVERLAYS)/dev

deploy-uat: render-uat
	kubectl apply -k $(OVERLAYS)/uat

deploy-prod: render-prod
	kubectl apply -k $(OVERLAYS)/prod

diff-dev: render-dev
	kubectl diff -k $(OVERLAYS)/dev

diff-uat: render-uat
	kubectl diff -k $(OVERLAYS)/uat

diff-prod: render-prod
	kubectl diff -k $(OVERLAYS)/prod

help:
	@echo "Available targets:"
	@echo "  build       - Build the application"
	@echo "  run         - Run the application (use ARGS='-kubeconfig=/path' for flags)"
	@echo "  dev         - Run in dev mode with default kubeconfig"
	@echo "  clean       - Clean build artifacts"
	@echo "  test        - Run tests"
	@echo "  mod         - Download and tidy dependencies"
	@echo "  fmt         - Format code"
	@echo "  docker-build- Build Docker image"
	@echo "  docker-run  - Run Docker container"
		@echo "  render      - Helm template to kustomize/base/rendered.yaml"
		@echo "  deploy-dev  - Deploy to dev"
		@echo "  deploy-uat  - Deploy to uat"
		@echo "  deploy-prod - Deploy to prod"
		@echo "  diff-dev    - Diff dev overlay"
		@echo "  diff-uat    - Diff uat overlay"
		@echo "  diff-prod   - Diff prod overlay"
