# Repository Guidelines

## Project Structure & Module Organization

`podwatcher` is a Go service that watches Kubernetes pods and exposes a Gin HTTP API. The executable lives in `cmd/server`; private domain logic is split by responsibility under `internal/config`, `internal/controller`, `internal/handler`, `internal/informer`, and `internal/service`. Reusable Kubernetes client code belongs in `pkg/k8s`. Deployment assets are in `charts/podwatcher`, `kustomize/overlays`, `docker`, and `scripts`; captured setup examples and logs are in `setup/`.

## Build, Test, and Development Commands

- `make build` - compile `cmd/server` into `bin/podwatcher`.
- `make run ARGS=-kubeconfig=$HOME/.kube/config` - run the server with custom flags.
- `make dev` - run locally with the default kubeconfig.
- `make test` - run all Go tests with verbose output.
- `make fmt` / `make lint` - format Go files and run `golangci-lint`.
- `make mod` - download and tidy Go modules.
- `make diff-dev` - render the dev chart and preview its Kustomize overlay before deployment.
- `./scripts/build_image.sh -t <tag> [-p]` - build the Docker image, optionally pushing it.

## Coding Style & Naming Conventions

Use standard Go formatting: tabs, gofmt-compatible ordering, and idiomatic error handling. Follow standard Go naming (`MixedCaps` for exported identifiers, `mixedCaps` for private ones). Keep imports grouped as standard library, this project, then external packages. Prefer small constructors such as `NewPodService`, and place reusable functionality in `pkg/`; keep it domain-specific in `internal/`.

## Testing Guidelines

There is currently no test suite. Add tests beside the code as `*_test.go`, for example `internal/service/pod_service_test.go`. Use table-driven tests for configuration parsing, event filtering, and API handlers. Run `make test` before opening a pull request; add focused tests for every bug fix or behavior change when practical.

## Commit & Pull Request Guidelines

Recent commits use short, imperative, lowercase summaries such as `update code` and `update api /spark-applications`. Continue that pattern, but be more specific, e.g. `add event pagination`. Pull requests should include a clear purpose, implementation notes, test results, and related issue links. Include before/after output or deployment diffs for API, Helm, or Kustomize changes.

## Security & Configuration Tips

Do not commit kubeconfigs, credentials, live cluster manifests, or private logs. Use `docker/config.example.yaml` and environment variables such as `PODWATCHER_CONFIG` for local overrides. Treat cluster-impacting changes cautiously and run the appropriate `make diff-*` target before deployment.
