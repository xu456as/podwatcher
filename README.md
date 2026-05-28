# podwatcher

## build & push image

```bash
./scripts/build_image.sh -t 0.0.2 -p
```

## deploy (Helm template + Kustomize)

```bash
# make targets
make deploy-dev
make deploy-uat
make deploy-prod

# diff before deploy
make diff-dev
make diff-uat
make diff-prod
```

actual commands:

```bash
# 1. helm template render
helm template podwatcher ./charts/podwatcher > ./kustomize/base/rendered.yaml

# 2. kubectl apply with overlay
kubectl apply -k ./kustomize/overlays/dev
kubectl apply -k ./kustomize/overlays/uat
kubectl apply -k ./kustomize/overlays/prod
```

## podwatcher deployment

```bash
kubectl rollout restart deployment podwatcher -n podwatcher
kubectl edit svc yunikorn-service -n yunikorn
```

## re-submit spark job

kubectl delete -f spark-pi.yml && sleep 1 && kubectl apply -f spark-pi.yml

## check

response of events `curl ip:8080/api/v1/events| jq .`: `setup/events_response.json

response of spark-applications `curl ip:8080/api/v1/spark-applications| jq .`: `setup/appications_response.json

pod log: `setup/pod.log`
