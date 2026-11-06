# podwatcher

## cluster name and Elasticsearch events

Configure the environment variable names and Elasticsearch connection in YAML:

```yaml
cluster:
  nameEnv: CLUSTER_NAME
elasticsearch:
  address: https://elasticsearch.example:9200
  username: podwatcher
  passwordEnv: ES_PASSWORD
  index: podwatcher-events
  timeoutSeconds: 20
  insecureSkipVerify: false
```

The cluster name is read once from `CLUSTER_NAME`; when `nameEnv` is empty,
the existing ConfigMap lookup is used. When `nameEnv` is configured but its
value is empty, a warning is logged and records have no cluster identity.
The password is read only from the configured environment variable, never
from YAML. An empty `address` disables Elasticsearch publishing. When enabled,
an invalid address/index or a missing password for a configured username
fails startup. Select your actual index name in `elasticsearch.index`.

All adds and deletes passing the existing `spark-role=driver|executor` and
`appSparkID` filter are written as separate ES documents. Startup snapshot
adds are included as `ADDED` with `isInitialSnapshot: true`; raw Pod updates are not exported.
Documents contain `type`, `cluster`, `@timestamp`, `isInitialSnapshot`, `metadata`,
and `node`. Pod spec and top-level Pod `status` are omitted. The existing `/events` diagnostic
API remains driver-only.

Both Pod events and application summaries expose top-level `applicationId`,
`podName`, and `queue` for queries and aggregations. Pod `applicationId` uses
the existing `appSparkID` resolution; `podName` uses the Pod name. Summaries
use the application's id and driver Pod name. `queue` comes from the Pod's
`queue` label (driver labels for summaries); absent values are empty strings.
Original metadata and application fields are retained except
`metadata.managedFields`, which is omitted from ES Pod documents.

Both add and delete documents carry `addedAt` from the Pod's Kubernetes
creation timestamp (omitted if unknown). Delete documents additionally carry
`deletedAt`, the time podwatcher observes the deletion, including replay or
relist reconciliation. It is not the actual historical deletion time or the
Pod's `metadata.deletionTimestamp` (deletion request time). `@timestamp` is
the publishing attempt time. `isInitialSnapshot` is a boolean source marker,
not a timestamp. This replaces the earlier `initial` field for new writes;
existing ES documents are not migrated.

Writes use HTTP Basic authentication and `PUT /<index>/_doc/<id>`. Document IDs
are derived from cluster, Pod UID (or namespace/name), resourceVersion, event
type and initial flag, so retrying/replaying the same event is idempotent.
The official Go SDK `github.com/elastic/go-elasticsearch/v8` is pinned to
`v8.19.7` (latest 8.x checked on 2026-10-08). Writes use its Index API with
`EnableCompatibilityMode: true`, sending `compatible-with=8` headers for
Elasticsearch 9.x compatibility. SDK retries are disabled because the existing
publishing loop owns retries and configurable per-attempt timeouts. SDK product
verification expects `X-Elastic-Product: Elasticsearch` on successful responses.
Transport failures, HTTP 429 and HTTP 5xx are retried up to three attempts.
Each request defaults to a 20-second timeout (`timeoutSeconds` overrides it;
zero uses the default, negative values are invalid). There is no separate
16-second handler deadline. Up to three timed-out attempts plus backoff take
approximately 60.6 seconds with the default setting. Publishing is synchronous and can
slow watch processing; after retries are exhausted, the error is logged and
watch processing continues. There is no durable delivery queue: prolonged ES
outages can lose exported events even though local state/checkpoints advance.

Set `elasticsearch.isDebug: true` to log the full actual JSON document
when ES publishing fails (Pod events and application summaries). It defaults
to false and requires no command-line verbosity flag. The debug payload
is accompanied by publishing-start and publishing-success logs. Startup always
logs the loaded configuration path and whether ES publishing is enabled;
an empty `elasticsearch.address` is reported as disabled.
The debug payload
reflects document sanitization and redacts the configured ES password; it
does not include authentication headers. Normal error logs omit this payload.

For a self-signed HTTPS endpoint, set `elasticsearch.insecureSkipVerify: true`
to disable server certificate chain and hostname verification. It defaults to
false; no CA certificate file is needed when enabled. HTTPS still encrypts the
connection but the server's identity is not verified. This applies only to the
ES client's transport, not the Kubernetes client or global HTTP transport.

For Helm/Kustomize deployments, set `config.cluster` and
`config.elasticsearch` in your environment values and inject variables with
`extraEnv`. Keep the ES password in an existing Kubernetes Secret:

```yaml
extraEnv:
  - name: CLUSTER_NAME
    value: dev-cluster
  - name: ES_PASSWORD
    valueFrom:
      secretKeyRef:
        name: podwatcher-es
        key: password
```

## completed applications in Elasticsearch

The same configured index also receives application summaries with
`type: APPLICATION_COMPLETED`. Driver success, failure or deletion completes
an application; executor completion does not. Already completed drivers in
the startup snapshot are also exported. Summary IDs depend on cluster,
namespace and applicationId, so later observations update one summary rather
than creating another record.

```json
{
  "type": "APPLICATION_COMPLETED",
  "applicationId": "app-123",
  "podName": "app-123-driver",
  "queue": "root.default",
  "cluster": "dev-cluster",
  "@timestamp": "2026-10-07T10:00:05Z",
  "indexedAt": "2026-10-07T10:00:05Z",
  "finishedAt": "2026-10-07T10:00:00Z",
  "application": {
    "applicationId": "app-123",
    "namespace": "default",
    "status": "succeeded",
    "finishedAt": "2026-10-07T10:00:00Z"
  }
}
```

`application` contains the full application view, including driver labels,
creation/change times and any deletion time. Status lives under
`application.status`; Pod events omit the top-level `status` field.
A driver deleted after success/failure retains that
completion outcome in the ES summary. A driver deleted before a terminal
phase has status `deleted`.

`finishedAt` is the driver's container finish time when available, otherwise
the first observation of its terminal state or deletion. It is retained on
later updates and is also exposed by the existing application API.
`indexedAt` and `@timestamp` record the current publishing attempt time;
replays refresh these fields but preserve `finishedAt`.

To discover newly received completion summaries, filter
`type=APPLICATION_COMPLETED` and poll by `indexedAt`, using an overlapping
time window and upserting by cluster/namespace/applicationId. Do not use
only `finishedAt` as a strict discovery cursor: restart snapshots can arrive
with historical finish times. Handle equal timestamps through pagination
and overlap; ES refresh delays and write retries can delay visibility.
The existing API also supports
`/api/v1/applications?since=<changedAt>&status=succeeded,failed,deleted`.
Completion publishing uses the same retry policy and delivery limitations
as Pod events described above.

## build & push image

The Dockerfile uses BuildKit cache mounts for Go modules and compilation.
It builds without `-a`, so subsequent builds reuse unchanged dependencies.
The first build on a new builder still compiles all dependencies; keep the
same builder and its cache for faster rebuilds. BuildKit is required.

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

## application discovery (incremental)

Downstream services discover applications with a timestamp cursor instead of
a start/end time window:

```bash
curl "ip:8080/api/v1/applications?since=2026-09-14T10:00:00Z&limit=100"
```

- `since`: RFC3339 timestamp of the last consumed change; omitted for the
  first call. Returns records whose `changedAt` is strictly after `since`,
  ordered by `changedAt` ascending.
- `limit`: page size, default 100, max 200; `hasMore=true` means another page
  exists. There is no `nextSince` field: persist the last record's
  `changedAt` from each page and pass it as the next `since`.
- optional `applicationId` and `status` filters are exact matches; `status`
  accepts a comma-separated list, e.g. `status=running,pending` (spaces around
  commas are ignored), and an empty value matches every status; terminal
  discovery uses `status=succeeded,failed,deleted`.
- response fields: `applications`, `count`, `hasMore`.
- `changedAt` advances only on lifecycle changes (new application, status
  change, finished/deleted); executor heartbeats only refresh
  `lastUpdatedAt`. Timestamps are strictly increasing with 1ms resolution;
  store `since` with at least millisecond precision.
- delivery is at-least-once; upsert by `applicationId` (a newer `changedAt`
  overwrites the older state).
- after a podwatcher restart all live applications are re-reported with fresh
  `changedAt` values (watch resume replays missed events, cold start re-lists
  the cluster); cluster nodes must be NTP-synced, otherwise a backward clock
  jump after rescheduling can make new changes older than a persisted since.
- deleted records are evicted past `maxApplications` (default 1000) and a
  resourceVersion older than the cluster watch-history window forces a full
  relist; a consumer whose `since` lags beyond either boundary may miss
  terminal states.

## resourceVersion checkpoint

The watch resourceVersion is checkpointed so a restart resumes the watch and
replays events missed during downtime instead of a cold full relist:

- `pods.checkpointUrl` (env `PODWATCHER_CHECKPOINT_URL`): base URL of the
  downstream checkpoint service. When empty, `pods.resumeFile` is used for
  local development; when both are empty resume is disabled.
- `pods.checkpointFlushSeconds` (env `PODWATCHER_CHECKPOINT_FLUSH_SECONDS`,
  default 10): maximum amount of watch progress lost on a hard crash; it must
  stay well below the cluster's watch-history (etcd compaction) window.
- checkpoint API contract (opaque JSON, no auth):
  - `GET {checkpointUrl}/checkpoints/podwatcher:resume:_all` -> 200 with
    `{"resourceVersion":"..."}`, or 404 when no checkpoint exists.
  - `PUT {checkpointUrl}/checkpoints/podwatcher:resume:_all` with the same
    JSON body and `Content-Type: application/json`.
- applications and pod records are not checkpointed externally; on disk-less
  deployments set `applicationsFile`/`podRecordsFile` to empty and keep the
  deployment at a single replica (checkpoint PUT is last-writer-wins).
## re-submit spark job

kubectl delete -f spark-pi.yml && sleep 1 && kubectl apply -f spark-pi.yml

## check

response of events `curl ip:8080/api/v1/events| jq .`: `setup/events_response.json

response of spark-applications `curl ip:8080/api/v1/spark-applications| jq .`: `setup/appications_response.json

pod log: `setup/pod.log`
