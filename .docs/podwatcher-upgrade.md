# Podwatcher 升级需求与改动

日期：2026-10-07  
分支：`feature/podwatcher-upgrade`，基于 `feature/enhancement` 创建。

## 需求与实现

| 需求 | 实现 |
| --- | --- |
| 1. 通过环境变量感知集群名，变量名写入配置 | 新增 `cluster.nameEnv`，启动时读取指定环境变量，写入应用记录、Pod 历史记录和 ES 文档。 |
| 2. ES 地址和 username 从配置读取，password 从环境变量读取，变量名写入配置 | 新增 `elasticsearch.address`、`username`、`passwordEnv`、`index`。密码只从环境变量读取，使用 HTTP Basic Authentication。 |
| 3. 所有 add/del 信息发送到 ES | 按已确认的范围，发送现有过滤规则匹配的 Spark driver 和 executor 新增、删除事件，包括启动快照和删除 tombstone。 |
| 4. 所有完成的 application 汇总写入 ES，与 add/del 文档区分 | 写入同一配置索引，使用 `type=APPLICATION_COMPLETED` 区分应用汇总；Pod 事件使用 `ADDED` / `DELETED`。 |
| 5. application 汇总包含完成时间，便于发现新完成应用 | 汇总包含 `finishedAt` 和 `indexedAt`；前者表示应用结束时间，后者用于发现新到达或更新的汇总。 |

## 配置与部署

```yaml
cluster:
  nameEnv: CLUSTER_NAME

elasticsearch:
  address: https://your-es:9200
  username: podwatcher
  passwordEnv: ES_PASSWORD
  index: your-index
  timeoutSeconds: 20
  insecureSkipVerify: false
```

- 集群名在启动时读取一次，去除两端空白。
- `cluster.nameEnv` 为空时保留原有 ConfigMap 读取方式；配置了变量名但变量值为空时记录警告，不回退 ConfigMap。
- `elasticsearch.address` 为空时关闭 ES 写入。
- 启用 ES 后，地址或索引无效会导致启动失败；配置 username 后，指定密码变量为空也会导致启动失败。
- 索引名可配置，默认 `podwatcher-events`；实际部署时填写业务指定索引。
- Helm 新增 `extraEnv`，支持直接设置集群名和通过已有 Kubernetes Secret 注入密码。

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

部署时将连接参数放入环境对应 values 的 `config.cluster` / `config.elasticsearch`，密码不写入配置文件或提交到仓库。

## Pod add/del 文档

Pod 事件与完成应用汇总均增加三个顶层字段，便于统一查询统计：

| 字段 | Pod add/del | 完成应用汇总 |
| --- | --- | --- |
| `applicationId` | 按现有规则读取 `appSparkID` label。 | 使用汇总的 `applicationId`。 |
| `podName` | Pod 的 `metadata.name`。 | 应用的 driver Pod 名称。 |
| `queue` | Pod 的 `queue` label。 | driver 的 `queue` label。 |

字段始终输出；queue 标签不存在时为空字符串。保留原有嵌套 metadata/application 数据，现有过滤条件不变。只调整新写入文档，未回填已有 ES 数据。

过滤条件保持为：`spark-role=driver|executor`，且 `appSparkID` 非空。其他 Pod 不发送。

| 字段 | 含义 |
| --- | --- |
| `type` | `ADDED` 或 `DELETED`。 |
| `cluster` | 集群名。 |
| `@timestamp` | 本次发送开始时间，UTC。 |
| `isInitialSnapshot` | 布尔标记，是否来自启动 List 快照；快照写作 `ADDED`，并设置为 `true`，不是时间。 |
| `addedAt` | Pod 的 Kubernetes 创建时间，add 和 delete 文档均携带；创建时间未知时省略，不以观察时间替代。 |
| `deletedAt` | 仅 delete 文档携带，表示 podwatcher 观察到删除事件的时间；回放或重新 List 对账时为本次观察时间。 |
| `metadata` | Pod ObjectMeta，包含名称、namespace、UID、resourceVersion、labels 等。 |
| `node` | Pod 所在节点。 |

不导出 Pod spec，也不导出与 `podName` 平级的 `status`（PodStatus 对象）。应用汇总中的 `application.status` 保留。该变更仅影响新写入文档，不清理已有 ES 文档。原有 `/events` 接口仍只保存 driver 诊断事件，原始 Pod MODIFIED 事件不写入 ES。

`metadata.managedFields` 也从 ES Pod 文档中省略。清理只作用于发送文档的 metadata 副本，不修改 informer 的原始 Pod；失败调试日志同样不包含该字段。

`deletedAt` 不表示 Kubernetes 历史上实际删除的时刻，也不使用 `metadata.deletionTimestamp`（该字段表示请求删除的时间）。删除文档同时保留创建时间，便于后续分析生命周期。`@timestamp` 仍表示本次发送开始时间。

此次将原先不够直观的 `initial` 字段改名为 `isInitialSnapshot`，仅改变新写入格式，未迁移 ES 中已经存在的旧文档。

通过 `PUT /<index>/_doc/<id>` 写入。文档 ID 由集群、Pod UID（缺失时使用 namespace/name）、resourceVersion、事件类型和 initial 标记计算，重试与相同事件回放更新同一文档。

## 完成应用汇总

driver 进入 `Succeeded`、`Failed`，或被删除时，认为应用达到终态。executor 的完成或删除不触发应用汇总。启动快照中的已完成 driver 也会发送汇总；后续 driver 更新或删除可以更新该汇总。

示例仅展示部分应用字段：

```json
{
  "type": "APPLICATION_COMPLETED",
  "applicationId": "app-123",
  "podName": "app-123-driver",
  "queue": "root.default",
  "cluster": "cluster-a",
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

`application` 包含完整 ApplicationView，包括应用标识、Spark 名称、namespace、driver 名称、标签、状态、创建时间、状态变更时间和删除时间等。应用状态保留在 `application.status`，Pod 事件不再导出顶层 `status`。

完成时间规则：

- 优先使用现有 `containerTimes` 逻辑提取的容器终止时间。
- 缺失时，使用首次观察到 driver 终态或删除的时间。
- 后续更新保留已有 `finishedAt`；应用 API 和本地持久化也包含该字段。
- driver 成功或失败后再删除，ES 汇总依据 driver phase 保留 `succeeded` / `failed` 结果；未观察到成功或失败就被删除的应用，结果为 `deleted`。

汇总文档 ID 由集群、namespace、applicationId 和汇总类型计算，同一应用更新同一文档，不逐次追加汇总。这里沿用现有 applicationId 表示一次应用的前提。

## 下游订阅发现

`finishedAt` 表示应用实际结束时间或缺失时的观察时间。`indexedAt` 与 `@timestamp` 表示本次发送开始时间，重放和汇总更新会刷新它们；它们不是 ES 确认写入的服务端时间。

下游筛选 `type=APPLICATION_COMPLETED`，按 `indexedAt` 使用重叠时间窗口轮询，并按 cluster/namespace/applicationId 做 upsert。重叠窗口需覆盖正常的写入重试、ES refresh 和消费延迟，分页时处理相同时间戳。

不要仅以 `finishedAt > 上次游标` 做严格增量发现：重启后发现的应用可能携带历史完成时间。该方案允许重复消费，不提供无遗漏的严格订阅保证。

现有 API 仍可通过以下方式发现终态应用：

```text
/api/v1/applications?since=<changedAt>&status=succeeded,failed,deleted
```

## 代码改动

| 文件 / 模块 | 改动 |
| --- | --- |
| `internal/config/config.go` | 新增集群名环境变量配置和 ES 配置及默认值。 |
| `cmd/server/main.go` | 读取集群名、构造 ES 客户端并注入 handler；将 `flag.Parse()` 前移，使 `-config` 和 `-version` 正确生效。 |
| `internal/elasticsearch/client.go` | 新增 ES HTTP 客户端，支持 Basic Auth、配置校验、Pod 事件与应用汇总写入、稳定文档 ID、超时和重试。 |
| `internal/handler/event_handler.go` | 在 Spark driver/executor add/delete 后发送 ES 事件，在 driver 达到终态后发送完整应用汇总。 |
| `internal/store/state_store.go` | driver 删除也设置缺失的 `finishedAt`，保留已经记录的完成时间。 |
| `charts/podwatcher/values.yaml` | 增加 ES、集群环境变量配置及 `extraEnv`。 |
| `charts/podwatcher/templates/deployment.yaml` | 将 `extraEnv` 注入容器，支持 Secret 引用。 |
| `docker/config.example.yaml`、`README.md` | 补充配置、文档结构、完成时间、订阅发现和交付限制说明。 |
| `cmd/server/main_test.go`、`internal/config/config_test.go` | 验证指定环境变量读取及新增 YAML 配置解析。 |
| `internal/elasticsearch/client_test.go`、`internal/handler/event_handler_test.go` | 验证认证、重试、去重、Pod 过滤、executor/tombstone、应用成功/失败/删除/启动快照及完成时间保留。 |

## 验证结果

- 完整 `make test` 通过，包含新增测试和已有回归测试。
- Helm lint 通过；dev、uat、prod 配置渲染通过；额外环境变量及 Secret 引用渲染通过。
- `gofmt` 和 `git diff --check` 通过。
- 本地 Go 模块/编译缓存曾出现 NUL 字符及链接器异常，验证使用重新下载的临时依赖和独立缓存完成：

```bash
GOMODCACHE=/tmp/podwatcher-upgrade-go-mod \
GOCACHE=/tmp/podwatcher-upgrade-go-cache make test
```

未连接真实 ES 或部署到集群。当前改动尚未提交。

## 当前限制

### 镜像构建加速（2026-10-13）

Dockerfile 移除 `go build -a`，不再强制重编译全部依赖；同时移除不再需要的
`-installsuffix cgo`，保留 `CGO_ENABLED=0`。通过 BuildKit cache mount 缓存
`/go/pkg/mod` 和 `/root/.cache/go-build`，使源码变化后仍可复用依赖下载和
编译缓存。需要 BuildKit；首次构建或更换 builder、清理缓存后仍需完整编译。

### ES 失败时的调试数据（2026-10-13）

Pod 事件和应用汇总发布失败时，在 handler 错误日志附近补充完整实际发送
JSON 的调试日志。通过配置 `elasticsearch.isDebug: true` 开启，
默认 false；无需命令行 `-v=2`。普通错误日志不包含 payload。
输出使用发送前字段清理后的文档，不截断字段；已配置的 ES 密码会脱敏，
不输出认证请求头。该日志用于排查 ES mapping 错误和核对实际数据字段。

为排查没有 ES 请求的情况，启动日志记录配置来源与 ES 是否启用；地址为空时明确警告发送已关闭。`isDebug: true` 时还记录 Pod 事件和应用汇总发送开始、发送成功，失败仍输出错误和完整数据。只处理 `spark-role=driver|executor` 且 `appSparkID` 非空的 Pod；普通运行状态更新不发送 add/del 文档。

### ES SDK（2026-10-08）

已将手写 HTTP 请求替换为官方 Go ES SDK 的 Index API，固定依赖
`github.com/elastic/go-elasticsearch/v8 v8.19.7`。通过 Go 模块代理查询
`@latest`，确认这是本次检查时最新的 8.x 版本。

启用 `EnableCompatibilityMode: true`，请求发送
`Accept` / `Content-Type: application/vnd.elasticsearch+json;compatible-with=8`，
用于连接目标 ES 9.5。相关官方说明：
[REST API compatibility](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/compatibility)。

关闭 SDK 内置重试，由原有发布逻辑保留三次尝试和每次请求超时，
避免两层重试叠加。认证、索引配置、文档结构、时间字段和去重
ID 规则保持一致。SDK 会校验成功响应的 `X-Elastic-Product: Elasticsearch`
头；测试 HTTP 服务已模拟该响应头，并验证 8.x 兼容请求头。

尚未连接真实 ES 9.5 做集成验证。

### 超时与自签证书配置（2026-10-08）

- `elasticsearch.timeoutSeconds`：每次请求超时，默认 20 秒；正数覆盖默认值，0 使用默认值，负数配置报错。
- 去除 handler 原先固定的 16 秒总超时，防止截断配置的请求超时。默认三次尝试加退避最多约 60.6 秒，Pod 事件与应用汇总分别计时。
- `elasticsearch.insecureSkipVerify`：默认 false；按需求设为 true 时，忽略 HTTPS 服务端证书链和主机名校验，无需 CA 文件。
- TLS 配置只应用到 ES 专用 transport，不影响 Kubernetes 客户端或全局 HTTP transport。HTTPS 连接仍加密，但开启忽略校验后不验证服务端身份。
- 测试覆盖默认值、YAML 配置覆盖、负数超时拒绝，以及自签 TLS 服务在默认配置下失败、开启忽略校验后成功。

- ES 写入是同步操作，会增加 watch 处理耗时，启动快照较大时也会影响初始同步耗时。
- 每个 HTTP 请求超时默认 20 秒，可配置。网络错误、HTTP 429 和 5xx 最多尝试三次；其他非成功状态不重试。
- 重试耗尽后记录错误并继续处理，本地状态和 checkpoint 可以继续推进。
- 没有持久化发送队列或故障恢复补发，持续 ES 故障期间可能丢失导出的 Pod 事件和应用汇总。
- 固定文档 ID 用于去重，但汇总更新时间会刷新，订阅消费者必须支持重复 upsert。
