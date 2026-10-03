# 请求关联标识与可观测性

本服务在每个请求生命周期内提供：关联标识（Correlation ID）、结构化 JSON 日志、
有界标签指标与跨阶段追踪片段。默认零外部依赖，追踪数据落本地 JSONL 文件。

## 关联标识

- 请求头：`X-Correlation-ID`（可配置）。
- **生成**：客户端未提供时自动生成 `cid-<32 位十六进制>`，并在响应头回写。
- **继承**：客户端提供且合法时原样接受，正常、异常、后台任务、流式响应
  各入口共用同一标识；响应头始终回写最终生效的标识。
- **拒绝**：提供但不合法时返回 `400`，`error.reason` 区分原因：

| reason              | 含义                                   |
| ------------------- | -------------------------------------- |
| `empty`             | 头部存在但为空 / 纯空白 / 带首尾空白   |
| `too_long`          | 长度超过上限（默认 128）               |
| `illegal_character` | 含不允许字符（仅允许字母数字与 `- _ . :`） |

非法值**不会**回显到响应体或响应头（防日志注入/响应头注入）。

### 并发与异步边界

- 上下文用 `contextvars.ContextVar` 存储：并发请求/协程天然隔离，互不污染。
- 跨 `await` 自动传播；跨线程（后台任务）通过 `copy_context()` 显式捕获，
  见 `app/background.py` 的 `bind_context()`；流式响应通过捕获上下文 +
  队列驱动，见 `app/streaming.py`。
- 未绑定上下文时 `require_correlation_id()` 显式抛 `LookupError`，
  装配缺陷可解释，不会静默丢失。

## 配置项（环境变量）

| 变量                          | 默认值             | 说明                                       |
| ----------------------------- | ------------------ | ------------------------------------------ |
| `OBS_CORRELATION_HEADER`      | `X-Correlation-ID` | 关联标识请求头名称                         |
| `OBS_CORRELATION_MAX_LENGTH`  | `128`              | 关联标识长度上限（合法区间 [8, 1024]）     |
| `OBS_LOG_LEVEL`               | `INFO`             | 日志级别（DEBUG/INFO/WARNING/ERROR）       |
| `OBS_SAMPLE_RATE`             | `1.0`              | 全局采样比例 [0.0, 1.0]；`0` 表示彻底关闭导出 |
| `OBS_SAMPLE_SEED`             | `0`                | 采样种子；同一种子下同一批关联标识取舍可复现 |
| `OBS_SAMPLE_ROUTE_RATES`      | `{}`               | 按入口路径前缀的采样比例（JSON 对象），见下方示例 |
| `OBS_SPANS_EXPORT_PATH`       | `spans.jsonl`      | 片段导出文件；空串表示不写文件             |
| `OBS_SPANS_BUFFER_BYTES`      | `65536`            | 写盘缓冲字节数，达到即落盘                 |
| `OBS_SPANS_FLUSH_INTERVAL_S`  | `1.0`              | 后台周期落盘间隔秒数；`0` 关闭周期落盘     |
| `OBS_SPANS_MAX_BYTES`         | `0`                | 单文件最大字节数，超过即滚动；`0` 不按大小滚动 |
| `OBS_SPANS_ROTATE_INTERVAL_S` | `0`                | 按时间滚动间隔秒数；`0` 不按时间滚动       |
| `OBS_SPANS_MAX_FILES`         | `5`                | 滚动保留文件数上限（含活跃文件）           |

`OBS_SAMPLE_ROUTE_RATES` 示例：

```bash
export OBS_SAMPLE_ROUTE_RATES='{"/health": 0.0, "/metrics": 0.01}'
```

精确匹配优先，否则最长前缀匹配（`/health` 同时覆盖 `/healthz`）。

非法配置值在启动期直接抛错（fail fast）。

## 采样与保留策略

### 采样判定（可复现、可解释）

- 采样决策只在**根片段**做出一次，整棵片段树继承同一决策，
  同一次请求的片段要么整体留下、要么整体不留，不会出现半棵树；
- 取舍由 `sha256("{seed}:{trace_id}")` 派生的 [0,1) 值与生效比例比较得出：
  **固定种子下同一批关联标识的取舍稳定复现**，压测前后可直接对比；
- 生效比例 = 路由专属比例（`OBS_SAMPLE_ROUTE_RATES`，精确/最长前缀匹配）
  否则全局 `OBS_SAMPLE_RATE`；健康检查、指标查询等高频低价值入口可单独
  调低甚至关闭；
- 判定依据写入根片段属性：`sample.rate` / `sample.seed`，导出时另有
  `sample.keep_reason`（见下）；`Tracer.stats()` 给出保留/丢弃计数。

### 保留策略（trace 收尾时整体判定）

| 情形                                   | 结果     | `sample.keep_reason` |
| -------------------------------------- | -------- | -------------------- |
| 生效比例 = 0                           | 整树丢弃 | `rate_zero`          |
| 头部命中（draw < rate）                | 整树保留 | `sampled`            |
| 头部未命中但任一片段 `ERROR`           | 整树保留 | `error_retained`     |
| 其余                                   | 整树丢弃 | `sampled_out`        |

- **比例 0 = 彻底关闭**：不新增任何导出（连失败样本也不写），
  只保留指标计数与日志；
- **比例 > 0 时失败样本必留**：最终失败或没跑完的请求（后台任务抛错、
  流式中途出错、客户端断开、关停强制收尾）整链保留；
- 片段按 trace 缓冲在内存，trace 收尾（根片段结束且无活动片段）才导出，
  因此落盘数据量与采样比例大致对齐；缓冲 trace 数有上限
  （`TracerConfig.max_buffered_traces`，默认 10000），超限强制收尾最老的
  trace 并按失败保留处理，长跑不膨胀。

### 指标标签（全部有界，防基数爆炸）

| 标签           | 取值域                                              |
| -------------- | --------------------------------------------------- |
| `route`        | 路由模板（如 `/items/{item_id}`），否则 `unmatched` |
| `method`       | 标准方法大写，其余 `OTHER`                          |
| `status_class` | `1xx`–`5xx`、`unknown`                              |
| `outcome`      | `success` / `client_error` / `server_error` / `client_disconnect` / `unknown` |

**结果分类语义（判定依据明确）**：

| outcome             | 判定依据                                                     |
| ------------------- | ------------------------------------------------------------ |
| `success`           | 响应状态码 < 400                                             |
| `client_error`      | 状态码 400–499                                               |
| `server_error`      | 状态码 500–599 或处理中抛出未捕获异常（计入 `error_rate`）   |
| `client_disconnect` | 写响应时连接异常（`send_failed:<异常类型>`），或响应完成前请求任务被取消（`task_cancelled_before_response_complete`） |
| `unknown`           | 无有效状态码且无法归类的其余场景                             |

`client_disconnect` **不算成功也不算服务端错误**：单独计数
（`totals.client_disconnect`），不进入 `error_rate`，
客户端自己走了不会拉高服务端错误率告警。断连时日志事件为
`client_disconnected` 并带 `basis` 字段说明判定依据；
根片段记 `ERROR` / `ClientDisconnect` 并带 `client_disconnect=true` 属性，
只要采样未彻底关闭，该链按 `error_retained` 整链保留。

**计数取舍**：每个请求在中间件出口**恰好计数一次**；异常经统一处理器
转为响应同样在出口计数；非法关联标识请求记一次 `4xx/unmatched`；
客户端断连记一次 `client_disconnect`；业务代码不直接计数，
避免重复计数与漏计。

### 错误码标签（有界）

`invalid_correlation_id`、`bad_request`、`not_found`、`timeout`、
`client_cancelled`、`rate_limited`、`internal_error`。

## 字段语义

### 日志（单行 JSON）

| 字段             | 语义                                       |
| ---------------- | ------------------------------------------ |
| `timestamp`      | UTC ISO8601                                |
| `level`          | 日志级别                                   |
| `logger`         | 日志器名                                   |
| `message`        | 消息（与 `event` 同名）                    |
| `correlation_id` | 当前上下文关联标识，未绑定时为 `null`      |
| `event`          | 事件名（如 `request_started`）             |
| 其余字段         | 事件维度（`method`/`path`/`duration_ms`…） |

异常对象序列化为 `{type, message}`；内部异常与含敏感关键字的消息
不透出到客户端，仅写服务端日志。

### 追踪片段（JSONL 每行一个）

| 字段            | 语义                                       |
| --------------- | ------------------------------------------ |
| `trace_id`      | 与关联标识一致（根片段）                   |
| `span_id`       | 片段唯一 ID                                |
| `parent_id`     | 父片段 ID，根为 `null`                     |
| `kind`          | `server` / `internal` / `background` / `stream` |
| `duration_ms`   | 耗时毫秒，未结束为 `null`                  |
| `status`        | `OK` / `ERROR` / `UNSET`                   |
| `error_type`    | 失败异常类型（如 `RuntimeError`、`ClientDisconnect`） |
| `error_message` | 失败原因（仅服务端可见，客户端不回显）     |
| `sampled`       | 本树头部采样决策                           |
| `attributes.sample.rate` / `attributes.sample.seed` | 采样判定依据（根片段） |
| `attributes.sample.keep_reason` | 保留依据：`sampled` / `error_retained`（导出时写入根片段） |

### 指标快照（`GET /metrics`）

- `series[]`：按标签分组的 `count` 与 `latency_ms.{sum,avg,max}`；
- `totals`：`requests` / `success` / `client_error` / `server_error` /
  `client_disconnect` / `error_rate`（`error_rate` 只含服务端错误）。

## 导出：缓冲、滚动与容量

- **不在请求主链路同步写盘**：导出先入内存缓冲，缓冲量达到
  `OBS_SPANS_BUFFER_BYTES` 或后台线程按 `OBS_SPANS_FLUSH_INTERVAL_S`
  周期落盘；多请求并发写入由导出器内部锁保证安全；
- **滚动**：活跃文件超过 `OBS_SPANS_MAX_BYTES` 或存活超过
  `OBS_SPANS_ROTATE_INTERVAL_S` 时滚动为 `spans.jsonl.1`、`.2`……；
  滚动在一批（通常是一整条 trace）写入**之前**判定，同一次请求的片段
  不会被拆到两个文件；跨文件也可用 `trace_id` 拼回完整链；
- **保留上限**：连活跃文件在内最多 `OBS_SPANS_MAX_FILES` 个，
  超出最老的删除，文件不会无限增长；
- 默认配置（不滚动、64 KiB 缓冲、1s 周期落盘）下行为与上一版兼容：
  同样的 JSONL 格式、同样的文件路径，只是落盘时机从"每请求同步刷盘"
  变为"缓冲+周期落盘"，进程正常退出时数据不丢（见下）。

## 进程退出与数据安全

- 关停（lifespan shutdown）时：强制结束所有未完成片段
  （`status=ERROR, error_type=TracerShutdown`），按保留策略导出，
  并把导出器缓冲区**全量刷盘**（flush + fsync），**不静默丢弃**；
- 导出文件以追加模式打开，重启后历史片段保留，逐行 JSON 可独立解析；
  滚动归档同样逐行 JSON，`trace_id` 是跨文件拼接依据。

## 本地验证

```bash
uv sync --dev
uv run pytest -q                 # 87 个测试（并发/异常/后台/流式/采样/滚动/断连/关停）
uv run pytest -s tests/test_sampling.py -q   # 测试日志含输入、关联标识与判定依据

# 手动验证
uv run uvicorn main:app --port 8000
curl -i localhost:8000/                       # 自动生成关联标识
curl -i -H 'X-Correlation-ID: my-trace-1' localhost:8000/   # 继承
curl -i -H 'X-Correlation-ID: bad id' localhost:8000/       # 400 + reason
curl -X POST 'localhost:8000/records?record_id=1'           # 后台任务
curl -N 'localhost:8000/stream?count=3'                     # 流式
curl -s localhost:8000/metrics | python -m json.tool        # 指标快照
cat spans.jsonl                               # 本地追踪片段
```

### 验证采样真实生效

```bash
# 10% 采样 + 固定种子 + 关闭 /metrics 采样
OBS_SAMPLE_RATE=0.1 OBS_SAMPLE_SEED=42 \
OBS_SAMPLE_ROUTE_RATES='{"/metrics": 0.0}' \
uv run uvicorn main:app --port 8000

for i in $(seq 1 200); do curl -s "localhost:8000/?i=$i" > /dev/null; done
curl -s localhost:8000/metrics > /dev/null
# 落盘 trace 数 ≈ 200 × 0.1（/metrics 不出现）；同种子重跑取舍完全一致
grep -c '"keep_reason": "sampled"' spans.jsonl
# 失败样本必留：多打几次 /boom，即使未命中采样也以 error_retained 保留
curl -s localhost:8000/boom > /dev/null
grep '"keep_reason": "error_retained"' spans.jsonl
```

### 验证滚动与容量

```bash
# 1 KiB 滚动、最多保留 3 个文件
OBS_SPANS_MAX_BYTES=1024 OBS_SPANS_MAX_FILES=3 OBS_SPANS_BUFFER_BYTES=1 \
uv run uvicorn main:app --port 8000
for i in $(seq 1 50); do curl -s localhost:8000/ > /dev/null; done
ls spans.jsonl*          # 最多 3 个文件；每条 trace 的片段在同一文件内
```

### 默认行为兼容范围

- 关联标识、日志格式、指标标签、错误码语义与上一版完全一致；
- 采样默认 `1.0`（全量）、种子默认 `0`（确定性，可复现）；
- 导出文件路径与 JSONL 格式不变；默认不滚动；
- 唯一的行为变化：落盘时机从"每请求同步 fsync"改为"缓冲 + 1s 周期落盘
  + 关停全量刷盘"（长压测下主链路不被写盘拖慢，正常退出不丢数据）；
  如需接近旧行为可把 `OBS_SPANS_BUFFER_BYTES` 调小。

## 目录结构

```
app/
  config.py        配置（环境变量、校验）
  correlation.py   关联标识生成/校验/上下文
  logging_setup.py JSON 结构化日志
  metrics.py       有界标签指标（含 client_disconnect 分类）
  sampling.py      确定性采样器（种子 + 按路由比例）
  tracing.py       片段、按 trace 缓冲与保留策略、滚动导出、关停恢复
  middleware.py    ASGI 中间件与装配（断连识别与分类）
  errors.py        异常分类与安全视图
  background.py    后台任务上下文继承
  streaming.py     流式响应上下文与片段
  main.py          应用装配与四类入口
tests/             87 个测试（单测日志打印输入/关联标识/判定依据）
```
