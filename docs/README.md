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

| 变量                        | 默认值             | 说明                                       |
| --------------------------- | ------------------ | ------------------------------------------ |
| `OBS_CORRELATION_HEADER`    | `X-Correlation-ID` | 关联标识请求头名称                         |
| `OBS_CORRELATION_MAX_LENGTH`| `128`              | 关联标识长度上限（合法区间 [8, 1024]）     |
| `OBS_LOG_LEVEL`             | `INFO`             | 日志级别（DEBUG/INFO/WARNING/ERROR）       |
| `OBS_SAMPLE_RATE`           | `1.0`              | 采样比例，区间 [0.0, 1.0]                  |
| `OBS_SPANS_EXPORT_PATH`     | `spans.jsonl`      | 片段导出文件；空串表示不写文件             |

非法配置值在启动期直接抛错（fail fast）。

## 采样与标签策略

### 采样

- 采样决策只在**根片段**做出一次，整棵片段树继承同一决策；
- 未被采样的片段仍完整记录（`sampled=false`），便于本地排查，
  仅在导出侧可按策略过滤（`export_finished(only_sampled=True)`）；
- 比例非法（<0 或 >1）构造期拒绝。

### 指标标签（全部有界，防基数爆炸）

| 标签           | 取值域                                              |
| -------------- | --------------------------------------------------- |
| `route`        | 路由模板（如 `/items/{item_id}`），否则 `unmatched` |
| `method`       | 标准方法大写，其余 `OTHER`                          |
| `status_class` | `1xx`–`5xx`、`unknown`                              |
| `outcome`      | `success` / `client_error` / `server_error` / `unknown` |

**计数取舍**：每个请求在中间件出口**恰好计数一次**；异常经统一处理器
转为响应同样在出口计数；非法关联标识请求记一次 `4xx/unmatched`；
业务代码不直接计数，避免重复计数与漏计。

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
| `error_type`    | 失败异常类型（如 `RuntimeError`）          |
| `error_message` | 失败原因（仅服务端可见，客户端不回显）     |
| `sampled`       | 本树采样决策                               |

### 指标快照（`GET /metrics`）

- `series[]`：按标签分组的 `count` 与 `latency_ms.{sum,avg,max}`；
- `totals`：`requests` / `success` / `client_error` / `server_error` / `error_rate`。

## 进程退出与数据安全

- 每个请求结束即导出片段并 `fsync` 刷盘；后台/流式片段在结束时立即导出；
- 关停（lifespan shutdown）时强制结束所有未完成片段
  （`status=ERROR, error_type=TracerShutdown`）并导出刷盘，**不静默丢弃**；
- 导出文件以追加模式打开，重启后历史片段保留，逐行 JSON 可独立解析。

## 本地验证

```bash
uv sync --dev
uv run pytest -q                 # 63 个测试（并发/异常/后台/流式/关停）

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

## 目录结构

```
app/
  config.py        配置（环境变量、校验）
  correlation.py   关联标识生成/校验/上下文
  logging_setup.py JSON 结构化日志
  metrics.py       有界标签指标
  tracing.py       片段、采样、导出、关停恢复
  middleware.py    ASGI 中间件与装配
  errors.py        异常分类与安全视图
  background.py    后台任务上下文继承
  streaming.py     流式响应上下文与片段
  main.py          应用装配与四类入口
tests/             63 个测试（单测日志打印输入/关联标识/判定依据）
```
