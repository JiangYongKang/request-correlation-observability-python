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
| `OBS_SAMPLE_RATE`             | `1.0`              | 基础采样比例，区间 [0.0, 1.0]              |
| `OBS_SAMPLE_SEED`             | `obs-v1`           | 采样判定种子；固定种子 ⇒ 取舍可复现        |
| `OBS_SAMPLE_RATE_OVERRIDES`   | 空                 | 按入口覆盖，如 `/health=0,/metrics=0.05,/internal/*=0.1` |
| `OBS_SPANS_EXPORT_PATH`       | `spans.jsonl`      | 片段导出文件；空串表示不写文件             |
| `OBS_SPANS_MAX_BYTES`         | `67108864`（64MB） | 单文件滚动阈值（字节，≥1024）              |
| `OBS_SPANS_MAX_FILES`         | `5`                | 滚动文件保留上限（含当前文件，≥1）         |
| `OBS_SPANS_ROTATE_INTERVAL_S` | `0`                | 按时间滚动间隔（秒），0 表示不按时间滚动   |
| `OBS_SPANS_QUEUE_SIZE`        | `10000`            | 异步写盘队列容量（背压上限）               |
| `OBS_SPANS_FLUSH_INTERVAL_S`  | `1.0`              | 后台周期刷盘间隔（秒），0 表示不周期刷写   |

非法配置值在启动期直接抛错（fail fast）。

## 采样与保留策略

### 判定方式（可复现）

- 判定值由 `sha256(f"{seed}:{trace_id}")` 派生，与进程、时间、并发无关：
  **固定种子下，同一批请求（同一组关联标识）的取舍稳定复现**，压测前后可对比。
- 采样决策只在**根片段**做出一次，整棵片段树继承同一决策：
  同一次请求的片段要么整体留下、要么整体不留，不会只剩半棵、父子对不上。
- 每个请求的判定依据以 DEBUG 日志 `sampling_decision` 输出
  （`sample_rate`/`sample_value`/`sample_kept`/`sample_reason`），不靠猜。

### 边界语义

- **`OBS_SAMPLE_RATE=0`（或某入口覆盖为 0）：彻底关闭**。不新增任何导出
  ——连失败样本也不再写，只保留计数（`tracer.sampling_stats()`）与日志。
- **比例 > 0：失败/中断样本强制保留**。采用尾部采样：已结束片段按 trace
  暂存，整树完成后统一取舍——任一片段为 `ERROR`（含后台任务抛错、流式
  中途出错、客户端断连）即整树导出；trace 判丢弃后迟到的失败片段
  （如后台任务）会**救回整链**（有界暂存，防内存膨胀）。
- 中间比例下，最终落盘的追踪数据量与比例大致对齐（统计意义），
  压测中能明显看出采样省掉的写入。

### 按入口覆盖

`OBS_SAMPLE_RATE_OVERRIDES` 支持精确路径（`/health=0`）与前缀
（`/internal/*=0.1`），精确优先、前缀取最长匹配。健康检查、指标查询等
高频低价值入口可调低甚至关掉，不影响其他入口。

### 指标标签（全部有界，防基数爆炸）

| 标签           | 取值域                                              |
| -------------- | --------------------------------------------------- |
| `route`        | 路由模板（如 `/items/{item_id}`），否则 `unmatched` |
| `method`       | 标准方法大写，其余 `OTHER`                          |
| `status_class` | `1xx`–`5xx`、`unknown`                              |
| `outcome`      | `success` / `client_error` / `server_error` / `client_disconnected` / `unknown` |

**结果分类语义（判定依据明确）**：

| outcome               | 判定依据                                                       |
| --------------------- | -------------------------------------------------------------- |
| `success`             | 响应状态码 < 400                                               |
| `client_error`        | 状态码 400–499（客户端请求本身有问题）                         |
| `server_error`        | 状态码 ≥ 500 或未捕获异常（计入 `error_rate`）                 |
| `client_disconnected` | 响应完成前收到 `http.disconnect`；请求任务被取消（CancelledError）；或写响应中途连接已断开（`BrokenPipeError`/`ConnectionResetError`/`ClientDisconnect` 等，见 `errors.is_client_disconnect_error`） |

`client_disconnected`（客户端自己走了）**既不算成功，也不计入服务端错误率**；
日志 `request_finished` 带 `outcome` 与 `outcome_reason`
（`task_cancelled` / `http_disconnect_before_response_complete` /
`response_write_failed_client_gone` / `status_<code>`），
断连还会单独记录 `client_disconnected` 事件（带 `disconnect_reason` 与原始
`error_type`），日志、指标、追踪三处结论一致。写响应中途断开的异常
**被中间件吸收、不再抛给上层**（任务取消 `CancelledError` 仍按 asyncio
语义继续传播，由服务器框架完成取消流程）。断连请求的根片段标记
`ERROR`/`ClientDisconnect`，与服务端异常（`RuntimeError` 等）在追踪数据中可区分，
且在采样未彻底关闭时整链保留（含子片段，不会只剩半棵树）。

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
- `totals`：`requests` / `success` / `client_error` / `server_error` /
  `client_disconnected` / `error_rate`（= `server_error / requests`，
  断连不抬高错误率）。

## 导出：滚动、容量与异步写盘

- **异步写盘**：默认导出器（`RotatingFileSpanExporter`）由后台线程落盘，
  请求主链路只做入队，不被磁盘 I/O 拖慢；队列容量 `OBS_SPANS_QUEUE_SIZE`，
  满时丢弃并计数（`exporter.stats()["dropped_spans"]`）+ 告警日志，不静默。
- **主链路不等待落盘**：请求/流式/后台路径**不做**逐请求同步刷盘，
  落盘（`fsync`）由写线程按 `OBS_SPANS_FLUSH_INTERVAL_S`（默认 1s）周期完成
  ——刷写次数与请求数解耦，不随并发线性增长，尾延迟不被磁盘拖住；
  数据可见性延迟以该间隔为上界。持续高压下队列不空也会按周期刷写。
- **滚动**：当前文件超过 `OBS_SPANS_MAX_BYTES` 或打开时长超过
  `OBS_SPANS_ROTATE_INTERVAL_S`（>0 时）即滚动：
  `spans.jsonl` → `spans.jsonl.1` → … 序号越大越旧；文件总数（含当前）
  不超过 `OBS_SPANS_MAX_FILES`，超出部分删除，**文件不无限增长**。
- **不丢数据**：`flush` 插入屏障并等待写线程排空队列后 `fsync`；进程正常
  退出（lifespan shutdown）时排空队列、`fsync`、关闭——缓冲区数据全部落盘。
- **重启可读**：文件以追加模式打开，重启后历史记录保留，逐行 JSON 可解析。
- **链路完整**：同一次请求的片段按 trace 批量写入同一文件；跨滚动文件的
  片段可用 `trace_id` + `parent_id` 拼回完整链。

## 进程退出与数据安全

- 每个请求结束即把已结束片段**异步入队**导出（不等待落盘）；后台/流式片段
  在结束时触发所在 trace 的统一取舍；落盘刷写由导出器后台线程按周期完成；
- 关停（lifespan shutdown）时强制结束所有未完成片段
  （`status=ERROR, error_type=TracerShutdown`）并导出刷盘，**不静默丢弃**；
- 关停日志 `shutdown_flush` 同时输出指标汇总与采样计数
  （`traces_kept/dropped/kept_for_error/rescued`、`spans_exported/dropped`），
  采样彻底关闭时仍有计数可查。

## 默认行为兼容范围

- 默认配置（不设任何新环境变量）下：关联标识、日志格式、指标标签、
  响应头行为与上一版完全一致；`OBS_SAMPLE_RATE` 默认 `1.0` 全量导出；
- 默认种子 `obs-v1` 使采样判定确定性可复现（仅影响 `sampled` 标记的取值，
  比例 1.0 时无可见差异）；
- 默认导出文件仍为 `spans.jsonl`（追加模式），滚动参数默认值
  （64MB × 5 个文件）下行为与单文件追加一致；
- 新增指标 `totals.client_disconnected` 默认为 0，不影响既有 `error_rate` 语义；
- 落盘方式由"逐请求同步刷盘"改为"异步入队 + 后台周期刷写
  （默认 1s）+ 关停排空刷盘"：数据可见性延迟以刷盘间隔为上界，
  正常退出不丢数据的保证不变。

## 本地验证

```bash
uv sync --dev
uv run pytest -q                 # 93 个测试（并发/异常/后台/流式/关停/采样/断连/滚动/异步落盘）

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

### 验证采样与保留

```bash
# 采样 30%，健康检查彻底关闭；固定种子保证两次运行取舍一致
OBS_SAMPLE_RATE=0.3 OBS_SAMPLE_RATE_OVERRIDES='/health=0' \
OBS_SAMPLE_SEED=loadtest-1 uv run uvicorn main:app --port 8000

for i in $(seq 1 100); do curl -s "localhost:8000/" -H "X-Correlation-ID: cid-run-$i" -o /dev/null; done
curl -s localhost:8000/boom -H 'X-Correlation-ID: cid-run-fail' -o /dev/null
wc -l spans.jsonl        # 约为 30% 请求量 + 全部失败样本
grep cid-run-fail spans.jsonl   # 失败样本即使未抽中也在
# 关停日志 shutdown_flush 中的 sampling_stats 可核对 kept/dropped 计数
```

### 验证滚动与容量

```bash
# 1MB 滚动、最多保留 3 个文件
OBS_SPANS_MAX_BYTES=1048576 OBS_SPANS_MAX_FILES=3 \
  uv run uvicorn main:app --port 8000
# 压测后观察：ls -l spans.jsonl* 不超过 3 个文件；
# 同一 trace_id 的片段可用 grep 拼回完整链
```

### 复现与验证：并发落盘不阻塞主链路

```bash
# 量化单测：慢盘（每次刷盘人为延迟 50ms）下 20 个并发请求——
# 请求期间刷写 0 次、主链路耗时毫秒级；旧行为为 20 次刷写、耗时 ≥1s
uv run pytest tests/test_async_export.py -q -s

# 手动对照：启动后加压，观察 exporter.stats() 的 flushes 增长
# 与请求数解耦（约每秒 1 次周期刷写），关停时统一排空刷盘
OBS_SPANS_FLUSH_INTERVAL_S=1 uv run uvicorn main:app --port 8000
```

### 复现与验证：客户端中途断开不计服务端错误

```bash
# 断连分类单测：写响应中途断开（BrokenPipe/ConnectionReset/ClientDisconnect）
# ⇒ client_disconnected，错误率保持 0，异常不上抛；rate=0 时不新增导出
uv run pytest tests/test_disconnect.py -q -s

# 手动复现：流式响应读到一半按 Ctrl-C 断开客户端
uv run uvicorn main:app --port 8000
curl -N 'localhost:8000/stream?count=100&delay_ms=200'   # 读几块后 Ctrl-C
curl -s localhost:8000/metrics | python -m json.tool
# totals.client_disconnected +1，server_error 与 error_rate 不变；
# 日志可见 client_disconnected 事件与 request_finished 的 outcome_reason；
# spans.jsonl 中该 trace 的根/流片段完整保留（ERROR/ClientDisconnect）
```

## 目录结构

```
app/
  config.py        配置（环境变量、校验）
  correlation.py   关联标识生成/校验/上下文
  logging_setup.py JSON 结构化日志
  metrics.py       有界标签指标（含 client_disconnected 分类）
  sampling.py      确定性采样器（种子/按入口覆盖/判定依据）
  tracing.py       片段、尾部采样（失败保留/整树一致）、关停恢复
  exporter.py      滚动文件导出器（大小/时间滚动、保留上限、异步写盘）
  middleware.py    ASGI 中间件与装配（断连检测、结果分类）
  errors.py        异常分类与安全视图
  background.py    后台任务上下文继承
  streaming.py     流式响应上下文与片段
  main.py          应用装配与四类入口
tests/             93 个测试（单测日志打印输入/关联标识/判定依据）
```
