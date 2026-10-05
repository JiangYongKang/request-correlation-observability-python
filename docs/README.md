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
| `OBS_SPANS_AUTOFLUSH_INTERVAL_S` | `0.2`          | 写线程 flush OS 缓冲的周期（秒，0 表示不周期 flush） |
| `OBS_SPANS_FSYNC_INTERVAL_S` | `1.0`              | 写线程 fsync 的最长间隔（秒，0 表示不周期 fsync；关停屏障仍会 fsync） |

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
  ——连失败/中断（含写响应中途客户端断开）样本也不再写，只保留计数
  （`tracer.sampling_stats()`）与日志。
- **比例 > 0：失败/中断样本强制保留**。采用尾部采样：已结束片段按 trace
  暂存，整树完成后统一取舍——任一片段为 `ERROR`（含后台任务抛错、流式
  中途出错、客户端断连——**包括响应写到一半对端关闭、任务被取消**）即整树
  导出；trace 判丢弃后迟到的失败片段（如后台任务）会**救回整链**（有界
  暂存，防内存膨胀）。中途断连时，断连点之前已经结束的所有子片段随根片段
  一起保留，**不会只剩半棵树、父子对不上**。
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
| `client_disconnected` | 响应完成前收到 `http.disconnect`；请求任务被取消（CancelledError）；或写响应（含流式中途/末帧）时 `send` 抛出“对端已关闭”异常 |

**什么算“客户端主动走了”（写响应中途断开）**：以下任一命中即判
`client_disconnected`，且**异常不再向上抛**（中间件干净收尾）：

1. 响应完成前收到 `http.disconnect`（reason：
   `http_disconnect_before_response_complete`）；
2. 请求任务被取消，典型为 uvicorn 在连接关闭时取消处理任务（reason：
   `task_cancelled`）；
3. 写响应时 `send` 抛出对端关闭语义的异常：`asyncio.CancelledError`、
   `ConnectionResetError`、`BrokenPipeError`、`h11.RemoteProtocolError`、
   h2 的 `StreamClosedError/StreamResetError`、
   `uvicorn.protocols.utils.ClientDisconnected`、
   `starlette.requests.ClientDisconnect`（reason：
   `send_failed_client_gone:<异常类型>`）。识别只按**异常类型**，不按消息
   文本猜，业务代码抛的同名文本 RuntimeError 不会被误判。

`client_disconnected`（客户端自己走了）**既不算成功，也不计入服务端错误率**；
日志 `request_finished` 带 `outcome` 与 `outcome_reason`
（`task_cancelled` / `http_disconnect_before_response_complete` /
`send_failed_client_gone:<异常类型>` / `status_<code>`），断连还会单独记录
`client_disconnected` 事件（含 `disconnect_reason`）。断连请求的根片段标记
`ERROR`/`ClientDisconnect`，与服务端异常（`RuntimeError` 等）在追踪数据中
可区分，且在采样未彻底关闭时整链保留（含全部已产生的子片段，父子关系完整）。

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

## 导出：滚动、容量与异步批量写盘

- **请求链路不等落盘**：默认导出器（`RotatingFileSpanExporter`）的 `export`
  只把序列化后的行**非阻塞入队**即返回；中间件/流式/后台片段结束时都**不再
  同步 flush**。磁盘 I/O 多慢都不会计入请求尾延迟。
- **批量合并写**：唯一的后台写线程被唤醒后一次性 drain 队列中所有待写项，
  多批行合并为尽量少的 `writelines`（同一请求的行不拆开，原子进同一文件）。
  因此**写盘/刷写次数随“批次”增长，而不随请求数线性增长**——并发洪峰下
  队列里排队的几百个请求可能只合成一两次物理写。
- **刷写频率与请求数解耦**：写线程每 `OBS_SPANS_AUTOFLUSH_INTERVAL_S`
  （默认 0.2s）把 OS 缓冲 flush 一次，每 `OBS_SPANS_FSYNC_INTERVAL_S`
  （默认 1s）至多 `fsync` 一次——常态每秒至多一次 fsync，而不是旧版的
  “每请求一次 fsync”。显式 `flush()`（排空 + fsync 的屏障）只在关停/测试
  等边界使用，请求路径不使用。
- **并发写不串数据**：所有文件写入只发生在写线程，单次 `writelines`
  不会在行之间交错；队列容量 `OBS_SPANS_QUEUE_SIZE`，满时丢弃并计数
  （`exporter.stats()["dropped_spans"]`）+ 告警日志，不静默、不反压请求。
- **滚动**：当前文件超过 `OBS_SPANS_MAX_BYTES` 或打开时长超过
  `OBS_SPANS_ROTATE_INTERVAL_S`（>0 时）即滚动：
  `spans.jsonl` → `spans.jsonl.1` → … 序号越大越旧；文件总数（含当前文件）
  不超过 `OBS_SPANS_MAX_FILES`，超出部分删除，**文件不无限增长**。
- **不丢数据**：进程正常退出（lifespan shutdown）时插入 stop 屏障，写线程
  排空队列、`fsync`、关闭后才放行——缓冲区数据全部落盘。
- **重启可读**：文件以追加模式打开，重启后历史记录保留，逐行 JSON 可解析。
- **链路完整**：同一次请求的片段按 trace 批量写入同一文件；跨滚动文件的
  片段可用 `trace_id` + `parent_id` 拼回完整链。
- **影响范围与取舍**：请求主链路只承担 JSON 序列化 + 有界队列 `put_nowait`
  （微秒级）；文件 `open/写/flush/fsync/滚动 rename` 全部在写线程。唯一的
  可观测取舍：非正常 kill（SIGKILL/断电）时，最多丢失最近约 1 个 fsync
  间隔内的数据；**正常退出零丢失**。
- 导出器计数可量化核对：`exporter.stats()` 返回 `written_spans /
  dropped_spans / rotations / write_batches（物理写次数）/ fsyncs /
  flush_barriers`，压测时可直接看到 `write_batches`、`fsyncs` 不随请求数
  线性增长。

## 进程退出与数据安全

- 每个请求结束只把片段**入队**（不同步刷盘）；后台/流式片段在结束时触发
  所在 trace 的统一取舍；写线程批量落盘并按固定周期 flush/fsync；
- 关停（lifespan shutdown）时先排空写队列并 `fsync`，再强制结束所有未完成
  片段（`status=ERROR, error_type=TracerShutdown`）导出，**不静默丢弃**；
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
- 新增指标 `totals.client_disconnected` 默认为 0，不影响既有 `error_rate` 语义。

## 本轮修复的行为变更（相对上一版）

- **落盘不再阻塞请求**：请求/流式/后台片段结束时移除了同步 `flush()`；
  写线程改为批量合并写入 + 每 0.2s flush / 每 1s fsync 的周期刷写。
  正常退出仍排空 + fsync，零丢失，重启追加可读的行为不变。
- **客户端断连不再上抛**：请求任务被取消或写响应时对端关闭，中间件捕获后
  统一按 `client_disconnected` 干净收尾（旧版会把 `CancelledError` 重新
  抛给 ASGI 上层）。
- **写响应中断的归类修正**：流式写到一半 `send` 抛出对端关闭异常
  （h11/h2/连接重置等）旧版走 500 `server_error` 分支并抬高错误率，
  现归入 `client_disconnected`；服务端自身在 `send` 处抛的其他异常
  仍计 `server_error`，有测试锁定边界。
- 新增两个导出调优环境变量（默认值即推荐值，无需调整）：
  `OBS_SPANS_AUTOFLUSH_INTERVAL_S` / `OBS_SPANS_FSYNC_INTERVAL_S`。

## 本地验证

```bash
uv sync --dev
uv run pytest -q                 # 96 个测试（并发落盘/断连分类/采样边界/异常/后台/流式/关停/采样/滚动）

# 聚焦本轮两处修复的用例（-s 可看到每个用例的输入、关联标识与判定依据）
uv run pytest tests/test_async_export.py -s       # 并发落盘不阻塞主链路（量化）
uv run pytest tests/test_disconnect.py -s         # 断连分类 + 采样开启/关闭边界

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

### 复现/验证并发落盘不阻塞主链路

```bash
# 自动化（可量化，稳定复现）：
uv run pytest tests/test_async_export.py -s
# 用例输出示例（输入与判定依据）：
#   输入=64 个并发请求（fsync_interval=3600s） 主链路墙钟=~20ms
#   判定=请求期间 fsyncs=0 flush_barriers=0（应与 64 脱钩）
#   关停后判定=落盘 64 行，fsyncs=1（仅关停屏障），dropped=0
#   输入=40 并发请求 + 每次物理写 50ms 主链路墙钟=~10ms
#   判定=请求期间物理写次数=1（旧行为≈40 次，≥2s）

# 手动加压（对比修复前后尾延迟；可用 iostress 或小而慢的磁盘放大效果）：
OBS_SPANS_FSYNC_INTERVAL_S=1 uv run uvicorn main:app --port 8000
# 另一终端并发打流式/普通接口，观察请求耗时平稳、
# spans.jsonl 仍持续增长（写线程在后台批量落盘）：
for i in $(seq 1 200); do curl -s localhost:8000/ -H "X-Correlation-ID: cid-load-$i" -o /dev/null & done; wait
wc -l spans.jsonl                # 关停前可能仍在写线程队列/OS 缓冲
# Ctrl-C 正常关停后再 wc -l：200 行全部落盘（排空 + fsync，不丢）
```

### 复现/验证写响应中途客户端断开

```bash
# 自动化：写响应（含流式中途、末帧）时 send 抛
# h11.RemoteProtocolError / ConnectionResetError / BrokenPipeError
uv run pytest tests/test_disconnect.py -s
# 期望：outcome=client_disconnected，error_rate=0.0，片段 ERROR/ClientDisconnect，
#       且中间件不向 ASGI 上层抛异常；采样 rate=0 时导出 0 条、计数仍在。

# 手动复现：流式接口输出过程中提前关闭客户端
uv run uvicorn main:app --port 8000
# --max-time 让 curl 在收到部分响应后主动断开：
curl -N --max-time 0.05 'localhost:8000/stream?count=100&delay_ms=50' -o /dev/null || true
curl -s localhost:8000/metrics | python -m json.tool
# 期望 totals.client_disconnected >= 1、server_error 不增加、error_rate 不上升；
# 服务端日志出现 event=client_disconnected（disconnect_reason 说明判定依据），
# request_finished.outcome=client_disconnected；spans.jsonl 中该 trace 根片段为
# ERROR/ClientDisconnect，且已产生的子片段整链都在（采样开启时）。
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
tests/             96 个测试（单测日志打印输入/关联标识/判定依据）
```
