# 请求关联标识与可观测性说明

本服务在 FastAPI / Starlette 之上实现了一套**零外部依赖即可本地验证**的
可观测性能力，覆盖三条主线：

1. **关联标识（Correlation ID）**：生成、合法性校验、跨异步边界透传；
2. **结构化日志 + 关键指标**：请求维度字段、请求量/时延/错误率；
3. **追踪片段（Trace Span）**：父子关系、异常标记、采样、本地导出与退出落盘。

---

## 1. 关联标识

### 1.1 载体与传递

- HTTP 请求头：`X-Correlation-ID`（头名可通过 `OBS_CORRELATION_HEADER` 配置）。
- 进程内载体：`contextvars.ContextVar`。`asyncio` 中每个任务在创建时
  拷贝独立上下文，因此：
  - 并发请求之间**天然隔离**，不会互相污染；
  - 子任务 / `BackgroundTasks` / 流式生成器自动继承；
  - 提交到线程池的阻塞工作通过 `contextvars.copy_context()` 显式继承。

### 1.2 生成与接受规则

| 场景 | 行为 |
|---|---|
| 无头 / 空串 / 纯空白 | 服务端生成 UUIDv4 十六进制（32 字符），响应头回写 |
| 合法头 | 原样接受，响应头回写同一值（先去除首尾空白） |
| 长度超过上限（默认 128） | `400 invalid_correlation_id_length` |
| 含非法字符 / 首尾非字母数字 | `400 invalid_correlation_id_format` |

合法字符集：`A-Z a-z 0-9 - _ .`；且首、尾字符必须是字母或数字
（避免以 `-`/`.` 开头引发日志注入或路径拼接歧义）。

拒绝原因通过**不同的错误码**区分，响应体不回显客户端原值，服务端日志
只记录提供值的**长度**（如 `supplied_length=200`），不记录原文。

### 1.3 各入口一致性

| 入口 | 行为 |
|---|---|
| 正常请求 | 中间件绑定，路由/日志/指标/追踪全程可见 |
| 异常请求 | 异常处理器与中间件错误响应均回写同一标识 |
| 后台任务 | `BackgroundTasks` 与流式生成器在请求上下文中执行；线程池工作显式传入 `copy_context()` |
| 流式响应 | 每个 SSE 帧的数据负载与响应头均为同一标识 |

在请求上下文之外调用 `get_correlation_id()` 会显式抛出
`CorrelationUnboundError`，而不是返回一个伪造标识。

---

## 2. 结构化日志

### 2.1 格式

默认单行 JSON（`OBS_LOG_JSON=false` 可切换为易读文本格式），字段：

| 字段 | 含义 |
|---|---|
| `timestamp` | UTC ISO-8601 时间戳 |
| `level` | 日志级别 |
| `logger` | logger 名称 |
| `event` | 事件名（如 `request.start` / `request.complete`） |
| `correlation_id` | 当前请求标识；无上下文时为 `-` |
| 其余字段 | 通过 `extra={"fields": {...}}` 传入的结构化字段 |

### 2.2 安全策略

- 字段名命中 `token/password/secret/authorization/cookie/apikey`
  （忽略 `- _ .` 分隔符）时，值整体替换为 `***redacted***`；
- 字符串中的密钥字面量（如 `password=hunter2`）按正则脱敏；
- 值中的 `\n \r \t` 被转义，防止外部输入伪造日志行；
- 异常路径只透出**异常类型名 + 脱敏后的消息**，不序列化依赖库内部对象；
- 非标量值、超长字符串（>500 字符）被收敛为有界表示。

关键事件：`request.start`、`request.complete`（含时延、状态、是否生成标识）、
`request.failed`、`correlation.rejected`、`background.start/done`、
`route.stream.frame` 等。

---

## 3. 指标

### 3.1 采集内容

- **请求量** `requests`
- **错误量/错误率** `errors` / `error_rate`（仅 5xx 与客户端断连 499 计为错误；业务 4xx 不计）
- **时延** 固定边界累积直方图（秒）：
  `0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, +Inf`，
  附带 `sum/count/mean`。

### 3.2 标签（有界）

标签为三元组 `method | route | status_class`：

| 标签 | 归一规则 |
|---|---|
| `method` | 白名单 `GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS`，其余 `_OTHER` |
| `route` | **路由模板**（`/items/{id}`）而非原始 URL；未匹配为 `__unmatched__`；非法标识拒绝流量为 `__invalid_correlation__`；最长 128 |
| `status_class` | `1xx/2xx/3xx/4xx/5xx`，其余 `_OTHER` |

### 3.3 计数取舍（避免重复计数与漏计）

- 每个 HTTP 请求**仅在中间件记录一次**：普通响应在最后一帧落账，
  流式响应在流结束时落账，因此流式时延即真实端到端时延；
- 客户端断连记一次 `499`；
- 后台任务**不**计入 HTTP 请求时延（它发生在响应发送之后），
  其耗时通过追踪片段 `background_job` 表达，避免与 HTTP 指标重复；
- 非法关联标识在进入应用前被拒绝，仍记录一次 400
  （route 标签 `__invalid_correlation__`），拒绝率因此可观测；
- 注册表线程安全；快照在锁内拷贝，写盘采用 `临时文件 + os.replace` 原子替换。

快照路径由 `OBS_METRICS_SNAPSHOT_PATH` 配置，应用关闭时自动写出；
也可以随时读取 `MetricsRegistry.snapshot()`。

---

## 4. 追踪片段（Tracing）

### 4.1 模型

- `trace_id`：一棵调用树一个，根片段决策采样后整棵树同取舍；
- `span_id` / `parent_id`：基于 `ContextVar` 的片段栈维护父子关系，
  异步任务间隔离；
- 每个片段携带：`name`、开始/结束时间、`duration_ms`、`status`、
  `correlation_id`、`attributes`、`sampled`。

`status` 取值：

| 状态 | 含义 |
|---|---|
| `ok` | 正常结束 |
| `error` | 片段内抛出异常；或最终响应为 5xx（含异常处理器生成的受控 5xx，标记 `HttpServerError`）；同时记录 `error_type` 与脱敏后的 `error_reason` |
| `interrupted` | 追踪器关闭（进程退出/重启）时片段尚未结束 |

> 根片段的失败标记采用"只设状态、不提前结束"的方式，因此即便 5xx 响应
> 已发送，随后执行的后台任务耗时仍计入根片段，耗时与真实生命周期一致。

典型链路：

```
http.request GET                 （根片段，含后台任务整体耗时）
├── echo.process                 （业务子片段）
├── background_job               （后台任务）
│   └── background_job.notify
└── stream.produce               （流式生产，逐帧 attribute）
```

### 4.2 采样

- 配置项 `OBS_TRACE_SAMPLE_RATE`，区间 `[0,1]`，默认 `1.0`（全采样）；
- 头采样：根片段开始时按伯努利决策，结果写入上下文，**整棵树一致**；
- 未被采样的树不导出明细（0 采样率时不产生任何导出 I/O）。

### 4.3 导出

| `OBS_TRACE_EXPORT` | 行为 |
|---|---|
| `file`（默认） | JSONL 追加写入 `OBS_TRACE_FILE_PATH`（默认 `logs/traces.jsonl`），每行一个片段，`flush + fsync` |
| `console` | 以 `[TRACE] {...}` 输出到标准输出 |
| `none` | 不导出 |

### 4.4 退出与重启不丢数

- 已结束片段在 span 关闭时立即导出；
- 应用 lifespan 关闭时调用 `tracer.shutdown()`：
  仍在进行中的片段被标记为 `interrupted`（附 `ProcessShutdown` 原因和
  结束时间）后导出；`shutdown()` 幂等；
- file 导出以 append 模式打开，进程重启后历史片段保留，新片段继续追加；
- 指标快照在关闭时原子写盘（与追踪 flush 相互隔离，互不阻断）。

---

## 5. 错误响应约定

```json
{
  "error": {
    "code": "invalid_correlation_id_format",
    "reason": "关联标识包含非法字符或首尾字符不合法",
    "correlation_id": null
  }
}
```

| code | HTTP 状态 | 触发场景 |
|---|---|---|
| `invalid_correlation_id_length` | 400 | 标识超长 |
| `invalid_correlation_id_format` | 400 | 字符集/首尾字符非法 |
| `invalid_request` | 422 | 请求参数校验失败 |
| `http_error` | 原始状态 | 受控 `HTTPException`（原因取白名单文案） |
| `upstream_timeout` | 504 | 超时 |
| `request_cancelled` | 499 | 客户端断连 |
| `internal_error` | 500 | 其余未受控异常（详情仅进服务端日志） |

响应头与响应体都带 `X-Correlation-ID`（标识非法时为 `null`）。

---

## 6. 配置项一览

所有配置使用 `OBS_` 前缀，启动时校验，非法值 fail-fast：

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `OBS_CORRELATION_HEADER` | `X-Correlation-ID` | 关联标识头名，非空 |
| `OBS_CORRELATION_LENGTH_MAX` | `128` | 标识最大长度，区间 [16,1024] |
| `OBS_LOG_JSON` | `true` | `true/false` 切换 JSON/文本日志 |
| `OBS_LOG_LEVEL` | `INFO` | DEBUG/INFO/WARNING/ERROR/CRITICAL |
| `OBS_METRICS_SNAPSHOT_PATH` | `logs/metrics-snapshot.json` | 指标快照路径 |
| `OBS_TRACE_SAMPLE_RATE` | `1.0` | 采样比例 [0,1] |
| `OBS_TRACE_EXPORT` | `file` | file/console/none |
| `OBS_TRACE_FILE_PATH` | `logs/traces.jsonl` | file 导出路径，非空 |

---

## 7. 本地验证

### 7.1 运行测试

```bash
uv sync --group dev
uv run pytest -s            # -s 可查看每条用例打印的输入/关联标识/判定依据
```

### 7.2 启动服务

```bash
uv run uvicorn main:app --port 8000
```

另开终端：

```bash
# 1) 未带标识 -> 自动生成（观察响应头与响应体一致）
curl -i http://127.0.0.1:8000/

# 2) 携带合法标识 -> 原样透传
curl -i -H 'X-Correlation-ID: demo-run-001' http://127.0.0.1:8000/echo?value=hello

# 3) 非法标识（字符集 vs 长度，原因可区分）
curl -i -H 'X-Correlation-ID: bad value' http://127.0.0.1:8000/
curl -i -H "X-Correlation-ID: $(python -c 'print("x"*200)')" http://127.0.0.1:8000/

# 4) 异常链路（500 响应体不含内部文本）
curl -i -H 'X-Correlation-ID: err-1' http://127.0.0.1:8000/error?kind=value

# 5) 后台任务：服务端日志可见 background.* 沿用同一标识
curl -i -H 'X-Correlation-ID: bg-1' http://127.0.0.1:8000/background

# 6) 流式响应：每帧 data 内 correlation_id 相同
curl -i -N -H 'X-Correlation-ID: stream-1' 'http://127.0.0.1:8000/stream?count=3'
```

停止服务（Ctrl-C 触发 lifespan shutdown）后检查：

```bash
tail -n 20 logs/traces.jsonl          # 每行一个 span JSON
cat logs/metrics-snapshot.json        # 请求量/时延/错误率快照
```

### 7.3 调整采样与导出

```bash
OBS_TRACE_SAMPLE_RATE=0.1 OBS_TRACE_EXPORT=console \
  uv run uvicorn main:app --port 8000
```
