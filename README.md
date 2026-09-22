# request-correlation-observability-python

为 FastAPI 服务补全贯穿请求生命周期的**关联标识**与**可观测性**能力：
并发、异常、后台处理与流式响应各链路下追踪结论一致且可解释，默认无需
任何外部服务即可在本地完成验证。

## 能力概览

- **关联标识**：缺失自动生成；提供时按字符集/长度规则校验，拒绝原因可区分；
  基于 `ContextVar` 实现，并发隔离、跨异步/线程边界继承。
- **结构化日志 + 指标**：日志携带 `correlation_id`，敏感字段/密钥字面量脱敏、
  防日志注入；指标覆盖请求量、时延直方图与错误率，标签取值有界，每请求仅计一次。
- **追踪片段**：父子关系与真实调用链一致，异常标记原因；采样比例与导出方式
  可配置（file/console/none），进程退出时未完成片段标记 `interrupted` 并落盘。
- **测试**：覆盖并发（200 路混合链路零串扰）、异常链路、后台与流式场景，
  用例日志打印输入、关联标识与判定依据。

## 快速开始

```bash
uv sync --group dev
uv run pytest -s                      # 运行全部测试（含判定依据输出）
uv run uvicorn main:app --port 8000   # 启动服务
```

## 体验链路

```bash
curl -i http://127.0.0.1:8000/                                    # 自动生成标识
curl -i -H 'X-Correlation-ID: demo-1' http://127.0.0.1:8000/echo?value=hi
curl -i -H 'X-Correlation-ID: bad value' http://127.0.0.1:8000/  # 字符集原因拒绝
curl -i -H 'X-Correlation-ID: bg-1' http://127.0.0.1:8000/background
curl -N -H 'X-Correlation-ID: s-1' 'http://127.0.0.1:8000/stream?count=3'
```

停止服务后查看 `logs/traces.jsonl` 与 `logs/metrics-snapshot.json`。

## 文档

- 配置项、采样与标签策略、字段语义、错误码与本地验证详见
  [`docs/observability.md`](docs/observability.md)。
