"""后台任务与流式响应的上下文继承测试。"""

from __future__ import annotations

import asyncio
import json

import pytest

from app.background import bind_context, capture_request_context
from app.correlation import correlation_context, get_correlation_id, require_correlation_id
from app.tracing import InMemorySpanExporter, Tracer, TracerConfig, current_span


def test_background_records_endpoint_spans(client, span_exporter):
    r = client.post("/records?record_id=77", headers={"X-Correlation-ID": "cid-bg-e2e"})
    assert r.status_code == 200
    body = r.json()
    print(f"输入=POST /records 77 关联标识={body['correlation_id']} 判定=后台任务挂载")
    assert body["correlation_id"] == "cid-bg-e2e"

    bg = [s for s in span_exporter.finished_spans() if s.kind == "background"]
    server = [s for s in span_exporter.finished_spans() if s.kind == "server"]
    names = sorted(s.name for s in bg)
    print(f"输入=同步+异步两个后台任务 判定=background 片段={names}")
    assert len(bg) == 2
    assert len(server) == 1
    root = server[0]
    assert all(s.trace_id == "cid-bg-e2e" for s in bg)
    assert all(s.parent_id == root.span_id for s in bg)
    assert all(s.status == "OK" for s in bg)


def test_bind_context_sync_thread_inherits():
    seen = {}

    def job(x):
        seen["cid"] = get_correlation_id()
        seen["span_kind"] = current_span().kind
        return x + 1

    async def scenario():
        exp = InMemorySpanExporter()
        tr = Tracer(TracerConfig(exporter=exp))
        with tr.span("root", trace_id="TR-BG1"):
            with correlation_context("cid-thread"):
                bound = bind_context(job, "cid-thread", tr)
        return await asyncio.to_thread(bound, 1), exp

    result, exp = asyncio.run(scenario())
    print(f"输入=线程池同步任务 判定=结果={result} 上下文={seen}")
    assert result == 2
    assert seen == {"cid": "cid-thread", "span_kind": "background"}
    assert exp.finished_spans()[0].trace_id == "TR-BG1"


def test_bind_context_async_task_inherits_and_isolates():
    async def job(i):
        await asyncio.sleep(0)
        return get_correlation_id()

    async def scenario():
        exp = InMemorySpanExporter()
        tr = Tracer(TracerConfig(exporter=exp))
        bound = []
        with tr.span("root", trace_id="TR-BG2"):
            for i in range(25):
                with correlation_context(f"cid-task-{i}"):
                    bound.append(bind_context(job, f"cid-task-{i}", tr))
            tasks = [f(i) for f, i in zip(bound, range(25))]
            return await asyncio.gather(*tasks), exp

    cids, exp = asyncio.run(scenario())
    print(f"输入=25 并发后台协程 判定=各自关联标识匹配: {cids == [f'cid-task-{i}' for i in range(25)]}")
    assert cids == [f"cid-task-{i}" for i in range(25)]
    bg = [s for s in exp.finished_spans() if s.kind == "background"]
    assert len(bg) == 25 and all(s.trace_id == "TR-BG2" for s in bg)


def test_background_exception_marks_span_error():
    async def failing():
        raise ValueError("bg-boom")

    async def scenario():
        exp = InMemorySpanExporter()
        tr = Tracer(TracerConfig(exporter=exp))
        with tr.span("root", trace_id="TR-BG3"):
            with correlation_context("cid-bgx"):
                bound = bind_context(failing, "cid-bgx", tr)
            with pytest.raises(ValueError):
                await bound()
        tr.export_finished()
        return exp

    exp = asyncio.run(scenario())
    bg = [s for s in exp.finished_spans() if s.kind == "background"][0]
    print(f"输入=后台抛 ValueError 判定=片段 {bg.status}/{bg.error_type}")
    assert bg.status == "ERROR" and bg.error_type == "ValueError"


def test_missing_correlation_id_is_rejected():
    """未绑定上下文时强取关联标识应被显式拒绝（装配缺陷可解释）。"""
    assert get_correlation_id() is None
    with pytest.raises(LookupError):
        require_correlation_id()
    print("输入=空上下文 require_correlation_id() 判定=LookupError 拒绝")


def test_streaming_logs_share_correlation_id(client, list_handler):
    r = client.get("/stream?count=2", headers={"X-Correlation-ID": "cid-streamlogs"})
    assert r.status_code == 200
    chunk_logs = [p for p in list_handler.payloads() if p.get("event") == "stream_chunk"]
    print(f"输入=stream 2 块 判定={len(chunk_logs)} 条 chunk 日志均带同一标识")
    assert len(chunk_logs) == 2
    assert all(p["correlation_id"] == "cid-streamlogs" for p in chunk_logs)


def test_stream_spans_exported_before_shutdown(client, span_exporter):
    """流式片段在请求结束时就已导出（无需等关停），避免进程退出丢数据。"""
    client.get("/stream?count=1")
    spans = span_exporter.finished_spans()
    kinds = sorted({s.kind for s in spans})
    print(f"输入=1 次流式请求（未关停） 判定=已导出片段类型={kinds}")
    assert "stream" in kinds and "server" in kinds


def test_metrics_endpoint_is_itself_correlated(client):
    r = client.get("/metrics", headers={"X-Correlation-ID": "cid-metrics"})
    payload = r.json()
    print(f"输入=GET /metrics 判定=totals={json.dumps(payload['totals'])}")
    assert payload["correlation_id"] == "cid-metrics"
    assert "series" in payload and "totals" in payload
