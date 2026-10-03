"""失败与客户端断连的分类语义：分得清、判定依据明确、断连不污染错误率。"""

from __future__ import annotations

import asyncio
import json

import pytest

from app.config import ObservabilitySettings
from app.logging_setup import configure_logging
from app.metrics import RequestMetrics
from app.middleware import ObservabilityMiddleware
from app.tracing import InMemorySpanExporter, Tracer, TracerConfig

configure_logging(10)


def _metrics_outcomes(metrics: RequestMetrics) -> dict[str, int]:
    snap = metrics.snapshot()
    return {s["labels"]["outcome"]: s["count"] for s in snap["series"]}  # type: ignore[index]


def _make_scope(path: str = "/work") -> dict:
    return {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [(b"x-correlation-id", b"cid-disconnect-1")],
    }


def _http_messages() -> list[dict]:
    return [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"ok"},
    ]


def test_metrics_client_disconnect_not_success_not_server_error():
    """client_disconnect：不算成功、不算服务端错误、不进入错误率。"""
    metrics = RequestMetrics()
    metrics.record(route="/ok", method="GET", status_code=200, duration_ms=1.0)
    metrics.record(route="/boom", method="GET", status_code=500, duration_ms=1.0)
    metrics.record(
        route="/gone", method="GET", status_code=None,
        duration_ms=1.0, outcome="client_disconnect",
    )
    totals = metrics.snapshot()["totals"]
    print(f"输入=1 成功 + 1 服务端错误 + 1 断连 判定=totals={totals}")
    assert totals["requests"] == 3
    assert totals["success"] == 1
    assert totals["server_error"] == 1
    assert totals["client_disconnect"] == 1
    # 错误率只统计服务端错误：1/3，断连不拉高也不摊薄分子
    assert totals["error_rate"] == round(1 / 3, 6)


def test_metrics_rejects_unknown_outcome():
    metrics = RequestMetrics()
    print("输入=outcome='weird' 判定=记录期拒绝非法分类")
    with pytest.raises(ValueError):
        metrics.record(route="/x", method="GET", status_code=200, duration_ms=0.1, outcome="weird")


def test_middleware_send_failure_classified_client_disconnect(list_handler):
    """写响应时连接异常 ⇒ client_disconnect，判定依据 send_failed:*。"""
    metrics = RequestMetrics()
    exp = InMemorySpanExporter()
    tracer = Tracer(TracerConfig(sample_rate=1.0, exporter=exp))
    settings = ObservabilitySettings(spans_export_path="")

    async def app(scope, receive, send):
        for message in _http_messages():
            await send(message)

    mw = ObservabilityMiddleware(app, settings=settings, tracer=tracer, metrics=metrics)

    async def broken_send(message):
        if message["type"] == "http.response.body":
            raise OSError("connection reset by peer")

    async def receive():
        return {"type": "http.request"}

    asyncio.run(mw(_make_scope(), receive, broken_send))

    outcomes = _metrics_outcomes(metrics)
    events = [p for p in list_handler.payloads() if p.get("event") == "client_disconnected"]
    spans = exp.finished_spans()
    root = next(s for s in spans if s.parent_id is None)
    print(
        f"输入=send 写 body 时 OSError 关联标识=cid-disconnect-1 "
        f"判定=outcome={outcomes}, basis={events[0]['basis']}, "
        f"根片段 error_type={root.error_type}"
    )
    assert outcomes == {"client_disconnect": 1}
    assert events and events[0]["basis"] == "send_failed:OSError"
    assert events[0]["correlation_id"] == "cid-disconnect-1"
    assert root.error_type == "ClientDisconnect"
    assert root.attributes["client_disconnect"] is True
    tracer.shutdown()


def test_middleware_cancellation_classified_client_disconnect(list_handler):
    """响应完成前任务被取消 ⇒ client_disconnect，判定依据 task_cancelled_*。"""
    metrics = RequestMetrics()
    exp = InMemorySpanExporter()
    tracer = Tracer(TracerConfig(sample_rate=1.0, exporter=exp))
    settings = ObservabilitySettings(spans_export_path="")

    async def app(scope, receive, send):
        raise asyncio.CancelledError()

    mw = ObservabilityMiddleware(app, settings=settings, tracer=tracer, metrics=metrics)
    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    async def receive():
        return {"type": "http.request"}

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(mw(_make_scope(), receive, send))

    outcomes = _metrics_outcomes(metrics)
    events = [p for p in list_handler.payloads() if p.get("event") == "client_disconnected"]
    root = next(s for s in exp.finished_spans() if s.parent_id is None)
    print(
        f"输入=任务在响应前被取消 关联标识=cid-disconnect-1 "
        f"判定=outcome={outcomes}, basis={events[0]['basis']}, 根片段 error_type={root.error_type}"
    )
    assert outcomes == {"client_disconnect": 1}
    assert events and events[0]["basis"] == "task_cancelled_before_response_complete"
    assert root.error_type == "ClientDisconnect"
    tracer.shutdown()


def test_server_error_still_classified_separately(list_handler):
    """服务端异常依旧计 server_error，与断连分类互不混淆。"""
    metrics = RequestMetrics()
    exp = InMemorySpanExporter()
    tracer = Tracer(TracerConfig(sample_rate=1.0, exporter=exp))
    settings = ObservabilitySettings(spans_export_path="")

    async def app(scope, receive, send):
        raise RuntimeError("db down")

    mw = ObservabilityMiddleware(app, settings=settings, tracer=tracer, metrics=metrics)
    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    async def receive():
        return {"type": "http.request"}

    asyncio.run(mw(_make_scope(), receive, send))

    outcomes = _metrics_outcomes(metrics)
    events = [p for p in list_handler.payloads() if p.get("event") == "unhandled_exception"]
    print(
        f"输入=业务抛 RuntimeError 关联标识=cid-disconnect-1 "
        f"判定=outcome={outcomes}, 事件={events[0]['event'] if events else None}"
    )
    assert outcomes == {"server_error": 1}
    assert events, "应记录 unhandled_exception 而非 client_disconnected"
    tracer.shutdown()


def test_stream_consumer_disconnect_marks_span_and_retained():
    """流式消费端提前关闭：stream 片段记 CancelledError，整链按失败保留。"""
    from app.streaming import instrumented_stream

    exp = InMemorySpanExporter()
    # 比例极小（>0）：成功样本必丢，断连样本必须兜底保留
    tracer = Tracer(TracerConfig(sample_rate=1e-9, exporter=exp))

    async def producer():
        for i in range(100):
            yield f"chunk-{i}\n".encode()
            await asyncio.sleep(0)

    async def main() -> None:
        with tracer.span("GET /stream", kind="server", trace_id="cid-stream-gone", path="/stream"):
            gen = instrumented_stream(producer(), correlation_id="cid-stream-gone", tracer=tracer)
            async for _ in gen:
                break  # 消费端拿一块就走 ⇒ 提前关闭生成器

    asyncio.run(main())
    tracer.export_finished()

    spans = exp.finished_spans()
    stream_span = next((s for s in spans if s.kind == "stream"), None)
    root = next((s for s in spans if s.parent_id is None), None)
    print(
        f"输入=消费端取 1 块后关闭, rate=1e-9 关联标识=cid-stream-gone "
        f"判定=保留 {len(spans)} 片, stream error_type="
        f"{stream_span.error_type if stream_span else None}"
    )
    assert stream_span is not None, "断连的流式链必须按失败样本保留"
    assert stream_span.status == "ERROR"
    assert stream_span.error_type in ("CancelledError", "GeneratorExit")
    assert root is not None and root.trace_id == "cid-stream-gone"
    tracer.shutdown()
