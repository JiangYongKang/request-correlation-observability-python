"""客户端断连与服务端错误的分类测试：指标、日志、追踪三处结论一致。

判定依据（每条用例打印）：
- 响应完成前收到 ``http.disconnect`` ⇒ client_disconnected
- 请求任务被取消（CancelledError）⇒ client_disconnected
- 未捕获异常 / 5xx ⇒ server_error（计入错误率）
client_disconnected 既不算成功，也不计入服务端错误率。
"""

from __future__ import annotations

import asyncio
import json

import pytest

from app.config import ObservabilitySettings
from app.metrics import RequestMetrics
from app.middleware import ObservabilityMiddleware
from app.tracing import InMemorySpanExporter, Tracer, TracerConfig


def _make_middleware(app, *, tail_sampling: bool = True):
    exporter = InMemorySpanExporter()
    tracer = Tracer(
        TracerConfig(sample_rate=1.0, exporter=exporter, tail_sampling=tail_sampling)
    )
    metrics = RequestMetrics()
    settings = ObservabilitySettings(spans_export_path="")
    mw = ObservabilityMiddleware(app, settings=settings, tracer=tracer, metrics=metrics)
    return mw, tracer, exporter, metrics


def _scope(path: str = "/slow") -> dict:
    return {"type": "http", "method": "GET", "path": path, "headers": [], "state": {}}


class _Send:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def __call__(self, message: dict) -> None:
        self.messages.append(message)


def test_disconnect_before_response_classified(list_handler):
    """响应完成前收到 http.disconnect ⇒ client_disconnected，不算成功也不算服务端错误。"""

    async def app(scope, receive, send):
        message = await receive()  # 客户端在响应前断开
        assert message["type"] == "http.disconnect"
        return  # 不再发送响应

    mw, tracer, exporter, metrics = _make_middleware(app)

    async def receive():
        return {"type": "http.disconnect"}

    asyncio.run(mw(_scope(), receive, _Send()))
    totals = metrics.snapshot()["totals"]
    spans = exporter.finished_spans()
    logs = [p for p in list_handler.payloads() if p.get("event") == "request_finished"]
    print(
        f"输入=响应前 http.disconnect 关联标识={spans[0].trace_id if spans else None} "
        f"判定=outcome={totals}，片段状态={spans[0].status if spans else None}/"
        f"{spans[0].error_type if spans else None}"
    )
    assert totals["client_disconnected"] == 1
    assert totals["success"] == 0
    assert totals["server_error"] == 0
    assert totals["error_rate"] == 0.0  # 不拉高错误率
    # 追踪链完整保留（采样未关闭 ⇒ 没跑完的请求必须留下）
    assert len(spans) == 1
    assert spans[0].status == "ERROR"
    assert spans[0].error_type == "ClientDisconnect"
    # 日志可区分且带判定依据
    assert logs and logs[0]["outcome"] == "client_disconnected"
    assert logs[0]["outcome_reason"] == "http_disconnect_before_response_complete"


def test_cancelled_task_classified_and_trace_retained(list_handler):
    """请求任务被取消 ⇒ client_disconnected；追踪链保留且与 5xx 可区分。"""

    async def app(scope, receive, send):
        raise asyncio.CancelledError()

    mw, tracer, exporter, metrics = _make_middleware(app)

    async def receive():
        await asyncio.sleep(3600)
        return {"type": "http.request"}

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(mw(_scope("/stream"), receive, _Send()))
    totals = metrics.snapshot()["totals"]
    spans = exporter.finished_spans()
    events = {p.get("event"): p for p in list_handler.payloads()}
    print(
        f"输入=任务被取消 关联标识={spans[0].trace_id if spans else None} "
        f"判定=outcome=client_disconnected(task_cancelled)，错误率={totals['error_rate']}"
    )
    assert totals["client_disconnected"] == 1
    assert totals["server_error"] == 0 and totals["error_rate"] == 0.0
    assert len(spans) == 1
    assert spans[0].status == "ERROR" and spans[0].error_type == "ClientDisconnect"
    assert "client_disconnected" in events
    assert events["client_disconnected"]["disconnect_reason"] == "task_cancelled"
    assert events["request_finished"]["outcome_reason"] == "task_cancelled"


def test_disconnect_after_response_complete_is_normal():
    """响应发完之后客户端才断开 ⇒ 正常成功，不误判。"""

    async def app(scope, receive, send):
        body = b"{}"
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": body})

    mw, tracer, exporter, metrics = _make_middleware(app)

    async def receive():
        return {"type": "http.disconnect"}  # 响应完成后才到达

    asyncio.run(mw(_scope("/"), receive, _Send()))
    totals = metrics.snapshot()["totals"]
    print(f"输入=响应完成后 disconnect 判定=success，totals={totals}")
    assert totals["success"] == 1
    assert totals["client_disconnected"] == 0


def test_server_error_still_server_error(list_handler):
    """服务端异常仍计 server_error 并抬高错误率，与断连分开算。"""

    async def app(scope, receive, send):
        raise RuntimeError("db down")

    mw, tracer, exporter, metrics = _make_middleware(app)

    async def receive():
        return {"type": "http.request", "body": b""}

    send = _Send()
    asyncio.run(mw(_scope("/boom"), receive, send))
    totals = metrics.snapshot()["totals"]
    spans = exporter.finished_spans()
    logs = [p for p in list_handler.payloads() if p.get("event") == "request_finished"]
    print(
        f"输入=未捕获 RuntimeError 判定=server_error，error_rate={totals['error_rate']}，"
        f"片段状态={spans[0].status}/{spans[0].error_type}"
    )
    assert totals["server_error"] == 1
    assert totals["client_disconnected"] == 0
    assert totals["error_rate"] == 1.0
    assert spans[0].status == "ERROR" and spans[0].error_type == "RuntimeError"
    assert logs[0]["outcome"] == "server_error"
    assert logs[0]["outcome_reason"] == "status_500"


def test_disconnect_not_exported_when_sampling_disabled():
    """采样彻底关闭（rate=0）时断连样本也不写，只留计数与日志。"""
    exporter = InMemorySpanExporter()
    tracer = Tracer(TracerConfig(sample_rate=0.0, exporter=exporter, tail_sampling=True))
    metrics = RequestMetrics()
    settings = ObservabilitySettings(spans_export_path="")

    async def app(scope, receive, send):
        raise asyncio.CancelledError()

    mw = ObservabilityMiddleware(app, settings=settings, tracer=tracer, metrics=metrics)

    async def receive():
        await asyncio.sleep(3600)
        return {"type": "http.request"}

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(mw(_scope(), receive, _Send()))
    stats = tracer.sampling_stats()
    print(
        f"输入=rate=0 + 断连 判定=导出 0 条，dropped={stats['traces_dropped']}，"
        f"指标 client_disconnected={metrics.snapshot()['totals']['client_disconnected']}"
    )
    assert exporter.finished_spans() == []
    assert stats["traces_dropped"] == 1
    assert metrics.snapshot()["totals"]["client_disconnected"] == 1


class _FailOnSecondBody:
    """前两条响应消息正常，第二条 body 开始抛出指定异常（模拟写盘时客户端已走）。"""

    def __init__(self, exc: BaseException) -> None:
        self.messages: list[dict] = []
        self._body_count = 0
        self._exc = exc

    async def __call__(self, message: dict) -> None:
        self.messages.append(message)
        if message["type"] == "http.response.body":
            self._body_count += 1
            if self._body_count >= 2:
                raise self._exc


class ClientDisconnect(Exception):
    """模拟 starlette/uvicorn 的同名断连异常（按类型名识别）。"""


async def _two_chunk_app(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"chunk-1", "more_body": True})
    await send({"type": "http.response.body", "body": b"chunk-2", "more_body": False})


@pytest.mark.parametrize(
    "exc_factory",
    [
        lambda: BrokenPipeError(32, "Broken pipe"),
        lambda: ConnectionResetError(104, "Connection reset by peer"),
        lambda: ClientDisconnect("client gone"),
    ],
    ids=["broken_pipe", "connection_reset", "client_disconnect_class"],
)
def test_disconnect_during_response_write_classified(list_handler, exc_factory):
    """写响应中途客户端断开 ⇒ client_disconnected：不算成功、不进错误率、异常不上抛。"""
    mw, tracer, exporter, metrics = _make_middleware(_two_chunk_app)

    async def receive():
        await asyncio.sleep(3600)
        return {"type": "http.request"}

    send = _FailOnSecondBody(exc_factory())
    exc = send._exc
    # 关键断言 0：异常不再抛给上层（调用正常返回）
    asyncio.run(mw(_scope("/stream"), receive, send))
    totals = metrics.snapshot()["totals"]
    spans = exporter.finished_spans()
    events = {p.get("event"): p for p in list_handler.payloads()}
    print(
        f"输入=第二块 body 写入抛 {type(exc).__name__} 关联标识={spans[0].trace_id if spans else None} "
        f"判定=outcome=client_disconnected(response_write_failed_client_gone)，"
        f"错误率={totals['error_rate']}，片段={spans[0].status if spans else None}/"
        f"{spans[0].error_type if spans else None}"
    )
    # 指标：不算成功，也不计入服务端错误率
    assert totals["client_disconnected"] == 1
    assert totals["success"] == 0
    assert totals["server_error"] == 0
    assert totals["error_rate"] == 0.0
    # 追踪：根片段标 ClientDisconnect，与服务端异常可区分；采样未关 ⇒ 整链保留
    assert len(spans) == 1
    assert spans[0].status == "ERROR" and spans[0].error_type == "ClientDisconnect"
    # 日志：三处结论一致（断连事件 + 收尾事件同一判定依据）
    assert events["client_disconnected"]["disconnect_reason"] == "response_write_failed_client_gone"
    assert events["client_disconnected"]["error_type"] == type(exc).__name__
    assert events["request_finished"]["outcome"] == "client_disconnected"
    assert events["request_finished"]["outcome_reason"] == "response_write_failed_client_gone"


def test_disconnect_write_failure_trace_tree_complete(list_handler):
    """断连时整棵树一起留下：根+子片段同 trace、父子对得上，不是半棵树。"""
    exporter = InMemorySpanExporter()
    tracer = Tracer(TracerConfig(sample_rate=1.0, exporter=exporter, tail_sampling=True))
    metrics = RequestMetrics()
    settings = ObservabilitySettings(spans_export_path="")

    async def app(scope, receive, send):
        with tracer.span("db-query"):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"c1", "more_body": True})
        await send({"type": "http.response.body", "body": b"c2", "more_body": False})

    mw = ObservabilityMiddleware(app, settings=settings, tracer=tracer, metrics=metrics)

    async def receive():
        await asyncio.sleep(3600)
        return {"type": "http.request"}

    asyncio.run(mw(_scope("/stream"), receive, _FailOnSecondBody(BrokenPipeError())))
    spans = exporter.finished_spans()
    by_name = {s.name: s for s in spans}
    print(
        f"输入=根+子两片断树写中途断连 关联标识={spans[0].trace_id if spans else None} "
        f"判定=导出 {len(spans)} 条，父子完整"
    )
    assert len(spans) == 2  # 整树保留，不是半棵
    root = next(s for s in spans if s.parent_id is None)
    child = by_name["db-query"]
    assert child.parent_id == root.span_id  # 父子对得上
    assert child.trace_id == root.trace_id
    assert root.status == "ERROR" and root.error_type == "ClientDisconnect"
    assert child.status == "OK"


def test_disconnect_write_failure_retained_when_sampled_out():
    """采样未彻底关闭（0<rate<1）且本 trace 未抽中：断连样本仍整链保留。"""
    from app.sampling import Sampler

    sampler = Sampler(0.5)  # 与 TracerConfig(sample_rate=0.5) 同默认种子
    cid = next(
        c for c in (f"cid-candidate-{i}" for i in range(1000))
        if not sampler.decide(c, "/slow").kept
    )
    exporter = InMemorySpanExporter()
    tracer = Tracer(TracerConfig(sample_rate=0.5, exporter=exporter, tail_sampling=True))
    metrics = RequestMetrics()
    settings = ObservabilitySettings(spans_export_path="")
    mw = ObservabilityMiddleware(_two_chunk_app, settings=settings, tracer=tracer, metrics=metrics)

    async def receive():
        await asyncio.sleep(3600)
        return {"type": "http.request"}

    scope = {
        "type": "http", "method": "GET", "path": "/slow",
        "headers": [(b"x-correlation-id", cid.encode("latin-1"))], "state": {},
    }
    asyncio.run(mw(scope, receive, _FailOnSecondBody(BrokenPipeError())))
    spans = exporter.finished_spans()
    stats = tracer.sampling_stats()
    print(
        f"输入=rate=0.5 且 {cid} 未抽中 + 写中途断连 "
        f"判定=失败保留导出 {len(spans)} 条，kept_for_error={stats['traces_kept_for_error']}"
    )
    assert len(spans) == 1  # 未抽中但断连 ⇒ 强制保留
    assert spans[0].trace_id == cid
    assert spans[0].error_type == "ClientDisconnect"
    assert stats["traces_kept_for_error"] == 1
    assert metrics.snapshot()["totals"]["client_disconnected"] == 1


def test_disconnect_write_failure_not_exported_when_sampling_disabled(list_handler):
    """采样彻底关闭（rate=0）：写中途断连也不新增导出，只留计数与日志。"""
    exporter = InMemorySpanExporter()
    tracer = Tracer(TracerConfig(sample_rate=0.0, exporter=exporter, tail_sampling=True))
    metrics = RequestMetrics()
    settings = ObservabilitySettings(spans_export_path="")
    mw = ObservabilityMiddleware(_two_chunk_app, settings=settings, tracer=tracer, metrics=metrics)

    async def receive():
        await asyncio.sleep(3600)
        return {"type": "http.request"}

    asyncio.run(mw(_scope(), receive, _FailOnSecondBody(BrokenPipeError())))
    stats = tracer.sampling_stats()
    totals = metrics.snapshot()["totals"]
    events = {p.get("event"): p for p in list_handler.payloads()}
    print(
        f"输入=rate=0 + 写中途断连 判定=导出 0 条，dropped={stats['traces_dropped']}，"
        f"指标 client_disconnected={totals['client_disconnected']}，"
        f"日志 outcome={events['request_finished']['outcome']}"
    )
    assert exporter.finished_spans() == []  # 不新增任何导出
    assert stats["traces_dropped"] == 1
    assert totals["client_disconnected"] == 1  # 计数仍在
    assert totals["server_error"] == 0 and totals["error_rate"] == 0.0
    assert events["request_finished"]["outcome"] == "client_disconnected"
    assert events["client_disconnected"]["disconnect_reason"] == "response_write_failed_client_gone"
