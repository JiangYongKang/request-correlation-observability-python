"""客户端断连与服务端错误的分类测试：指标、日志、追踪三处结论一致。

判定依据（每条用例打印）：
- 响应完成前收到 ``http.disconnect`` ⇒ client_disconnected
- 请求任务被取消（CancelledError）⇒ client_disconnected
- **写响应（含流式中途/末帧）时 send 抛出"对端已关闭"异常**
  （h11.RemoteProtocolError / ConnectionResetError / BrokenPipeError 等）
  ⇒ client_disconnected
- 未捕获异常 / 5xx ⇒ server_error（计入错误率）
client_disconnected 既不算成功，也不计入服务端错误率，且不再向上抛。
"""

from __future__ import annotations

import asyncio

import h11
import pytest

from app.config import ObservabilitySettings
from app.errors import is_client_disconnect
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
    """请求任务被取消 ⇒ client_disconnected；追踪链保留、与 5xx 可区分，且不再上抛。"""

    async def app(scope, receive, send):
        raise asyncio.CancelledError()

    mw, tracer, exporter, metrics = _make_middleware(app)

    async def receive():
        await asyncio.sleep(3600)
        return {"type": "http.request"}

    # 新契约：客户端断连不再把异常抛给上层，中间件干净收尾
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

    # rate=0 + 断连：同样不上抛，中间件干净收尾
    asyncio.run(mw(_scope(), receive, _Send()))
    stats = tracer.sampling_stats()
    print(
        f"输入=rate=0 + 断连 判定=导出 0 条，dropped={stats['traces_dropped']}，"
        f"指标 client_disconnected={metrics.snapshot()['totals']['client_disconnected']}"
    )
    assert exporter.finished_spans() == []
    assert stats["traces_dropped"] == 1
    assert metrics.snapshot()["totals"]["client_disconnected"] == 1


# ---- 写响应中途客户端断开（核心修复场景） ----


class _SendRaising:
    """前 n_ok 次正常，之后写响应抛出指定异常（模拟对端已关闭）。"""

    def __init__(self, exc: BaseException, *, fail_on: int = 2) -> None:
        self._exc = exc
        self._fail_on = fail_on
        self.calls = 0

    async def __call__(self, message: dict) -> None:
        self.calls += 1
        if self.calls >= self._fail_on:
            raise self._exc


@pytest.mark.parametrize(
    "exc_factory",
    [
        lambda: h11.RemoteProtocolError("server disconnected before reply"),
        lambda: ConnectionResetError(104, "Connection reset by peer"),
        lambda: BrokenPipeError(32, "Broken pipe"),
    ],
    ids=["h11.RemoteProtocolError", "ConnectionResetError", "BrokenPipeError"],
)
def test_mid_response_send_failure_is_client_disconnect(exc_factory, list_handler):
    """响应写到一半对端关闭：归为 client_disconnected，不抬错误率、不上抛。"""
    exc = exc_factory()

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"partial\n", "more_body": True})
        # 下一块写入失败：模拟客户端在读到一半时断开
        await send({"type": "http.response.body", "body": b"more\n", "more_body": True})

    mw, tracer, exporter, metrics = _make_middleware(app)

    async def receive():
        await asyncio.sleep(3600)
        return {"type": "http.request"}

    # 必须不抛异常给上层
    asyncio.run(mw(_scope("/stream"), receive, _SendRaising(exc, fail_on=3)))
    totals = metrics.snapshot()["totals"]
    spans = exporter.finished_spans()
    events = {p.get("event"): p for p in list_handler.payloads()}
    print(
        f"输入=流式中途 send 抛 {type(exc).__name__} 关联标识={spans[0].trace_id} "
        f"判定=client_disconnected，error_rate={totals['error_rate']}，"
        f"outcome_reason={events['request_finished']['outcome_reason']}"
    )
    assert totals["client_disconnected"] == 1
    assert totals["server_error"] == 0
    assert totals["success"] == 0
    assert totals["error_rate"] == 0.0
    # 追踪：根片段 ClientDisconnect，而不是服务端异常类型
    assert len(spans) == 1
    assert spans[0].status == "ERROR"
    assert spans[0].error_type == "ClientDisconnect"
    # 日志三处结论一致
    assert events["request_finished"]["outcome"] == "client_disconnected"
    assert events["request_finished"]["outcome_reason"].startswith(
        "send_failed_client_gone:"
    )
    assert events["client_disconnected"]["disconnect_reason"].startswith(
        "send_failed_client_gone:"
    )
    assert "unhandled_exception" not in events
    assert "response_aborted_after_start" not in events


def test_final_body_send_failure_is_client_disconnect(list_handler):
    """末帧（more_body=False）发送时对端关闭：同样归为断连，而非服务端错误。"""

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"only"})  # 末帧失败

    mw, tracer, exporter, metrics = _make_middleware(app)

    async def receive():
        await asyncio.sleep(3600)
        return {"type": "http.request"}

    asyncio.run(
        mw(
            _scope("/stream"),
            receive,
            _SendRaising(h11.RemoteProtocolError("gone"), fail_on=2),
        )
    )
    totals = metrics.snapshot()["totals"]
    spans = exporter.finished_spans()
    print(
        f"输入=末帧 send 抛 RemoteProtocolError 关联标识={spans[0].trace_id} "
        f"判定=client_disconnected，error_rate={totals['error_rate']}"
    )
    assert totals["client_disconnected"] == 1
    assert totals["server_error"] == 0 and totals["error_rate"] == 0.0
    assert spans[0].error_type == "ClientDisconnect"


def test_server_side_send_error_still_server_error(list_handler):
    """send 抛出非"对端关闭"异常仍是服务端错误，不能被断连判定吞掉。"""

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"x", "more_body": True})

    mw, tracer, exporter, metrics = _make_middleware(app)

    async def receive():
        return {"type": "http.request", "body": b""}

    asyncio.run(mw(_scope("/stream"), receive, _SendRaising(RuntimeError("disk full"), fail_on=2)))
    totals = metrics.snapshot()["totals"]
    spans = exporter.finished_spans()
    print(
        f"输入=send 抛 RuntimeError(disk full) 判定=server_error，"
        f"error_rate={totals['error_rate']}，片段={spans[0].error_type}"
    )
    assert totals["server_error"] == 1
    assert totals["client_disconnected"] == 0
    assert totals["error_rate"] == 1.0
    assert spans[0].status == "ERROR" and spans[0].error_type == "RuntimeError"


def test_is_client_disconnect_classifier():
    """识别函数的判定边界：只认对端关闭语义，不按消息文本猜。"""
    assert is_client_disconnect(asyncio.CancelledError())
    assert is_client_disconnect(ConnectionResetError())
    assert is_client_disconnect(BrokenPipeError())
    assert is_client_disconnect(h11.RemoteProtocolError("x"))
    # 服务端自身错误不得误判
    assert not is_client_disconnect(RuntimeError("connection reset in business logic"))
    assert not is_client_disconnect(ValueError("bad input"))


# ---- 断连样本的采样边界：开启则整链保留，彻底关闭则零导出 ----


def _disconnect_middleware(rate: float):
    exporter = InMemorySpanExporter()
    tracer = Tracer(
        TracerConfig(sample_rate=rate, exporter=exporter, tail_sampling=True)
    )
    metrics = RequestMetrics()
    settings = ObservabilitySettings(spans_export_path="")

    async def app(scope, receive, send):
        # 服务端子片段：验证断连后父子链完整、不是只剩半棵树
        with tracer.span("db.query"):
            await asyncio.sleep(0)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"p1", "more_body": True})
        with tracer.span("render.next-chunk"):
            await asyncio.sleep(0)
        await send({"type": "http.response.body", "body": b"p2", "more_body": True})

    mw = ObservabilityMiddleware(app, settings=settings, tracer=tracer, metrics=metrics)
    return mw, tracer, exporter, metrics


def test_mid_stream_disconnect_trace_kept_whole_when_sampling_on(list_handler):
    """采样开启（rate>0）：中途断连的请求整链保留，父子关系完整对得上。"""
    mw, tracer, exporter, metrics = _disconnect_middleware(rate=1.0)

    async def receive():
        await asyncio.sleep(3600)
        return {"type": "http.request"}

    asyncio.run(
        mw(_scope("/stream"), receive, _SendRaising(
            h11.RemoteProtocolError("gone"), fail_on=3))
    )
    spans = exporter.finished_spans()
    by_id = {s.span_id: s for s in spans}
    print(
        f"输入=rate=1.0 + 流式中途断开（根+2 子片段） 关联标识={spans[0].trace_id if spans else None} "
        f"判定=保留 {len(spans)} 个片段：{[(s.name, s.parent_id is not None) for s in spans]}"
    )
    # 整棵树都在：根 + db.query + render.next-chunk
    assert len(spans) == 3
    roots = [s for s in spans if s.parent_id is None]
    assert len(roots) == 1
    root = roots[0]
    assert root.error_type == "ClientDisconnect" and root.status == "ERROR"
    for child in spans:
        if child is root:
            continue
        assert child.parent_id in by_id  # 父片段找得到，父子对得上
        assert child.trace_id == root.trace_id  # 同 trace
    # 采样统计：1 棵 trace 因失败/中断保留
    stats = tracer.sampling_stats()
    assert stats["traces_kept"] == 1
    assert stats["traces_kept_for_error"] == 1
    # 指标与日志结论一致
    totals = metrics.snapshot()["totals"]
    assert totals["client_disconnected"] == 1 and totals["error_rate"] == 0.0
    events = {p.get("event"): p for p in list_handler.payloads()}
    assert events["request_finished"]["outcome"] == "client_disconnected"


def test_mid_stream_disconnect_no_export_when_sampling_off(list_handler):
    """采样彻底关闭（rate=0）：中途断连也不新增任何导出，只留计数与日志。"""
    mw, tracer, exporter, metrics = _disconnect_middleware(rate=0.0)

    async def receive():
        await asyncio.sleep(3600)
        return {"type": "http.request"}

    asyncio.run(
        mw(_scope("/stream"), receive, _SendRaising(
            h11.RemoteProtocolError("gone"), fail_on=3))
    )
    spans = exporter.finished_spans()
    stats = tracer.sampling_stats()
    totals = metrics.snapshot()["totals"]
    print(
        f"输入=rate=0 + 流式中途断开（根+2 子片段） 判定=导出 {len(spans)} 条，"
        f"dropped_traces={stats['traces_dropped']}，client_disconnected={totals['client_disconnected']}"
    )
    # 彻底关闭：不新增任何导出（连失败/中断样本也不写）
    assert spans == []
    assert stats["traces_dropped"] == 1
    assert stats["spans_dropped"] == 3  # 三个片段全部判丢，计数仍在
    # 计数与日志仍保留，结论仍是客户端断连而非服务端错误
    assert totals["client_disconnected"] == 1
    assert totals["server_error"] == 0 and totals["error_rate"] == 0.0
    events = {p.get("event"): p for p in list_handler.payloads()}
    assert events["request_finished"]["outcome"] == "client_disconnected"


def test_server_error_then_terminator_reveals_client_gone(list_handler):
    """流式中途先抛服务端异常，发终止帧时才发现对端关闭：改判 client_disconnected。

    判定依据：根片段虽然一度按 RuntimeError/500 结束，但终止帧 send 抛出
    对端关闭异常（h11.RemoteProtocolError），说明客户端已走——最终指标、
    日志、追踪三处结论都以 client_disconnected 为准，不抬错误率。
    """

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"partial\n", "more_body": True})
        raise RuntimeError("render failed")

    mw, tracer, exporter, metrics = _make_middleware(app)

    async def receive():
        return {"type": "http.request", "body": b""}

    class _Send:
        def __init__(self):
            self.n = 0

        async def __call__(self, message):
            self.n += 1
            # 第 3 次（终止帧）才发现对端已关闭
            if self.n == 3:
                raise h11.RemoteProtocolError("peer closed")

    asyncio.run(mw(_scope("/stream"), receive, _Send()))
    totals = metrics.snapshot()["totals"]
    spans = exporter.finished_spans()
    events = list_handler.payloads()
    finished = [p for p in events if p.get("event") == "request_finished"][0]
    print(
        f"输入=流式 RuntimeError 后终止帧 RemoteProtocolError 关联标识={spans[0].trace_id} "
        f"判定=改判 client_disconnected，error_rate={totals['error_rate']}，"
        f"片段={spans[0].error_type}"
    )
    assert totals["client_disconnected"] == 1
    assert totals["server_error"] == 0 and totals["error_rate"] == 0.0
    assert finished["outcome"] == "client_disconnected"
    assert finished["outcome_reason"].startswith("send_failed_client_gone:")
    assert spans[0].error_type == "ClientDisconnect"
