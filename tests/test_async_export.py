"""异步落盘与请求主链路解耦的量化测试。

判定依据（每条用例打印）：
- 请求主链路只做入队，不同步等待落盘/刷写：并发 N 个请求期间
  刷写（fsync）次数为 0，总耗时远小于 N × 单次刷盘耗时
  （旧实现逐请求同步刷盘 ⇒ 刷写次数 == 请求数，耗时下限 N × 刷盘耗时）；
- 周期刷写兜底：不做任何显式 flush，数据也会在
  ``flush_interval_s`` 内由后台线程落盘（可见性延迟有界）；
- 关停时排空队列并刷盘：缓冲区数据不丢，重启后历史可读（既有行为不退化）。
"""

from __future__ import annotations

import asyncio
import json
import time

from app.config import ObservabilitySettings
from app.exporter import RotatingFileSpanExporter
from app.metrics import RequestMetrics
from app.middleware import ObservabilityMiddleware
from app.tracing import Tracer, TracerConfig


class _SlowFlushExporter(RotatingFileSpanExporter):
    """每次实际刷盘人为延迟（模拟慢盘），并记录刷写次数。"""

    def __init__(self, *args, flush_delay_s: float = 0.05, **kwargs):
        super().__init__(*args, **kwargs)
        self.flush_delay_s = flush_delay_s
        self.flush_calls = 0

    def _flush_disk(self) -> None:
        if self._fh is not None and self._dirty:
            self.flush_calls += 1
            time.sleep(self.flush_delay_s)
        super()._flush_disk()


def _scope(path: str = "/items/1") -> dict:
    return {"type": "http", "method": "GET", "path": path, "headers": [], "state": {}}


class _Send:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def __call__(self, message: dict) -> None:
        self.messages.append(message)


async def _receive() -> dict:
    return {"type": "http.request", "body": b""}


async def _ok_app(scope, receive, send) -> None:
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"{}"})


def test_request_path_not_blocked_by_disk_flush(tmp_path):
    """并发 N 个请求：主链路零同步刷写，耗时不随请求数乘刷盘耗时增长。"""
    path = str(tmp_path / "spans.jsonl")
    n, flush_delay = 20, 0.05
    # 周期刷写拉到 1 小时：测试窗口内只可能剩"逐请求同步刷写"这一种来源
    exporter = _SlowFlushExporter(
        path, flush_interval_s=3600.0, flush_delay_s=flush_delay
    )
    tracer = Tracer(TracerConfig(sample_rate=1.0, exporter=exporter, tail_sampling=True))
    metrics = RequestMetrics()
    settings = ObservabilitySettings(spans_export_path="")
    mw = ObservabilityMiddleware(_ok_app, settings=settings, tracer=tracer, metrics=metrics)

    started = time.perf_counter()

    async def run_all() -> None:
        await asyncio.gather(
            *[mw(_scope(f"/items/{i}"), _receive, _Send()) for i in range(n)]
        )

    asyncio.run(run_all())
    elapsed = time.perf_counter() - started
    flushes_during = exporter.flush_calls
    print(
        f"输入={n} 个并发请求，单次刷盘人为延迟 {flush_delay}s "
        f"判定=请求期间刷写 {flushes_during} 次（旧实现={n} 次），"
        f"主链路耗时 {elapsed:.3f}s（旧实现下限 {n * flush_delay:.2f}s）"
    )
    # 量化断言 1：请求主链路没有发生任何同步刷写（不随请求数线性增长）
    assert flushes_during == 0
    # 量化断言 2：主链路耗时远小于 N × 单次刷盘耗时（留 2 倍余量仍远低于旧下限）
    assert elapsed < n * flush_delay / 2

    # 既有行为不退化：关停排空队列并刷盘，缓冲区数据不丢
    tracer.shutdown()
    rows = [json.loads(line) for line in open(path, encoding="utf-8")]
    print(
        f"输入=关停后读盘 判定=落盘 {len(rows)} 条，"
        f"关停刷写后总刷写 {exporter.flush_calls} 次"
    )
    assert len(rows) == n
    assert exporter.flush_calls >= 1  # 关停时统一刷写一次
    assert exporter.stats()["written_spans"] == n
    assert metrics.snapshot()["totals"]["success"] == n


def test_periodic_flush_makes_data_durable_without_explicit_flush(tmp_path):
    """周期刷写兜底：无任何显式 flush，数据也在间隔内由后台线程落盘。"""
    path = str(tmp_path / "spans.jsonl")
    exporter = RotatingFileSpanExporter(path, flush_interval_s=0.05)
    tracer = Tracer(TracerConfig(exporter=exporter))
    with tracer.span("periodic", trace_id="cid-periodic"):
        pass
    tracer.export_finished()  # 仅入队，不刷盘

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and exporter.stats()["flushes"] < 1:
        time.sleep(0.01)
    stats = exporter.stats()
    print(
        f"输入=1 个片段，flush_interval_s=0.05，无显式 flush "
        f"判定=后台刷写 {stats['flushes']} 次，落盘 {stats['written_spans']} 条"
    )
    assert stats["flushes"] >= 1  # 周期刷写生效，可见性延迟以间隔为上界
    rows = [json.loads(line) for line in open(path, encoding="utf-8")]
    assert [row["name"] for row in rows] == ["periodic"]
    tracer.shutdown()
