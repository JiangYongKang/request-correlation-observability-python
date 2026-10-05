"""并发落盘不得阻塞请求主链路的量化测试。

修复前：中间件在每个请求出口同步 ``exporter.flush()``——插入屏障并等待
写线程 fsync，刷写次数与请求数约 1:1，磁盘一慢尾延迟就被抬高。
修复后：请求出口只把片段入队（非阻塞），写线程批量合并写入并按固定
周期刷盘；显式 flush 屏障只在关停等边界使用。

判定依据（每条用例打印输入与量化结果）：
- 并发 N 个请求期间发生的 fsync 次数 / flush 屏障次数不随 N 增长；
- 主链路墙钟耗时与"每请求一次慢速 I/O"的旧行为拉开数量级差距；
- 全部片段最终落盘（关停排空不丢）、行不串数据。
"""

from __future__ import annotations

import asyncio
import glob
import json
import time

from app.config import ObservabilitySettings
from app.exporter import RotatingFileSpanExporter
from app.middleware import ObservabilityMiddleware
from app.metrics import RequestMetrics
from app.tracing import Tracer, TracerConfig


def _scope(path: str) -> dict:
    return {"type": "http", "method": "GET", "path": path, "headers": []}


async def _ok_app(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"{}"})


async def _never_receive():
    await asyncio.sleep(3600)
    return {"type": "http.request"}


def _build_app(exporter):
    tracer = Tracer(
        TracerConfig(sample_rate=1.0, exporter=exporter, tail_sampling=True)
    )
    metrics = RequestMetrics()
    settings = ObservabilitySettings(spans_export_path="")
    mw = ObservabilityMiddleware(_ok_app, settings=settings, tracer=tracer, metrics=metrics)
    return mw, tracer, metrics


class _CountingSend:
    def __init__(self) -> None:
        self.final = False

    async def __call__(self, message: dict) -> None:
        if message["type"] == "http.response.body" and not message.get("more_body"):
            self.final = True


def test_concurrent_requests_not_blocked_by_disk_io(tmp_path):
    """并发 64 请求：主链路不等落盘；fsync 次数与请求数脱钩（量化）。"""
    path = str(tmp_path / "spans.jsonl")
    # fsync 间隔拉长到测试全程都不会触发周期 fsync：请求期间 fsync 必须为 0
    exporter = RotatingFileSpanExporter(
        path,
        max_bytes=10**9,
        fsync_interval_s=3600,
        autoflush_interval_s=3600,
    )
    mw, tracer, metrics = _build_app(exporter)
    n = 64

    async def one(i: int) -> None:
        await mw(_scope(f"/item/{i}"), _never_receive, _CountingSend())

    async def _run_all():
        await asyncio.gather(*(one(i) for i in range(n)))

    started = time.perf_counter()
    asyncio.run(_run_all())
    elapsed_ms = (time.perf_counter() - started) * 1000
    during = exporter.stats()
    print(
        f"输入={n} 个并发请求（fsync_interval=3600s） 主链路墙钟={elapsed_ms:.1f}ms "
        f"判定=请求期间 fsyncs={during['fsyncs']} flush_barriers={during['flush_barriers']} "
        f"write_batches={during['write_batches']}（应与 {n} 脱钩）"
    )
    # 量化断言：请求主链路不发生任何 fsync / flush 屏障
    assert during["fsyncs"] == 0
    assert during["flush_barriers"] == 0
    # 纯入队的主链路应当非常快（留足 CI 余量，仍远小于逐请求落盘）
    assert elapsed_ms < 2000, f"主链路疑似被落盘拖住：{elapsed_ms:.1f}ms"

    # 关停排空：全部片段落盘、零丢失
    tracer.shutdown()
    rows = []
    for fpath in glob.glob(f"{path}*"):
        with open(fpath, encoding="utf-8") as fh:
            rows.extend(json.loads(line) for line in fh)
    final = exporter.stats()
    print(
        f"关停后判定=落盘 {len(rows)} 行，written={final['written_spans']}，"
        f"fsyncs={final['fsyncs']}（仅关停屏障），dropped={final['dropped_spans']}"
    )
    assert len(rows) == n
    assert final["written_spans"] == n
    assert final["dropped_spans"] == 0
    assert final["fsyncs"] >= 1  # 关停屏障确实 fsync 了
    totals = metrics.snapshot()["totals"]
    assert totals["requests"] == n and totals["success"] == n
    # 每行合法 JSON 且 trace_id 唯一 ⇒ 并发写没有串数据
    trace_ids = {row["trace_id"] for row in rows}
    assert len(trace_ids) == n


def test_slow_disk_does_not_inflate_request_latency(tmp_path):
    """慢速磁盘（每次 write 50ms）下：批量合并使主链路耗时与请求数脱钩。

    旧行为（每请求一次同步刷盘）下 N=40 至少 ~2s（40 × 50ms）；
    新行为请求只入队，写线程把一批合并为少数几次 write。
    """
    path = str(tmp_path / "spans.jsonl")
    exporter = RotatingFileSpanExporter(
        path,
        max_bytes=10**9,
        fsync_interval_s=3600,
        autoflush_interval_s=3600,
    )
    slow_writes = {"count": 0}

    # 延迟打开后替换文件对象：每次物理 writelines 都 sleep，
    # 导出器在写线程内调用，因此不影响主链路，只用于验证批次远少于请求数。
    class SlowFile:
        def __init__(self, fh):
            self._fh = fh

        def writelines(self, lines):
            slow_writes["count"] += 1
            time.sleep(0.05)
            return self._fh.writelines(lines)

        def __getattr__(self, name):
            return getattr(self._fh, name)

    started_thread = {"on": False}
    orig_ensure = exporter._ensure_open

    def _ensure_open_slow():
        orig_ensure()
        if exporter._fh is not None and not isinstance(exporter._fh, SlowFile):
            exporter._fh = SlowFile(exporter._fh)

    exporter._ensure_open = _ensure_open_slow  # type: ignore[method-assign]

    mw, tracer, _ = _build_app(exporter)
    n = 40

    async def one(i: int) -> None:
        await mw(_scope(f"/item/{i}"), _never_receive, _CountingSend())

    async def _run_all():
        await asyncio.gather(*(one(i) for i in range(n)))

    t0 = time.perf_counter()
    asyncio.run(_run_all())
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(
        f"输入={n} 并发请求 + 每次物理写 50ms 主链路墙钟={elapsed_ms:.1f}ms "
        f"判定=请求期间物理写次数={slow_writes['count']}（旧行为≈{n} 次，≥2s）"
    )
    # 主链路不等待写线程：即使物理写很慢，请求侧也很快完成
    assert elapsed_ms < 1000, f"主链路被慢速磁盘拖住：{elapsed_ms:.1f}ms"
    tracer.shutdown()
    assert exporter.stats()["written_spans"] == n
    # 批量合并：物理写次数显著小于请求数（drain 一次性合并全部入队项）
    assert slow_writes["count"] <= 5, (
        f"写盘次数疑似随请求数线性增长：{slow_writes['count']}"
    )
