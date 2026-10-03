"""高并发下按 trace 缓冲不串线：整树完整、父子对得上、计数守恒。"""

from __future__ import annotations

import asyncio
import random
import threading

from app.tracing import InMemorySpanExporter, Tracer, TracerConfig


def test_concurrent_traces_stay_intact():
    """100 条并发 trace（根+子+孙）：导出后每条都是完整一棵树。"""
    exp = InMemorySpanExporter()
    tracer = Tracer(TracerConfig(sample_rate=1.0, exporter=exp))
    n = 100

    async def one(i: int) -> None:
        cid = f"cid-conc-{i}"
        with tracer.span(f"GET /p{i}", kind="server", trace_id=cid, path="/p"):
            await asyncio.sleep(random.random() / 500)
            with tracer.span("child"):
                await asyncio.sleep(0)
                with tracer.span("grand"):
                    pass

    async def main() -> None:
        await asyncio.gather(*(one(i) for i in range(n)))

    asyncio.run(main())
    tracer.export_finished()

    spans = exp.finished_spans()
    by_trace: dict[str, list] = {}
    for s in spans:
        by_trace.setdefault(s.trace_id, []).append(s)
    print(
        f"输入={n} 条并发 trace（各 3 片） "
        f"判定=导出 {len(spans)} 片 / {len(by_trace)} 条 trace, 每条恰好 3 片"
    )
    assert len(by_trace) == n
    for cid, group in by_trace.items():
        assert len(group) == 3, f"{cid} 出现半棵树: {[s.name for s in group]}"
        roots = [s for s in group if s.parent_id is None]
        assert len(roots) == 1
        root = roots[0]
        child = next(s for s in group if s.name == "child")
        grand = next(s for s in group if s.name == "grand")
        # 父子关系在本树内闭合，不指向别的 trace
        assert child.parent_id == root.span_id
        assert grand.parent_id == child.span_id
        assert all(s.trace_id == cid for s in group)
    assert tracer.stats()["traces_kept"] == n
    tracer.shutdown()


def test_concurrent_mixed_outcomes_accounting():
    """并发下成功/失败混合：失败链必留，计数与导出守恒。"""
    exp = InMemorySpanExporter()
    tracer = Tracer(TracerConfig(sample_rate=1.0, exporter=exp))
    n = 60

    async def one(i: int) -> None:
        cid = f"cid-mix-{i}"
        try:
            with tracer.span("root", trace_id=cid, kind="server"):
                await asyncio.sleep(0)
                if i % 3 == 0:
                    raise RuntimeError(f"boom-{i}")
        except RuntimeError:
            pass

    async def main() -> None:
        await asyncio.gather(*(one(i) for i in range(n)))

    asyncio.run(main())
    tracer.export_finished()

    spans = exp.finished_spans()
    errors = [s for s in spans if s.status == "ERROR"]
    print(
        f"输入={n} 条并发 trace, 每 3 条失败 1 条 "
        f"判定=导出 {len(spans)} 片, ERROR 根 {len(errors)} 条（预期 {n // 3}）"
    )
    assert len(spans) == n
    assert len(errors) == n // 3
    stats = tracer.stats()
    assert stats["traces_kept"] == n and stats["traces_dropped"] == 0
    tracer.shutdown()


def test_file_exporter_thread_safe_concurrent_writes(tmp_path):
    """多线程并发导出同一文件：行数守恒、每行都是合法 JSON。"""
    from app.tracing import FileSpanExporter

    path = tmp_path / "spans.jsonl"
    exp = FileSpanExporter(str(path), buffer_bytes=256, flush_interval_s=0)
    tracer = Tracer(TracerConfig(sample_rate=1.0, exporter=exp))
    n_threads, per_thread = 8, 25

    def worker(t: int) -> None:
        for i in range(per_thread):
            with tracer.span("root", trace_id=f"cid-th-{t}-{i}", kind="server"):
                pass

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    tracer.shutdown()

    lines = path.read_text(encoding="utf-8").strip().splitlines()
    import json

    parsed = [json.loads(line) for line in lines]
    print(
        f"输入={n_threads} 线程 × {per_thread} 条 "
        f"判定=落盘 {len(parsed)} 行, 全部合法 JSON, trace 数={len({r['trace_id'] for r in parsed})}"
    )
    assert len(parsed) == n_threads * per_thread
    assert len({r["trace_id"] for r in parsed}) == n_threads * per_thread
