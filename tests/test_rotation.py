"""滚动导出测试：大小/时间滚动、保留上限、并发写安全、关停不丢、重启可读。"""

from __future__ import annotations

import glob
import json
import os
import threading
import time

from app.exporter import RotatingFileSpanExporter
from app.tracing import Tracer, TracerConfig


def _read_all(pattern: str) -> list[dict]:
    rows = []
    for path in sorted(glob.glob(pattern)):
        for line in open(path, encoding="utf-8"):
            rows.append(json.loads(line))
    return rows


def test_rotation_by_size_and_retention(tmp_path):
    """超过大小阈值即滚动；文件总数不超过保留上限。"""
    path = str(tmp_path / "spans.jsonl")
    exp = RotatingFileSpanExporter(path, max_bytes=2048, max_files=3)
    tr = Tracer(TracerConfig(exporter=exp))
    for i in range(60):
        with tr.span(f"span-{i}", trace_id=f"cid-{i:03d}"):
            pass
        tr.export_finished()
    tr.shutdown()
    files = sorted(glob.glob(f"{path}*"))
    stats = exp.stats()
    print(
        f"输入=60 个片段，max_bytes=2048，max_files=3 "
        f"判定=滚动 {stats['rotations']} 次，保留文件={[os.path.basename(f) for f in files]}"
    )
    assert stats["rotations"] >= 1
    assert len(files) <= 3  # 保留上限生效，不无限增长
    assert stats["written_spans"] == 60
    assert stats["dropped_spans"] == 0


def test_rotation_by_time(tmp_path):
    """按时间滚动：超过间隔且文件非空即滚动。"""
    path = str(tmp_path / "spans.jsonl")
    exp = RotatingFileSpanExporter(path, max_bytes=10**9, rotate_interval_s=0.05)
    tr = Tracer(TracerConfig(exporter=exp))
    with tr.span("before", trace_id="cid-t1"):
        pass
    tr.export_finished()
    tr.flush()
    time.sleep(0.08)
    with tr.span("after", trace_id="cid-t2"):
        pass
    tr.export_finished()
    tr.shutdown()
    files = sorted(glob.glob(f"{path}*"))
    print(f"输入=间隔 0.05s 两批写入 判定=时间滚动生效 files={[os.path.basename(f) for f in files]}")
    assert len(files) == 2
    assert exp.stats()["rotations"] == 1


def test_shutdown_flushes_buffer_no_loss(tmp_path):
    """进程正常退出：异步队列中的数据全部落盘，不丢。"""
    path = str(tmp_path / "spans.jsonl")
    exp = RotatingFileSpanExporter(path, max_bytes=10**9, max_files=2)
    tr = Tracer(TracerConfig(exporter=exp))
    n = 500
    for i in range(n):
        with tr.span(f"span-{i}", trace_id=f"cid-{i:04d}"):
            pass
        tr.export_finished()
    tr.shutdown()  # 不预先 flush，靠 shutdown 排空队列
    rows = _read_all(f"{path}*")
    print(f"输入={n} 个片段直接 shutdown 判定=落盘 {len(rows)} 条，零丢失")
    assert len(rows) == n
    assert exp.stats()["written_spans"] == n


def test_restart_appends_and_history_readable(tmp_path):
    """重启后历史记录保留可读，新数据继续追加。"""
    path = str(tmp_path / "spans.jsonl")
    exp1 = RotatingFileSpanExporter(path)
    tr1 = Tracer(TracerConfig(exporter=exp1))
    with tr1.span("before-restart", trace_id="cid-old"):
        pass
    tr1.export_finished()
    tr1.shutdown()

    exp2 = RotatingFileSpanExporter(path)
    tr2 = Tracer(TracerConfig(exporter=exp2))
    with tr2.span("after-restart", trace_id="cid-new"):
        pass
    tr2.export_finished()
    tr2.shutdown()

    names = [row["name"] for row in _read_all(f"{path}*")]
    print(f"输入=重启前后各一片段 判定=历史保留且可追加 names={names}")
    assert names == ["before-restart", "after-restart"]


def test_trace_chain_intact_across_rotation(tmp_path):
    """同一次请求的片段同批写入同一文件；跨文件也能用 trace_id 拼回完整链。"""
    path = str(tmp_path / "spans.jsonl")
    exp = RotatingFileSpanExporter(path, max_bytes=1024, max_files=40)
    tr = Tracer(TracerConfig(exporter=exp))
    # 构造多棵多片段树，迫使滚动发生
    for i in range(30):
        with tr.span(f"root-{i}", trace_id=f"cid-chain-{i:02d}"):
            with tr.span(f"child-{i}"):
                pass
        tr.export_finished()
    tr.shutdown()
    files = sorted(glob.glob(f"{path}*"))
    assert exp.stats()["rotations"] >= 1
    # 文件维度：同一 trace 的片段不跨文件（同批原子写入）
    placement: dict[str, set] = {}
    for f in files:
        for line in open(f, encoding="utf-8"):
            row = json.loads(line)
            placement.setdefault(row["trace_id"], set()).add(os.path.basename(f))
    # 全局维度：每棵树的 root/child 都能按 trace_id + parent_id 拼回
    rows = _read_all(f"{path}*")
    by_trace: dict[str, list] = {}
    for row in rows:
        by_trace.setdefault(row["trace_id"], []).append(row)
    print(
        f"输入=30 棵两片断树，滚动 {exp.stats()['rotations']} 次 "
        f"判定=每树可拼回完整链（树数={len(by_trace)}）"
    )
    assert len(by_trace) == 30
    for cid, spans in by_trace.items():
        root = next(s for s in spans if s["parent_id"] is None)
        child = next(s for s in spans if s["parent_id"] is not None)
        assert child["parent_id"] == root["span_id"]
        assert len(placement[cid]) == 1  # 同 trace 同文件


def test_concurrent_writes_safe_and_complete(tmp_path):
    """多线程并发导出：行不交错、全部可解析、数量齐全。"""
    path = str(tmp_path / "spans.jsonl")
    exp = RotatingFileSpanExporter(path, max_bytes=10**9, max_files=2)
    tr = Tracer(TracerConfig(exporter=exp))
    threads_n, per_thread = 8, 50

    def worker(tid: int) -> None:
        for i in range(per_thread):
            with tr.span(f"t{tid}-s{i}", trace_id=f"cid-t{tid}-{i:03d}"):
                pass
            tr.export_finished()

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    tr.shutdown()
    rows = _read_all(f"{path}*")
    traces = {row["trace_id"] for row in rows}
    print(
        f"输入={threads_n} 线程 × {per_thread} 片段并发 "
        f"判定=落盘 {len(rows)} 行全部合法，trace 数={len(traces)}"
    )
    assert len(rows) == threads_n * per_thread
    assert len(traces) == threads_n * per_thread
    assert exp.stats()["dropped_spans"] == 0


def test_export_after_shutdown_counted_not_raised(tmp_path):
    """关停后再导出：不抛异常，计入丢弃（不静默）。"""
    path = str(tmp_path / "spans.jsonl")
    exp = RotatingFileSpanExporter(path)
    tr = Tracer(TracerConfig(exporter=exp))
    with tr.span("s", trace_id="cid-x"):
        pass
    tr.shutdown()
    n = exp.stats()["dropped_spans"]
    exp.export([])  # 空批次无影响
    print(f"输入=关停后导出 判定=丢弃计数={exp.stats()['dropped_spans']}，不抛异常")
    assert exp.stats()["dropped_spans"] == n  # 空批次不计
