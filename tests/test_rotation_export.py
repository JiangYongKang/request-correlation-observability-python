"""导出器：缓冲写盘、大小/时间滚动、保留上限、关停与重启不丢数据。"""

from __future__ import annotations

import json
import os
import time

from app.tracing import FileSpanExporter, Tracer, TracerConfig


def _make_tracer(path, **exporter_kwargs) -> tuple[FileSpanExporter, Tracer]:
    exp = FileSpanExporter(str(path), **exporter_kwargs)
    return exp, Tracer(TracerConfig(sample_rate=1.0, exporter=exp))


def _run_trace(tr: Tracer, trace_id: str) -> None:
    with tr.span("root", trace_id=trace_id, kind="server"):
        with tr.span("child"):
            pass


def _read_all_lines(path) -> list[dict]:
    """读取活跃文件与全部归档，按行解析 JSON。"""
    files = [str(path)] + [f"{path}.{i}" for i in range(1, 20)]
    lines: list[dict] = []
    for f in files:
        if os.path.exists(f):
            with open(f, encoding="utf-8") as fh:
                lines.extend(json.loads(line) for line in fh.read().splitlines())
    return lines


def test_buffered_write_not_sync_per_request(tmp_path):
    """导出先进缓冲：未达阈值且未刷盘时文件不出现；flush 后可见。"""
    path = tmp_path / "spans.jsonl"
    exp, tr = _make_tracer(path, buffer_bytes=1024 * 1024, flush_interval_s=0)
    _run_trace(tr, "cid-buffered")
    print(f"输入=一条 trace, buffer=1MiB, 无周期刷盘 判定=导出后文件未创建: {not path.exists()}")
    assert not path.exists()  # 主链路不同步写盘
    exp.flush()
    lines = _read_all_lines(path)
    print(f"判定=flush 后落盘 {len(lines)} 条")
    assert len(lines) == 2
    tr.shutdown()


def test_size_rotation_and_trace_not_split(tmp_path):
    """按大小滚动：一批（一条完整 trace）不跨文件拆分，历史可读。"""
    path = tmp_path / "spans.jsonl"
    # 一条 trace 约 600-800 字节；max_bytes 取 1.5 条大小，迫使每条滚动
    exp, tr = _make_tracer(path, max_bytes=1000, max_files=10, buffer_bytes=1)
    ids = [f"cid-rot-{i}" for i in range(6)]
    for cid in ids:
        _run_trace(tr, cid)
    tr.shutdown()

    archives = exp.rotated_files()
    print(f"输入=6 条 trace, max_bytes=1000 判定=产生归档 {len(archives)} 个")
    assert archives, "应发生大小滚动"
    # 每个文件内不出现"半条 trace"：同文件内每个 trace_id 的片段构成完整树
    files = [str(path)] + archives
    trace_files: dict[str, set[str]] = {}
    for f in files:
        with open(f, encoding="utf-8") as fh:
            for line in fh.read().splitlines():
                rec = json.loads(line)
                trace_files.setdefault(rec["trace_id"], set()).add(f)
    split = {tid: fs for tid, fs in trace_files.items() if len(fs) > 1}
    print(f"判定=每条 trace 落在单一文件, 跨文件 trace={list(split)}")
    assert not split
    # 数据不丢：6 条 trace × 2 片全部可读
    all_lines = _read_all_lines(path)
    assert {r["trace_id"] for r in all_lines} == set(ids)
    assert len(all_lines) == 12


def test_retention_max_files(tmp_path):
    """保留上限：连活跃文件在内最多 max_files 个，最老被删除。"""
    path = tmp_path / "spans.jsonl"
    exp, tr = _make_tracer(path, max_bytes=500, max_files=3, buffer_bytes=1)
    for i in range(10):
        _run_trace(tr, f"cid-cap-{i}")
    tr.shutdown()
    remaining = [str(path)] + exp.rotated_files()
    print(f"输入=10 条 trace, max_files=3 判定=剩余文件 {len(remaining)} 个: {remaining}")
    assert len(remaining) <= 3
    # 最新数据仍在
    latest = _read_all_lines(path)
    assert any(r["trace_id"] == "cid-cap-9" for r in latest)


def test_time_based_rotation(tmp_path):
    """按时间滚动：文件存活超过间隔即滚动。"""
    path = tmp_path / "spans.jsonl"
    exp, tr = _make_tracer(path, rotate_interval_s=0.05, max_files=5, buffer_bytes=1)
    _run_trace(tr, "cid-time-1")
    exp.flush()
    time.sleep(0.08)
    _run_trace(tr, "cid-time-2")
    tr.shutdown()
    archives = exp.rotated_files()
    print(f"输入=间隔 50ms 两条 trace 判定=时间滚动归档 {len(archives)} 个")
    assert archives
    assert {r["trace_id"] for r in _read_all_lines(path)} == {"cid-time-1", "cid-time-2"}


def test_shutdown_flushes_buffer_no_loss(tmp_path):
    """进程正常退出：缓冲区数据全量落盘，不丢。"""
    path = tmp_path / "spans.jsonl"
    exp, tr = _make_tracer(path, buffer_bytes=1024 * 1024, flush_interval_s=0)
    for i in range(20):
        _run_trace(tr, f"cid-exit-{i}")
    assert not path.exists()  # 全在缓冲里
    tr.shutdown()
    lines = _read_all_lines(path)
    print(f"输入=20 条 trace 全在缓冲 判定=shutdown 后落盘 {len(lines)} 条")
    assert len(lines) == 40
    assert {r["trace_id"] for r in lines} == {f"cid-exit-{i}" for i in range(20)}


def test_restart_appends_and_history_readable(tmp_path):
    """重启后历史记录保留可读，新数据追加；滚动后同样能拼回完整链。"""
    path = tmp_path / "spans.jsonl"
    exp1, tr1 = _make_tracer(path, buffer_bytes=1)
    _run_trace(tr1, "cid-before-restart")
    tr1.shutdown()

    exp2, tr2 = _make_tracer(path, buffer_bytes=1)
    _run_trace(tr2, "cid-after-restart")
    tr2.shutdown()

    lines = _read_all_lines(path)
    ids = {r["trace_id"] for r in lines}
    print(f"输入=重启前后各一条 判定=历史+新数据均可读: {sorted(ids)}")
    assert ids == {"cid-before-restart", "cid-after-restart"}
    for rec in lines:
        assert rec["trace_id"] and rec["span_id"]  # 每行独立可解析
