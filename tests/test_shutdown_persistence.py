"""进程退出/重启时未完成追踪数据不得静默丢弃。"""

from __future__ import annotations

import contextvars
import json

from app.tracing import FileSpanExporter, Tracer, TracerConfig


def test_unfinished_spans_flushed_on_shutdown(tmp_path):
    path = tmp_path / "spans.jsonl"
    exporter = FileSpanExporter(str(path))
    tr = Tracer(TracerConfig(sample_rate=1.0, exporter=exporter))

    cm = tr.span("request-root", trace_id="cid-leak")
    cm.__enter__()  # 请求进行中“进程被要求退出”，未显式结束

    assert not path.exists()  # 延迟打开：关停前不产生文件
    tr.shutdown()

    lines = [json.loads(line) for line in path.read_text().strip().splitlines()]
    print(f"输入=关停时未结束片段 判定=落盘 {len(lines)} 条，status={lines[0]['status']}")
    assert len(lines) == 1
    assert lines[0]["trace_id"] == "cid-leak"
    assert lines[0]["status"] == "ERROR"
    assert lines[0]["error_type"] == "TracerShutdown"
    assert lines[0]["duration_ms"] is not None


def test_spans_survive_restart_append_mode(tmp_path):
    """重启后以追加模式继续导出，历史片段仍在文件中。"""
    path = tmp_path / "spans.jsonl"

    exp1 = FileSpanExporter(str(path))
    tr1 = Tracer(TracerConfig(exporter=exp1))
    with tr1.span("span-before-restart", trace_id="cid-1"):
        pass
    tr1.shutdown()

    # 模拟进程重启：新导出器指向同一文件
    exp2 = FileSpanExporter(str(path))
    tr2 = Tracer(TracerConfig(exporter=exp2))
    with tr2.span("span-after-restart", trace_id="cid-2"):
        pass
    tr2.shutdown()

    names = [json.loads(line)["name"] for line in path.read_text().strip().splitlines()]
    print(f"输入=重启前后各一片段 判定=文件保留 {names}")
    assert names == ["span-before-restart", "span-after-restart"]
    # 每一行都是合法 JSON，便于本地/外部工具解析
    for line in path.read_text().strip().splitlines():
        json.loads(line)
