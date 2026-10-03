"""追踪片段父子关系、异常标记、采样与关停不丢数据测试。"""

from __future__ import annotations

import contextvars
import json

import pytest

from app.tracing import (
    InMemorySpanExporter,
    Span,
    Tracer,
    TracerConfig,
    current_span,
)


def test_parent_child_trace_consistency():
    exp = InMemorySpanExporter()
    tr = Tracer(TracerConfig(exporter=exp))
    with tr.span("root", trace_id="T-1", correlation_id="cid-x") as root:
        with tr.span("child-a") as a:
            pass
        with tr.span("child-b") as b:
            with tr.span("grand") as g:
                g.set_attribute("k", "v")
    tr.export_finished()
    spans = {s.name: s for s in exp.finished_spans()}
    print(
        f"输入=嵌套片段 关联标识=cid-x trace={root.trace_id} "
        f"判定=父子 id 串接，trace 一致"
    )
    assert root.trace_id == "T-1"
    assert all(s.trace_id == "T-1" for s in spans.values())
    assert spans["child-a"].parent_id == root.span_id
    assert spans["child-b"].parent_id == root.span_id
    assert spans["grand"].parent_id == b.span_id
    assert spans["grand"].attributes["k"] == "v"
    assert all(s.status == "OK" for s in spans.values())
    assert all(s.duration_ms is not None and s.duration_ms >= 0 for s in spans.values())


def test_exception_marks_span_with_reason():
    exp = InMemorySpanExporter()
    tr = Tracer(TracerConfig(exporter=exp))
    with pytest.raises(RuntimeError):
        with tr.span("root", trace_id="T-2"):
            with tr.span("failing"):
                raise RuntimeError("boom-reason")
    tr.export_finished()
    by_name = {s.name: s for s in exp.finished_spans()}
    print(
        f"输入=RuntimeError('boom-reason') 关联标识=trace T-2 "
        f"判定=failing 与 root 均 ERROR 且保留类型/原因"
    )
    failing = by_name["failing"]
    assert failing.status == "ERROR"
    assert failing.error_type == "RuntimeError"
    assert "boom-reason" in (failing.error_message or "")
    assert by_name["root"].status == "ERROR"


def test_sampling_decision_inherited_by_tree():
    """整树继承同一采样决策；比例 0 彻底不导出，比例 1 整树导出。"""

    def case(rate: float) -> tuple[list[Span], list[Span]]:
        exp = InMemorySpanExporter()
        tr = Tracer(TracerConfig(sample_rate=rate, exporter=exp))
        captured: list[Span] = []
        with tr.span("r") as root:
            captured.append(root)
            with tr.span("c") as child:
                captured.append(child)
        tr.export_finished()
        return captured, exp.finished_spans()

    spans, exported = contextvars.copy_context().run(lambda: case(0.0))
    print(
        f"输入=sample_rate=0.0 判定=整树 sampled=False: {[s.sampled for s in spans]}, "
        f"导出 {len(exported)} 条（彻底关闭）"
    )
    assert all(not s.sampled for s in spans)
    assert exported == []  # 比例 0：彻底不新增导出

    spans, exported = contextvars.copy_context().run(lambda: case(1.0))
    print(
        f"输入=sample_rate=1.0 判定=整树 sampled=True: {[s.sampled for s in spans]}, "
        f"导出 {len(exported)} 条（全量）"
    )
    assert all(s.sampled for s in spans)
    assert len(exported) == 2  # 整树导出，不多不少


def test_sampling_bounds_rejected():
    print("输入=sample_rate=1.5 判定=构造期拒绝")
    with pytest.raises(ValueError):
        Tracer(TracerConfig(sample_rate=1.5))
    with pytest.raises(ValueError):
        Tracer(TracerConfig(sample_rate=-0.1))


def test_shutdown_recovers_unfinished_spans():
    exp = InMemorySpanExporter()
    tr = Tracer(TracerConfig(exporter=exp))
    cm = tr.span("leaked")
    cm.__enter__()  # 故意不退出
    assert not exp.exported
    tr.shutdown()
    print(
        f"输入=未结束片段 判定=shutdown 强制收尾并导出，状态="
        f"{exp.finished_spans()[0].status}, 错误类型={exp.finished_spans()[0].error_type}"
    )
    assert tr.closed
    assert len(exp.finished_spans()) == 1
    span = exp.finished_spans()[0]
    assert span.status == "ERROR"
    assert span.error_type == "TracerShutdown"
    assert span.end_ns is not None  # 未静默丢弃


def test_file_exporter_persists_jsonl(tmp_path):
    from app.tracing import FileSpanExporter

    path = tmp_path / "spans.jsonl"
    exp = InMemorySpanExporter()
    tr = Tracer(TracerConfig(exporter=exp))
    with tr.span("persisted", trace_id="T-9"):
        pass
    tr.export_finished()

    file_exp = FileSpanExporter(str(path))
    file_exp.export(exp.finished_spans())
    file_exp.shutdown()

    lines = path.read_text(encoding="utf-8").strip().splitlines()
    payload = json.loads(lines[0])
    print(f"输入=文件导出路径 {path} 判定=JSONL 落盘 name={payload['name']}")
    assert len(lines) == 1
    assert payload["trace_id"] == "T-9"
    assert payload["status"] == "OK"


def test_shutdown_idempotent():
    exp = InMemorySpanExporter()
    tr = Tracer(TracerConfig(exporter=exp))
    with tr.span("s"):
        pass
    tr.shutdown()
    n = len(exp.finished_spans())
    tr.shutdown()  # 再次关停不应重复导出
    print(f"输入=重复 shutdown 判定=导条数稳定 {n}")
    assert len(exp.finished_spans()) == n


def test_current_span_stack_isolation():
    exp = InMemorySpanExporter()
    tr = Tracer(TracerConfig(exporter=exp))
    with tr.span("outer") as outer:
        assert current_span() is outer
        with tr.span("inner") as inner:
            assert current_span() is inner
        assert current_span() is outer
    assert current_span() is None
    print("输入=嵌套进出 判定=上下文栈正确弹回，出栈后为空")
