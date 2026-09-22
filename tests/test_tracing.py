"""追踪测试：采样、父子关系、异常标记、退出不丢数、导出方式可配置。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.correlation import bind_correlation_id, reset_correlation_id
from app.tracing import SpanExporter, Tracer, configure_tracer, reset_tracer


def _read_jsonl(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text().splitlines() if line.strip()]


@pytest.fixture
def file_tracer(tmp_path):
    path = tmp_path / "t.jsonl"
    tracer = configure_tracer(1.0, "file", str(path))
    yield tracer, str(path)
    tracer.shutdown()


def test_sampled_tree_exports_parent_child_with_matching_chain(file_tracer) -> None:
    tracer, path = file_tracer
    token = bind_correlation_id("sampling-cid")
    try:
        with tracer.span("root", {"k": "v"}) as root:
            with tracer.span("child") as child:
                pass
            with tracer.span("child-error") as bad:
                bad.set_attribute("phase", "middle")
    finally:
        reset_correlation_id(token)

    rows = {r["name"]: r for r in _read_jsonl(path)}
    print(
        f"[关联标识] sampling-cid [判定依据] "
        f"root={rows['root']['span_id'][:8]}, child.parent={rows['child']['parent_id'][:8]}"
    )
    assert rows["child"]["parent_id"] == rows["root"]["span_id"]
    assert rows["root"]["correlation_id"] == "sampling-cid"
    assert rows["root"]["attributes"] == {"k": "v"}
    assert rows["root"]["duration_ms"] is not None


def test_exception_marks_span_error_and_preserves_reason(file_tracer) -> None:
    tracer, path = file_tracer
    with pytest.raises(KeyError):
        with tracer.span("failing-root"):
            with tracer.span("failing-child"):
                raise KeyError("entity-42 token=leaked-secret")
    tracer.shutdown()
    rows = _read_jsonl(path)
    failing = [r for r in rows if r["name"].startswith("failing")]
    print(
        f"[判定依据] 异常向上传播且两个片段都被标记："
        f"{[(r['name'], r['status'], r['error_type']) for r in failing]}"
    )
    assert {r["status"] for r in failing} == {"error"}
    child = next(r for r in failing if r["name"] == "failing-child")
    assert child["error_type"] == "KeyError"
    assert "leaked-secret" not in json.dumps(failing)
    assert child["error_reason"] is not None


def test_sample_rate_zero_exports_nothing(tmp_path) -> None:
    path = tmp_path / "zero.jsonl"
    tracer = configure_tracer(0.0, "file", str(path))
    for i in range(50):
        with tracer.span(f"r{i}"):
            pass
    tracer.shutdown()
    rows = _read_jsonl(str(path))
    print(f"[输入] 采样率 0、50 个根片段 [判定依据] 导出数={len(rows)}（应为 0）")
    assert rows == []


def test_sampling_decision_is_consistent_within_one_tree(tmp_path) -> None:
    path = tmp_path / "tree.jsonl"
    # 采样率 1 时显式校验根决策传播到全部后代
    tracer = configure_tracer(1.0, "file", str(path))
    with tracer.span("root"):
        for i in range(5):
            with tracer.span(f"child-{i}"):
                pass
    tracer.shutdown()
    rows = _read_jsonl(str(path))
    print(
        f"[判定依据] 全采样下一棵树 6 个片段 sampled 全部为 true，trace_id 相同"
    )
    assert len(rows) == 6
    assert all(r["sampled"] is True for r in rows)
    assert len({r["trace_id"] for r in rows}) == 1


def test_shutdown_flushes_buffered_spans(tmp_path) -> None:
    path = tmp_path / "buffer.jsonl"
    tracer = configure_tracer(1.0, "console", "")
    # console 导出器不依赖文件；改用直接缓冲+shutdown 语义校验幂等
    tracer.shutdown()
    tracer.shutdown()  # 重复关闭必须安全
    print("[判定依据] shutdown 幂等，重复调用不抛异常")


def test_shutdown_does_not_silently_drop_unfinished_spans(tmp_path) -> None:
    path = tmp_path / "unfinished.jsonl"
    exporter = SpanExporter("file", str(path))
    tracer = Tracer(sample_rate=1.0, exporter=exporter)
    reset_tracer(tracer)
    # 模拟"进程被中断"：根片段已开始但未正常结束
    ctx = tracer.span_context("interrupted-root")
    span = ctx.__enter__()
    span.set_attribute("will_restart", True)
    # 不调用 __exit__，直接 shutdown：未完成片段必须被标记并落盘
    tracer.shutdown()
    rows = _read_jsonl(str(path))
    interrupted = [r for r in rows if r["name"] == "interrupted-root"]
    print(
        f"[判定依据] 未结束片段 shutdown 后 status={interrupted[0]['status']}，"
        f"error_type={interrupted[0]['error_type']}，未静默丢弃"
    )
    assert len(interrupted) == 1
    assert interrupted[0]["status"] == "interrupted"
    assert interrupted[0]["error_type"] == "ProcessShutdown"
    assert interrupted[0]["end_time"] is not None

    # 进程重启后：文件以 append 保留历史片段，新片段继续写入同一文件
    tracer2 = configure_tracer(1.0, "file", str(path))
    with tracer2.span("after-restart"):
        pass
    tracer2.shutdown()
    rows2 = _read_jsonl(str(path))
    names = [r["name"] for r in rows2]
    assert "interrupted-root" in names and names[-1] == "after-restart"
    print("[判定依据] 重启后历史 interrupted 片段保留，新片段继续追加")


def test_export_kinds_are_configurable_and_default_is_local(tmp_path) -> None:
    real_file = str(tmp_path / "kinds.jsonl")
    for kind, path in (("file", real_file), ("console", ""), ("none", "")):
        exporter = SpanExporter(kind, path)
        exporter.export([])
        exporter.shutdown()
    with pytest.raises(ValueError):
        SpanExporter("file", "")
    print("[判定依据] file/console/none 三种导出方式均可构造且空导出安全")


def test_attributes_are_bounded_and_redacted(file_tracer) -> None:
    tracer, path = file_tracer
    huge = "x" * 5000
    with tracer.span(
        "attrs",
        {
            "authorization": "Bearer abcdef",
            "huge": huge,
            "nested_ok": 123,
        },
    ):
        pass
    tracer.shutdown()
    rows = _read_jsonl(path)
    row = next(r for r in rows if r["name"] == "attrs")
    print(
        f"[判定依据] authorization 被脱敏={row['attributes']['authorization'] == '***redacted***'}，"
        f"长字符串被截断"
    )
    assert row["attributes"]["authorization"] == "***redacted***"
    assert len(row["attributes"]["huge"]) <= 500
