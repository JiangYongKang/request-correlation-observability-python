"""采样比例真实生效：边界（0/1）、抽样统计、可复现、按路由、失败保留。"""

from __future__ import annotations

import contextvars

from app.sampling import Sampler
from app.tracing import InMemorySpanExporter, Tracer, TracerConfig


def _run_trace(tr: Tracer, trace_id: str, *, path: str = "/work", fail: bool = False) -> None:
    """在隔离上下文中跑一条两片 trace（根 + 子），可选失败。"""
    def body() -> None:
        try:
            with tr.span("root", trace_id=trace_id, kind="server", path=path):
                with tr.span("child"):
                    if fail:
                        raise RuntimeError("boom")
        except RuntimeError:
            pass
    contextvars.copy_context().run(body)


def _kept_ids(exp: InMemorySpanExporter) -> set[str]:
    return {s.trace_id for s in exp.finished_spans()}


def test_rate_zero_exports_nothing_even_on_failure():
    """比例 0 = 彻底关闭：连失败样本也不导出，只保留计数与日志。"""
    exp = InMemorySpanExporter()
    tr = Tracer(TracerConfig(sample_rate=0.0, exporter=exp))
    _run_trace(tr, "cid-off-ok")
    _run_trace(tr, "cid-off-fail", fail=True)
    stats = tr.stats()
    print(
        f"输入=sample_rate=0.0, 一成功一失败 关联标识=cid-off-ok/cid-off-fail "
        f"判定=导出 0 条, stats={stats}"
    )
    assert exp.finished_spans() == []
    assert stats["traces_dropped"] == 2
    assert stats["traces_kept"] == 0


def test_rate_one_exports_everything():
    """比例 1 = 全量：每条 trace 整树导出。"""
    exp = InMemorySpanExporter()
    tr = Tracer(TracerConfig(sample_rate=1.0, exporter=exp))
    for i in range(5):
        _run_trace(tr, f"cid-full-{i}")
    kept = _kept_ids(exp)
    print(f"输入=sample_rate=1.0, 5 条 trace 判定=整树全保留: {sorted(kept)}")
    assert kept == {f"cid-full-{i}" for i in range(5)}
    assert tr.stats()["traces_kept"] == 5


def test_sampling_ratio_actually_saves_volume():
    """比例 0.3：落盘 trace 数与比例大致对齐（确定性种子下统计断言）。"""
    total = 2000
    exp = InMemorySpanExporter()
    tr = Tracer(TracerConfig(sample_rate=0.3, sample_seed=7, exporter=exp))
    for i in range(total):
        _run_trace(tr, f"cid-load-{i}")
    kept = tr.stats()["traces_kept"]
    ratio = kept / total
    print(
        f"输入=sample_rate=0.3, seed=7, {total} 条 trace "
        f"判定=保留 {kept} 条, 实际比例={ratio:.3f} ∈ [0.24, 0.36]"
    )
    assert 0.24 <= ratio <= 0.36
    # 每条保留的 trace 都是整树（根+子两片），不存在半棵树
    assert exp.finished_spans() and len(exp.finished_spans()) == kept * 2


def test_sampling_reproducible_with_fixed_seed():
    """固定种子：同一批关联标识的取舍稳定复现；换种子允许不同。"""
    ids = [f"cid-repro-{i}" for i in range(200)]

    def kept_with_seed(seed: int) -> set[str]:
        exp = InMemorySpanExporter()
        tr = Tracer(TracerConfig(sample_rate=0.5, sample_seed=seed, exporter=exp))
        for cid in ids:
            _run_trace(tr, cid)
        return _kept_ids(exp)

    first = kept_with_seed(42)
    second = kept_with_seed(42)
    other = kept_with_seed(1)
    print(
        f"输入=200 条固定关联标识, rate=0.5 "
        f"判定=seed=42 两次结果一致({len(first)} 条), seed=1 结果={len(other)} 条"
    )
    assert first == second
    assert 0 < len(first) < len(ids)
    # 不同种子几乎必然产生不同取舍集合（确定性构造下直接断言不相等即可）
    assert first != other


def test_error_trace_retained_when_sampled_out():
    """比例 >0 但未命中：失败 trace 必须整树保留，成功 trace 丢弃。"""
    exp = InMemorySpanExporter()
    # 比例极小（>0）：成功样本全部丢弃，失败样本兜底保留
    tr = Tracer(TracerConfig(sample_rate=1e-9, sample_seed=3, exporter=exp))
    _run_trace(tr, "cid-ok-drop")
    _run_trace(tr, "cid-fail-keep", fail=True)
    kept = _kept_ids(exp)
    spans = exp.finished_spans()
    root = next(s for s in spans if s.parent_id is None)
    print(
        f"输入=rate=1e-9, 一成功一失败 关联标识=cid-ok-drop/cid-fail-keep "
        f"判定=仅失败链保留, keep_reason={root.attributes.get('sample.keep_reason')}"
    )
    assert kept == {"cid-fail-keep"}
    # 整树保留：根+子都在，父子对得上
    assert len(spans) == 2
    child = next(s for s in spans if s.parent_id is not None)
    assert child.parent_id == root.span_id
    assert root.attributes["sample.keep_reason"] == "error_retained"
    assert tr.stats()["traces_dropped"] == 1


def test_route_specific_rates():
    """按入口单独调比例：/health 关闭、/api 全量，其余走全局默认。"""
    exp = InMemorySpanExporter()
    tr = Tracer(
        TracerConfig(
            sample_rate=1.0,
            route_sample_rates={"/health": 0.0, "/metrics": 0.0},
            exporter=exp,
        )
    )
    _run_trace(tr, "cid-health", path="/health")
    _run_trace(tr, "cid-healthz", path="/healthz")  # 最长前缀命中 /health
    _run_trace(tr, "cid-metrics", path="/metrics")
    _run_trace(tr, "cid-api", path="/api")
    kept = _kept_ids(exp)
    print(
        f"输入=route_rates={{/health:0, /metrics:0}}, 全局 1.0 "
        f"判定=仅 /api 保留: {sorted(kept)}"
    )
    assert kept == {"cid-api"}


def test_sampler_decision_basis_explainable():
    """判定依据可复现说明：rate/draw/seed 完整记录，draw<rate 即命中。"""
    sampler = Sampler(default_rate=0.5, seed=9)
    decision = sampler.decide("cid-explain", "/work")
    again = sampler.decide("cid-explain", "/work")
    print(
        f"输入=cid-explain, rate=0.5, seed=9 "
        f"判定=draw={decision.draw:.6f}, sampled={decision.sampled}, 复现一致={decision == again}"
    )
    assert decision == again
    assert decision.sampled == (decision.draw < decision.rate)


def test_root_span_records_sample_basis():
    """根片段属性记录采样判定依据（比例与种子）。"""
    exp = InMemorySpanExporter()
    tr = Tracer(TracerConfig(sample_rate=1.0, sample_seed=11, exporter=exp))
    _run_trace(tr, "cid-basis")
    root = next(s for s in exp.finished_spans() if s.parent_id is None)
    print(
        f"输入=cid-basis 判定=根片段属性 sample.rate={root.attributes['sample.rate']}, "
        f"sample.seed={root.attributes['sample.seed']}, "
        f"keep_reason={root.attributes['sample.keep_reason']}"
    )
    assert root.attributes["sample.rate"] == 1.0
    assert root.attributes["sample.seed"] == 11
    assert root.attributes["sample.keep_reason"] == "sampled"


def test_end_to_end_route_sampling_via_app():
    """应用级：/metrics 关闭采样，/ 全量；落盘（内存导出器）只含 / 的链。"""
    from fastapi.testclient import TestClient

    from app.main import create_app
    from app.config import ObservabilitySettings

    exp = InMemorySpanExporter()
    tracer = Tracer(
        TracerConfig(
            sample_rate=1.0,
            route_sample_rates={"/metrics": 0.0},
            exporter=exp,
        )
    )
    settings = ObservabilitySettings(spans_export_path="", route_sample_rates={"/metrics": 0.0})
    app = create_app(settings=settings, tracer=tracer)
    with TestClient(app) as client:
        r1 = client.get("/")
        r2 = client.get("/metrics")
    kept = _kept_ids(exp)
    cid_root = r1.headers["x-correlation-id"]
    cid_metrics = r2.headers["x-correlation-id"]
    print(
        f"输入=GET / 与 GET /metrics, /metrics 采样=0 "
        f"关联标识=/→{cid_root}, /metrics→{cid_metrics} "
        f"判定=仅 / 的链保留: {sorted(kept)}"
    )
    assert cid_root in kept
    assert cid_metrics not in kept
