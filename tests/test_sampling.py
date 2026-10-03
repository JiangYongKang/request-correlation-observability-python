"""采样策略测试：比例真实生效、边界、失败保留、整树一致、可复现、并发隔离。"""

from __future__ import annotations

import asyncio
import contextvars

import pytest

from app.sampling import Sampler, parse_overrides
from app.tracing import InMemorySpanExporter, Tracer, TracerConfig


def _make_tracer(rate: float, *, seed: str = "test-seed", overrides=None):
    exp = InMemorySpanExporter()
    sampler = Sampler(rate, seed=seed, overrides=overrides)
    tr = Tracer(TracerConfig(sampler=sampler, exporter=exp, tail_sampling=True))
    return tr, exp


def _run_trace(tr: Tracer, cid: str, *, path: str = "/x", fail: bool = False) -> None:
    async def main() -> None:
        try:
            with tr.span("root", trace_id=cid, sample_path=path):
                with tr.span("child"):
                    if fail:
                        raise RuntimeError("boom")
        except RuntimeError:
            pass

    asyncio.run(main())


def test_rate_zero_exports_nothing_even_failures():
    """彻底关闭：连失败样本也不写，只保留计数。"""
    tr, exp = _make_tracer(0.0)
    cids = [f"cid-off-{i}" for i in range(20)]
    for i, cid in enumerate(cids):
        _run_trace(tr, cid, fail=(i % 2 == 0))
    tr.flush()
    stats = tr.sampling_stats()
    print(
        f"输入=sample_rate=0.0，{len(cids)} 个请求（一半失败） "
        f"判定=导出 0 条，计数 dropped={stats['traces_dropped']}"
    )
    assert exp.finished_spans() == []
    assert stats["traces_dropped"] == len(cids)
    assert stats["spans_exported"] == 0
    assert stats["spans_dropped"] == len(cids) * 2


def test_rate_one_exports_everything():
    """全量边界：所有 trace 整树落盘。"""
    tr, exp = _make_tracer(1.0)
    cids = [f"cid-full-{i}" for i in range(20)]
    for cid in cids:
        _run_trace(tr, cid)
    tr.flush()
    exported = exp.finished_spans()
    print(f"输入=sample_rate=1.0，{len(cids)} 个请求 判定=导出 {len(exported)} 条（每 trace 2 片段）")
    assert len(exported) == len(cids) * 2
    assert {s.trace_id for s in exported} == set(cids)


def test_rate_statistically_effective_and_tree_consistent():
    """中间比例：落盘量与比例大致对齐；同 trace 要么整树留下要么整树不留。"""
    rate = 0.3
    tr, exp = _make_tracer(rate)
    cids = [f"cid-stat-{i:04d}" for i in range(2000)]
    for cid in cids:
        _run_trace(tr, cid)
    tr.flush()
    by_trace: dict[str, list] = {}
    for s in exp.finished_spans():
        by_trace.setdefault(s.trace_id, []).append(s)
    kept_ratio = len(by_trace) / len(cids)
    stats = tr.sampling_stats()
    print(
        f"输入=sample_rate={rate}，{len(cids)} 个请求 "
        f"判定=命中 {len(by_trace)}（{kept_ratio:.3f}），"
        f"kept={stats['traces_kept']} dropped={stats['traces_dropped']}，"
        f"每 trace 片段数={sorted({len(v) for v in by_trace.values()})}"
    )
    assert 0.24 < kept_ratio < 0.36  # 比例真实生效（统计容差）
    assert all(len(v) == 2 for v in by_trace.values())  # 不留半棵树
    for spans in by_trace.values():
        root = next(s for s in spans if s.parent_id is None)
        child = next(s for s in spans if s.parent_id is not None)
        assert child.parent_id == root.span_id  # 父子对得上


def test_failures_retained_when_rate_positive():
    """比例 > 0 时失败 trace 必须整树保留，即使未被抽中。"""
    tr, exp = _make_tracer(0.05)
    ok_cids = [f"cid-ok-{i:03d}" for i in range(200)]
    fail_cids = [f"cid-fail-{i:03d}" for i in range(50)]
    for cid in ok_cids:
        _run_trace(tr, cid)
    for cid in fail_cids:
        _run_trace(tr, cid, fail=True)
    tr.flush()
    kept = {s.trace_id for s in exp.finished_spans()}
    stats = tr.sampling_stats()
    print(
        f"输入=sample_rate=0.05，200 成功 + 50 失败 "
        f"判定=失败全部保留（{len(set(fail_cids) & kept)}/50），"
        f"error_retention={stats['traces_kept_for_error']}"
    )
    assert set(fail_cids) <= kept
    assert stats["traces_kept_for_error"] == 50


def test_decisions_reproducible_with_fixed_seed():
    """固定种子：同一批关联标识的取舍稳定复现（压测前后可对比）。"""
    cids = [f"cid-repro-{i:03d}" for i in range(300)]
    tr1, exp1 = _make_tracer(0.4, seed="fixed-seed")
    tr2, exp2 = _make_tracer(0.4, seed="fixed-seed")
    for cid in cids:
        _run_trace(tr1, cid)
        _run_trace(tr2, cid)
    tr1.flush()
    tr2.flush()
    kept1 = {s.trace_id for s in exp1.finished_spans()}
    kept2 = {s.trace_id for s in exp2.finished_spans()}
    print(f"输入=同种子同批 {len(cids)} 个关联标识 判定=两次命中集合完全一致（{len(kept1)} 个）")
    assert kept1 == kept2
    # 不同种子判定值不同（不排除巧合一致，用判定值直接断言）
    s1 = Sampler(0.4, seed="fixed-seed").decide("cid-repro-0001")
    s2 = Sampler(0.4, seed="other-seed").decide("cid-repro-0001")
    print(f"判定依据: seed=fixed-seed value={s1.value:.6f} vs seed=other-seed value={s2.value:.6f}")
    assert s1.value != s2.value


def test_route_overrides():
    """按入口覆盖：健康检查可调到 0，其余入口用基础比例。"""
    overrides = {"/health": 0.0, "/internal/*": 0.0}
    tr, exp = _make_tracer(1.0, overrides=overrides)
    for i in range(10):
        _run_trace(tr, f"cid-health-{i}", path="/health")
        _run_trace(tr, f"cid-internal-{i}", path="/internal/deep")
        _run_trace(tr, f"cid-normal-{i}", path="/items")
    tr.flush()
    kept = {s.trace_id for s in exp.finished_spans()}
    print(
        f"输入=overrides /health=0,/internal/*=0，基础 1.0 "
        f"判定=health/internal 全丢，/items 全留: kept={len(kept)}"
    )
    assert not any(c.startswith("cid-health") for c in kept)
    assert not any(c.startswith("cid-internal") for c in kept)
    assert sum(1 for c in kept if c.startswith("cid-normal")) == 10


def test_override_parse_and_validation():
    assert parse_overrides("/health=0,/metrics=0.05") == {"/health": 0.0, "/metrics": 0.05}
    with pytest.raises(ValueError):
        parse_overrides("/health")  # 缺少 '='
    with pytest.raises(ValueError):
        Sampler(0.5, overrides={"/x": 1.5})
    with pytest.raises(ValueError):
        Sampler(-0.1)
    print("输入=非法覆盖/比例 判定=构造期拒绝")


def test_late_error_rescues_dropped_trace():
    """trace 判丢弃后，迟到的失败片段（如后台任务）救回整链。"""
    tr, exp = _make_tracer(0.0 + 0.5, seed="rescue-seed")
    # 找一个未被抽中的 cid
    cid = next(
        c for c in (f"cid-rescue-{i}" for i in range(1000))
        if not tr.sampler.decide(c).kept
    )
    with tr.span("root", trace_id=cid):
        pass  # 根片段结束 → trace 完成 → 判丢弃
    stats_before = tr.sampling_stats()
    assert stats_before["traces_dropped"] == 1
    # 迟到的后台片段失败：救回
    late = tr.start_span("background:late", trace_id=cid, parent_id="missing-parent")
    tr.end_span(late, status="ERROR", error_type="RuntimeError", error_message="late failure")
    stats = tr.sampling_stats()
    exported = exp.finished_spans()
    print(
        f"输入=未抽中 trace 的迟到失败片段 关联标识={cid} "
        f"判定=整链救回 {len(exported)} 条，rescued={stats['traces_rescued']}"
    )
    assert stats["traces_rescued"] == 1
    assert {s.trace_id for s in exported} == {cid}
    assert len(exported) == 2  # 根 + 迟到片段都在


def test_sampling_decisions_isolated_under_concurrency():
    """高并发下各请求采样判定互不串扰：每个 trace 的判定只取决于自身 cid。"""
    tr, exp = _make_tracer(0.5)

    async def worker(cid: str) -> None:
        with tr.span("root", trace_id=cid, sample_path="/x"):
            with tr.span("child"):
                await asyncio.sleep(0)

    async def main() -> None:
        cids = [f"cid-conc-{i:03d}" for i in range(200)]
        await asyncio.gather(*(worker(c) for c in cids))

    asyncio.run(main())
    tr.flush()
    kept = {s.trace_id for s in exp.finished_spans()}
    expected = {c for c in (f"cid-conc-{i:03d}" for i in range(200)) if tr.sampler.decide(c).kept}
    print(
        f"输入=200 并发请求 判定=命中集合与逐一定义判定完全一致 "
        f"（kept={len(kept)} expected={len(expected)}）"
    )
    assert kept == expected


def test_http_level_sampling_e2e(metrics):
    """HTTP 端到端：中间件按路径生效采样，失败保留，落盘量与比例对齐。"""
    from fastapi.testclient import TestClient

    from app.config import ObservabilitySettings
    from app.main import create_app

    exporter = InMemorySpanExporter()
    sampler = Sampler(0.5, seed="e2e-seed", overrides={"/health": 0.0})
    tracer = Tracer(TracerConfig(sampler=sampler, exporter=exporter, tail_sampling=True))
    settings = ObservabilitySettings(spans_export_path="")
    application = create_app(settings=settings, tracer=tracer)
    with TestClient(application) as client:
        ok_cids = [f"cid-e2e-{i:03d}" for i in range(100)]
        for cid in ok_cids:
            r = client.get("/", headers={"X-Correlation-ID": cid})
            assert r.status_code == 200
        for i in range(10):  # 失败请求：必须全部保留
            client.get("/boom", headers={"X-Correlation-ID": f"cid-e2e-boom-{i}"})
        for i in range(10):  # 健康检查：覆盖为 0，不导出
            client.get("/health", headers={"X-Correlation-ID": f"cid-e2e-health-{i}"})
    tracer.flush()
    kept = {s.trace_id for s in exporter.finished_spans()}
    expected_ok = {c for c in ok_cids if sampler.decide(c, "/").kept}
    stats = tracer.sampling_stats()
    print(
        f"输入=100 正常 + 10 失败 + 10 健康检查（/health=0） "
        f"判定=正常命中 {len(expected_ok & kept)}/{len(expected_ok)}，"
        f"失败保留 {sum(1 for c in kept if 'boom' in c)}/10，"
        f"健康检查导出 {sum(1 for c in kept if 'health' in c)}/10，stats={stats}"
    )
    assert expected_ok <= kept  # 抽中的正常请求都在
    assert not any("health" in c for c in kept)  # 覆盖为 0 彻底不导出
    assert sum(1 for c in kept if "boom" in c) == 10  # 失败全保留
    assert stats["traces_kept_for_error"] >= 10
