"""并发场景测试：上下文串扰必须为零，指标计数必须精确。"""

from __future__ import annotations

import asyncio
import random

import httpx
import pytest

HEADER = "X-Correlation-ID"


async def test_concurrent_requests_keep_distinct_correlation_ids(
    client: httpx.AsyncClient,
) -> None:
    total = 200

    async def one_call(idx: int) -> tuple[str, str, str]:
        cid = f"concurrent-cid-{idx:04d}"
        # 随机走普通/流式/后台链路，交错放大串扰概率
        route = random.choice(["/", "/echo?value=x", "/stream?count=2", "/background"])
        response = await client.get(route, headers={HEADER: cid})
        body = response.text
        return cid, response.headers[HEADER], body

    results = await asyncio.gather(*(one_call(i) for i in range(total)))
    mismatches = [
        (sent, got) for sent, got, _ in results if sent != got
    ]
    print(
        f"[输入] {total} 个并发请求、4 种链路随机混合 "
        f"[判定依据] 响应头标识与发送标识逐一比对，串扰数={len(mismatches)}"
    )
    assert not mismatches, f"发现上下文串扰: {mismatches[:5]}"

    # 每个响应体中只应出现自己的标识；抽查其它标识绝不出现
    bodies = {sent: body for sent, _, body in results}
    for idx in range(0, total, 37):
        own = f"concurrent-cid-{idx:04d}"
        other = f"concurrent-cid-{(idx + 1) % total:04d}"
        assert own in bodies[own]
        assert other not in bodies[own]


async def test_metrics_recorded_exactly_once_per_request(
    client: httpx.AsyncClient, metrics_snapshot
) -> None:
    total = 120

    async def one_call(i: int) -> int:
        cid = f"metrics-cid-{i:04d}"
        # 约 1/3 请求打到错误链路
        kind = "business" if i % 3 == 0 else None
        url = f"/error?kind={kind}" if kind else "/"
        response = await client.get(url, headers={HEADER: cid})
        return response.status_code

    statuses = await asyncio.gather(*(one_call(i) for i in range(total)))
    snap = metrics_snapshot()
    total_requests = snap["totals"]["requests"]
    print(
        f"[输入] {total} 并发请求（含 {statuses.count(400)} 个业务错误） "
        f"[判定依据] totals.requests={total_requests}，重复/漏计均为失败"
    )
    # 业务 400 不计入 error；另含非法关联标识的拒绝计数不在本批内
    assert total_requests == total
    assert snap["totals"]["errors"] == 0
    assert 0.0 <= snap["totals"]["error_rate"] <= 1.0
    # 标签取值有界：不存在按原始 URL 展开的高基数列
    for label in snap["series"]:
        method, route, status_class = label.split("|")
        assert method in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "_OTHER"}
        assert status_class in {"1xx", "2xx", "3xx", "4xx", "5xx", "_OTHER"}
        assert "{" not in route and "}" not in route


async def test_traces_from_concurrent_requests_do_not_cross_wires(
    client: httpx.AsyncClient, trace_rows
) -> None:
    total = 80

    async def one_call(i: int) -> str:
        cid = f"trace-cross-{i:04d}"
        response = await client.get("/echo?value=z", headers={HEADER: cid})
        assert response.status_code == 200
        return cid

    cids = await asyncio.gather(*(one_call(i) for i in range(total)))
    await asyncio.sleep(0.05)  # 等待缓冲导出
    rows = trace_rows()

    # 每个 trace_id 只允许对应一个关联标识
    trace_to_cids: dict[str, set[str]] = {}
    cid_to_traces: dict[str, set[str]] = {}
    for row in rows:
        cid = row["correlation_id"]
        trace_to_cids.setdefault(row["trace_id"], set()).add(cid)
        if cid and cid.startswith("trace-cross-"):
            cid_to_traces.setdefault(cid, set()).add(row["trace_id"])

    crossed = {t: cs for t, cs in trace_to_cids.items() if len(cs) > 1}
    print(
        f"[输入] {total} 并发请求 [判定依据] trace_id<->correlation_id 一一对应，"
        f"串线 trace 数={len(crossed)}"
    )
    assert not crossed
    for cid in cids:
        assert len(cid_to_traces[cid]) == 1

    # 每个请求有且仅有一个根片段（parent_id 为 None）
    for cid in cids:
        traces = cid_to_traces[cid]
        trace_id = next(iter(traces))
        roots = [
            r for r in rows
            if r["trace_id"] == trace_id and r["parent_id"] is None
        ]
        assert len(roots) == 1
