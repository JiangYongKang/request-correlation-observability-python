"""并发请求不串扰、指标有界标签与端到端关联标识测试。"""

from __future__ import annotations

import concurrent.futures

from fastapi.testclient import TestClient


def test_concurrent_requests_do_not_cross_contaminate(client):
    """线程级并发：每个响应体、响应头与日志中的关联标识必须各自一致。"""
    n = 40
    cids = [f"cid-concurrent-{i}" for i in range(n)]

    def hit(cid: str):
        r = client.get("/", headers={"X-Correlation-ID": cid})
        return cid, r.headers["X-Correlation-ID"], r.json()["correlation_id"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(hit, cids))

    bad = [r for r in results if not (r[0] == r[1] == r[2])]
    print(f"输入={n} 并发线程各自携带 cid-concurrent-* 判定=串扰数 {len(bad)}")
    assert not bad
    assert len({r[1] for r in results}) == n  # 无一被他人覆盖


def test_generated_cids_unique_under_concurrency(client):
    def hit(_):
        return client.get("/").headers["X-Correlation-ID"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(hit, range(40)))
    print(f"输入=40 无头并发 判定=生成标识去重后 {len(set(ids))} 个")
    assert len(set(ids)) == 40


def test_metrics_recorded_once_with_bounded_labels(client, metrics):
    client.get("/items/1")
    client.get("/items/2")
    client.get("/items/999")
    client.get("/boom")
    client.get("/nope-404")
    client.get("/", headers={"X-Correlation-ID": "x x"})  # 400 非法标识

    snap = metrics.snapshot()
    totals = snap["totals"]
    print(f"输入=6 个请求 判定=totals={totals}")
    assert totals["requests"] == 6
    assert totals["success"] == 3
    assert totals["server_error"] == 1
    assert totals["client_error"] == 2
    assert 0 < totals["error_rate"] < 1

    # 路由模板有界：/items/1 /items/2 /items/999 合并为一个标签
    routes = sorted({s["labels"]["route"] for s in snap["series"]})
    print(f"输入=路径参数多个值 判定=路由标签集合={routes}")
    assert "/items/{item_id}" in routes
    assert not any(r.startswith("/items/1") and r != "/items/{item_id}" for r in routes)

    item_series = next(s for s in snap["series"] if s["labels"]["route"] == "/items/{item_id}")
    assert item_series["count"] == 3  # 恰好计数，无重复/漏计

    # 所有标签取值均在有界集合内
    allowed_classes = {"1xx", "2xx", "3xx", "4xx", "5xx", "unknown"}
    allowed_outcomes = {"success", "client_error", "server_error", "unknown"}
    for s in snap["series"]:
        assert s["labels"]["status_class"] in allowed_classes
        assert s["labels"]["outcome"] in allowed_outcomes
    print("输入=全部序列 判定=status_class/outcome 标签取值有界")


def test_root_span_covers_full_streaming_lifecycle(client, span_exporter):
    r = client.get("/stream?count=3", headers={"X-Correlation-ID": "cid-full"})
    assert r.status_code == 200
    assert r.text == "chunk-0\nchunk-1\nchunk-2\n"
    server_spans = [s for s in span_exporter.finished_spans() if s.kind == "server"]
    stream_spans = [s for s in span_exporter.finished_spans() if s.kind == "stream"]
    print(
        f"输入=stream 3 块 关联标识=cid-full "
        f"判定=server={len(server_spans)} stream={len(stream_spans)} trace 一致"
    )
    assert len(server_spans) == 1 and len(stream_spans) == 1
    root, stream = server_spans[0], stream_spans[0]
    assert root.trace_id == stream.trace_id == "cid-full"
    assert stream.parent_id == root.span_id
    assert root.status == "OK" and stream.status == "OK"
    assert stream.attributes["stream.chunks"] == 3
    assert root.duration_ms is not None and stream.duration_ms is not None
    assert root.duration_ms >= stream.duration_ms  # 根片段完整覆盖流式阶段


def test_stream_midway_error_consistent(client, span_exporter, metrics):
    r = client.get("/stream/boom", headers={"X-Correlation-ID": "cid-sb"})
    body = r.text
    server = [s for s in span_exporter.finished_spans() if s.kind == "server"][0]
    stream = [s for s in span_exporter.finished_spans() if s.kind == "stream"][0]
    totals = metrics.snapshot()["totals"]
    print(
        f"输入=stream/boom 关联标识=cid-sb body={body!r} "
        f"判定=server.status={server.status} stream.status={stream.status} "
        f"5xx 计数={totals['server_error']}"
    )
    assert "before" in body
    assert "secret" not in body and "xxx" not in body
    assert stream.status == "ERROR" and stream.error_type == "RuntimeError"
    assert server.status == "ERROR"
    assert server.trace_id == stream.trace_id == "cid-sb"
    assert totals["server_error"] == 1


def test_each_request_exactly_one_server_span(client, span_exporter):
    for i in range(5):
        client.get(f"/items/{i}")
    server_spans = [s for s in span_exporter.finished_spans() if s.kind == "server"]
    print(f"输入=5 个请求 判定=server 片段数={len(server_spans)}（恰好一请求一片段）")
    assert len(server_spans) == 5
