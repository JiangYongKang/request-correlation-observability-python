"""指标与结构化日志测试。"""

from __future__ import annotations

import json
import logging
from io import StringIO

import httpx
import pytest

from app.correlation import bind_correlation_id, reset_correlation_id
from app.logging_setup import (
    JsonFormatter,
    configure_logging,
    sanitize_value,
)
from app.metrics import (
    DURATION_BOUNDARIES,
    MetricsRegistry,
    normalize_method,
    normalize_route,
    normalize_status_class,
)

HEADER = "X-Correlation-ID"


def test_label_normalization_is_bounded() -> None:
    assert normalize_method("get") == "GET"
    assert normalize_method("BREW") == "_OTHER"
    assert normalize_route(None) == "__unmatched__"
    assert normalize_route("/items/{id}") == "/items/{id}"
    assert normalize_status_class(204) == "2xx"
    assert normalize_status_class(503) == "5xx"
    assert normalize_status_class(999) == "_OTHER"
    # 超长路由标签也被截断，防止无界标签
    assert len(normalize_route("x" * 500)) == 128
    print("[输入] 各类原始标签 [判定依据] method/route/status 全部收敛到有界集合")


def test_registry_aggregates_count_error_rate_and_histogram() -> None:
    registry = MetricsRegistry()
    for _ in range(80):
        registry.record_request("GET", "/items/{id}", 200, 0.02, False)
    for _ in range(20):
        registry.record_request("GET", "/items/{id}", 500, 0.4, True)
    # 未知路径单独成列
    registry.record_request("POST", None, 404, 0.001, False)

    snap = registry.snapshot()
    print(
        f"[输入] 80 成功 + 20 失败 + 1 未匹配 [判定依据] "
        f"requests={snap['totals']['requests']}, error_rate={snap['totals']['error_rate']}"
    )
    assert snap["totals"]["requests"] == 101
    assert snap["totals"]["errors"] == 20
    assert snap["totals"]["error_rate"] == pytest.approx(20 / 101, abs=1e-6)

    series = snap["series"]["GET|/items/{id}|2xx"]
    assert series["count"] == 80
    assert series["duration_seconds"]["count"] == 80
    assert series["duration_seconds"]["mean"] > 0
    buckets = series["duration_seconds"]["buckets"]
    assert buckets["le_+Inf"] == 80
    # 直方图桶计数单调不减
    counts = list(buckets.values())
    assert counts == sorted(counts)
    assert len(counts) == len(DURATION_BOUNDARIES) + 1
    print(f"[判定依据] 直方图 {len(counts)} 个桶计数单调不减，+Inf=总数")


def test_snapshot_is_written_atomically(tmp_path) -> None:
    path = tmp_path / "m.json"
    registry = MetricsRegistry(snapshot_path=str(path))
    registry.record_request("GET", "/", 200, 0.01, False)
    registry.write_snapshot()
    loaded = json.loads(path.read_text())
    assert loaded["totals"]["requests"] == 1
    assert loaded["label_names"] == ["method", "route", "status_class"]
    print("[输入] 快照写盘 [判定依据] JSON 可解析、含 label_names 语义说明")


async def test_request_metrics_visible_after_http_calls(
    client: httpx.AsyncClient, metrics_snapshot
) -> None:
    for _ in range(5):
        await client.get("/", headers={HEADER: "metrics-http-1"})
    await client.get("/error?kind=value", headers={HEADER: "metrics-http-err"})
    snap = metrics_snapshot()
    print(
        f"[输入] 5 成功 + 1 内部错误 [判定依据] totals={snap['totals']}"
    )
    assert snap["totals"]["requests"] == 6
    assert snap["totals"]["errors"] == 1
    # 路由使用模板标签，非法关联标识拒绝流量也自成有界列
    routes = {label.split("|")[1] for label in snap["series"]}
    assert "/" in routes and "/error" in routes


def test_sanitize_redacts_sensitive_fields_and_injection() -> None:
    assert sanitize_value("Authorization", "Bearer x") == "***redacted***"
    assert sanitize_value("x-api-key", "k") == "***redacted***"
    cleaned = sanitize_value("note", "line1\nFAKE 200 ok")
    assert "\n" not in cleaned and "\\n" in cleaned
    masked = sanitize_value("msg", "password=hunter2 ok")
    assert "hunter2" not in masked
    print(
        f"[输入] 含敏感键/CRLF/密钥字面量 [判定依据] "
        f"脱敏与转义生效，示例={masked}"
    )


def test_json_log_carries_correlation_and_safe_fields() -> None:
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("test.json.logger")
    logger.handlers = [handler]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    token = bind_correlation_id("log-cid-1")
    try:
        logger.info(
            "demo.event",
            extra={"fields": {"user": "alice", "token": "t-123", "n": 3}},
        )
    finally:
        reset_correlation_id(token)

    line = stream.getvalue().strip()
    payload = json.loads(line)
    print(
        f"[关联标识] {payload['correlation_id']} [输入] user/token/n "
        f"[判定依据] 单行 JSON，token 被脱敏，字段齐全"
    )
    assert payload["correlation_id"] == "log-cid-1"
    assert payload["event"] == "demo.event"
    assert payload["user"] == "alice" and payload["n"] == 3
    assert payload["token"] == "***redacted***"
    assert "timestamp" in payload and "level" in payload


def test_unbound_log_uses_dash_placeholder() -> None:
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("test.json.logger.2")
    logger.handlers = [handler]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.info("outside.request")
    payload = json.loads(stream.getvalue().strip())
    assert payload["correlation_id"] == "-"
    print("[判定依据] 请求上下文之外的日志使用 '-' 占位而非伪造标识")
