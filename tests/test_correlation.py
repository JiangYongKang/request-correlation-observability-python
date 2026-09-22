"""关联标识生成、继承、透传与非法原因区分测试。"""

from __future__ import annotations

import asyncio

import pytest

from app.correlation import (
    InvalidCorrelationIdError,
    correlation_context,
    generate_correlation_id,
    get_correlation_id,
    validate_correlation_id,
)

HEADER = "X-Correlation-ID"


# ---------------------------------------------------------------- 单元测试
def test_generate_unique_and_wellformed():
    ids = {generate_correlation_id() for _ in range(2000)}
    sample = next(iter(ids))
    print(f"输入=<2000 次生成> 关联标识样例={sample} 判定=全部唯一且前缀 cid-")
    assert len(ids) == 2000
    assert all(x.startswith("cid-") and len(x) == 36 for x in ids)


@pytest.mark.parametrize(
    "value,reason",
    [
        ("", "empty"),
        ("   ", "empty"),
        (" x", "empty"),  # 首尾空白
        ("x" * 129, "too_long"),
        ("bad id", "illegal_character"),
        ("a\tb", "illegal_character"),
        ("a\nb", "illegal_character"),
        ("cid\u0000x", "illegal_character"),
        ("中文标识", "illegal_character"),
        ("a,b", "illegal_character"),
    ],
)
def test_validate_rejects_with_distinct_reason(value, reason):
    with pytest.raises(InvalidCorrelationIdError) as exc:
        validate_correlation_id(value)
    print(f"输入={value!r} 关联标识=<拒绝> 判定=reason {exc.value.reason}")
    assert exc.value.reason == reason
    assert exc.value.detail  # 每个拒绝都有可展示原因


@pytest.mark.parametrize("value", ["A", "cid-abc_123.45:67-89", "x" * 128, "0" * 8])
def test_validate_accepts_legal_values(value):
    print(f"输入={value!r} 判定=合法原样接受")
    assert validate_correlation_id(value) == value


def test_boundary_length_rule():
    assert validate_correlation_id("x" * 128) == "x" * 128
    with pytest.raises(InvalidCorrelationIdError) as exc:
        validate_correlation_id("x" * 129)
    print(f"输入=129 字符 判定={exc.value.reason}（上限 128）")
    assert exc.value.reason == "too_long"


async def _worker(i: int) -> str:
    with correlation_context(f"cid-concurrent-{i}"):
        await asyncio.sleep(0)
        seen = get_correlation_id()
        assert seen == f"cid-concurrent-{i}"
        return seen


async def _run_concurrent():
    return await asyncio.gather(*(_worker(i) for i in range(100)))


def test_context_isolation_under_concurrency():
    """并发协程的关联标识互不污染。"""
    results = asyncio.run(_run_concurrent())
    print(f"输入=100 并发协程 关联标识={results[:3]}... 判定=各自独立无串扰")
    assert results == [f"cid-concurrent-{i}" for i in range(100)]


# ---------------------------------------------------------------- 集成测试
def test_generates_when_absent_and_echoes_header(client):
    r = client.get("/")
    cid = r.headers[HEADER]
    print(f"输入=<无头> 关联标识={cid} 判定=自动生成并回写响应头")
    assert r.status_code == 200
    assert cid.startswith("cid-")
    assert r.json()["correlation_id"] == cid


@pytest.mark.parametrize("value", ["trace-001", "ABCdef_012.:-z"])
def test_inherits_when_provided(client, value):
    r = client.get("/", headers={HEADER: value})
    print(f"输入={value!r} 关联标识={r.headers[HEADER]} 判定=原样继承透传")
    assert r.status_code == 200
    assert r.headers[HEADER] == value
    assert r.json()["correlation_id"] == value


@pytest.mark.parametrize(
    "value,reason",
    [
        ("", "empty"),
        ("   ", "empty"),
        ("x" * 129, "too_long"),
        ("has space", "illegal_character"),
        ("a\nb", "illegal_character"),
    ],
)
def test_rejects_illegal_header_with_reason(client, value, reason):
    r = client.get("/", headers={HEADER: value})
    body = r.json()["error"]
    print(f"输入={value!r} 关联标识=None 判定=400 reason={body['reason']}")
    assert r.status_code == 400
    assert body["code"] == "invalid_correlation_id"
    assert body["reason"] == reason
    assert HEADER not in r.headers  # 非法值不回显


def test_same_correlation_id_consistent_in_logs(client, list_handler):
    r = client.get("/", headers={HEADER: "cid-logcheck"})
    assert r.status_code == 200
    cid_payloads = [p for p in list_handler.payloads() if p["correlation_id"]]
    print(
        f"输入=cid-logcheck 关联标识出现在 {len(cid_payloads)} 条日志 判定=请求维度日志一致携带"
    )
    assert cid_payloads
    assert all(p["correlation_id"] == "cid-logcheck" for p in cid_payloads)
