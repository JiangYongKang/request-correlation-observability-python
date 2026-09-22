"""关联标识单测：生成、校验、拒绝原因、上下文隔离与跨边界继承。"""

from __future__ import annotations

import asyncio
import contextvars

import pytest

from app.correlation import (
    CorrelationFormatError,
    CorrelationLengthError,
    CorrelationUnboundError,
    bind_correlation_id,
    current_context,
    generate_correlation_id,
    get_correlation_id,
    get_correlation_id_or_none,
    reset_correlation_id,
    resolve_correlation_id,
    validate_correlation_id,
)

VALID_SAMPLES = [
    "a", "A1", "abc-123", "req_42.X", "9f8e7d6c5b4a",
    "a" * 128, "CORR.2026-09_22",
]
FORMAT_INVALID = [
    "ab cd",       # 空格
    "a/b",         # 斜杠
    "../etc",      # 路径穿越特征
    "a;b",         # 命令分隔
    "a\nb",        # 换行（日志注入）
    "a\rb",        # 回车
    "naïve-1",     # 非 ASCII
    "-abc",        # 非法首字符
    "abc-",        # 非法尾字符
    ".abc",        # 点开头
    "a,b",         # 逗号
    "a:b",         # 冒号（可能被解释为 header 片段）
    "",            # 空串
    "中文字符",
]


@pytest.mark.parametrize("value", VALID_SAMPLES)
def test_validate_accepts_legal_values(value: str) -> None:
    print(f"[输入] {value!r} [判定依据] 仅含白名单字符且长度合规 -> 接受")
    assert validate_correlation_id(value, 128) == value


@pytest.mark.parametrize("value", FORMAT_INVALID)
def test_validate_rejects_illegal_chars_with_distinct_reason(value: str) -> None:
    with pytest.raises(CorrelationFormatError) as exc_info:
        validate_correlation_id(value, 128)
    cid = get_correlation_id_or_none()
    print(
        f"[输入] {value!r} [关联标识上下文] {cid} "
        f"[判定依据] reason={exc_info.value.reason} -> 按字符集原因拒绝"
    )
    assert exc_info.value.reason == "invalid_correlation_id_format"
    # 异常消息不得回显原值，避免敏感内容随日志扩散
    assert value not in str(exc_info.value) or value == ""


def test_validate_rejects_overlong_with_distinct_reason() -> None:
    value = "a" * 129
    with pytest.raises(CorrelationLengthError) as exc_info:
        validate_correlation_id(value, 128)
    print(
        f"[输入] 长度={len(value)} [判定依据] 超过上限 128，"
        f"reason={exc_info.value.reason} -> 按长度原因拒绝"
    )
    assert exc_info.value.reason == "invalid_correlation_id_length"


def test_length_takes_priority_over_format() -> None:
    value = "bad value " * 20
    with pytest.raises(CorrelationLengthError):
        validate_correlation_id(value, 128)
    print(f"[输入] 同时超长且含空格 [判定依据] 长度规则优先 -> length 原因")


def test_generate_is_unique_and_legal() -> None:
    ids = {generate_correlation_id() for _ in range(1000)}
    print(f"[输入] 1000 次生成 [判定依据] 全部唯一={len(ids) == 1000}")
    assert len(ids) == 1000
    for cid in ids:
        validate_correlation_id(cid, 128)


def test_resolve_generates_when_missing_or_blank() -> None:
    for raw in (None, "", "   "):
        cid, generated = resolve_correlation_id(raw, 128)
        print(f"[输入] {raw!r} [关联标识] {cid} [判定依据] 缺失 -> 服务端生成")
        assert generated is True
        validate_correlation_id(cid, 128)


def test_reserve_strips_surrounding_whitespace() -> None:
    cid, generated = resolve_correlation_id("  client-cid  ", 128)
    assert cid == "client-cid" and generated is False
    print(f"[输入] '  client-cid  ' [关联标识] {cid} [判定依据] 去空白后接受")


async def test_concurrent_tasks_do_not_pollute_each_other() -> None:
    async def worker(idx: int, observed: dict[int, str]) -> None:
        cid = f"corr-{idx}"
        token = bind_correlation_id(cid)
        # 交错挂起，制造串扰窗口
        await asyncio.sleep(0.002)
        observed[idx] = get_correlation_id()
        reset_correlation_id(token)

    observed: dict[int, str] = {}
    await asyncio.gather(*(worker(i, observed) for i in range(100)))
    for idx, cid in observed.items():
        print(f"[任务 {idx}] [关联标识] {cid} [判定依据] 必须等于 corr-{idx}")
        assert cid == f"corr-{idx}"
    assert len(observed) == 100


async def test_context_inherited_across_async_boundary() -> None:
    token = bind_correlation_id("inherited-cid")
    try:
        captured: list[str] = []

        async def child() -> None:
            await asyncio.sleep(0)
            captured.append(get_correlation_id())

        task = asyncio.create_task(child())
        await task
        print(f"[关联标识] {captured[0]} [判定依据] 子任务继承创建时上下文")
        assert captured == ["inherited-cid"]
    finally:
        reset_correlation_id(token)


def test_copied_context_propagates_into_thread_pool() -> None:
    token = bind_correlation_id("thread-cid")
    try:
        ctx = current_context()
        result = ctx.run(get_correlation_id)
        print(f"[关联标识] {result} [判定依据] copy_context 快照可在线程侧读取")
        assert result == "thread-cid"
    finally:
        reset_correlation_id(token)


def test_unbound_access_is_explicit() -> None:
    with pytest.raises(CorrelationUnboundError):
        get_correlation_id()
    assert get_correlation_id_or_none() is None
    print("[判定依据] 上下文外访问显式失败而非返回伪造标识")
