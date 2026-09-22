"""关联标识的生成、合法性校验与异步安全的上下文存取。

合法性规则（拒绝原因可区分）：
- ``empty``：头部存在但值为空 / 纯空白
- ``too_long``：长度超过配置上限
- ``illegal_character``：含非允许字符（仅允许 ASCII 字母、数字与 ``- _ . :``）
  空白、控制字符等一律拒绝，避免日志注入与响应头注入。

上下文使用 :class:`contextvars.ContextVar`：asyncio 任务之间天然隔离，
不会在并发请求间串扰；跨线程时需用 :func:`app.background.bind_context`
显式拷贝（``copy_context()``），跨 ``await`` 边界自动随任务传播。
"""

from __future__ import annotations

import re
import uuid
from contextlib import contextmanager
from contextvars import ContextVar, Token
from collections.abc import Iterator
from typing import Final

from app.config import DEFAULT_CORRELATION_MAX_LENGTH

CORRELATION_HEADER: Final[str] = "X-Correlation-ID"
CORRELATION_RESPONSE_HEADER: Final[str] = CORRELATION_HEADER

# 允许字符：字母、数字、- _ . : （与合法 HTTP token 接近，且不含空白）
_ALLOWED_PATTERN: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9_.:-]+")
_GENERATE_PREFIX: Final[str] = "cid-"

_correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)


class InvalidCorrelationIdError(ValueError):
    """客户端提供的关联标识不合法。

    :attr reason: 机器可读的拒绝原因（empty/too_long/illegal_character）
    :attr detail: 可安全展示给客户端的补充说明
    """

    def __init__(self, value: str, reason: str, detail: str = "") -> None:
        self.value = value
        self.reason = reason
        self.detail = detail
        super().__init__(f"非法的关联标识: reason={reason} {detail}".rstrip())


def generate_correlation_id() -> str:
    """生成服务端关联标识：``cid-`` + UUID4 十六进制（共 36 字符）。"""
    return f"{_GENERATE_PREFIX}{uuid.uuid4().hex}"


def validate_correlation_id(value: str, max_length: int = DEFAULT_CORRELATION_MAX_LENGTH) -> str:
    """校验客户端传入值，合法则原样返回，否则抛 :class:`InvalidCorrelationIdError`。"""
    if value is None or value == "":
        raise InvalidCorrelationIdError("", "empty", "关联标识为空")
    if value != value.strip() or not value.strip():
        raise InvalidCorrelationIdError(value, "empty", "关联标识为纯空白或带首尾空白")
    if len(value) > max_length:
        raise InvalidCorrelationIdError(
            value,
            "too_long",
            f"长度 {len(value)} 超过上限 {max_length}",
        )
    if not _ALLOWED_PATTERN.fullmatch(value):
        bad = next((ch for ch in value if not _ALLOWED_PATTERN.fullmatch(ch)), "?")
        raise InvalidCorrelationIdError(
            value,
            "illegal_character",
            f"包含不允许的字符 {bad!r}（仅允许字母数字与 - _ . :）",
        )
    return value


def get_correlation_id() -> str | None:
    """读取当前上下文关联标识；未设置时为 ``None``。"""
    return _correlation_id_var.get()


def require_correlation_id() -> str:
    """读取当前上下文关联标识；未设置时抛 :class:`LookupError`（属于装配缺陷）。"""
    value = _correlation_id_var.get()
    if value is None:
        raise LookupError("当前上下文缺少关联标识")
    return value


def set_correlation_id(value: str) -> Token[str | None]:
    """在当前上下文写入关联标识，返回用于恢复的 token。"""
    return _correlation_id_var.set(value)


def reset_correlation_id(token: Token[str | None]) -> None:
    """按 token 恢复关联标识上下文。"""
    _correlation_id_var.reset(token)


@contextmanager
def correlation_context(value: str) -> Iterator[None]:
    """在 ``with`` 作用域内绑定关联标识，退出时恢复原值。"""
    token = set_correlation_id(value)
    try:
        yield
    finally:
        reset_correlation_id(token)
