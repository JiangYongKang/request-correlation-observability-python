"""关联标识的生成、校验与异步上下文。

设计要点:
- 标识载体为 :class:`contextvars.ContextVar`，在 ``asyncio`` 下每个任务
  拷贝独立上下文，因此并发请求之间天然隔离、不会互相污染；
- 客户端提供的标识按"字符集 / 首尾字符 / 长度"三条规则校验，分别抛出
  带有不同 ``reason`` 的异常，调用方可以据此给出可区分的拒绝原因；
- 空串视为"未提供"，由服务端生成兜底，而不是当作非法请求拒绝。
"""

from __future__ import annotations

import contextvars
import uuid
from typing import Final

CORRELATION_HEADER: Final[str] = "X-Correlation-ID"

#: 允许出现在关联标识中的字符：字母数字、``-``、``_``、``.``
_ALLOWED_CHARS: Final[frozenset[str]] = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._"
)

_correlation_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "request_correlation_id", default=None
)


class CorrelationError(ValueError):
    """关联标识非法的基类。

    :attr reason: 机器可区分的拒绝原因码。
    """

    reason = "invalid_correlation_id"

    def __init__(self, value: str) -> None:
        # 不把原始 value 放进异常消息，避免敏感信息随日志/响应透出
        self.value = value
        super().__init__(self.reason)


class CorrelationFormatError(CorrelationError):
    """包含非法字符或首尾字符不合法。"""

    reason = "invalid_correlation_id_format"


class CorrelationLengthError(CorrelationError):
    """长度超出允许上限。"""

    reason = "invalid_correlation_id_length"


class CorrelationUnboundError(LookupError):
    """代码在请求上下文之外读取关联标识。"""

    reason = "correlation_context_unbound"


def generate_correlation_id() -> str:
    """生成一个新的关联标识（UUIDv4 十六进制，32 字符）。"""

    return uuid.uuid4().hex


def validate_correlation_id(value: str, max_length: int) -> str:
    """按合法性规则校验客户端提供的关联标识。

    规则:
      1. 非空（空值应在调用方按"未提供"处理）；
      2. 长度不超过 ``max_length``；
      3. 仅包含字母数字、``-``、``_``、``.``；
      4. 首字符与尾字符必须是字母或数字（避免 ``.``、``-`` 等引发
         日志注入或路径拼接歧义）。

    长度规则先于字符规则判定，原因码因此稳定可预期。
    """

    if len(value) > max_length:
        raise CorrelationLengthError(value)

    if (
        not value
        or any(ch not in _ALLOWED_CHARS for ch in value)
        or not value[0].isalnum()
        or not value[-1].isalnum()
    ):
        raise CorrelationFormatError(value)

    return value


def resolve_correlation_id(raw: str | None, max_length: int) -> tuple[str, bool]:
    """解析入口关联标识。

    返回 ``(标识, 是否为服务端生成)``。``raw`` 为 None 或全空白时生成
    新标识；非空时执行严格校验，非法则原样抛出 :class:`CorrelationError`，
    由调用方映射为可区分原因的 4xx 响应。
    """

    if raw is None or not raw.strip():
        return generate_correlation_id(), True
    return validate_correlation_id(raw.strip(), max_length), False


def bind_correlation_id(value: str) -> contextvars.Token[str | None]:
    """把关联标识绑定到当前上下文，返回用于还原的 token。"""

    return _correlation_id_var.set(value)


def reset_correlation_id(token: contextvars.Token[str | None]) -> None:
    """按 token 还原上下文绑定。"""

    _correlation_id_var.reset(token)


def get_correlation_id_or_none() -> str | None:
    """读取当前上下文的关联标识；尚未绑定时返回 None。"""

    return _correlation_id_var.get()


def get_correlation_id() -> str:
    """读取当前上下文的关联标识；未绑定时抛出 :class:`CorrelationUnboundError`。"""

    value = _correlation_id_var.get()
    if value is None:
        raise CorrelationUnboundError("当前执行上下文未绑定关联标识")
    return value


def current_context() -> contextvars.Context:
    """捕获当前上下文副本，供跨异步边界（后台线程/独立任务）继承。"""

    return contextvars.copy_context()
