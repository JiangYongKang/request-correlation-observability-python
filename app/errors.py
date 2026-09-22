"""异常安全处理：不透出依赖库内部异常与敏感信息。

- 面向客户端统一返回 :class:`SafeErrorView`（错误码 + 安全文案 + 关联标识）；
- ``code`` 取值有界，来自 :func:`classify_exception`，可作为指标/日志标签；
- 依赖库内部异常统一归类为 ``internal_error``，原始类型与消息仅写服务端日志，
  客户端只看到通用文案；包含明显敏感关键字的消息同样不透出。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from app.logging_setup import log_event

# 错误码取值集合（有界标签）
CODE_INVALID_CORRELATION = "invalid_correlation_id"
CODE_BAD_REQUEST = "bad_request"
CODE_NOT_FOUND = "not_found"
CODE_TIMEOUT = "timeout"
CODE_CLIENT_CANCEL = "client_cancelled"
CODE_RATE_LIMITED = "rate_limited"
CODE_INTERNAL = "internal_error"

_SENSITIVE_MARKERS = (
    "password", "passwd", "secret", "token", "authorization", "api_key",
    "apikey", "private_key", "credential", "cookie",
)


@dataclass
class SafeErrorView:
    """面向客户端的安全错误视图。"""

    code: str
    message: str
    correlation_id: str
    status_code: int = 500
    reason: str | None = None  # 仅在 4xx 且可安全区分原因时填充（如非法关联标识原因）


# 惰性识别已知的 HTTP/客户端异常类型，避免在导入期硬依赖
def _module_qualname(exc: BaseException) -> str:
    return f"{type(exc).__module__}.{type(exc).__name__}"


def _http_status_of(exc: BaseException) -> int | None:
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        return status
    return None


def classify_exception(exc: BaseException) -> str:
    """把异常映射为有界的错误类型标签。"""
    from app.correlation import InvalidCorrelationIdError

    if isinstance(exc, InvalidCorrelationIdError):
        return CODE_INVALID_CORRELATION
    status = _http_status_of(exc)
    if status is not None:
        if status == 404:
            return CODE_NOT_FOUND
        if status in (401, 403):
            return CODE_BAD_REQUEST
        if status == 408:
            return CODE_TIMEOUT
        if status == 429:
            return CODE_RATE_LIMITED
        if 400 <= status < 500:
            return CODE_BAD_REQUEST
        return CODE_INTERNAL
    # 内置具体类型优先（TimeoutError 是 OSError 别名，需在通用判断之前）
    if isinstance(exc, TimeoutError):
        return CODE_TIMEOUT
    if isinstance(exc, ValueError):
        return CODE_BAD_REQUEST
    if isinstance(exc, LookupError):
        return CODE_NOT_FOUND
    # 依赖库内部异常一律不透出细节
    qualname = _module_qualname(exc)
    if qualname.startswith(("starlette.", "fastapi.", "anyio.")):
        return CODE_INTERNAL
    return CODE_INTERNAL


def safe_message(exc: BaseException, *, code: str | None = None) -> str:
    """返回可安全透出的消息；内部错误与含敏感词的消息只给通用文案。"""
    code = code or classify_exception(exc)
    if code == CODE_INTERNAL:
        return "服务内部错误，请凭关联标识联系管理员"
    raw = str(exc).strip()
    lowered = raw.lower()
    if any(marker in lowered for marker in _SENSITIVE_MARKERS):
        return "请求内容不合法" if code == CODE_BAD_REQUEST else "请求处理失败"
    # 非法关联标识使用我们自己构造的 detail，避免回显原始值
    from app.correlation import InvalidCorrelationIdError

    if isinstance(exc, InvalidCorrelationIdError):
        return exc.detail or "关联标识不合法"
    if not raw:
        return "请求处理失败"
    return raw[:200]


def status_code_for(code: str) -> int:
    """错误码到 HTTP 状态码的映射。"""
    return {
        CODE_INVALID_CORRELATION: 400,
        CODE_BAD_REQUEST: 400,
        CODE_NOT_FOUND: 404,
        CODE_TIMEOUT: 504,
        CODE_CLIENT_CANCEL: 499,
        CODE_RATE_LIMITED: 429,
        CODE_INTERNAL: 500,
    }.get(code, 500)


def build_safe_view(exc: BaseException, correlation_id: str) -> SafeErrorView:
    """从异常构造安全视图。"""
    code = classify_exception(exc)
    view = SafeErrorView(
        code=code,
        message=safe_message(exc, code=code),
        correlation_id=correlation_id,
        status_code=status_code_for(code),
    )
    from app.correlation import InvalidCorrelationIdError

    if isinstance(exc, InvalidCorrelationIdError):
        view.reason = exc.reason
    return view


def log_exception(
    logger: logging.Logger,
    exc: BaseException,
    correlation_id: str,
    *,
    span_name: str = "request",
) -> SafeErrorView:
    """服务端记录完整异常（类型+消息+堆栈），返回给客户端的安全视图不含内部细节。"""
    view = build_safe_view(exc, correlation_id)
    log_event(
        logger,
        logging.ERROR if view.status_code >= 500 else logging.WARNING,
        "request_exception",
        correlation_id=correlation_id,
        error_code=view.code,
        error_type=type(exc).__name__,
        error_module=type(exc).__module__,
        detail=str(exc)[:500],
        where=span_name,
        exc_info=exc if view.status_code >= 500 else None,
    )
    return view

