"""异常脱敏与统一错误响应。

规则:
- 对外只暴露稳定的错误码与简短原因，不返回堆栈、内部主机名/SQL 等；
- 关联标识非法时按具体子类型给出可区分原因（长度/字符集）；
- 未知异常统一映射为 ``internal_error``，详细信息仅进服务端日志；
- 错误响应体与响应头都带关联标识，便于客户端与服务端对账。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .correlation import (
    CorrelationError,
    CorrelationFormatError,
    CorrelationLengthError,
    get_correlation_id_or_none,
)

ERR_VALIDATION = "invalid_request"
ERR_CORR_FORMAT = CorrelationFormatError.reason
ERR_CORR_LENGTH = CorrelationLengthError.reason
ERR_HTTP = "http_error"
ERR_INTERNAL = "internal_error"
ERR_TIMEOUT = "upstream_timeout"
ERR_CANCELLED = "request_cancelled"

#: 允许透出的少量白名单 HTTP 状态原因
_HTTP_DETAIL_ALLOWLIST: dict[int, str] = {
    400: "请求参数不合法",
    401: "未通过身份认证",
    403: "禁止访问",
    404: "资源不存在",
    405: "方法不被允许",
    409: "资源状态冲突",
    413: "请求体过大",
    422: "请求参数校验失败",
    429: "请求过于频繁",
    503: "服务暂不可用",
}


def describe_error(exc: BaseException) -> tuple[str, str, int]:
    """把异常映射为 ``(错误码, 安全原因, HTTP 状态码)``。

    原因字符串是面向客户端的简短说明，不拼接任何异常内部文本。
    """

    if isinstance(exc, CorrelationLengthError):
        return ERR_CORR_LENGTH, "关联标识长度超过允许上限", 400
    if isinstance(exc, CorrelationFormatError):
        return ERR_CORR_FORMAT, "关联标识包含非法字符或首尾字符不合法", 400
    if isinstance(exc, RequestValidationError):
        return ERR_VALIDATION, "请求参数校验失败", 422
    if isinstance(exc, asyncio.CancelledError):
        return ERR_CANCELLED, "请求已取消", 499
    if isinstance(exc, TimeoutError):
        return ERR_TIMEOUT, "上游处理超时", 504
    if isinstance(exc, StarletteHTTPException):
        status = exc.status_code if 400 <= exc.status_code < 600 else 500
        return ERR_HTTP, _HTTP_DETAIL_ALLOWLIST.get(status, "请求处理失败"), status
    return ERR_INTERNAL, "服务内部错误", 500


def _resolve_cid(request: Request) -> str:
    return get_correlation_id_or_none() or getattr(
        request.state, "correlation_id", "-"
    )


def build_error_body(request: Request, exc: BaseException) -> tuple[dict, int, str]:
    """构造脱敏错误体，返回 ``(body, status_code, correlation_id)``。"""

    code, reason, status_code = describe_error(exc)
    cid = _resolve_cid(request)
    body = {
        "error": {
            "code": code,
            "reason": reason,
            "correlation_id": cid,
        }
    }
    return body, status_code, cid


def safe_error_response(
    request: Request, exc: BaseException, header_name: str
) -> JSONResponse:
    """构造统一脱敏 JSON 响应（供中间件在未进入处理器时复用）。"""

    body, status_code, cid = build_error_body(request, exc)
    return JSONResponse(
        status_code=status_code, content=body, headers={header_name: cid}
    )


def register_exception_handlers(
    app: FastAPI, logger: logging.Logger, header_name: str
) -> None:
    """注册统一异常处理器：记录脱敏日志并返回安全响应。"""

    async def _handle(request: Request, exc: BaseException) -> JSONResponse:
        code, reason, status_code = describe_error(exc)
        cid = _resolve_cid(request)
        # 服务端日志保留异常类型与堆栈，便于排查；消息经日志过滤器脱敏
        logger.error(
            "request.failed",
            exc_info=exc,
            extra={
                "fields": {
                    "error_code": code,
                    "http_status": status_code,
                    "path": request.url.path,
                    "method": request.method,
                }
            },
        )
        return JSONResponse(
            status_code=status_code,
            content={
                "error": {
                    "code": code,
                    "reason": reason,
                    "correlation_id": cid,
                }
            },
            headers={header_name: cid},
        )

    app.add_exception_handler(CorrelationError, _handle)
    app.add_exception_handler(RequestValidationError, _handle)
    app.add_exception_handler(StarletteHTTPException, _handle)
    app.add_exception_handler(Exception, _handle)
