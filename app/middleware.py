"""关联标识 + 指标 + 追踪的纯 ASGI 中间件。

关键取舍:
- 使用纯 ASGI 实现而非 ``BaseHTTPMiddleware``：流式响应的每个数据帧都经过
  包装后的 ``send``，请求指标在**最后一帧**落账，时延即真实响应时延；
- 根追踪片段包裹整个 ``await self.app(...)``，Starlette 的后台任务在响应
  发送后于同一调用内执行，因此后台片段天然是请求片段的子片段；
- 指标每个请求严格记录一次（``_recorded`` 标志），客户端断连记 499，
- 客户端提供非法关联标识时在进入应用前直接拒绝，原因码可区分，
  仍记录一次 4xx 指标，便于观测拒绝率；
- 关联标识响应头在响应开始帧统一回写。
"""

from __future__ import annotations

import logging
import time
from typing import Any

from .correlation import (
    bind_correlation_id,
    reset_correlation_id,
    resolve_correlation_id,
)
from .errors import describe_error
from .metrics import normalize_route, get_registry
from .tracing import get_tracer

#: 客户端主动断连使用的非标准状态码标签
_CANCELLED_STATUS = 499


class ObservabilityMiddleware:
    """在 HTTP 入口统一处理关联标识、追踪与指标。"""

    def __init__(self, app: Any, settings: Any, logger: logging.Logger) -> None:
        self.app = app
        self.settings = settings
        self.logger = logger

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "")
        raw_path = scope.get("path", "")
        header_name = self.settings.correlation_header
        header_lower = header_name.lower()

        raw_cid = None
        for key, value in scope.get("headers", []):
            if key.decode("latin-1").lower() == header_lower:
                raw_cid = value.decode("latin-1")
                break

        try:
            correlation_id, generated = resolve_correlation_id(
                raw_cid, self.settings.correlation_length_max
            )
        except Exception as exc:  # noqa: BLE001 - 需要在入口统一转为安全响应
            await self._reject_invalid_correlation(
                scope, send, method, raw_path, exc
            )
            return

        token = bind_correlation_id(correlation_id)
        scope["state"] = dict(scope.get("state") or {})
        start_perf = time.perf_counter()
        status_holder = {"code": 0}
        recorded = {"done": False}
        root_span_holder: dict[str, Any] = {}

        def _record_once(status_code: int, is_error: bool) -> None:
            if recorded["done"]:
                return
            recorded["done"] = True
            duration = time.perf_counter() - start_perf
            # 路由模板在路由匹配后写入 scope["route"]，取不到则归一标签
            route = scope.get("route")
            route_path = getattr(route, "path", None)
            get_registry().record_request(
                method=method,
                path_template=normalize_route(route_path),
                status_code=status_code,
                duration_seconds=duration,
                is_error=is_error,
            )
            self.logger.info(
                "request.complete",
                extra={
                    "fields": {
                        "method": method,
                        "path_template": normalize_route(route_path),
                        "http_status": status_code,
                        "duration_ms": round(duration * 1000, 3),
                        "correlation_generated": generated,
                        "is_error": is_error,
                    }
                },
            )

        async def send_wrapper(message: dict) -> None:
            if message["type"] == "http.response.start":
                status_holder["code"] = int(message.get("status", 0))
                headers = message.setdefault("headers", [])
                headers.append(
                    (header_name.encode("latin-1"), correlation_id.encode("latin-1"))
                )
            elif message["type"] == "http.response.body":
                if not message.get("more_body", False):
                    code = status_holder["code"] or 200
                    _record_once(code, code >= 500)
                    span = root_span_holder.get("span")
                    if span is not None:
                        span.set_attribute("http.status_code", code)
                        # 受控 5xx（异常处理器已生成响应）也需在追踪上体现失败，
                        # 但不提前结束片段：后台任务的耗时仍计入根片段
                        if code >= 500 and span.status == "ok":
                            span.set_failure(
                                "HttpServerError",
                                f"请求以 {code} 状态结束",
                            )
            await send(message)

        tracer = get_tracer()
        self.logger.info(
            "request.start",
            extra={
                "fields": {
                    "method": method,
                    "path": raw_path,
                    "correlation_generated": generated,
                }
            },
        )

        try:
            with tracer.span(
                f"http.request {method}",
                attributes={
                    "http.method": method,
                    "http.target": raw_path[:128],
                    "correlation.generated": generated,
                },
            ) as root_span:
                root_span_holder["span"] = root_span
                await self.app(scope, receive, send_wrapper)
        except BaseException as exc:  # noqa: BLE001
            code, _reason, http_status = describe_error(exc)
            span = root_span_holder.get("span")
            if span is not None:
                span.set_attribute("error_code", code)
            # CancelledError 由 span 上下文标记 error 后继续向上抛
            if isinstance(exc, BaseException) and code == "request_cancelled":
                _record_once(_CANCELLED_STATUS, True)
                raise
            if not recorded["done"]:
                _record_once(http_status, http_status >= 500)
            # 响应尚未开始则就地返回脱敏错误；否则只能记录，交给上层
            if status_holder["code"] == 0:
                from fastapi.responses import JSONResponse

                body = {
                    "error": {
                        "code": code,
                        "reason": _reason,
                        "correlation_id": correlation_id,
                    }
                }
                response = JSONResponse(
                    status_code=http_status,
                    content=body,
                    headers={header_name: correlation_id},
                )
                await response(scope, receive, send)
                recorded["done"] = True
            else:
                raise
        finally:
            # 兜底：极端情况下未在帧结束时落账（如无 body 的响应中断）
            if not recorded["done"]:
                code = status_holder["code"] or 200
                _record_once(code, code >= 500)
            reset_correlation_id(token)

    async def _reject_invalid_correlation(
        self,
        scope: dict,
        send: Any,
        method: str,
        raw_path: str,
        exc: BaseException,
    ) -> None:
        """非法关联标识：直接返回可区分原因的 400，并记录一次指标。"""

        from fastapi.responses import JSONResponse

        code, reason, http_status = describe_error(exc)
        supplied = next(
            (
                v.decode("latin-1")
                for k, v in scope.get("headers", [])
                if k.decode("latin-1").lower() == self.settings.correlation_header.lower()
            ),
            "",
        )
        self.logger.warning(
            "correlation.rejected",
            extra={
                "fields": {
                    "reason": code,
                    "method": method,
                    "path": raw_path,
                    # 只记录长度，绝不回显客户端原值
                    "supplied_length": len(supplied),
                }
            },
        )
        get_registry().record_request(
            method=method,
            path_template="__invalid_correlation__",
            status_code=400,
            duration_seconds=0.0,
            is_error=False,
        )
        body = {
            "error": {
                "code": code,
                "reason": reason,
                "correlation_id": None,
            }
        }
        response = JSONResponse(status_code=http_status, content=body)

        async def _noop_receive() -> dict:
            return {"type": "http.disconnect"}

        await response(scope, _noop_receive, send)
