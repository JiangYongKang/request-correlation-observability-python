"""贯穿请求生命周期的中间件：关联标识、根片段、日志与指标。

采用纯 ASGI 中间件（非 BaseHTTPMiddleware）：``await app(...)`` 在
流式响应的最后一个 body 块发送完毕后才返回，因此根 server 片段能
**完整覆盖**流式响应生命周期，指标也恰好在响应结束后计数一次。

每个请求的处理顺序：
1. 读取关联标识头：缺失则生成；存在则按合法性规则校验，
   非法时直接返回 400（``reason`` 可区分 empty/too_long/illegal_character）；
2. 绑定上下文 + 开启根 server 片段（``trace_id`` 即关联标识，全链一致）；
3. 透传关联标识到响应头；
4. 无论正常/异常/断连/流式，出口处恰好记录一次指标并记录收尾日志；
   片段导出由追踪器按 trace 收尾自动完成，写盘由导出器缓冲+周期落盘，
   请求主链路不做同步刷盘。

结果分类（判定依据明确）：
- 服务端异常：``except Exception`` 捕获且非断连 ⇒ ``server_error``；
- 客户端断连：写响应时连接异常（``send`` 抛出），或响应完成前请求任务
  被取消（``CancelledError``）⇒ ``client_disconnect``，不算成功也不算
  服务端错误，不计入错误率。
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

from starlette.responses import Response

from app.config import ObservabilitySettings, get_settings
from app.correlation import (
    CORRELATION_RESPONSE_HEADER,
    InvalidCorrelationIdError,
    correlation_context,
    generate_correlation_id,
    validate_correlation_id,
)
from app.errors import build_safe_view
from app.logging_setup import configure_logging, get_logger, log_event
from app.metrics import get_metrics
from app.tracing import FileSpanExporter, Tracer, TracerConfig

Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]

_logger = get_logger("app.observability")


class ObservabilityMiddleware:
    """关联标识 + 追踪 + 指标一体化 ASGI 中间件。"""

    def __init__(
        self,
        app: Callable[..., Awaitable[None]],
        *,
        settings: ObservabilitySettings,
        tracer: Tracer,
        metrics: Any,
    ) -> None:
        self.app = app
        self.settings = settings
        self.tracer = tracer
        self.metrics = metrics
        self.header_bytes = settings.correlation_header.lower().encode("latin-1")

    async def _send_json(self, send: Send, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json; charset=utf-8"),
                    (b"content-length", str(len(body)).encode("latin-1")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    async def __call__(self, scope: dict[str, Any], receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "GET")
        raw_path = scope.get("path", "/")
        header_value = None
        for key, value in scope.get("headers", []):
            if key == self.header_bytes:
                header_value = value.decode("latin-1", errors="replace")
                break

        # 1) 关联标识：生成或校验
        if header_value is None:
            correlation_id = generate_correlation_id()
            provided = False
        else:
            try:
                correlation_id = validate_correlation_id(
                    header_value, max_length=self.settings.correlation_max_length
                )
            except InvalidCorrelationIdError as exc:
                # 非法请求：不建片段、不绑定，按可区分原因直接拒绝，计数一次
                log_event(
                    _logger,
                    30,
                    "correlation_id_rejected",
                    header=self.settings.correlation_header,
                    reason=exc.reason,
                    detail=exc.detail,
                    method=method,
                    path=raw_path,
                )
                self.metrics.record(
                    route=None, method=method, status_code=400, duration_ms=0.0
                )
                await self._send_json(
                    send,
                    400,
                    {
                        "error": {
                            "code": "invalid_correlation_id",
                            "message": exc.detail or "关联标识不合法",
                            "correlation_id": None,
                            "reason": exc.reason,
                        }
                    },
                )
                return
            provided = True

        start = time.perf_counter()
        status_holder = {"code": 500}
        response_started = {"value": False}
        # 断连判定依据：send 写连接失败 / 响应完成前任务被取消
        disconnect = {"basis": None}
        outcome_holder = {"value": None}

        async def wrapped_send(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                response_started["value"] = True
                status_holder["code"] = int(message.get("status", 500))
                headers = list(message.get("headers", []))
                headers.append(
                    (
                        CORRELATION_RESPONSE_HEADER.lower().encode("latin-1"),
                        correlation_id.encode("latin-1"),
                    )
                )
                message = dict(message, headers=headers)
            try:
                await send(message)
            except Exception as exc:
                # 写响应时连接已断：记录判定依据后原样抛出，
                # 由外层归类为 client_disconnect（不是服务端错误）
                disconnect["basis"] = f"send_failed:{type(exc).__name__}"
                raise

        log_event(
            _logger,
            20,
            "request_started",
            correlation_id=correlation_id,
            method=method,
            path=raw_path,
            correlation_id_provided=provided,
        )

        try:
            with correlation_context(correlation_id):
                with self.tracer.span(
                    f"{method} {raw_path}",
                    kind="server",
                    trace_id=correlation_id,
                    correlation_id=correlation_id,
                    method=method,
                    path=raw_path,
                ) as root_span:
                    try:
                        await self.app(scope, receive, wrapped_send)
                    except asyncio.CancelledError:
                        # 响应完成前任务被取消：客户端主动断开（或进程关停）。
                        # 判定依据写进日志与片段，指标计 client_disconnect，
                        # 既不算成功也不混入服务端错误率。
                        disconnect["basis"] = disconnect["basis"] or "task_cancelled_before_response_complete"
                        outcome_holder["value"] = "client_disconnect"
                        log_event(
                            _logger,
                            30,
                            "client_disconnected",
                            correlation_id=correlation_id,
                            method=method,
                            path=raw_path,
                            basis=disconnect["basis"],
                            response_started=response_started["value"],
                        )
                        if root_span.end_ns is None:
                            root_span.set_attribute("client_disconnect", True)
                            root_span.end(
                                "ERROR",
                                error_type="ClientDisconnect",
                                error_message=disconnect["basis"],
                            )
                        raise
                    except Exception as exc:  # 漏网异常：安全化 500
                        if disconnect["basis"] is not None:
                            # 写响应时连接已断：无法回送任何响应，按断连归类
                            outcome_holder["value"] = "client_disconnect"
                            log_event(
                                _logger,
                                30,
                                "client_disconnected",
                                correlation_id=correlation_id,
                                method=method,
                                path=raw_path,
                                basis=disconnect["basis"],
                                response_started=response_started["value"],
                            )
                            if root_span.end_ns is None:
                                root_span.set_attribute("client_disconnect", True)
                                root_span.end(
                                    "ERROR",
                                    error_type="ClientDisconnect",
                                    error_message=disconnect["basis"],
                                )
                            return
                        view = build_safe_view(exc, correlation_id)
                        log_event(
                            _logger,
                            40,
                            "unhandled_exception",
                            correlation_id=correlation_id,
                            error_code=view.code,
                            error_type=type(exc).__name__,
                        )
                        if root_span.end_ns is None:
                            root_span.set_attribute("error", True)
                        if not response_started["value"]:
                            status_holder["code"] = view.status_code
                            root_span.end(
                                "ERROR",
                                error_type=type(exc).__name__,
                                error_message=view.message,
                            )
                            await self._send_json(
                                wrapped_send,
                                view.status_code,
                                {"error": {
                                    "code": view.code,
                                    "message": view.message,
                                    "correlation_id": correlation_id,
                                }},
                            )
                        else:
                            # 响应已开始（流式中途异常）：HTTP 状态码无法更改，
                            # 但要保证可观测结论一致：根片段标 ERROR、指标记 5xx，
                            # 并发送终止帧干净收尾（客户端观察到流被截断），
                            # 不让依赖库异常穿透到服务端。
                            status_holder["code"] = 500
                            root_span.end(
                                "ERROR",
                                error_type=type(exc).__name__,
                                error_message=view.message,
                            )
                            log_event(
                                _logger,
                                40,
                                "response_aborted_after_start",
                                correlation_id=correlation_id,
                                error_code=view.code,
                                error_type=type(exc).__name__,
                            )
                            await wrapped_send(
                                {"type": "http.response.body", "body": b"", "more_body": False}
                            )
                    finally:
                        route = None
                        matched = scope.get("route")
                        if matched is not None:
                            route = getattr(matched, "path", None)
                        duration_ms = (time.perf_counter() - start) * 1000
                        outcome = outcome_holder["value"]
                        # 断连且响应未开始时无有效状态码：status_class 记 unknown，
                        # 避免被误读为服务端 5xx
                        code_for_metrics = status_holder["code"]
                        if outcome == "client_disconnect" and not response_started["value"]:
                            code_for_metrics = None
                        self.metrics.record(
                            route=route,
                            method=method,
                            status_code=code_for_metrics,
                            duration_ms=duration_ms,
                            outcome=outcome,
                        )
                        if root_span.end_ns is None:
                            root_span.set_attribute("http.status_code", status_holder["code"])
                            root_span.set_attribute("http.route", route or "unmatched")
        finally:
            duration_ms = (time.perf_counter() - start) * 1000
            # 片段导出由追踪器在 trace 收尾时自动完成；此处仅兜底收尾，
            # 不在请求主链路同步刷盘（导出器缓冲 + 周期落盘 + 关停刷盘）
            self.tracer.export_finished()
            log_event(
                _logger,
                20,
                "request_finished",
                correlation_id=correlation_id,
                method=method,
                status_code=status_holder["code"],
                duration_ms=round(duration_ms, 3),
            )


def install_observability(
    app: Any,
    settings: ObservabilitySettings | None = None,
    *,
    tracer: Tracer | None = None,
) -> Tracer:
    """向 FastAPI 应用挂载中间件、异常处理器与关停钩子；返回追踪器。"""
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    if tracer is None:
        exporter: Any = FileSpanExporter(
            settings.spans_export_path,
            max_bytes=settings.spans_max_bytes,
            rotate_interval_s=settings.spans_rotate_interval_s,
            max_files=settings.spans_max_files,
            buffer_bytes=settings.spans_buffer_bytes,
            flush_interval_s=settings.spans_flush_interval_s,
        )
        tracer = Tracer(
            TracerConfig(
                sample_rate=settings.sample_rate,
                exporter=exporter,
                sample_seed=settings.sample_seed,
                route_sample_rates=settings.route_sample_rates,
            )
        )

    metrics = get_metrics()

    @asynccontextmanager
    async def lifespan(app: Any) -> Any:
        try:
            yield
        finally:
            # 进程关停：先刷指标汇总日志，再强制收尾并导出所有未完成片段
            totals = metrics.snapshot()["totals"]
            log_event(_logger, 20, "shutdown_flush", metrics_totals=totals)
            tracer.shutdown()

    app.router.lifespan_context = lifespan

    app.state.observability_settings = settings
    app.state.tracer = tracer
    app.state.metrics = metrics

    app.add_middleware(
        ObservabilityMiddleware,
        settings=settings,
        tracer=tracer,
        metrics=metrics,
    )

    @app.exception_handler(InvalidCorrelationIdError)
    async def _invalid_cid_handler(request: Any, exc: InvalidCorrelationIdError) -> Response:
        view = build_safe_view(exc, "")
        return Response(
            content=json.dumps(
                {
                    "error": {
                        "code": view.code,
                        "message": view.message,
                        "correlation_id": None,
                        "reason": exc.reason,
                    }
                },
                ensure_ascii=False,
            ),
            status_code=400,
            media_type="application/json",
        )

    return tracer
