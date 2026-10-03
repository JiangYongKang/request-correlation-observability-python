"""贯穿请求生命周期的中间件：关联标识、根片段、日志与指标。

采用纯 ASGI 中间件（非 BaseHTTPMiddleware）：``await app(...)`` 在
流式响应的最后一个 body 块发送完毕后才返回，因此根 server 片段能
**完整覆盖**流式响应生命周期，指标也恰好在响应结束后计数一次。

每个请求的处理顺序：
1. 读取关联标识头：缺失则生成；存在则按合法性规则校验，
   非法时直接返回 400（``reason`` 可区分 empty/too_long/illegal_character）；
2. 绑定上下文 + 开启根 server 片段（``trace_id`` 即关联标识，全链一致）；
3. 透传关联标识到响应头；
4. 无论正常/异常/流式，出口处恰好记录一次指标、导出片段并记录收尾日志。
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
from app.exporter import RotatingFileSpanExporter
from app.logging_setup import configure_logging, get_logger, log_event
from app.metrics import OUTCOME_CLIENT_DISCONNECTED, get_metrics, outcome_for
from app.sampling import Sampler
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
        response_complete = {"value": False}
        disconnected_early = {"value": False}
        cancelled = {"value": False}
        outcome_holder: dict[str, Any] = {"outcome": None, "reason": None}

        async def wrapped_receive() -> dict[str, Any]:
            message = await receive()
            # 判定依据：响应尚未发完就收到 http.disconnect ⇒ 客户端自己走了
            if message.get("type") == "http.disconnect" and not response_complete["value"]:
                disconnected_early["value"] = True
            return message

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
            elif message["type"] == "http.response.body" and not message.get("more_body"):
                response_complete["value"] = True
            await send(message)

        log_event(
            _logger,
            20,
            "request_started",
            correlation_id=correlation_id,
            method=method,
            path=raw_path,
            correlation_id_provided=provided,
        )
        sampling = self.tracer.sampler.decide(correlation_id, raw_path)
        log_event(
            _logger,
            10,
            "sampling_decision",
            correlation_id=correlation_id,
            path=raw_path,
            sample_rate=sampling.rate,
            sample_value=round(sampling.value, 6),
            sample_kept=sampling.kept,
            sample_reason=sampling.reason,
        )

        try:
            with correlation_context(correlation_id):
                with self.tracer.span(
                    f"{method} {raw_path}",
                    kind="server",
                    trace_id=correlation_id,
                    sample_path=raw_path,
                    correlation_id=correlation_id,
                    method=method,
                    path=raw_path,
                ) as root_span:
                    try:
                        await self.app(scope, wrapped_receive, wrapped_send)
                    except asyncio.CancelledError:
                        # 判定依据：请求任务被取消（uvicorn 在客户端断连时
                        # 取消处理任务）⇒ 客户端自己走了，不是服务端错误。
                        cancelled["value"] = True
                        if root_span.end_ns is None:
                            root_span.end(
                                "ERROR",
                                error_type="ClientDisconnect",
                                error_message="响应完成前客户端断开（任务被取消）",
                            )
                        log_event(
                            _logger,
                            30,
                            "client_disconnected",
                            correlation_id=correlation_id,
                            method=method,
                            path=raw_path,
                            disconnect_reason="task_cancelled",
                        )
                        raise
                    except Exception as exc:  # 漏网异常：安全化 500
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
                        # 结果分类（判定依据明确）：
                        # - 任务被取消 / 响应发完前收到 disconnect ⇒ 客户端自己走了
                        # - 否则按状态码推导 success/client_error/server_error
                        if cancelled["value"]:
                            outcome = OUTCOME_CLIENT_DISCONNECTED
                            outcome_reason = "task_cancelled"
                            record_status: int | None = None
                        elif disconnected_early["value"]:
                            outcome = OUTCOME_CLIENT_DISCONNECTED
                            outcome_reason = "http_disconnect_before_response_complete"
                            record_status = None
                            if root_span.end_ns is None:
                                root_span.end(
                                    "ERROR",
                                    error_type="ClientDisconnect",
                                    error_message="响应完成前客户端断开（http.disconnect）",
                                )
                        else:
                            outcome = outcome_for(status_holder["code"])
                            outcome_reason = f"status_{status_holder['code']}"
                            record_status = status_holder["code"]
                        outcome_holder["outcome"] = outcome
                        outcome_holder["reason"] = outcome_reason
                        self.metrics.record(
                            route=route,
                            method=method,
                            status_code=record_status,
                            duration_ms=duration_ms,
                            outcome=outcome,
                        )
                        if root_span.end_ns is None:
                            root_span.set_attribute("http.status_code", status_holder["code"])
                            root_span.set_attribute("http.route", route or "unmatched")
                        root_span.set_attribute("http.outcome", outcome)
        finally:
            duration_ms = (time.perf_counter() - start) * 1000
            self.tracer.export_finished()
            self.tracer.exporter.flush()
            log_event(
                _logger,
                20,
                "request_finished",
                correlation_id=correlation_id,
                method=method,
                status_code=status_holder["code"],
                duration_ms=round(duration_ms, 3),
                outcome=outcome_holder["outcome"],
                outcome_reason=outcome_holder["reason"],
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
        sampler = Sampler(
            settings.sample_rate,
            seed=settings.sample_seed,
            overrides=settings.sample_rate_overrides,
        )
        if settings.spans_export_path:
            exporter: Any = RotatingFileSpanExporter(
                settings.spans_export_path,
                max_bytes=settings.spans_max_bytes,
                max_files=settings.spans_max_files,
                rotate_interval_s=settings.spans_rotate_interval_s,
                queue_size=settings.spans_queue_size,
            )
        else:
            exporter = FileSpanExporter("")
        tracer = Tracer(
            TracerConfig(
                sample_rate=settings.sample_rate,
                exporter=exporter,
                sampler=sampler,
                tail_sampling=True,
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
            log_event(
                _logger,
                20,
                "shutdown_flush",
                metrics_totals=totals,
                sampling_stats=tracer.sampling_stats(),
            )
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
