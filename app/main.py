"""FastAPI 应用装配：正常、异常、后台与流式四类入口。

所有入口共享同一套关联标识 / 日志 / 指标 / 追踪中间件，
响应头 ``X-Correlation-ID`` 回传最终生效的关联标识。
"""

from __future__ import annotations

import asyncio

from fastapi import BackgroundTasks, FastAPI
from fastapi.responses import JSONResponse, StreamingResponse

from app.background import bind_context, run_background_async
from app.config import ObservabilitySettings, get_settings
from app.correlation import get_correlation_id
from app.logging_setup import get_logger, log_event
from app.middleware import install_observability
from app.metrics import get_metrics
from app.streaming import instrumented_stream
from app.tracing import Tracer

logger = get_logger("app.routes")


def _sync_side_effect(record_id: int) -> None:
    """同步后台副作用（线程池执行）。"""
    log_event(logger, 20, "sync_background_done", input=record_id)


async def _async_side_effect(record_id: int) -> None:
    """异步后台副作用（事件循环执行），同样继承关联标识与追踪栈。"""
    await asyncio.sleep(0)
    log_event(logger, 20, "async_background_done", input=record_id)


async def _stream_producer(count: int, delay_ms: int) -> "object":
    for i in range(count):
        if delay_ms:
            await asyncio.sleep(delay_ms / 1000)
        log_event(logger, 10, "stream_chunk", input=i)
        yield f"chunk-{i}\n".encode("utf-8")


def create_app(
    settings: ObservabilitySettings | None = None,
    tracer: Tracer | None = None,
) -> FastAPI:
    """创建应用实例；测试可注入配置与自定义追踪器/导出器。"""
    settings = settings or get_settings()
    application = FastAPI(title="request-correlation-observability", version="0.1.0")
    used_tracer = install_observability(application, settings=settings, tracer=tracer)

    @application.get("/")
    async def read_root() -> dict[str, str]:
        """正常入口。"""
        cid = get_correlation_id()
        log_event(logger, 20, "hello", input="root")
        return {"Hello": "World", "correlation_id": cid}

    @application.get("/items/{item_id}")
    async def read_item(item_id: int) -> dict[str, object]:
        """路径参数入口（用于验证路由模板作为有界指标标签）。"""
        return {"item_id": item_id, "correlation_id": get_correlation_id()}

    @application.get("/boom")
    async def boom() -> JSONResponse:
        """异常入口：服务端异常不透出内部细节，仅回关联标识。"""
        raise RuntimeError("db password=hunter2 connection failed")

    @application.get("/bad-request")
    async def bad_request(n: int | None = None) -> JSONResponse:
        """4xx 异常入口。"""
        if n is None:
            raise ValueError("缺少必填参数 n")
        return JSONResponse({"n": n})

    @application.post("/records")
    async def create_record(background_tasks: BackgroundTasks, record_id: int = 1) -> dict[str, object]:
        """后台入口：同步/异步后台任务都继承同一关联标识与 trace。"""
        cid = get_correlation_id()
        # FastAPI BackgroundTasks 在线程池运行同步函数：显式绑定捕获上下文
        background_tasks.add_task(bind_context(_sync_side_effect, cid, used_tracer), record_id)
        # 异步 fire-and-forget：以捕获上下文排期
        run_background_async(
            _async_side_effect, record_id, correlation_id=cid, tracer=used_tracer
        )
        return {"accepted": True, "record_id": record_id, "correlation_id": cid}

    @application.get("/stream")
    async def stream(count: int = 3, delay_ms: int = 0) -> StreamingResponse:
        """流式入口：整段流共用同一关联标识与 stream 子片段。"""
        cid = get_correlation_id()
        body = instrumented_stream(
            _stream_producer(count, delay_ms),
            correlation_id=cid,
            tracer=used_tracer,
        )
        return StreamingResponse(body, media_type="text/plain")

    @application.get("/stream/boom")
    async def stream_boom() -> StreamingResponse:
        """流式中途异常入口。"""
        cid = get_correlation_id()

        async def producer() -> "object":
            yield b"before\n"
            await asyncio.sleep(0)
            raise RuntimeError("stream secret=xxx failed")

        body = instrumented_stream(producer(), correlation_id=cid, tracer=used_tracer)
        return StreamingResponse(body, media_type="text/plain")

    @application.get("/metrics")
    async def metrics_snapshot() -> dict[str, object]:
        """指标查看（本地验证用）。"""
        snapshot = get_metrics().snapshot()
        snapshot["correlation_id"] = get_correlation_id()
        return snapshot

    @application.get("/health")
    async def health() -> dict[str, str]:
        """健康检查（高频低价值入口，可用采样覆盖调低或关闭）。"""
        return {"status": "ok", "correlation_id": get_correlation_id() or ""}

    return application


# 默认进程级应用（uvicorn main:app 使用）
app = create_app()
