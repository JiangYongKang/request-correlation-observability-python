"""应用装配。

- 启动时：加载配置、初始化结构化日志、追踪器与指标注册表；
- 关闭时：写出指标快照并关闭追踪器（强制落盘未导出片段），
  保证进程退出/重启时观测数据不静默丢失。
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from .config import Settings, load_settings
from .errors import register_exception_handlers
from .logging_setup import configure_logging
from .metrics import get_registry, reset_registry
from .middleware import ObservabilityMiddleware
from .routers.demo import router as demo_router
from .tracing import configure_tracer


def create_app(settings: Settings | None = None) -> FastAPI:
    """创建并装配 FastAPI 应用。"""

    settings = settings or load_settings()
    logger = configure_logging(settings.log_json, settings.log_level)

    reset_registry(settings.metrics_snapshot_path)
    tracer = configure_tracer(
        sample_rate=settings.trace_sample_rate,
        export_kind=settings.trace_export,
        file_path=settings.trace_file_path,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        logger.info("app.startup", extra={"fields": {"trace_export": settings.trace_export}})
        try:
            yield
        finally:
            # 先落指标快照，再关追踪器；任何一步失败都不能阻止另一步执行
            try:
                get_registry().write_snapshot()
                logger.info("app.metrics_flushed")
            except Exception:  # noqa: BLE001
                logger.exception("app.metrics_flush_failed")
            try:
                tracer.shutdown()
                logger.info("app.tracer_shutdown")
            except Exception:  # noqa: BLE001
                logger.exception("app.tracer_shutdown_failed")

    app = FastAPI(title="request-correlation-observability", lifespan=lifespan)
    app.include_router(demo_router)
    app.add_middleware(
        ObservabilityMiddleware, settings=settings, logger=logger
    )
    register_exception_handlers(app, logger, settings.correlation_header)
    app.state.settings = settings
    app.state.logger = logger
    return app
