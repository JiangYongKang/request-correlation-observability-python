"""后台处理：验证跨异步/线程边界的上下文继承。

FastAPI 的 ``BackgroundTasks`` 与请求共享同一个 :class:`contextvars.Context`，
因此可以直接读到关联标识；但提交到线程池或独立调度的任务必须显式携带
``contextvars.copy_context()`` 的副本。本模块两条路径都覆盖。
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import time
from typing import Any

from .correlation import (
    bind_correlation_id,
    current_context,
    get_correlation_id_or_none,
    reset_correlation_id,
)
from .tracing import get_tracer


def _blocking_step(payload: str) -> str:
    """模拟在线程池中执行的阻塞工作。"""

    time.sleep(0.01)
    return f"processed:{payload}"


async def background_observation(
    correlation_id: str,
    payload: str,
    logger: logging.Logger,
) -> dict[str, Any]:
    """后台处理入口。

    - 显式以触发请求的 ``correlation_id`` 绑定上下文，即便调用方在
      复制的上下文之外触发也不丢失；
    - 创建 ``background_job`` 追踪片段，父子关系挂在请求片段之下；
    - 返回结构化结果，异常向上传播由调用方决定记录方式（不静默吞掉）。
    """

    tracer = get_tracer()
    async with tracer.async_span(
        "background_job",
        attributes={"component": "background", "payload_kind": payload},
    ) as span:
        token = bind_correlation_id(correlation_id)
        started = time.perf_counter()
        try:
            logger.info(
                "background.start",
                extra={
                    "fields": {
                        "component": "background",
                        "expected_correlation_id": correlation_id,
                    }
                },
            )

            # 1) 线程池执行阻塞工作：显式传入当前上下文副本
            ctx: contextvars.Context = current_context()
            loop = asyncio.get_running_loop()
            processed = await loop.run_in_executor(
                None, ctx.run, _blocking_step, payload
            )

            # 2) 再嵌套一个异步子片段，验证后台链路中的父子关系
            async with tracer.async_span(
                "background_job.notify", attributes={"component": "background"}
            ):
                await asyncio.sleep(0)

            observed = get_correlation_id_or_none()
            elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
            span.set_attribute("background.elapsed_ms", elapsed_ms)

            # 若观察到的标识与触发请求不一致，属于上下文错配，显式标记
            context_ok = observed == correlation_id
            span.set_attribute("background.context_ok", context_ok)
            if not context_ok:
                raise RuntimeError("后台任务上下文中的关联标识与触发请求不一致")

            logger.info(
                "background.done",
                extra={
                    "fields": {
                        "component": "background",
                        "elapsed_ms": elapsed_ms,
                        "context_ok": context_ok,
                    }
                },
            )
            return {
                "correlation_id": observed,
                "processed": processed,
                "elapsed_ms": elapsed_ms,
                "context_ok": context_ok,
            }
        finally:
            reset_correlation_id(token)
