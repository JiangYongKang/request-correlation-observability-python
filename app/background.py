"""后台任务：显式继承调用方的关联标识与追踪上下文。

两条边界都覆盖：
- 同步函数被丢进线程池 / 新线程时：用 :func:`contextvars.copy_context`
  捕获调用方上下文，在捕获的上下文中 ``run``；
- 协程被 ``create_task`` 排期时：把捕获上下文作为 ``context`` 传入，
  避免新任务落到空上下文导致关联标识丢失、追踪片段错配。

片段以 ``background`` 类型挂在请求根片段之下，``trace_id`` 与父子关系
与真实调用链一致；后台函数抛出的异常会在片段上记录 ERROR 后继续向上抛。
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from app.correlation import correlation_context
from app.tracing import SPAN_KIND_BACKGROUND, Tracer, current_span

T = TypeVar("T")


def capture_request_context() -> contextvars.Context:
    """捕获调用方当前上下文（含关联标识与追踪栈）。"""
    return contextvars.copy_context()


def bind_context(
    func: Callable[..., Any],
    correlation_id: str,
    tracer: Tracer | None = None,
    *,
    span_name: str | None = None,
) -> Callable[..., Any]:
    """包装函数，使其在后台线程/任务中运行时仍持有同一上下文。"""
    ctx = capture_request_context()
    name = span_name or f"background:{getattr(func, '__name__', 'task')}"

    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def awrapper(*args: Any, **kwargs: Any) -> Any:
            async def body() -> Any:
                with correlation_context(correlation_id):
                    if tracer is not None:
                        try:
                            with tracer.span(name, kind=SPAN_KIND_BACKGROUND):
                                return await func(*args, **kwargs)
                        finally:
                            # 后台任务可能晚于请求结束：片段结束即异步入队导出，
                            # 避免请求级导出先于后台片段完成而漏掉它；
                            # 刷盘由导出器后台周期完成，不在此同步等待。
                            tracer.export_finished()
                    return await func(*args, **kwargs)

            # 在捕获的调用方上下文中排期，追踪父栈随上下文带过去
            child = asyncio.create_task(body(), context=ctx)
            return await child

        awrapper.__bound_context__ = ctx  # type: ignore[attr-defined]
        return awrapper

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        def body() -> Any:
            with correlation_context(correlation_id):
                if tracer is not None:
                    try:
                        with tracer.span(name, kind=SPAN_KIND_BACKGROUND):
                            return func(*args, **kwargs)
                    finally:
                        tracer.export_finished()
                return func(*args, **kwargs)

        return ctx.run(body)

    wrapper.__bound_context__ = ctx  # type: ignore[attr-defined]
    return wrapper


def run_background_async(
    func: Callable[..., Awaitable[T]],
    *args: Any,
    correlation_id: str,
    tracer: Tracer | None = None,
    span_name: str | None = None,
) -> asyncio.Task[T]:
    """以捕获上下文排期一个后台协程（fire-and-forget 但可观测、可追踪）。"""
    bound = bind_context(func, correlation_id, tracer, span_name=span_name)
    return asyncio.create_task(bound(*args))
