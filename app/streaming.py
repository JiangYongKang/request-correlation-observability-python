"""流式响应：整段流共用同一关联标识与追踪片段。

为什么需要专门处理：``StreamingResponse`` 的 body 在路由返回之后才被
服务端迭代，中间件的 ``with`` 块此时可能已经退出。此处：

- 在捕获的 :class:`contextvars.Context` 副本中完成绑定与迭代，块内代码
  与日志看到同一关联标识；对上下文变量的赋值只存在于该副本，不污染
  其他并发请求；
- ``stream`` 片段继承请求根片段的 ``trace_id`` / ``parent_id``；
- 正常结束标记 ``OK``；生产者异常标记 ``ERROR`` 并保留原因；
  消费端提前断连（``GeneratorExit``）标记 ``ERROR``/``CancelledError``；
- 片段结束后立即导出刷盘，避免响应结束、进程退出时流式片段丢失。
"""

from __future__ import annotations

import contextvars
import inspect
from collections.abc import AsyncIterable, AsyncIterator, Callable

from app.correlation import reset_correlation_id, set_correlation_id
from app.tracing import SPAN_KIND_STREAM, Tracer, Span, current_span, _stack_var

Producer = Callable[[], AsyncIterator[bytes]]


def _as_async_iterable(source: Producer | AsyncIterable[bytes]) -> AsyncIterable[bytes]:
    """同时接受生产者函数与已创建的异步生成器/可异步迭代对象。"""
    if inspect.isasyncgen(source) or hasattr(source, "__aiter__"):
        return source  # type: ignore[return-value]
    return source()  # type: ignore[operator]


async def empty_producer() -> AsyncIterator[bytes]:
    """测试用空生产者。"""
    return
    yield b""  # pragma: no cover


async def instrumented_stream(
    source: Producer | AsyncIterable[bytes],
    *,
    correlation_id: str,
    tracer: Tracer | None = None,
    span_name: str = "stream",
) -> AsyncIterator[bytes]:
    """为异步生产者的每一块与异常/收尾附加同一关联标识与追踪片段。"""
    import asyncio

    captured = contextvars.copy_context()
    parent = current_span()
    queue: asyncio.Queue[tuple[str, object]] = asyncio.Queue()
    state: dict[str, Span | None] = {"span": None}

    async def run_in_context() -> None:
        token_cid = set_correlation_id(correlation_id)
        stream_span: Span | None = None
        token_stack = None
        if tracer is not None:
            stream_span = tracer.start_span(
                span_name,
                kind=SPAN_KIND_STREAM,
                trace_id=parent.trace_id if parent is not None else None,
                parent_id=parent.span_id if parent is not None else None,
                correlation_id=correlation_id,
            )
            state["span"] = stream_span
            token_stack = _stack_var.set(_stack_var.get() + (stream_span,))
        chunks = 0
        try:
            async for chunk in _as_async_iterable(source):
                chunks += 1
                if stream_span is not None:
                    stream_span.set_attribute("stream.chunks", chunks)
                queue.put_nowait(("chunk", chunk))
        except BaseException as exc:  # 含 CancelledError/GeneratorExit
            if stream_span is not None:
                tracer.end_span(  # type: ignore[union-attr]
                    stream_span,
                    status="ERROR",
                    error_type=type(exc).__name__,
                    error_message=str(exc) or repr(exc),
                    export=True,
                )
            queue.put_nowait(("error", exc))
        else:
            if stream_span is not None:
                stream_span.set_attribute("stream.completed", True)
                tracer.end_span(stream_span, status="OK", export=True)  # type: ignore[union-attr]
            queue.put_nowait(("done", None))
        finally:
            if token_stack is not None:
                _stack_var.reset(token_stack)
            reset_correlation_id(token_cid)

    task = asyncio.create_task(captured.run(run_in_context))

    try:
        while True:
            kind, value = await queue.get()
            if kind == "chunk":
                yield value  # type: ignore[misc]
            elif kind == "error":
                raise value  # type: ignore[misc]
            else:
                await task  # 传播 run_in_context 自身可能产生的异常
                return
    except GeneratorExit:
        # 消费端提前关闭：取消内部迭代任务；run_in_context 的 except 分支
        # 会把片段标记为 CancelledError 并导出。
        task.cancel()
        try:
            await task
        except BaseException:
            pass
        raise
