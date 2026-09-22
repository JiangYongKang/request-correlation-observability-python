"""演示各条请求链路的路由。

每条链路的日志都显式打印:
- 输入参数；
- 当前上下文解析到的关联标识（以及它是否与入参一致）；
- 判定依据（例如是否采样、后台执行是否成功）。
"""

from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator

from fastapi import APIRouter, BackgroundTasks, Query
from fastapi.responses import StreamingResponse

from ..correlation import get_correlation_id
from ..tasks import background_observation
from ..tracing import get_tracer

router = APIRouter()
logger = logging.getLogger("app")


@router.get("/")
async def read_root() -> dict:
    """正常链路：返回当前关联标识。"""

    cid = get_correlation_id()
    logger.info(
        "route.root",
        extra={"fields": {"input": "<none>", "correlation_id": cid, "decision": "ok"}},
    )
    return {"correlation_id": cid, "message": "ok"}


@router.get("/echo")
async def echo(value: str = Query("", max_length=256)) -> dict:
    """回显链路：打印输入、关联标识与判定依据。"""

    cid = get_correlation_id()
    tracer = get_tracer()
    async with tracer.async_span(
        "echo.process", attributes={"input_length": len(value)}
    ) as span:
        decision = "empty_input_defaulted" if value == "" else "echoed"
        span.set_attribute("decision", decision)
        logger.info(
            "route.echo",
            extra={
                "fields": {
                    "input": value,
                    "correlation_id": cid,
                    "decision": decision,
                    "sampled": span.sampled,
                }
            },
        )
        return {
            "correlation_id": cid,
            "echo": value,
            "decision": decision,
        }


@router.get("/error")
async def raise_error(kind: str = Query("value", max_length=32)) -> dict:
    """异常链路：抛出不同类型异常，验证追踪标记与脱敏响应。"""

    cid = get_correlation_id()
    logger.info(
        "route.error",
        extra={
            "fields": {
                "input": kind,
                "correlation_id": cid,
                "decision": f"raise:{kind}",
            }
        },
    )
    if kind == "value":
        # 不含真实敏感数据；脱敏逻辑由单测单独构造
        raise ValueError("演示用业务参数错误")
    if kind == "timeout":
        raise TimeoutError("演示用上游超时")
    if kind == "business":
        from fastapi import HTTPException

        # 受控业务错误：使用 HTTPException，状态语义透传、详情白名单化
        raise HTTPException(status_code=400, detail="invalid kind parameter")
    if kind == "unavailable":
        from fastapi import HTTPException

        raise HTTPException(status_code=503, detail="dependency unavailable")
    # kind=value 或其他：非受控异常，统一映射为 internal_error(500)
    raise RuntimeError("演示用未预期异常")


@router.get("/background")
async def run_background(
    background_tasks: BackgroundTasks,
    kind: str = Query("ok", max_length=32),
) -> dict:
    """后台任务链路：响应先返回，任务在同一上下文/追踪树下执行。"""

    cid = get_correlation_id()
    payload = "demo-payload"
    logger.info(
        "route.background.schedule",
        extra={
            "fields": {
                "input": kind,
                "correlation_id": cid,
                "decision": "scheduled_background_task",
            }
        },
    )

    async def _job() -> None:
        try:
            result = await background_observation(cid, payload, logger)
            logger.info(
                "route.background.result",
                extra={"fields": {"correlation_id": cid, "result": result}},
            )
        except Exception as exc:  # noqa: BLE001 - 后台失败要可解释但不影响已返回的响应
            logger.error(
                "route.background.failed",
                exc_info=exc,
                extra={
                    "fields": {
                        "correlation_id": cid,
                        "error_type": type(exc).__name__,
                    }
                },
            )

    background_tasks.add_task(_job)
    return {
        "correlation_id": cid,
        "scheduled": True,
        "decision": "scheduled_background_task",
    }


@router.get("/stream")
async def stream_events(count: int = Query(3, ge=1, le=20)) -> StreamingResponse:
    """流式响应链路：每一帧都处于同一关联上下文，整体包裹在追踪片段中。"""

    cid = get_correlation_id()
    tracer = get_tracer()
    logger.info(
        "route.stream.start",
        extra={
            "fields": {
                "input": count,
                "correlation_id": cid,
                "decision": f"will_emit_{count}_frames",
            }
        },
    )

    async def event_stream() -> AsyncIterator[bytes]:
        async with tracer.async_span(
            "stream.produce", attributes={"frame_count": count}
        ) as span:
            for index in range(count):
                frame_cid = get_correlation_id()
                same = frame_cid == cid
                span.set_attribute(f"frame_{index}_context_ok", same)
                logger.info(
                    "route.stream.frame",
                    extra={
                        "fields": {
                            "frame": index,
                            "correlation_id": frame_cid,
                            "decision": "context_match" if same else "CONTEXT_MISMATCH",
                        }
                    },
                )
                yield (
                    f"event: frame\n"
                    f'id: {index}\n'
                    f'data: {{"frame":{index},"correlation_id":"{frame_cid}","same":{str(same).lower()}}}\n\n'
                ).encode("utf-8")
                await asyncio.sleep(0)
            span.set_attribute("stream.completed", True)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
    )
