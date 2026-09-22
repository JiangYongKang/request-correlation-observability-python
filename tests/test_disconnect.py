"""客户端断连（CancelledError）路径测试。

直接以 ASGI 原语模拟：流式响应吐出第一帧后，请求任务被外部取消，
等价于客户端中途断开。要求：
- CancelledError 正常向上传播，不被吞掉；
- 指标恰好记录一次（499），不得重复计数也不得漏计；
- 追踪根片段被标记 error。
"""

from __future__ import annotations

import asyncio

from app.metrics import get_registry

HEADER = "X-Correlation-ID"


async def test_disconnect_during_stream_records_exactly_once(
    app, trace_rows
) -> None:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/stream",
        "headers": [(b"x-correlation-id", b"disc-cid-1")],
        "query_string": b"count=20",
        "state": {},
    }
    holder: dict = {"frames": 0, "task": None}

    async def receive() -> dict:
        await asyncio.sleep(3600)  # 连接挂起：不主动产生 disconnect 事件
        return {"type": "http.disconnect"}

    async def send(message: dict) -> None:
        if message["type"] == "http.response.body" and message.get("more_body"):
            if holder["frames"] == 0:
                # 在第一帧发出后、于下一个调度点外部取消请求任务
                asyncio.get_running_loop().call_soon(holder["task"].cancel)
            holder["frames"] += 1

    async def run_request() -> None:
        await app(scope, receive, send)

    request_task = asyncio.create_task(run_request())
    holder["task"] = request_task
    try:
        await asyncio.wait_for(request_task, timeout=5)
        completed = True
    except asyncio.CancelledError:
        completed = False
    except asyncio.TimeoutError:  # pragma: no cover - 防止挂死
        request_task.cancel()
        raise AssertionError("断连时请求处理挂死")

    print(
        f"[输入] 流式第一帧后取消 [判定依据] "
        f"正常完成={completed}（应为 False），取消已传播"
    )
    assert completed is False

    await asyncio.sleep(0.02)  # 等待 span 导出
    snap = get_registry().snapshot()
    status_classes = [label.split("|")[2] for label in snap["series"]]
    print(
        f"[判定依据] requests={snap['totals']['requests']} "
        f"errors={snap['totals']['errors']} 状态列={status_classes}"
    )
    assert snap["totals"]["requests"] == 1
    assert snap["totals"]["errors"] == 1
    assert status_classes == ["4xx"]  # 499 -> 4xx

    rows = trace_rows()
    request_row = next(r for r in rows if r["name"] == "http.request GET")
    print(f"[判定依据] 根追踪片段 status={request_row['status']}")
    assert request_row["status"] == "error"
    assert request_row["correlation_id"] == "disc-cid-1"
