"""接口链路测试：正常/异常/后台/流式下的关联标识一致性与拒绝原因。"""

from __future__ import annotations

import re

import httpx
import pytest

HEADER = "X-Correlation-ID"


async def test_generated_correlation_id_is_returned_in_body_and_header(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/")
    body = response.json()
    cid = response.headers[HEADER]
    print(
        f"[输入] 无关联标识头 [关联标识] {cid} "
        f"[判定依据] 服务端生成 32 位 hex，body/header 一致"
    )
    assert response.status_code == 200
    assert re.fullmatch(r"[0-9a-f]{32}", cid)
    assert body["correlation_id"] == cid


async def test_supplied_correlation_id_is_accepted_and_echoed(
    client: httpx.AsyncClient,
) -> None:
    cid = "client-supplied-2026.09_22"
    response = await client.get("/echo?value=ping", headers={HEADER: cid})
    body = response.json()
    print(
        f"[输入] value=ping, header={cid} [关联标识] {response.headers[HEADER]} "
        f"[判定依据] 原样透传且进入业务上下文"
    )
    assert response.status_code == 200
    assert response.headers[HEADER] == cid
    assert body["correlation_id"] == cid
    assert body["echo"] == "ping"


async def test_blank_header_is_treated_as_missing(client: httpx.AsyncClient) -> None:
    response = await client.get("/", headers={HEADER: "   "})
    print(
        f"[输入] 空白头 [关联标识] {response.headers[HEADER]} "
        f"[判定依据] 等同缺失 -> 生成而非拒绝"
    )
    assert response.status_code == 200
    assert re.fullmatch(r"[0-9a-f]{32}", response.headers[HEADER])


@pytest.mark.parametrize(
    ("bad_value", "expected_reason"),
    [
        ("has space", "invalid_correlation_id_format"),
        ("a;b", "invalid_correlation_id_format"),
        ("../x", "invalid_correlation_id_format"),
        ("-dash", "invalid_correlation_id_format"),
        ("x" * 129, "invalid_correlation_id_length"),
        ("line\nbreak", "invalid_correlation_id_format"),
    ],
)
async def test_illegal_correlation_id_rejected_with_distinct_reason(
    client: httpx.AsyncClient, bad_value: str, expected_reason: str
) -> None:
    response = await client.get("/", headers={HEADER: bad_value})
    body = response.json()
    print(
        f"[输入] {bad_value!r} [关联标识响应头] {response.headers.get(HEADER)} "
        f"[判定依据] 400 + code={body['error']['code']}，原因可区分"
    )
    assert response.status_code == 400
    assert body["error"]["code"] == expected_reason
    assert body["error"]["correlation_id"] is None
    # 拒绝响应不得回显客户端提供的原值
    assert bad_value.strip()[:8] not in response.text


async def test_exception_path_returns_safe_body_and_same_cid(
    client: httpx.AsyncClient,
) -> None:
    cid = "err-trace-cid"
    response = await client.get("/error?kind=value", headers={HEADER: cid})
    body = response.json()
    print(
        f"[输入] kind=value [关联标识] {cid} "
        f"[判定依据] 非受控异常 -> 500 internal_error，消息不外泄"
    )
    assert response.status_code == 500
    assert body["error"]["code"] == "internal_error"
    assert body["error"]["correlation_id"] == cid
    assert response.headers[HEADER] == cid
    assert "演示" not in response.text  # 内部异常文本不得透出


async def test_timeout_and_http_exceptions_keep_distinct_codes(
    client: httpx.AsyncClient,
) -> None:
    r1 = await client.get("/error?kind=timeout")
    r2 = await client.get("/error?kind=business")
    r3 = await client.get("/error?kind=unavailable")
    print(
        f"[判定依据] timeout={r1.json()['error']['code']}, "
        f"business={r2.json()['error']['code']}, "
        f"unavailable={r3.json()['error']['code']} 三者可区分"
    )
    assert (r1.status_code, r1.json()["error"]["code"]) == (504, "upstream_timeout")
    assert (r2.status_code, r2.json()["error"]["code"]) == (400, "http_error")
    assert (r3.status_code, r3.json()["error"]["code"]) == (503, "http_error")


async def test_server_error_response_marks_root_span_failed(
    client: httpx.AsyncClient, trace_rows
) -> None:
    response = await client.get(
        "/error?kind=unavailable", headers={HEADER: "span-503-cid"}
    )
    assert response.status_code == 503
    rows = trace_rows()
    request_row = next(r for r in rows if r["name"] == "http.request GET")
    print(
        f"[判定依据] 受控 503 响应的根片段 status={request_row['status']}，"
        f"error_type={request_row['error_type']}，追踪结论与真实状态一致"
    )
    assert request_row["status"] == "error"
    assert request_row["error_type"] == "HttpServerError"
    assert request_row["attributes"]["http.status_code"] == 503


async def test_background_task_inherits_request_correlation_id(
    client: httpx.AsyncClient, trace_rows
) -> None:
    cid = "bg-cid-001"
    response = await client.get("/background?kind=ok", headers={HEADER: cid})
    print(
        f"[输入] kind=ok [关联标识] {cid} "
        f"[判定依据] 后台任务在响应后执行，需沿用同一标识与 trace 树"
    )
    assert response.status_code == 200
    assert response.json()["correlation_id"] == cid

    rows = trace_rows()
    bg_rows = [r for r in rows if r["name"].startswith("background_job")]
    assert len(bg_rows) == 2  # 任务主片段 + notify 子片段
    request_row = next(r for r in rows if r["name"] == "http.request GET")
    for row in bg_rows:
        assert row["correlation_id"] == cid
        assert row["trace_id"] == request_row["trace_id"]
        assert row["status"] == "ok"
    # 后台主片段是请求根片段的子片段，notify 是主片段的子片段
    main_bg = next(r for r in bg_rows if r["name"] == "background_job")
    notify_bg = next(r for r in bg_rows if r["name"] == "background_job.notify")
    assert main_bg["parent_id"] == request_row["span_id"]
    assert notify_bg["parent_id"] == main_bg["span_id"]
    assert main_bg["attributes"]["background.context_ok"] is True


async def test_stream_keeps_same_correlation_id_in_every_frame(
    client: httpx.AsyncClient,
) -> None:
    cid = "stream-cid-007"
    async with client.stream(
        "GET", "/stream?count=4", headers={HEADER: cid}
    ) as response:
        assert response.status_code == 200
        assert response.headers[HEADER] == cid
        chunks: list[str] = []
        async for chunk in response.aiter_text():
            chunks.append(chunk)
    body = "".join(chunks)
    print(f"[输入] count=4 [关联标识] {cid} [判定依据] 4 帧全部携带同一标识")
    frames = re.findall(r'data: (\{"frame".*?\})', body)
    assert len(frames) == 4
    assert body.count(cid) == 4
    assert '"same":true' in body
    assert "CONTEXT_MISMATCH" not in body


async def test_unknown_route_is_normalized_and_carries_cid(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/definitely-not-exists")
    print(
        f"[关联标识] {response.headers[HEADER]} [判定依据] 404 仍回写标识"
    )
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "http_error"
    assert response.headers[HEADER]
