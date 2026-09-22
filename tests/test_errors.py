"""异常路径安全化处理测试。"""

from __future__ import annotations

import pytest

from app.correlation import InvalidCorrelationIdError
from app.errors import (
    CODE_BAD_REQUEST,
    CODE_INTERNAL,
    CODE_NOT_FOUND,
    CODE_RATE_LIMITED,
    CODE_TIMEOUT,
    build_safe_view,
    classify_exception,
    safe_message,
)


class _HttpLike(Exception):
    def __init__(self, status_code: int, msg: str = ""):
        super().__init__(msg)
        self.status_code = status_code


@pytest.mark.parametrize(
    "exc,code,status",
    [
        (ValueError("x"), CODE_BAD_REQUEST, 400),
        (LookupError("x"), CODE_NOT_FOUND, 404),
        (TimeoutError("slow"), CODE_TIMEOUT, 504),
        (_HttpLike(429), CODE_RATE_LIMITED, 429),
        (_HttpLike(422), CODE_BAD_REQUEST, 400),
        (_HttpLike(503), CODE_INTERNAL, 500),
        (RuntimeError("boom"), CODE_INTERNAL, 500),
    ],
)
def test_classification_bounded(exc, code, status):
    view = build_safe_view(exc, "cid-z")
    print(f"输入={type(exc).__name__}: {exc} 关联标识=cid-z 判定={view.code}/{view.status_code}")
    assert view.code == code
    assert view.status_code == status
    assert view.correlation_id == "cid-z"


def test_internal_error_hides_details():
    exc = RuntimeError("psycopg2 password=hunter2 host=10.0.0.5 failed")
    view = build_safe_view(exc, "cid-secret")
    print(f"输入=含敏感信息内部异常 判定=只回通用文案: {view.message!r}")
    assert view.status_code == 500
    body = view.message
    assert "hunter2" not in body and "10.0.0.5" not in body and "psycopg2" not in body
    assert "cid-secret" in view.correlation_id


def test_sensitive_keyword_hidden_even_for_value_error():
    msg = safe_message(ValueError("invalid token abcdef"))
    print(f"输入=ValueError 含 token 判定=脱敏文案: {msg!r}")
    assert "abcdef" not in msg


def test_invalid_correlation_keeps_reason_not_raw_value():
    exc = InvalidCorrelationIdError("a b", "illegal_character", "包含不允许的字符")
    view = build_safe_view(exc, "cid-q")
    print(f"输入='a b' 判定=reason 保留但不回显原值: reason={view.reason}")
    assert view.reason == "illegal_character"
    assert "a b" not in view.message
    assert view.status_code == 400


# ---------------------------------------------------------------- 集成测试
def test_boom_endpoint_returns_safe_500(client):
    r = client.get("/boom")
    body = r.json()["error"]
    print(
        f"输入=GET /boom 关联标识={r.headers.get('X-Correlation-ID')} "
        f"判定=500 安全视图 code={body['code']}"
    )
    assert r.status_code == 500
    assert body["code"] == "internal_error"
    assert "hunter2" not in r.text and "password" not in r.text
    assert body["correlation_id"] == r.headers["X-Correlation-ID"]


def test_bad_request_endpoint(client):
    r = client.get("/bad-request")
    body = r.json()["error"]
    print(f"输入=GET /bad-request 判定=400 code={body['code']} msg={body['message']}")
    assert r.status_code == 400
    assert body["code"] == "bad_request"
    assert body["message"] == "缺少必填参数 n"


def test_boom_marks_root_span_error(client, span_exporter):
    client.get("/boom")
    server = [s for s in span_exporter.finished_spans() if s.kind == "server"][0]
    print(
        f"输入=GET /boom 判定=根片段 status={server.status} "
        f"error_type={server.error_type} 错误消息={server.error_message!r}"
    )
    assert server.status == "ERROR"
    assert server.error_type == "RuntimeError"
    # 片段上保留的是安全化文案，不含敏感细节
    assert "hunter2" not in (server.error_message or "")
