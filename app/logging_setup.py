"""结构化 JSON 日志。

- 每条日志自动携带当前上下文的 ``correlation_id``（未设置时为 ``null``）。
- 通过 :func:`log_event` 传入的字段平铺输出；异常只记录类型与安全摘要，
  不记录可能含敏感信息的 ``repr``。
- :func:`configure_logging` 幂等，重复装配不会产生重复 handler / 重复日志行。
"""

from __future__ import annotations

import contextvars
import datetime as dt
import json
import logging
from typing import Any

from app.correlation import get_correlation_id

CORRELATION_FIELD = "correlation_id"
EVENT_FIELD = "event"
_RESERVED = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "taskName",
}


def _json_default(value: Any) -> str:
    """兜底序列化：不抛异常、不输出敏感对象原文。"""
    if isinstance(value, BaseException):
        return f"{type(value).__name__}: {value}"
    return f"<{type(value).__name__}>"


class JsonFormatter(logging.Formatter):
    """把日志记录渲染为单行 JSON。"""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            CORRELATION_FIELD: getattr(record, CORRELATION_FIELD, None),
            EVENT_FIELD: getattr(record, EVENT_FIELD, None),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED or key in payload or key.startswith("_"):
                continue
            if key == "exc_info" and value:
                continue
            try:
                json.dumps(value, default=_json_default)
                payload[key] = value
            except (TypeError, ValueError):
                payload[key] = _json_default(value)
        if record.exc_info:
            exc_type, exc, _ = record.exc_info
            payload["exception"] = {
                "type": exc_type.__name__ if exc_type else None,
                "message": str(exc) if exc is not None else None,
            }
        return json.dumps(payload, ensure_ascii=False, default=_json_default)


class CorrelationFilter(logging.Filter):
    """为每条日志补充当前上下文关联标识。"""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, CORRELATION_FIELD):
            setattr(record, CORRELATION_FIELD, get_correlation_id())
        return True


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """配置根日志器（幂等）。"""
    root = logging.getLogger()
    if any(getattr(h, "_obs_json_handler", False) for h in root.handlers):
        root.setLevel(level)
        return root
    handler = logging.StreamHandler()
    handler._obs_json_handler = True  # type: ignore[attr-defined]
    handler.setFormatter(JsonFormatter())
    handler.addFilter(CorrelationFilter())
    root.addHandler(handler)
    root.setLevel(level)
    return root


def get_logger(name: str) -> logging.Logger:
    """获取命名日志器。"""
    return logging.getLogger(name)


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    """以结构化字段记录一个事件。"""
    safe: dict[str, Any] = {}
    for key, value in fields.items():
        if isinstance(value, BaseException):
            safe[key] = {"type": type(value).__name__, "message": str(value)}
        else:
            safe[key] = value
    logger.log(level, event, extra={EVENT_FIELD: event, **safe})
