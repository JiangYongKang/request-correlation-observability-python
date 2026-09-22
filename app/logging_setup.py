"""结构化日志。

- 每条日志自动携带 ``correlation_id`` 请求维度字段；
- JSON 模式输出单行 JSON，避免多行伪造日志条目（对值中的控制字符做转义）；
- 字段名命中敏感词（token/password/secret/authorization 等）时脱敏，
  异常路径只记录异常类型与安全化后的消息，不直接透出依赖库内部对象。
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import re
from typing import Any

from .correlation import get_correlation_id_or_none

#: 命中这些子串（小写匹配）的字段名不记录原值
_SENSITIVE_KEY_PARTS: frozenset[str] = frozenset(
    {
        "token", "password", "passwd", "secret", "authorization", "cookie",
        "apikey", "api_key",
    }
)
_REDACTED = "***redacted***"
#: 形如 password=xxx / token: xxx / authorization "xxx" 的密钥模式
_SECRET_PATTERN = re.compile(
    r"(?i)(password|passwd|secret|token|authorization|api[_-]?key)"
    r"(\s*[:=]\s*|\"|:\s*\")([^\s\"',;]+)"
)
#: 允许直接出现在日志里的结构化字段名（标签取值有界，避免键名膨胀）
_RESERVED_RECORD_ATTRS: frozenset[str] = frozenset(
    {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "taskName", "asctime", "message",
    }
)


def _is_sensitive_key(key: str) -> bool:
    # 归一化分隔符后做子串匹配：x-api-key / x.api_key / XApiKey 均可命中
    lowered = key.lower().replace("-", "").replace("_", "").replace(".", "")
    return any(part.replace("_", "") in lowered for part in _SENSITIVE_KEY_PARTS)


def _mask_secret_patterns(text: str) -> str:
    """把文本中的密钥字面量（如 ``password=hunter2``）替换为脱敏值。"""

    return _SECRET_PATTERN.sub(r"\1\2" + _REDACTED, text)


def _clean_text(value: str) -> str:
    """转义 CR/LF/Tab 防日志注入，并脱敏内嵌密钥。"""

    value = _mask_secret_patterns(value)
    return (
        value.replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def sanitize_value(key: str, value: Any) -> Any:
    """敏感键脱敏；非标量值收敛为有界的字符串表示。"""

    if _is_sensitive_key(key):
        return _REDACTED
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        return _clean_text(value)[:500]
    if isinstance(value, (list, tuple)):
        return [sanitize_value(key, item) for item in value[:10]]
    return _clean_text(repr(value))[:200]


class CorrelationFilter(logging.Filter):
    """为日志记录补充当前上下文的关联标识。"""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = get_correlation_id_or_none() or "-"
        return True


class JsonFormatter(logging.Formatter):
    """单行 JSON 格式化器。

    通过 ``extra={"fields": {...}}`` 传递的结构化字段会被安全化后输出。
    """

    def format(self, record: logging.LogRecord) -> str:
        fields: dict[str, Any] = dict(getattr(record, "fields", {}) or {})

        payload: dict[str, Any] = {
            "timestamp": _dt.datetime.fromtimestamp(
                record.created, tz=_dt.timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
            "correlation_id": getattr(
                record, "correlation_id", None
            )
            or get_correlation_id_or_none()
            or "-",
        }

        for key, value in fields.items():
            payload[key] = sanitize_value(key, value)

        if record.exc_info:
            exc_type, exc_value, _ = record.exc_info
            payload["error"] = {
                # 只透出异常类型名与经过安全化的消息，不透出完整堆栈对象
                "type": exc_type.__name__ if exc_type else "Unknown",
                "message": sanitize_value("error_message", str(exc_value))[:300],
            }
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


class TextFormatter(logging.Formatter):
    """便于本地阅读的键值文本格式。"""

    def format(self, record: logging.LogRecord) -> str:
        record.correlation_id = getattr(record, "correlation_id", "-")
        base = (
            f"{self.formatTime(record, '%Y-%m-%dT%H:%M:%S')} "
            f"{record.levelname:<7} [{record.correlation_id}] "
            f"{record.name}: {record.getMessage()}"
        )
        fields = getattr(record, "fields", None)
        if fields:
            safe = {k: sanitize_value(k, v) for k, v in fields.items()}
            base = f"{base} {safe}"
        return base


_CONFIGURED: set[str] = set()


def configure_logging(json_logs: bool, level: str) -> logging.Logger:
    """配置应用日志器（幂等，重复调用不会叠加 handler）。"""

    logger = logging.getLogger("app")
    logger.setLevel(level)
    logger.propagate = False

    if "configured" in _CONFIGURED:
        return logger

    handler = logging.StreamHandler()
    handler.addFilter(CorrelationFilter())
    formatter: logging.Formatter = (
        JsonFormatter() if json_logs else TextFormatter()
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    _CONFIGURED.add("configured")
    return logger


def log_event(
    logger: logging.Logger, level: int, event: str, **fields: Any
) -> None:
    """以结构化字段记录一条事件日志。"""

    logger.log(level, event, extra={"fields": fields}, stacklevel=2)
