"""运行期可观测性配置。

所有配置均来自环境变量，统一前缀 ``OBS_``；取值不合法时在启动阶段
快速失败（fail-fast），错误信息只包含配置项名与取值类别，不回显敏感内容。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

ENV_PREFIX: Final[str] = "OBS_"

#: 支持的追踪导出方式
EXPORT_KINDS: Final[frozenset[str]] = frozenset({"file", "console", "none"})


@dataclass(frozen=True, slots=True)
class Settings:
    """关联标识、日志、指标、追踪相关配置。

    属性:
        correlation_header: 读取/回写关联标识的 HTTP 头名称（小写比较）。
        correlation_length_max: 客户端关联标识允许的最大长度。
        log_json: 日志是否使用单行 JSON 格式。
        log_level: 日志级别名称（DEBUG/INFO/...）。
        metrics_snapshot_path: 指标快照本地文件路径（空串表示不落盘）。
        trace_sample_rate: 追踪采样比例，区间 [0, 1]；1 表示全采样。
        trace_export: 追踪导出方式 file/console/none。
        trace_file_path: file 导出方式下的 JSONL 文件路径。
    """

    correlation_header: str
    correlation_length_max: int
    log_json: bool
    log_level: str
    metrics_snapshot_path: str
    trace_sample_rate: float
    trace_export: str
    trace_file_path: str


def _env(name: str, default: str) -> str:
    return os.environ.get(f"{ENV_PREFIX}{name}", default)


def _parse_bool(name: str, default: bool) -> bool:
    raw = _env(name, "true" if default else "false").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        f"配置项 {ENV_PREFIX}{name} 必须是布尔值(true/false)，实际取值无法识别"
    )


def _parse_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = _env(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"配置项 {ENV_PREFIX}{name} 必须是整数，实际取值无法识别"
        ) from exc
    if not minimum <= value <= maximum:
        raise ValueError(
            f"配置项 {ENV_PREFIX}{name} 必须位于区间 [{minimum}, {maximum}]"
        )
    return value


def _parse_float(name: str, default: float, minimum: float, maximum: float) -> float:
    raw = _env(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(
            f"配置项 {ENV_PREFIX}{name} 必须是浮点数，实际取值无法识别"
        ) from exc
    if not minimum <= value <= maximum:
        raise ValueError(
            f"配置项 {ENV_PREFIX}{name} 必须位于区间 [{minimum}, {maximum}]"
        )
    return value


def load_settings() -> Settings:
    """从环境变量加载配置，取值非法时抛出 ``ValueError``。"""

    header = _env("CORRELATION_HEADER", "X-Correlation-ID").strip()
    if not header:
        raise ValueError(f"配置项 {ENV_PREFIX}CORRELATION_HEADER 不能为空")

    length_max = _parse_int("CORRELATION_LENGTH_MAX", 128, 16, 1024)
    log_json = _parse_bool("LOG_JSON", True)

    level = _env("LOG_LEVEL", "INFO").strip().upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ValueError(
            f"配置项 {ENV_PREFIX}LOG_LEVEL 必须是 DEBUG/INFO/WARNING/ERROR/CRITICAL 之一"
        )

    sample_rate = _parse_float("TRACE_SAMPLE_RATE", 1.0, 0.0, 1.0)
    export_kind = _env("TRACE_EXPORT", "file").strip().lower()
    if export_kind not in EXPORT_KINDS:
        raise ValueError(
            f"配置项 {ENV_PREFIX}TRACE_EXPORT 必须是 {sorted(EXPORT_KINDS)} 之一"
        )

    file_path = _env("TRACE_FILE_PATH", "logs/traces.jsonl")
    metrics_path = _env("METRICS_SNAPSHOT_PATH", "logs/metrics-snapshot.json")
    if export_kind == "file" and not file_path.strip():
        raise ValueError(
            f"配置项 {ENV_PREFIX}TRACE_FILE_PATH 在 file 导出方式下不能为空"
        )

    return Settings(
        correlation_header=header,
        correlation_length_max=length_max,
        log_json=log_json,
        log_level=level,
        metrics_snapshot_path=metrics_path,
        trace_sample_rate=sample_rate,
        trace_export=export_kind,
        trace_file_path=file_path,
    )
