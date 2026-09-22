"""配置项：关联标识规则、采样、导出与指标标签策略。

所有配置均有默认值，默认零外部依赖：
追踪片段默认追加写入本地 JSONL 文件，进程退出时统一刷盘。

可通过环境变量覆盖：
- ``OBS_CORRELATION_HEADER``：关联标识请求头名称
- ``OBS_CORRELATION_MAX_LENGTH``：关联标识最大长度
- ``OBS_LOG_LEVEL``：日志级别（DEBUG/INFO/WARNING/ERROR）
- ``OBS_SAMPLE_RATE``：采样比例，区间 [0.0, 1.0]
- ``OBS_SPANS_EXPORT_PATH``：片段导出文件路径，空串表示不写文件
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

DEFAULT_CORRELATION_HEADER = "X-Correlation-ID"
DEFAULT_CORRELATION_MAX_LENGTH = 128
DEFAULT_SAMPLE_RATE = 1.0
DEFAULT_SPANS_EXPORT_PATH = "spans.jsonl"


@dataclass(frozen=True)
class ObservabilitySettings:
    """可观测性相关配置（不可变，全部有默认值）。"""

    correlation_header: str = DEFAULT_CORRELATION_HEADER
    correlation_max_length: int = DEFAULT_CORRELATION_MAX_LENGTH
    log_level: int = logging.INFO
    sample_rate: float = DEFAULT_SAMPLE_RATE
    spans_export_path: str = DEFAULT_SPANS_EXPORT_PATH

    def __post_init__(self) -> None:
        if not self.correlation_header.strip():
            raise ValueError("correlation_header 不能为空")
        if self.correlation_max_length < 8 or self.correlation_max_length > 1024:
            raise ValueError("correlation_max_length 必须在 [8, 1024] 区间内")
        if not 0.0 <= self.sample_rate <= 1.0:
            raise ValueError("sample_rate 必须在 [0.0, 1.0] 区间内")


def _log_level_from_env(raw: str | None) -> int:
    if raw is None:
        return logging.INFO
    return logging.getLevelName(raw.strip().upper())


def get_settings() -> ObservabilitySettings:
    """从环境变量读取并构造配置；非法值直接抛出配置错误。"""
    header = os.environ.get("OBS_CORRELATION_HEADER", DEFAULT_CORRELATION_HEADER)
    max_length = int(os.environ.get("OBS_CORRELATION_MAX_LENGTH", str(DEFAULT_CORRELATION_MAX_LENGTH)))
    level = _log_level_from_env(os.environ.get("OBS_LOG_LEVEL"))
    sample_rate = float(os.environ.get("OBS_SAMPLE_RATE", str(DEFAULT_SAMPLE_RATE)))
    export_path = os.environ.get("OBS_SPANS_EXPORT_PATH", DEFAULT_SPANS_EXPORT_PATH)
    return ObservabilitySettings(
        correlation_header=header,
        correlation_max_length=max_length,
        log_level=level,
        sample_rate=sample_rate,
        spans_export_path=export_path,
    )
