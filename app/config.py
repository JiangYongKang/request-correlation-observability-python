"""配置项：关联标识规则、采样、导出与指标标签策略。

所有配置均有默认值，默认零外部依赖：
追踪片段默认追加写入本地 JSONL 文件，进程退出时统一刷盘。

可通过环境变量覆盖：
- ``OBS_CORRELATION_HEADER``：关联标识请求头名称
- ``OBS_CORRELATION_MAX_LENGTH``：关联标识最大长度
- ``OBS_LOG_LEVEL``：日志级别（DEBUG/INFO/WARNING/ERROR）
- ``OBS_SAMPLE_RATE``：采样比例，区间 [0.0, 1.0]
- ``OBS_SAMPLE_SEED``：采样判定种子（固定种子 ⇒ 取舍可复现）
- ``OBS_SAMPLE_RATE_OVERRIDES``：按入口覆盖，如 ``/health=0,/metrics=0.05``
- ``OBS_SPANS_EXPORT_PATH``：片段导出文件路径，空串表示不写文件
- ``OBS_SPANS_MAX_BYTES``：单个导出文件滚动阈值（字节）
- ``OBS_SPANS_MAX_FILES``：滚动文件保留上限（含当前文件）
- ``OBS_SPANS_ROTATE_INTERVAL_S``：按时间滚动间隔（秒），0 表示不按时间滚动
- ``OBS_SPANS_QUEUE_SIZE``：异步写盘队列容量（背压上限）
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from app.sampling import DEFAULT_SAMPLE_SEED, parse_overrides

DEFAULT_CORRELATION_HEADER = "X-Correlation-ID"
DEFAULT_CORRELATION_MAX_LENGTH = 128
DEFAULT_SAMPLE_RATE = 1.0
DEFAULT_SPANS_EXPORT_PATH = "spans.jsonl"
DEFAULT_SPANS_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_SPANS_MAX_FILES = 5
DEFAULT_SPANS_ROTATE_INTERVAL_S = 0.0
DEFAULT_SPANS_QUEUE_SIZE = 10000


@dataclass(frozen=True)
class ObservabilitySettings:
    """可观测性相关配置（不可变，全部有默认值）。"""

    correlation_header: str = DEFAULT_CORRELATION_HEADER
    correlation_max_length: int = DEFAULT_CORRELATION_MAX_LENGTH
    log_level: int = logging.INFO
    sample_rate: float = DEFAULT_SAMPLE_RATE
    sample_seed: str = DEFAULT_SAMPLE_SEED
    sample_rate_overrides: dict[str, float] | None = None
    spans_export_path: str = DEFAULT_SPANS_EXPORT_PATH
    spans_max_bytes: int = DEFAULT_SPANS_MAX_BYTES
    spans_max_files: int = DEFAULT_SPANS_MAX_FILES
    spans_rotate_interval_s: float = DEFAULT_SPANS_ROTATE_INTERVAL_S
    spans_queue_size: int = DEFAULT_SPANS_QUEUE_SIZE

    def __post_init__(self) -> None:
        if not self.correlation_header.strip():
            raise ValueError("correlation_header 不能为空")
        if self.correlation_max_length < 8 or self.correlation_max_length > 1024:
            raise ValueError("correlation_max_length 必须在 [8, 1024] 区间内")
        if not 0.0 <= self.sample_rate <= 1.0:
            raise ValueError("sample_rate 必须在 [0.0, 1.0] 区间内")
        if not self.sample_seed:
            raise ValueError("sample_seed 不能为空")
        for pattern, value in (self.sample_rate_overrides or {}).items():
            if not pattern:
                raise ValueError("采样覆盖路径不能为空")
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"采样覆盖比例必须在 [0.0, 1.0] 区间内: {pattern}={value}")
        if self.spans_max_bytes < 1024:
            raise ValueError("spans_max_bytes 至少 1024 字节")
        if self.spans_max_files < 1:
            raise ValueError("spans_max_files 至少为 1")
        if self.spans_rotate_interval_s < 0:
            raise ValueError("spans_rotate_interval_s 不能为负")
        if self.spans_queue_size < 1:
            raise ValueError("spans_queue_size 至少为 1")


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
    sample_seed = os.environ.get("OBS_SAMPLE_SEED", DEFAULT_SAMPLE_SEED)
    overrides_raw = os.environ.get("OBS_SAMPLE_RATE_OVERRIDES", "")
    export_path = os.environ.get("OBS_SPANS_EXPORT_PATH", DEFAULT_SPANS_EXPORT_PATH)
    max_bytes = int(os.environ.get("OBS_SPANS_MAX_BYTES", str(DEFAULT_SPANS_MAX_BYTES)))
    max_files = int(os.environ.get("OBS_SPANS_MAX_FILES", str(DEFAULT_SPANS_MAX_FILES)))
    rotate_interval = float(
        os.environ.get("OBS_SPANS_ROTATE_INTERVAL_S", str(DEFAULT_SPANS_ROTATE_INTERVAL_S))
    )
    queue_size = int(os.environ.get("OBS_SPANS_QUEUE_SIZE", str(DEFAULT_SPANS_QUEUE_SIZE)))
    return ObservabilitySettings(
        correlation_header=header,
        correlation_max_length=max_length,
        log_level=level,
        sample_rate=sample_rate,
        sample_seed=sample_seed,
        sample_rate_overrides=parse_overrides(overrides_raw),
        spans_export_path=export_path,
        spans_max_bytes=max_bytes,
        spans_max_files=max_files,
        spans_rotate_interval_s=rotate_interval,
        spans_queue_size=queue_size,
    )
