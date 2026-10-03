"""配置项：关联标识规则、采样、导出滚动与指标标签策略。

所有配置均有默认值，默认零外部依赖：
追踪片段默认追加写入本地 JSONL 文件，缓冲落盘，进程退出时统一刷盘。

可通过环境变量覆盖：
- ``OBS_CORRELATION_HEADER``：关联标识请求头名称
- ``OBS_CORRELATION_MAX_LENGTH``：关联标识最大长度
- ``OBS_LOG_LEVEL``：日志级别（DEBUG/INFO/WARNING/ERROR）
- ``OBS_SAMPLE_RATE``：全局采样比例，区间 [0.0, 1.0]；0 表示彻底关闭导出
- ``OBS_SAMPLE_SEED``：采样种子（整数）；同一种子下同一批关联标识取舍可复现
- ``OBS_SAMPLE_ROUTE_RATES``：按入口路径前缀的采样比例，JSON 对象，
  如 ``{"/health": 0.0, "/metrics": 0.01}``；精确匹配优先，否则最长前缀匹配
- ``OBS_SPANS_EXPORT_PATH``：片段导出文件路径，空串表示不写文件
- ``OBS_SPANS_BUFFER_BYTES``：写盘缓冲字节数，达到即落盘（默认 64 KiB）
- ``OBS_SPANS_FLUSH_INTERVAL_S``：后台周期落盘间隔秒数；0 表示关闭周期落盘
- ``OBS_SPANS_MAX_BYTES``：单个导出文件最大字节数，超过即滚动；0 表示不按大小滚动
- ``OBS_SPANS_ROTATE_INTERVAL_S``：按时间滚动间隔秒数；0 表示不按时间滚动
- ``OBS_SPANS_MAX_FILES``：滚动保留文件数上限（含活跃文件），超出删除最老
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field

DEFAULT_CORRELATION_HEADER = "X-Correlation-ID"
DEFAULT_CORRELATION_MAX_LENGTH = 128
DEFAULT_SAMPLE_RATE = 1.0
DEFAULT_SAMPLE_SEED = 0
DEFAULT_SPANS_EXPORT_PATH = "spans.jsonl"
DEFAULT_SPANS_BUFFER_BYTES = 64 * 1024
DEFAULT_SPANS_FLUSH_INTERVAL_S = 1.0
DEFAULT_SPANS_MAX_BYTES = 0
DEFAULT_SPANS_ROTATE_INTERVAL_S = 0.0
DEFAULT_SPANS_MAX_FILES = 5


@dataclass(frozen=True)
class ObservabilitySettings:
    """可观测性相关配置（不可变，全部有默认值）。"""

    correlation_header: str = DEFAULT_CORRELATION_HEADER
    correlation_max_length: int = DEFAULT_CORRELATION_MAX_LENGTH
    log_level: int = logging.INFO
    sample_rate: float = DEFAULT_SAMPLE_RATE
    sample_seed: int = DEFAULT_SAMPLE_SEED
    route_sample_rates: dict[str, float] = field(default_factory=dict)
    spans_export_path: str = DEFAULT_SPANS_EXPORT_PATH
    spans_buffer_bytes: int = DEFAULT_SPANS_BUFFER_BYTES
    spans_flush_interval_s: float = DEFAULT_SPANS_FLUSH_INTERVAL_S
    spans_max_bytes: int = DEFAULT_SPANS_MAX_BYTES
    spans_rotate_interval_s: float = DEFAULT_SPANS_ROTATE_INTERVAL_S
    spans_max_files: int = DEFAULT_SPANS_MAX_FILES

    def __post_init__(self) -> None:
        if not self.correlation_header.strip():
            raise ValueError("correlation_header 不能为空")
        if self.correlation_max_length < 8 or self.correlation_max_length > 1024:
            raise ValueError("correlation_max_length 必须在 [8, 1024] 区间内")
        if not 0.0 <= self.sample_rate <= 1.0:
            raise ValueError("sample_rate 必须在 [0.0, 1.0] 区间内")
        for prefix, rate in self.route_sample_rates.items():
            if not prefix.startswith("/"):
                raise ValueError(f"route_sample_rates 的键必须是路径前缀（以 / 开头）: {prefix!r}")
            if not 0.0 <= rate <= 1.0:
                raise ValueError(f"路由 {prefix!r} 的采样比例必须在 [0.0, 1.0] 区间内")
        if self.spans_buffer_bytes < 0:
            raise ValueError("spans_buffer_bytes 不能为负")
        if self.spans_flush_interval_s < 0:
            raise ValueError("spans_flush_interval_s 不能为负")
        if self.spans_max_bytes < 0:
            raise ValueError("spans_max_bytes 不能为负")
        if self.spans_rotate_interval_s < 0:
            raise ValueError("spans_rotate_interval_s 不能为负")
        if self.spans_max_files < 1:
            raise ValueError("spans_max_files 必须 >= 1")


def _log_level_from_env(raw: str | None) -> int:
    if raw is None:
        return logging.INFO
    return logging.getLevelName(raw.strip().upper())


def _route_rates_from_env(raw: str | None) -> dict[str, float]:
    """解析 ``OBS_SAMPLE_ROUTE_RATES``（JSON 对象：路径前缀 → 比例）。"""
    if raw is None or not raw.strip():
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("OBS_SAMPLE_ROUTE_RATES 必须是 JSON 对象，如 {\"/health\": 0.0}")
    return {str(k): float(v) for k, v in parsed.items()}


def get_settings() -> ObservabilitySettings:
    """从环境变量读取并构造配置；非法值直接抛出配置错误。"""
    header = os.environ.get("OBS_CORRELATION_HEADER", DEFAULT_CORRELATION_HEADER)
    max_length = int(os.environ.get("OBS_CORRELATION_MAX_LENGTH", str(DEFAULT_CORRELATION_MAX_LENGTH)))
    level = _log_level_from_env(os.environ.get("OBS_LOG_LEVEL"))
    sample_rate = float(os.environ.get("OBS_SAMPLE_RATE", str(DEFAULT_SAMPLE_RATE)))
    sample_seed = int(os.environ.get("OBS_SAMPLE_SEED", str(DEFAULT_SAMPLE_SEED)))
    route_rates = _route_rates_from_env(os.environ.get("OBS_SAMPLE_ROUTE_RATES"))
    export_path = os.environ.get("OBS_SPANS_EXPORT_PATH", DEFAULT_SPANS_EXPORT_PATH)
    buffer_bytes = int(os.environ.get("OBS_SPANS_BUFFER_BYTES", str(DEFAULT_SPANS_BUFFER_BYTES)))
    flush_interval = float(os.environ.get("OBS_SPANS_FLUSH_INTERVAL_S", str(DEFAULT_SPANS_FLUSH_INTERVAL_S)))
    max_bytes = int(os.environ.get("OBS_SPANS_MAX_BYTES", str(DEFAULT_SPANS_MAX_BYTES)))
    rotate_interval = float(os.environ.get("OBS_SPANS_ROTATE_INTERVAL_S", str(DEFAULT_SPANS_ROTATE_INTERVAL_S)))
    max_files = int(os.environ.get("OBS_SPANS_MAX_FILES", str(DEFAULT_SPANS_MAX_FILES)))
    return ObservabilitySettings(
        correlation_header=header,
        correlation_max_length=max_length,
        log_level=level,
        sample_rate=sample_rate,
        sample_seed=sample_seed,
        route_sample_rates=route_rates,
        spans_export_path=export_path,
        spans_buffer_bytes=buffer_bytes,
        spans_flush_interval_s=flush_interval,
        spans_max_bytes=max_bytes,
        spans_rotate_interval_s=rotate_interval,
        spans_max_files=max_files,
    )
