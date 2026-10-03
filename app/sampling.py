"""采样策略：确定性比例采样、按入口/路由覆盖、判定依据可复现。

设计要点：
- **确定性**：判定值由 ``sha256(f"{seed}:{trace_id}")`` 派生，与进程、
  时间、并发无关。固定种子下，同一批请求（同一组关联标识）的取舍
  在压测前后可稳定复现。
- **边界**：``rate=0.0`` 彻底不采样（``kept`` 恒为 ``False``，连失败样本
  也不在导出侧保留，只留计数与日志）；``rate=1.0`` 全量。
- **按入口覆盖**：``overrides`` 以请求路径为键，支持精确匹配
  （``/health``）与前缀匹配（``/internal/*``），用于把健康检查、
  指标查询等高频低价值入口调低甚至关掉；未命中的入口用基础比例。
- 判定结果 :class:`SamplingDecision` 携带 ``rate``/``value``/``reason``，
  日志与单测可直接看到判定依据，不靠猜。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

DEFAULT_SAMPLE_SEED = "obs-v1"


@dataclass(frozen=True)
class SamplingDecision:
    """一次采样判定的结果与依据（用于日志与测试断言）。"""

    trace_id: str
    path: str | None
    rate: float       # 实际生效的比例（含覆盖）
    value: float      # 由 (seed, trace_id) 派生的确定性判定值，区间 [0, 1)
    kept: bool
    reason: str       # sampled / sampled_out / disabled / full


class Sampler:
    """确定性比例采样器。

    :param rate: 基础采样比例，区间 [0.0, 1.0]
    :param seed: 判定种子；相同种子 + 相同 trace_id ⇒ 相同判定
    :param overrides: 路径覆盖表，键为精确路径或以 ``*`` 结尾的前缀
    """

    def __init__(
        self,
        rate: float = 1.0,
        *,
        seed: str = DEFAULT_SAMPLE_SEED,
        overrides: dict[str, float] | None = None,
    ) -> None:
        if not 0.0 <= rate <= 1.0:
            raise ValueError("sample rate 必须在 [0.0, 1.0] 区间内")
        self._base_rate = rate
        self._seed = seed
        self._exact: dict[str, float] = {}
        self._prefix: list[tuple[str, float]] = []
        for pattern, value in (overrides or {}).items():
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"覆盖比例必须在 [0.0, 1.0] 区间内: {pattern}={value}")
            if not pattern:
                raise ValueError("覆盖路径不能为空")
            if pattern.endswith("*"):
                self._prefix.append((pattern[:-1], value))
            else:
                self._exact[pattern] = value
        # 前缀长的优先，保证更具体的规则先生效
        self._prefix.sort(key=lambda item: len(item[0]), reverse=True)

    @property
    def base_rate(self) -> float:
        return self._base_rate

    @property
    def seed(self) -> str:
        return self._seed

    def rate_for(self, path: str | None) -> float:
        """返回指定入口路径生效的比例（精确匹配优先，其次最长前缀）。"""
        if path is None:
            return self._base_rate
        if path in self._exact:
            return self._exact[path]
        for prefix, value in self._prefix:
            if path.startswith(prefix):
                return value
        return self._base_rate

    def _value_for(self, trace_id: str) -> float:
        digest = hashlib.sha256(f"{self._seed}:{trace_id}".encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") / 2**64

    def decide(self, trace_id: str, path: str | None = None) -> SamplingDecision:
        """对给定 trace_id 做可复现的取舍判定（含判定依据）。"""
        rate = self.rate_for(path)
        value = self._value_for(trace_id)
        if rate <= 0.0:
            kept, reason = False, "disabled"
        elif rate >= 1.0:
            kept, reason = True, "full"
        elif value < rate:
            kept, reason = True, "sampled"
        else:
            kept, reason = False, "sampled_out"
        return SamplingDecision(
            trace_id=trace_id, path=path, rate=rate, value=value, kept=kept, reason=reason
        )


def parse_overrides(raw: str) -> dict[str, float]:
    """解析环境变量格式的覆盖表：``/health=0,/metrics=0.05,/internal/*=0.1``。"""
    overrides: dict[str, float] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"采样覆盖项缺少 '=': {item!r}")
        pattern, _, value = item.partition("=")
        pattern = pattern.strip()
        if not pattern:
            raise ValueError(f"采样覆盖项路径为空: {item!r}")
        overrides[pattern] = float(value.strip())
    return overrides
