"""确定性采样器：固定种子可复现，支持按入口/路由单独调比例。

判定依据（可复现、可解释）：
- 每个 trace 的取舍由 ``sha256(f"{seed}:{trace_id}")`` 派生的 [0,1) 区间
  伪随机值与生效比例比较得出：同一批关联标识 + 同一种子 ⇒ 取舍完全一致，
  压测前后可稳定复现对比；
- 生效比例 = 路由专属比例（最长前缀匹配）否则全局默认比例；
- 比例 0 表示彻底关闭：``decide`` 恒返回不采样，且调用方（Tracer）对
  比例 0 的 trace 连失败样本也不再导出，只保留计数与日志；
- 比例 > 0 时本模块只负责"头部取舍"，失败样本的兜底保留由 Tracer 在
  trace 收尾时判定（尾部保留），见 :mod:`app.tracing`。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

_UINT64 = 1 << 64


@dataclass(frozen=True)
class SampleDecision:
    """一次采样判定及其依据。"""

    sampled: bool
    rate: float  # 生效比例（路由专属或全局默认）
    draw: float  # 确定性伪随机值，draw < rate 即命中
    seed: int
    path: str | None


@dataclass
class Sampler:
    """确定性采样器。

    :param default_rate: 全局默认比例，区间 [0.0, 1.0]
    :param route_rates: 入口路径前缀 → 比例；精确匹配优先，否则最长前缀匹配
    :param seed: 固定种子；同一种子下同一 trace_id 的取舍稳定复现
    """

    default_rate: float = 1.0
    route_rates: dict[str, float] = field(default_factory=dict)
    seed: int = 0

    def __post_init__(self) -> None:
        if not 0.0 <= self.default_rate <= 1.0:
            raise ValueError("default_rate 必须在 [0.0, 1.0] 区间内")
        for prefix, rate in self.route_rates.items():
            if not prefix.startswith("/"):
                raise ValueError(f"路由采样键必须是路径前缀（以 / 开头）: {prefix!r}")
            if not 0.0 <= rate <= 1.0:
                raise ValueError(f"路由 {prefix!r} 的采样比例必须在 [0.0, 1.0] 区间内")

    def rate_for_path(self, path: str | None) -> float:
        """返回某入口路径的生效比例：精确匹配优先，否则最长前缀匹配。"""
        if not path or not self.route_rates:
            return self.default_rate
        if path in self.route_rates:
            return self.route_rates[path]
        best: str | None = None
        for prefix in self.route_rates:
            if path.startswith(prefix) and (best is None or len(prefix) > len(best)):
                best = prefix
        return self.route_rates[best] if best is not None else self.default_rate

    def draw(self, trace_id: str) -> float:
        """由种子与 trace_id 派生 [0,1) 的确定性伪随机值。"""
        digest = hashlib.sha256(f"{self.seed}:{trace_id}".encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") / _UINT64

    def decide(self, trace_id: str, path: str | None = None) -> SampleDecision:
        """对一棵 trace 做一次头部取舍判定（判定依据完整保留在返回值中）。"""
        rate = self.rate_for_path(path)
        draw = self.draw(trace_id)
        return SampleDecision(
            sampled=rate > 0.0 and draw < rate,
            rate=rate,
            draw=draw,
            seed=self.seed,
            path=path,
        )
