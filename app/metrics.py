"""关键指标采集：请求量、时延、错误率；标签取值有界。

标签（全部有界，避免基数爆炸）：
- ``route``：路由模板（如 ``/items/{id}``），无法解析时归为 ``"unmatched"``
- ``method``：大写 HTTP 方法，非标准方法归为 ``"OTHER"``
- ``status_class``：``"1xx".."5xx"`` 或 ``"unknown"``
- ``outcome``：``"success" | "client_error" | "server_error" | "client_disconnect" | "unknown"``

结果分类语义（判定依据明确，不靠猜）：
- ``success``：响应状态码 < 400；
- ``client_error``：状态码 400–499（客户端请求本身有问题）；
- ``server_error``：状态码 500–599 或处理中抛出未捕获异常（计入错误率）；
- ``client_disconnect``：响应完成前客户端主动断开（判定依据：写响应时
  连接异常、或请求任务在响应完成前被取消）。既不算成功，也不算服务端
  错误，**不计入错误率**，避免客户端行为拉高服务端告警；
- ``unknown``：无有效状态码且无法归类的其余场景。

计数取舍（明确避免重复计数与漏计）：
- 每个请求在中间件出口**恰好计数一次**，业务代码不直接计数；
- 异常经由统一异常处理器转换为响应，同样在出口计数（server_error）；
- 客户端断连计为 ``client_disconnect`` 一次，不重计、不漏计。

时延使用单调时钟，单位毫秒；以计数/总和/最大值/总和平方的形式汇总，
快照可直接得到平均时延与错误率，而不保存逐请求样本。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

_ALLOWED_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"})
_STATUS_CLASSES = frozenset({"1xx", "2xx", "3xx", "4xx", "5xx", "unknown"})
_OUTCOMES = frozenset(
    {"success", "client_error", "server_error", "client_disconnect", "unknown"}
)

LabelKey = tuple[str, str, str, str]


def normalize_method(method: str) -> str:
    """方法名归一化到有界集合。"""
    upper = method.upper()
    return upper if upper in _ALLOWED_METHODS else "OTHER"


def normalize_route(route: str | None) -> str:
    """路由归一化：空/含换行的路由归为 ``unmatched``。"""
    if not route or not route.strip() or any(ch in route for ch in "\r\n"):
        return "unmatched"
    return route


def status_class_for(status_code: int | None) -> str:
    """状态码映射到有界分类。"""
    if status_code is None or not 100 <= status_code <= 599:
        return "unknown"
    return f"{status_code // 100}xx"


def outcome_for(status_code: int | None) -> str:
    """状态码映射到结果分类。"""
    cls = status_class_for(status_code)
    if cls == "unknown":
        return "unknown"
    code = status_code  # type: ignore[assignment]
    if code < 400:
        return "success"
    if code < 500:
        return "client_error"
    return "server_error"


@dataclass
class _Aggregate:
    count: int = 0
    total_ms: float = 0.0
    max_ms: float = 0.0
    sum_sq_ms: float = 0.0

    def add(self, duration_ms: float) -> None:
        self.count += 1
        self.total_ms += duration_ms
        self.sum_sq_ms += duration_ms * duration_ms
        if duration_ms > self.max_ms:
            self.max_ms = duration_ms


@dataclass
class RequestMetrics:
    """请求维度指标的内存存储（有界标签、线程安全）。"""

    _series: dict[LabelKey, _Aggregate] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record(
        self,
        *,
        route: str | None,
        method: str,
        status_code: int | None,
        duration_ms: float,
        outcome: str | None = None,
    ) -> LabelKey:
        """记录一次请求；返回归一化后的标签键。

        ``outcome`` 缺省时按状态码推导；调用方可显式传入
        （如 ``client_disconnect``：判定依据在调用点，见中间件）。
        """
        if outcome is not None and outcome not in _OUTCOMES:
            raise ValueError(f"非法 outcome: {outcome!r}")
        labels = (
            normalize_route(route),
            normalize_method(method),
            status_class_for(status_code),
            outcome if outcome is not None else outcome_for(status_code),
        )
        with self._lock:
            agg = self._series.setdefault(labels, _Aggregate())
            agg.add(max(duration_ms, 0.0))
        return labels

    def snapshot(self) -> dict[str, object]:
        """导出指标快照：按标签分组的计数/时延汇总与全局错误率。"""
        with self._lock:
            series: list[dict[str, object]] = []
            total = success = client_error = server_error = client_disconnect = 0
            for (route, method, status_class, outcome), agg in sorted(self._series.items()):
                avg = agg.total_ms / agg.count if agg.count else 0.0
                series.append(
                    {
                        "labels": {
                            "route": route,
                            "method": method,
                            "status_class": status_class,
                            "outcome": outcome,
                        },
                        "count": agg.count,
                        "latency_ms": {
                            "sum": round(agg.total_ms, 3),
                            "avg": round(avg, 3),
                            "max": round(agg.max_ms, 3),
                        },
                    }
                )
                total += agg.count
                success += agg.count if outcome == "success" else 0
                client_error += agg.count if outcome == "client_error" else 0
                server_error += agg.count if outcome == "server_error" else 0
                client_disconnect += agg.count if outcome == "client_disconnect" else 0
            return {
                "series": series,
                "totals": {
                    "requests": total,
                    "success": success,
                    "client_error": client_error,
                    "server_error": server_error,
                    "client_disconnect": client_disconnect,
                    "error_rate": round(server_error / total, 6) if total else 0.0,
                },
            }

    def reset(self) -> None:
        """清空指标（测试用）。"""
        with self._lock:
            self._series.clear()


_metrics: RequestMetrics | None = None
_metrics_lock = threading.Lock()


def get_metrics() -> RequestMetrics:
    """返回进程级单例指标存储。"""
    global _metrics
    with _metrics_lock:
        if _metrics is None:
            _metrics = RequestMetrics()
        return _metrics
