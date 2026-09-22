"""关键指标采集：请求量、时延、错误率。

取舍说明:
- **只计一次**：每个 HTTP 请求仅在中间件请求结束（含流式响应最后一帧）
  时记录一次，路由内部不再计数，避免重复计数；后台任务不计入 HTTP 时延，
  其耗时由追踪片段(trace span)单独表达，避免漏计/重复计。
- **标签有界**：method 归一到 HTTP 方法白名单，路径使用路由模板（如
  ``/items/{item_id}``）而非原始 URL，状态码归一到 ``Nxx`` 类别，
  未匹配路由归一为 ``__unmatched__``，杜绝高基数标签。
- **时延**：固定边界直方图 + 总和/计数，可在本地直接计算均值与分位近似。
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import threading
from dataclasses import dataclass, field

#: 时延直方图边界（秒），最后一个隐式桶为 +Inf
DURATION_BOUNDARIES: tuple[float, ...] = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
)
_KNOWN_METHODS: frozenset[str] = frozenset(
    {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
)
_STATUS_CLASSES: frozenset[str] = frozenset({"1xx", "2xx", "3xx", "4xx", "5xx"})
_UNMATCHED_ROUTE = "__unmatched__"
_OTHER = "_OTHER"
_MAX_ROUTE_LABEL_LEN = 128


def normalize_method(method: str | None) -> str:
    method = (method or "").upper()
    return method if method in _KNOWN_METHODS else _OTHER


def normalize_route(route: str | None) -> str:
    if not route:
        return _UNMATCHED_ROUTE
    return route[:_MAX_ROUTE_LABEL_LEN]


def normalize_status_class(status_code: int) -> str:
    cls = f"{status_code // 100}xx"
    return cls if cls in _STATUS_CLASSES else _OTHER


@dataclass
class _Series:
    count: int = 0
    errors: int = 0
    duration_sum: float = 0.0
    # 累积直方图：bucket_counts[i] 表示 <= DURATION_BOUNDARIES[i] 的样本数，
    # 末尾桶为 +Inf（恒等于 count）
    bucket_counts: list[int] = field(
        default_factory=lambda: [0] * (len(DURATION_BOUNDARIES) + 1)
    )

    def to_dict(self) -> dict:
        buckets = {
            **{f"le_{b:g}": n for b, n in zip(DURATION_BOUNDARIES, self.bucket_counts[:-1])},
            "le_+Inf": self.bucket_counts[-1],
        }
        return {
            "count": self.count,
            "errors": self.errors,
            "duration_seconds": {
                "sum": round(self.duration_sum, 6),
                "count": self.count,
                "mean": round(self.duration_sum / self.count, 6) if self.count else 0.0,
                "buckets": buckets,
            },
        }


@dataclass
class MetricsRegistry:
    """进程内指标注册表，线程安全。"""

    snapshot_path: str = ""
    _series: dict[tuple[str, str, str], _Series] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_request(
        self,
        method: str,
        path_template: str,
        status_code: int,
        duration_seconds: float,
        is_error: bool,
    ) -> None:
        """记录一次请求样本。每个请求只应调用一次。"""

        labels = (
            normalize_method(method),
            normalize_route(path_template),
            normalize_status_class(status_code),
        )
        if duration_seconds < 0:
            duration_seconds = 0.0
        with self._lock:
            series = self._series.get(labels)
            if series is None:
                series = _Series()
                self._series[labels] = series
            series.count += 1
            if is_error:
                series.errors += 1
            series.duration_sum += duration_seconds
            # 累积直方图：落入第 k 个边界桶的样本，对所有 >= k 的桶 +1
            placed = False
            for idx, boundary in enumerate(DURATION_BOUNDARIES):
                if duration_seconds <= boundary:
                    for j in range(idx, len(series.bucket_counts)):
                        series.bucket_counts[j] += 1
                    placed = True
                    break
            if not placed:
                series.bucket_counts[-1] += 1

    def snapshot(self) -> dict:
        """导出一致快照（持锁拷贝，避免并发读到中间状态）。"""

        with self._lock:
            series_data = {
                "|".join(labels): s.to_dict()
                for labels, s in sorted(self._series.items())
            }
            total = sum(s.count for s in self._series.values())
            errors = sum(s.errors for s in self._series.values())

        return {
            "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "label_names": ["method", "route", "status_class"],
            "duration_boundaries_seconds": list(DURATION_BOUNDARIES),
            "totals": {
                "requests": total,
                "errors": errors,
                "error_rate": round(errors / total, 6) if total else 0.0,
            },
            "series": series_data,
        }

    def write_snapshot(self) -> None:
        """把快照原子写入本地 JSON 文件（temp 文件 + rename）。"""

        if not self.snapshot_path:
            return
        directory = os.path.dirname(self.snapshot_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp_path = f"{self.snapshot_path}.tmp.{os.getpid()}"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(self.snapshot(), handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, self.snapshot_path)


_registry: MetricsRegistry | None = None
_registry_lock = threading.Lock()


def get_registry() -> MetricsRegistry:
    """返回进程内单例注册表。"""

    global _registry
    with _registry_lock:
        if _registry is None:
            _registry = MetricsRegistry()
        return _registry


def reset_registry(snapshot_path: str = "") -> MetricsRegistry:
    """重置单例注册表（主要供测试使用）。"""

    global _registry
    with _registry_lock:
        _registry = MetricsRegistry(snapshot_path=snapshot_path)
        return _registry
