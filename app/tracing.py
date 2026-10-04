"""跨阶段追踪片段：父子关系、状态、耗时、采样与本地持久化导出。

设计要点：
- 父子栈存放于 :class:`contextvars.ContextVar`，``asyncio`` 并发下
  每个请求拥有独立栈，跨 ``await`` 自动传播，不会串线。
- ``trace_id`` 在根片段生成并随栈继承；同一请求在正常、异常、后台、
  流式各入口下根 ``trace_id`` 与关联标识保持一致。
- 采样决策只在根片段做出一次（确定性、可复现，见 :mod:`app.sampling`），
  整棵树同决策——同一次请求的片段要么整体留下、要么整体不留。
- 尾部采样（``tail_sampling=True``）：已结束片段按 trace 暂存，整树完成
  后统一取舍。比例为 0 时彻底不导出（连失败样本也不写，只留计数）；
  比例大于 0 时，含 ``ERROR`` 片段的 trace（失败/中断）整树强制保留，
  判丢弃后迟到的失败片段可救回整链（有界暂存）。
- 导出默认写入本地 JSONL 文件（无需外部服务）。进程退出调用
  :meth:`Tracer.shutdown`：未结束片段以 ``ERROR``/``shutdown`` 强制收尾后导出，
  未刷盘数据不静默丢弃。
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

from app.sampling import Sampler

SPAN_KIND_SERVER = "server"
SPAN_KIND_INTERNAL = "internal"
SPAN_KIND_BACKGROUND = "background"
SPAN_KIND_STREAM = "stream"

_STATUS_UNSET = "UNSET"
_STATUS_OK = "OK"
_STATUS_ERROR = "ERROR"


@dataclass
class Span:
    """一个追踪片段。"""

    trace_id: str
    span_id: str
    parent_id: str | None
    name: str
    kind: str = SPAN_KIND_SERVER
    start_ns: int = 0
    end_ns: int | None = None
    status: str = _STATUS_UNSET
    error_type: str | None = None
    error_message: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    sampled: bool = True

    @property
    def duration_ms(self) -> float | None:
        """片段耗时（毫秒），未结束时为 ``None``。"""
        if self.end_ns is None:
            return None
        return (self.end_ns - self.start_ns) / 1_000_000

    def set_attribute(self, key: str, value: Any) -> None:
        """写入一个属性；异常对象只保留类型与安全消息。"""
        if isinstance(value, BaseException):
            self.attributes[key] = {"type": type(value).__name__, "message": str(value)}
        else:
            self.attributes[key] = value

    def end(
        self,
        status: str = _STATUS_OK,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """结束片段并记录状态与失败原因；重复结束以第一次为准。"""
        if self.end_ns is not None:
            return
        self.end_ns = time.perf_counter_ns()
        self.status = status
        self.error_type = error_type
        self.error_message = error_message

    def to_dict(self) -> dict[str, Any]:
        """序列化为可导出字典。"""
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_id": self.parent_id,
            "name": self.name,
            "kind": self.kind,
            "start_unix_ns": self.start_ns,
            "end_unix_ns": self.end_ns,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "attributes": self.attributes,
            "sampled": self.sampled,
        }


class SpanExporter:
    """追踪片段导出器接口。"""

    def export(self, spans: list[Span]) -> None:
        """导出一批已结束的片段。"""
        raise NotImplementedError

    def flush(self) -> None:
        """强制刷盘。"""

    def shutdown(self) -> None:
        """关停导出器。"""


class FileSpanExporter(SpanExporter):
    """默认本地导出器：追加写入 JSONL 文件；路径为空时只保留在内存。"""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._fh: Any = None
        self._closed = False

    def _ensure_open(self) -> Any:
        # 延迟打开：模块导入/应用构造都不产生文件副作用
        if self._fh is None and not self._closed and self.path:
            parent = os.path.dirname(self.path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            self._fh = open(self.path, "a", encoding="utf-8")
        return self._fh

    def export(self, spans: list[Span]) -> None:
        if not spans or self._closed or not self.path:
            return
        lines = [json.dumps(s.to_dict(), ensure_ascii=False, default=str) + "\n" for s in spans]
        with self._lock:
            fh = self._ensure_open()
            if fh is not None:
                fh.writelines(lines)

    def flush(self) -> None:
        with self._lock:
            fh = self._ensure_open()
            if fh is not None:
                fh.flush()
                os.fsync(fh.fileno())

    def shutdown(self) -> None:
        with self._lock:
            fh = self._fh
            if fh is not None and not self._closed:
                fh.flush()
                fh.close()
                self._closed = True


class InMemorySpanExporter(SpanExporter):
    """测试用内存导出器。"""

    def __init__(self) -> None:
        self.exported: list[Span] = []
        self._lock = threading.Lock()
        self._closed = False

    def export(self, spans: list[Span]) -> None:
        with self._lock:
            if not self._closed:
                self.exported.extend(spans)

    def flush(self) -> None:
        return None

    def shutdown(self) -> None:
        with self._lock:
            self._closed = True

    def finished_spans(self) -> list[Span]:
        """返回已导出片段（导出的片段必然已结束）。"""
        with self._lock:
            return list(self.exported)

    def reset(self) -> None:
        """清空已导出片段（测试用）。"""
        with self._lock:
            self.exported.clear()
            self._closed = False


@dataclass
class TracerConfig:
    """追踪器配置。

    :param sample_rate: 基础采样比例（``sampler`` 缺省时用于构造采样器）
    :param exporter: 导出器；缺省为内存导出器
    :param sampler: 自定义采样器（含种子与按入口覆盖）；缺省按 ``sample_rate`` 构造
    :param tail_sampling: 开启后按 trace 缓存已结束片段，待整树完成后统一
        取舍：失败/中断的 trace 在比例大于 0 时强制保留，未命中的 trace
        整树丢弃——导出量与采样比例真正对齐，且不会留下半棵树
    :param dropped_buffer_traces: 已丢弃 trace 的暂存上限（用于迟到失败片段
        救回整链），有界，防内存膨胀
    """

    sample_rate: float = 1.0
    exporter: SpanExporter | None = None
    sampler: Sampler | None = None
    tail_sampling: bool = False
    dropped_buffer_traces: int = 256


_stack_var: ContextVar[tuple[Span, ...]] = ContextVar("span_stack", default=())


def current_span() -> Span | None:
    """读取当前上下文栈顶片段。"""
    stack = _stack_var.get()
    return stack[-1] if stack else None


class Tracer:
    """追踪器：创建片段、维护父子栈、采样与关停导出。"""

    def __init__(self, config: TracerConfig | None = None) -> None:
        config = config or TracerConfig()
        if not 0.0 <= config.sample_rate <= 1.0:
            raise ValueError("sample_rate 必须在 [0.0, 1.0] 区间内")
        self.sampler: Sampler = config.sampler or Sampler(config.sample_rate)
        self.sample_rate = self.sampler.base_rate
        self.exporter: SpanExporter = config.exporter or InMemorySpanExporter()
        self.tail_sampling = config.tail_sampling
        self._dropped_buffer_traces = max(config.dropped_buffer_traces, 0)
        self._active: dict[int, Span] = {}
        self._finished: list[Span] = []
        # 尾部采样状态（仅 tail_sampling=True 时使用）
        self._trace_active: dict[str, int] = {}
        self._trace_buffers: dict[str, list[Span]] = {}
        self._trace_rates: dict[str, float] = {}
        self._decisions: OrderedDict[str, bool] = OrderedDict()
        self._dropped: OrderedDict[str, list[Span]] = OrderedDict()
        self._stats = {
            "traces_kept": 0,
            "traces_dropped": 0,
            "traces_kept_for_error": 0,
            "traces_rescued": 0,
            "spans_exported": 0,
            "spans_dropped": 0,
        }
        self._lock = threading.Lock()
        self._closed = False

    def _decide_sampled(self, trace_id: str, path: str | None = None) -> bool:
        return self.sampler.decide(trace_id, path).kept

    def _register(self, span: Span) -> None:
        with self._lock:
            self._active[id(span)] = span
            if self.tail_sampling:
                self._trace_active[span.trace_id] = (
                    self._trace_active.get(span.trace_id, 0) + 1
                )

    def _register_finished(self, span: Span) -> None:
        to_export: list[Span] = []
        with self._lock:
            self._active.pop(id(span), None)
            if not self.tail_sampling:
                self._finished.append(span)
                return
            trace_id = span.trace_id
            remaining = self._trace_active.get(trace_id, 1) - 1
            decided = self._decisions.get(trace_id)
            if decided is True:
                # 已判定保留的 trace：迟到片段（如后台任务）直接导出
                to_export.append(span)
                self._stats["spans_exported"] += 1
            elif decided is False:
                to_export.extend(self._handle_late_span_after_drop(span))
            else:
                self._trace_buffers.setdefault(trace_id, []).append(span)
                if remaining <= 0:
                    self._trace_active.pop(trace_id, None)
                    to_export.extend(self._finalize_trace_locked(trace_id))
                else:
                    self._trace_active[trace_id] = remaining
        if to_export:
            self._export_sorted(to_export)

    def _handle_late_span_after_drop(self, span: Span) -> list[Span]:
        """trace 已判丢弃后迟到的片段：失败且比例>0 时救回整链。"""
        trace_id = span.trace_id
        rate = self._trace_rates.get(trace_id, self.sampler.base_rate)
        if span.status == _STATUS_ERROR and rate > 0.0:
            rescued = self._dropped.pop(trace_id, [])
            self._decisions[trace_id] = True
            self._stats["traces_rescued"] += 1
            self._stats["traces_dropped"] -= 1
            self._stats["traces_kept"] += 1
            self._stats["traces_kept_for_error"] += 1
            self._stats["spans_dropped"] -= len(rescued)
            self._stats["spans_exported"] += len(rescued) + 1
            return rescued + [span]
        buffer = self._dropped.setdefault(trace_id, [])
        buffer.append(span)
        self._stats["spans_dropped"] += 1
        return []

    def _finalize_trace_locked(self, trace_id: str) -> list[Span]:
        """整树完成后统一取舍（尾部采样核心）；调用方须持锁。"""
        buffer = self._trace_buffers.pop(trace_id, [])
        rate = self._trace_rates.get(trace_id, self.sampler.base_rate)
        if not buffer:
            return []
        has_error = any(s.status == _STATUS_ERROR for s in buffer)
        sampled = any(s.sampled for s in buffer)
        if rate <= 0.0:
            keep, reason = False, "disabled"
        elif has_error:
            keep, reason = True, "error_retention"
        elif sampled:
            keep, reason = True, "sampled"
        else:
            keep, reason = False, "sampled_out"
        self._remember_decision(trace_id, keep)
        if keep:
            self._stats["traces_kept"] += 1
            if reason == "error_retention":
                self._stats["traces_kept_for_error"] += 1
            self._stats["spans_exported"] += len(buffer)
            return buffer
        self._stats["traces_dropped"] += 1
        self._stats["spans_dropped"] += len(buffer)
        if self._dropped_buffer_traces > 0:
            self._dropped[trace_id] = buffer
            while len(self._dropped) > self._dropped_buffer_traces:
                self._dropped.popitem(last=False)
        return []

    def _remember_decision(self, trace_id: str, kept: bool) -> None:
        self._decisions[trace_id] = kept
        # 判定缓存有界：超出后淘汰最旧（迟到片段退化为按当前信息处理）
        bound = max(self._dropped_buffer_traces * 2, 64)
        while len(self._decisions) > bound:
            self._decisions.popitem(last=False)

    def _export_sorted(self, spans: list[Span]) -> None:
        # 同一 trace 内按开始时间排序，保证父子片段导出顺序可读
        spans.sort(key=lambda s: (s.trace_id, s.start_ns, s.span_id))
        self.exporter.export(spans)

    def _note_root_rate(self, trace_id: str, path: str | None) -> None:
        """根片段做出采样决策时记录生效比例（供尾部采样的失败保留判定）。"""
        if self.tail_sampling:
            with self._lock:
                self._trace_rates[trace_id] = self.sampler.rate_for(path)

    def start_span(
        self,
        name: str,
        kind: str = SPAN_KIND_INTERNAL,
        *,
        trace_id: str | None = None,
        parent_id: str | None = None,
        sample_path: str | None = None,
        **attributes: Any,
    ) -> Span:
        """手动开启片段（适用于流式等生命周期跨出 ``with`` 块的场景）。

        需配对调用 :meth:`end_span`。父关系优先取显式 ``parent_id``，
        否则取当前上下文栈顶。``sample_path`` 仅在成为根片段时参与
        采样判定（按入口覆盖）。
        """
        parent = current_span()
        if parent_id is not None:
            span_trace_id = trace_id or (parent.trace_id if parent is not None else uuid.uuid4().hex)
            if parent is not None:
                sampled = parent.sampled
            else:
                sampled = self._decide_sampled(span_trace_id, sample_path)
                self._note_root_rate(span_trace_id, sample_path)
        elif parent is not None:
            span_trace_id = parent.trace_id
            parent_id = parent.span_id
            sampled = parent.sampled
        else:
            span_trace_id = trace_id or uuid.uuid4().hex
            sampled = self._decide_sampled(span_trace_id, sample_path)
            self._note_root_rate(span_trace_id, sample_path)
        span = Span(
            trace_id=span_trace_id,
            span_id=uuid.uuid4().hex,
            parent_id=parent_id,
            name=name,
            kind=kind,
            start_ns=time.perf_counter_ns(),
            sampled=sampled,
        )
        for key, value in attributes.items():
            span.set_attribute(key, value)
        self._register(span)
        return span

    def end_span(
        self,
        span: Span,
        *,
        status: str = _STATUS_OK,
        error_type: str | None = None,
        error_message: str | None = None,
        export: bool = False,
    ) -> None:
        """手动结束片段；``export=True`` 时立即把已结束片段交给导出器。

        导出为异步入队（不阻塞调用方）；落盘刷写由导出器后台周期完成，
        显式 ``flush()`` / ``shutdown()`` 仍会同步排空并刷盘。
        """
        span.end(status, error_type=error_type, error_message=error_message)
        self._register_finished(span)
        if export:
            self.export_finished()

    @contextmanager
    def span(
        self,
        name: str,
        kind: str = SPAN_KIND_INTERNAL,
        trace_id: str | None = None,
        sample_path: str | None = None,
        **attributes: Any,
    ) -> Iterator[Span]:
        """开启一个片段。

        - 栈内存在父片段时自动继承 ``trace_id`` 与 ``parent_id`` 及采样决策；
        - 否则成为根片段：可用 ``trace_id`` 显式指定（通常传关联标识派生值），
          ``sample_path`` 参与按入口覆盖的采样判定；
        - 抛出异常时片段标记 ``ERROR`` 并保留异常类型与消息后重新抛出。
        """
        parent = current_span()
        if parent is not None:
            span_trace_id = parent.trace_id
            parent_id: str | None = parent.span_id
            sampled = parent.sampled
        else:
            span_trace_id = trace_id or uuid.uuid4().hex
            parent_id = None
            sampled = self._decide_sampled(span_trace_id, sample_path)
            self._note_root_rate(span_trace_id, sample_path)

        span = Span(
            trace_id=span_trace_id,
            span_id=uuid.uuid4().hex,
            parent_id=parent_id,
            name=name,
            kind=kind,
            start_ns=time.perf_counter_ns(),
            sampled=sampled,
        )
        for key, value in attributes.items():
            span.set_attribute(key, value)
        self._register(span)

        token = _stack_var.set(_stack_var.get() + (span,))
        try:
            yield span
        except BaseException as exc:  # 含 CancelledError，也要落片段
            if span.end_ns is None:
                span.end(
                    _STATUS_ERROR,
                    error_type=type(exc).__name__,
                    error_message=str(exc) or repr(exc),
                )
            self._register_finished(span)
            _stack_var.reset(token)
            raise
        else:
            if span.end_ns is None:
                span.end(_STATUS_OK)
            self._register_finished(span)
            _stack_var.reset(token)

    def export_finished(self, *, only_sampled: bool = False) -> int:
        """导出当前所有已结束片段；返回导出条数。

        尾部采样模式下片段在整树完成时已按策略导出，此处通常无待导出项。
        """
        with self._lock:
            pending = [s for s in self._finished if only_sampled is False or s.sampled]
            self._finished.clear()
            self._stats["spans_exported"] += len(pending)
        if pending:
            self._export_sorted(pending)
        return len(pending)

    def flush(self) -> None:
        """导出已结束片段并刷盘。"""
        self.export_finished()
        self.exporter.flush()

    def sampling_stats(self) -> dict[str, int]:
        """采样与导出计数快照（彻底关闭时仍有计数可查）。"""
        with self._lock:
            return dict(self._stats)

    def shutdown(self) -> None:
        """关停：强制结束并导出所有未完成片段，禁止静默丢弃。"""
        if self._closed:
            return
        with self._lock:
            active = list(self._active.values())
        # 跨线程的后台片段也在 _active 中登记，因此关停恢复不依赖上下文栈
        for span in active:
            if span.end_ns is None:
                span.end(
                    _STATUS_ERROR,
                    error_type="TracerShutdown",
                    error_message="追踪器关停时片段尚未结束",
                )
                self._register_finished(span)
        if self.tail_sampling:
            # 收尾仍滞留的 trace（如计数残留）：按同一策略统一取舍
            pending: list[Span] = []
            with self._lock:
                for trace_id in list(self._trace_buffers):
                    pending.extend(self._finalize_trace_locked(trace_id))
                self._trace_active.clear()
            if pending:
                self._export_sorted(pending)
        self.export_finished()
        self.exporter.flush()
        self.exporter.shutdown()
        self._closed = True

    @property
    def closed(self) -> bool:
        """是否已关停。"""
        return self._closed
