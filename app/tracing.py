"""跨阶段追踪片段：父子关系、状态、耗时、采样保留策略与本地持久化导出。

设计要点：
- 父子栈存放于 :class:`contextvars.ContextVar`，``asyncio`` 并发下
  每个请求拥有独立栈，跨 ``await`` 自动传播，不会串线。
- ``trace_id`` 在根片段生成并随栈继承；同一请求在正常、异常、后台、
  流式各入口下根 ``trace_id`` 与关联标识保持一致。
- 采样按 **trace 整体** 取舍：根片段创建时由 :class:`app.sampling.Sampler`
  做一次确定性头部判定，整棵树继承；片段先按 trace 缓冲，trace 收尾时
  （根片段结束且无活动片段）统一决定保留或丢弃：
  - 生效比例为 0：彻底不导出（连失败样本也不写，只保留计数与日志）；
  - 头部命中：整树保留；
  - 头部未命中但任一片段为 ``ERROR``（失败/中途打断/关停强制收尾）：
    整树保留（``sample.keep_reason=error_retained``）；
  - 其余：整树丢弃，落盘数据量与采样比例大致对齐。
- 导出默认写入本地 JSONL 文件：写入先进入内存缓冲，按缓冲量/时间间隔
  落盘，不在请求主链路同步刷盘；支持按大小/时间滚动与保留上限；
  进程退出调用 :meth:`Tracer.shutdown`：未结束片段以 ``ERROR``/``shutdown``
  强制收尾后按上述策略导出，缓冲区全量刷盘，未刷盘数据不静默丢弃。
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
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
    """本地 JSONL 导出器：缓冲写盘、按大小/时间滚动、保留上限。

    - 写入先进内存缓冲，缓冲量达到 ``buffer_bytes`` 或由后台线程按
      ``flush_interval_s`` 周期落盘；``flush()``/``shutdown()`` 强制全量落盘，
      正常退出不丢缓冲数据；
    - 滚动：当前文件超过 ``max_bytes`` 或存活超过 ``rotate_interval_s``
      时滚动为 ``<path>.1``、``<path>.2``……，连活跃文件在内最多保留
      ``max_files`` 个，超出最老的删除；滚动在一批（通常是一整条 trace）
      写入之前判定，同一次请求的片段不会被拆到两个文件；
    - 路径为空时只缓冲不落盘（测试/本地内存模式）。
    """

    def __init__(
        self,
        path: str,
        *,
        max_bytes: int = 0,
        rotate_interval_s: float = 0.0,
        max_files: int = 5,
        buffer_bytes: int = 64 * 1024,
        flush_interval_s: float = 1.0,
    ) -> None:
        if max_bytes < 0:
            raise ValueError("max_bytes 不能为负")
        if max_files < 1:
            raise ValueError("max_files 必须 >= 1")
        self.path = path
        self.max_bytes = max_bytes
        self.rotate_interval_s = rotate_interval_s
        self.max_files = max_files
        self.buffer_bytes = buffer_bytes
        self.flush_interval_s = flush_interval_s
        self._lock = threading.Lock()
        self._fh: Any = None
        self._closed = False
        self._buf: list[str] = []
        self._buf_bytes = 0
        self._size = 0  # 当前活跃文件已写字节数
        self._opened_at: float | None = None
        self._stop = threading.Event()
        self._flusher: threading.Thread | None = None

    # ---- 文件生命周期 ----

    def _ensure_open(self) -> Any:
        # 延迟打开：模块导入/应用构造都不产生文件副作用
        if self._fh is None and not self._closed and self.path:
            parent = os.path.dirname(self.path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            self._size = os.path.getsize(self.path) if os.path.exists(self.path) else 0
            self._fh = open(self.path, "a", encoding="utf-8")
            self._opened_at = time.monotonic()
        return self._fh

    def _archive_path(self, index: int) -> str:
        return f"{self.path}.{index}"

    def _rotate_locked(self) -> None:
        """滚动文件（调用方须持锁）：归档现役文件并应用保留上限。"""
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        if not self.path:
            return
        if self.max_files >= 2:
            oldest = self._archive_path(self.max_files - 1)
            if os.path.exists(oldest):
                os.remove(oldest)
            for index in range(self.max_files - 1, 1, -1):
                src = self._archive_path(index - 1)
                if os.path.exists(src):
                    os.replace(src, self._archive_path(index))
            if os.path.exists(self.path):
                os.replace(self.path, self._archive_path(1))
        elif os.path.exists(self.path):
            os.remove(self.path)
        self._size = 0
        self._opened_at = None

    def _should_rotate_locked(self, incoming_bytes: int) -> bool:
        if self._fh is None:
            return False
        if self.rotate_interval_s > 0 and self._opened_at is not None:
            if time.monotonic() - self._opened_at >= self.rotate_interval_s:
                return True
        if self.max_bytes > 0 and self._size > 0:
            # 整批写入前判定：同一批（通常一条完整 trace）不跨文件拆分
            if self._size + incoming_bytes > self.max_bytes:
                return True
        return False

    def _write_locked(self) -> None:
        if not self._buf:
            return
        fh = self._ensure_open()
        if fh is None:
            # 无路径模式：缓冲直接丢弃语义等同丢弃导出，清空避免无限增长
            self._buf.clear()
            self._buf_bytes = 0
            return
        if self._should_rotate_locked(self._buf_bytes):
            self._rotate_locked()
            fh = self._ensure_open()
        fh.writelines(self._buf)
        self._size += self._buf_bytes
        self._buf.clear()
        self._buf_bytes = 0

    def _start_flusher_locked(self) -> None:
        if self._flusher is None and self.flush_interval_s > 0 and self.path:
            self._flusher = threading.Thread(
                target=self._flush_loop, name="span-exporter-flush", daemon=True
            )
            self._flusher.start()

    def _flush_loop(self) -> None:
        while not self._stop.wait(self.flush_interval_s):
            try:
                self.flush()
            except Exception:  # 后台刷盘失败不影响主链路，下周期重试
                pass

    # ---- 导出器接口 ----

    def export(self, spans: list[Span]) -> None:
        if not spans or not self.path:
            return
        lines = [json.dumps(s.to_dict(), ensure_ascii=False, default=str) + "\n" for s in spans]
        with self._lock:
            if self._closed:
                return
            self._start_flusher_locked()
            for line in lines:
                self._buf.append(line)
                self._buf_bytes += len(line.encode("utf-8"))
            if self._buf_bytes >= self.buffer_bytes:
                self._write_locked()

    def flush(self) -> None:
        with self._lock:
            self._write_locked()
            fh = self._fh
            if fh is not None:
                fh.flush()
                os.fsync(fh.fileno())

    def shutdown(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._stop.set()
        flusher = self._flusher
        if flusher is not None:
            flusher.join(timeout=5)
        with self._lock:
            # 先落盘再标记关闭：缓冲数据不得因关闭标志被丢弃
            self._write_locked()
            fh = self._fh
            if fh is not None:
                fh.flush()
                os.fsync(fh.fileno())
                fh.close()
                self._fh = None
            self._closed = True

    def rotated_files(self) -> list[str]:
        """返回当前存在的归档文件（``<path>.1`` 起，按新旧排序）。"""
        files = []
        index = 1
        while os.path.exists(self._archive_path(index)):
            files.append(self._archive_path(index))
            index += 1
        return files


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

    :param sample_rate: 全局默认采样比例（兼容旧配置；``sampler`` 优先）
    :param sampler: 自定义采样器；缺省时按 ``sample_rate``/``sample_seed``/
        ``route_sample_rates`` 构造确定性采样器
    :param max_buffered_traces: 内存中同时缓冲的未收尾 trace 上限，
        超限强制收尾最老的 trace（按失败保留策略处理），防止长跑内存膨胀
    """

    sample_rate: float = 1.0
    exporter: SpanExporter | None = None
    sampler: Sampler | None = None
    sample_seed: int = 0
    route_sample_rates: dict[str, float] | None = None
    max_buffered_traces: int = 10000


@dataclass
class _TraceState:
    """一条 trace 的缓冲状态：片段集、活动计数与采样判定。"""

    sampled: bool = True
    rate: float = 1.0
    active: int = 0
    root_ended: bool = False
    spans: list[Span] = field(default_factory=list)


_stack_var: ContextVar[tuple[Span, ...]] = ContextVar("span_stack", default=())


def current_span() -> Span | None:
    """读取当前上下文栈顶片段。"""
    stack = _stack_var.get()
    return stack[-1] if stack else None


class Tracer:
    """追踪器：创建片段、维护父子栈、按 trace 缓冲与采样保留、关停导出。"""

    def __init__(self, config: TracerConfig | None = None) -> None:
        config = config or TracerConfig()
        if not 0.0 <= config.sample_rate <= 1.0:
            raise ValueError("sample_rate 必须在 [0.0, 1.0] 区间内")
        self.sampler = config.sampler or Sampler(
            default_rate=config.sample_rate,
            route_rates=dict(config.route_sample_rates or {}),
            seed=config.sample_seed,
        )
        self.exporter: SpanExporter = config.exporter or InMemorySpanExporter()
        self.max_buffered_traces = max(1, config.max_buffered_traces)
        self._traces: dict[str, _TraceState] = {}
        self._active_spans: dict[int, Span] = {}
        self._lock = threading.Lock()
        self._closed = False
        self._stats = {
            "traces_kept": 0,
            "traces_dropped": 0,
            "spans_exported": 0,
            "spans_dropped": 0,
        }

    # ---- trace 缓冲与保留策略 ----

    def _state_for(self, trace_id: str) -> _TraceState:
        state = self._traces.get(trace_id)
        if state is None:
            state = _TraceState()
            self._traces[trace_id] = state
        return state

    def _register(self, span: Span, *, is_root: bool, rate: float) -> None:
        with self._lock:
            state = self._state_for(span.trace_id)
            state.active += 1
            self._active_spans[id(span)] = span
            if is_root:
                state.sampled = span.sampled
                state.rate = rate
            self._evict_if_needed_locked()

    def _register_finished(self, span: Span, *, is_root: bool) -> None:
        with self._lock:
            state = self._state_for(span.trace_id)
            state.active = max(0, state.active - 1)
            self._active_spans.pop(id(span), None)
            state.spans.append(span)
            if is_root:
                state.root_ended = True
            self._finalize_if_complete_locked(span.trace_id, state)

    def _finalize_if_complete_locked(self, trace_id: str, state: _TraceState) -> None:
        if not (state.root_ended and state.active == 0):
            return
        self._traces.pop(trace_id, None)
        self._export_trace_locked(trace_id, state)

    def _export_trace_locked(self, trace_id: str, state: _TraceState) -> None:
        """按保留策略决定整树取舍并导出（调用方须持锁）。"""
        spans = state.spans
        if not spans:
            return
        root = next((s for s in spans if s.parent_id is None), None)
        if state.rate <= 0.0:
            keep, reason = False, "rate_zero"
        elif state.sampled:
            keep, reason = True, "sampled"
        elif any(s.status == _STATUS_ERROR for s in spans):
            # 失败/中途打断（含关停强制收尾）样本优先保留
            keep, reason = True, "error_retained"
        else:
            keep, reason = False, "sampled_out"
        if root is not None:
            root.set_attribute("sample.keep_reason", reason)
        if keep:
            spans.sort(key=lambda s: (s.start_ns, s.span_id))
            self._stats["traces_kept"] += 1
            self._stats["spans_exported"] += len(spans)
            # 导出在锁外进行会引入乱序复杂度；导出器自身有锁且写缓冲，
            # 此处持锁调用代价可接受
            self.exporter.export(spans)
        else:
            self._stats["traces_dropped"] += 1
            self._stats["spans_dropped"] += len(spans)

    def _evict_if_needed_locked(self) -> None:
        """缓冲 trace 数超限时，强制收尾最老的 trace（按失败保留处理）。"""
        while len(self._traces) > self.max_buffered_traces:
            oldest_id = next(iter(self._traces))
            state = self._traces[oldest_id]
            for span in list(self._active_spans.values()):
                if span.trace_id != oldest_id:
                    continue
                if span.end_ns is None:
                    span.end(
                        _STATUS_ERROR,
                        error_type="TraceBufferOverflow",
                        error_message="缓冲 trace 超限，强制收尾",
                    )
                self._active_spans.pop(id(span), None)
                state.active = max(0, state.active - 1)
                state.spans.append(span)
            state.root_ended = True
            self._traces.pop(oldest_id, None)
            self._export_trace_locked(oldest_id, state)

    # ---- 片段生命周期 ----

    def _decide_root(self, trace_id: str, path: str | None) -> tuple[bool, float]:
        decision = self.sampler.decide(trace_id, path)
        return decision.sampled, decision.rate

    def start_span(
        self,
        name: str,
        kind: str = SPAN_KIND_INTERNAL,
        *,
        trace_id: str | None = None,
        parent_id: str | None = None,
        **attributes: Any,
    ) -> Span:
        """手动开启片段（适用于流式等生命周期跨出 ``with`` 块的场景）。

        需配对调用 :meth:`end_span`。父关系优先取显式 ``parent_id``，
        否则取当前上下文栈顶。
        """
        parent = current_span()
        is_root = False
        rate = 1.0
        if parent_id is not None:
            span_trace_id = trace_id or (parent.trace_id if parent is not None else uuid.uuid4().hex)
            if parent is not None:
                sampled = parent.sampled
            else:
                sampled, rate = self._decide_root(span_trace_id, attributes.get("path"))
        elif parent is not None:
            span_trace_id = parent.trace_id
            parent_id = parent.span_id
            sampled = parent.sampled
        else:
            is_root = True
            span_trace_id = trace_id or uuid.uuid4().hex
            sampled, rate = self._decide_root(span_trace_id, attributes.get("path"))
        span = Span(
            trace_id=span_trace_id,
            span_id=uuid.uuid4().hex,
            parent_id=parent_id,
            name=name,
            kind=kind,
            start_ns=time.perf_counter_ns(),
            sampled=sampled,
        )
        if is_root:
            span.set_attribute("sample.rate", rate)
            span.set_attribute("sample.seed", self.sampler.seed)
        for key, value in attributes.items():
            span.set_attribute(key, value)
        self._register(span, is_root=is_root, rate=rate)
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
        """手动结束片段；``export=True`` 时立即尝试收尾导出已完成 trace。"""
        span.end(status, error_type=error_type, error_message=error_message)
        self._register_finished(span, is_root=span.parent_id is None)
        if export:
            self.export_finished()

    @contextmanager
    def span(
        self,
        name: str,
        kind: str = SPAN_KIND_INTERNAL,
        trace_id: str | None = None,
        **attributes: Any,
    ) -> Iterator[Span]:
        """开启一个片段。

        - 栈内存在父片段时自动继承 ``trace_id`` 与 ``parent_id`` 及采样决策；
        - 否则成为根片段：可用 ``trace_id`` 显式指定（通常传关联标识派生值），
          采样判定依据（比例/种子）写入根片段属性；
        - 抛出异常时片段标记 ``ERROR`` 并保留异常类型与消息后重新抛出。
        """
        parent = current_span()
        is_root = parent is None
        rate = 1.0
        if parent is not None:
            span_trace_id = parent.trace_id
            parent_id: str | None = parent.span_id
            sampled = parent.sampled
        else:
            span_trace_id = trace_id or uuid.uuid4().hex
            parent_id = None
            sampled, rate = self._decide_root(span_trace_id, attributes.get("path"))

        span = Span(
            trace_id=span_trace_id,
            span_id=uuid.uuid4().hex,
            parent_id=parent_id,
            name=name,
            kind=kind,
            start_ns=time.perf_counter_ns(),
            sampled=sampled,
        )
        if is_root:
            span.set_attribute("sample.rate", rate)
            span.set_attribute("sample.seed", self.sampler.seed)
        for key, value in attributes.items():
            span.set_attribute(key, value)
        self._register(span, is_root=is_root, rate=rate)

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
            self._register_finished(span, is_root=is_root)
            _stack_var.reset(token)
            raise
        else:
            if span.end_ns is None:
                span.end(_STATUS_OK)
            self._register_finished(span, is_root=is_root)
            _stack_var.reset(token)

    def export_finished(self, *, only_sampled: bool = False) -> int:
        """收尾并导出当前已完成的 trace；返回本次导出条数。

        ``only_sampled`` 为兼容旧接口保留：保留策略已改为按 trace 整体
        取舍（失败样本兜底保留），该参数不再逐片段过滤。
        """
        del only_sampled  # 树级保留策略取代逐片段过滤
        with self._lock:
            completed = [
                (tid, st)
                for tid, st in self._traces.items()
                if st.root_ended and st.active == 0
            ]
            for tid, st in completed:
                self._traces.pop(tid, None)
                self._export_trace_locked(tid, st)
            return sum(len(st.spans) for _, st in completed)

    def flush(self) -> None:
        """导出已完成 trace 并强制刷盘。"""
        self.export_finished()
        self.exporter.flush()

    def shutdown(self) -> None:
        """关停：强制结束并导出所有未完成片段，禁止静默丢弃。"""
        if self._closed:
            return
        with self._lock:
            # 跨线程的后台片段也在 _active_spans 中登记，关停恢复不依赖上下文栈
            active = list(self._active_spans.values())
        for span in active:
            if span.end_ns is None:
                span.end(
                    _STATUS_ERROR,
                    error_type="TracerShutdown",
                    error_message="追踪器关停时片段尚未结束",
                )
            self._register_finished(span, is_root=span.parent_id is None)
        with self._lock:
            remaining = list(self._traces.items())
            self._traces.clear()
            for tid, st in remaining:
                st.root_ended = True
                self._export_trace_locked(tid, st)
        self.exporter.flush()
        self.exporter.shutdown()
        self._closed = True

    @property
    def closed(self) -> bool:
        """是否已关停。"""
        return self._closed

    def stats(self) -> dict[str, int]:
        """采样保留统计：保留/丢弃的 trace 与片段数（判定依据可核对）。"""
        with self._lock:
            return dict(self._stats)
