"""跨阶段追踪片段（span）、采样与本地导出。

能力:
- 片段之间通过 :class:`contextvars.ContextVar` 维护栈式父子关系，
  ``asyncio`` 任务间自动隔离，与关联标识共享同一套上下文传播机制；
- 片段携带 ``correlation_id``，同一条请求链路（正常/异常/后台/流式）
  归属同一 ``trace_id``；
- 异常退出的片段被标记为 ``error`` 并保留失败原因（异常类型 + 安全化消息）；
- 采样比例可配置（请求级头采样：在根片段开始时决定，整棵树同取舍）；
- 导出方式 ``file``（JSONL，默认，无需外部服务）/``console``/``none``；
- 进程退出调用 :meth:`Tracer.shutdown` 时，把已结束但尚未导出的片段
  强制 flush，未开始/未结束的片段也标记 ``interrupted`` 后落盘，
  保证追踪数据不会静默丢弃。
"""

from __future__ import annotations

import contextvars
import json
import logging
import os
import random
import threading
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Iterator

from .correlation import get_correlation_id_or_none
from .logging_setup import sanitize_value

_STATUS_OK = "ok"
_STATUS_ERROR = "error"
_STATUS_INTERRUPTED = "interrupted"

_MAX_ATTR_VALUE_LEN = 500
_SPAN_NAME_MAX_LEN = 120

#: 当前打开的片段栈（任务隔离）
_span_stack_var: contextvars.ContextVar[tuple["Span", ...]] = contextvars.ContextVar(
    "trace_span_stack", default=()
)
#: 所在追踪树是否被采样（根片段决策，整树继承）
_trace_sampled_var: contextvars.ContextVar[bool | None] = contextvars.ContextVar(
    "trace_sampled", default=None
)
#: 当前追踪树的 trace_id
_trace_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "trace_id", default=None
)


def _new_id() -> str:
    return uuid.uuid4().hex


def _safe_attributes(attributes: dict[str, Any] | None) -> dict[str, Any]:
    if not attributes:
        return {}
    result: dict[str, Any] = {}
    for key, value in list(attributes.items())[:32]:
        key = str(key)[:64]
        result[key] = sanitize_value(key, value)
    return result


@dataclass
class Span:
    """单个追踪片段。"""

    name: str
    span_id: str = field(default_factory=_new_id)
    trace_id: str = field(default_factory=_new_id)
    parent_id: str | None = None
    start_time: float = field(default_factory=time.time)
    end_time: float | None = None
    status: str = _STATUS_OK
    error_type: str | None = None
    error_reason: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    sampled: bool = True
    correlation_id: str | None = None

    @property
    def duration_ms(self) -> float | None:
        if self.end_time is None:
            return None
        return round((self.end_time - self.start_time) * 1000.0, 3)

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[str(key)[:64]] = sanitize_value(key, value)

    def end(
        self,
        status: str = _STATUS_OK,
        error_type: str | None = None,
        error_reason: str | None = None,
    ) -> None:
        if self.end_time is not None:
            return
        self.end_time = time.time()
        self.status = status
        self.error_type = error_type
        if error_reason is not None:
            self.error_reason = sanitize_value("error_message", str(error_reason))[
                :_MAX_ATTR_VALUE_LEN
            ]

    def mark_error(self, exc: BaseException) -> None:
        """以异常信息标记片段失败，消息经脱敏处理。"""

        self.set_failure(type(exc).__name__, str(exc))

    def set_failure(self, error_type: str, reason: str) -> None:
        """标记片段失败但不写结束时间。

        用于"响应已由异常处理器生成、调用却正常返回"的场景（如受控 5xx），
        真正的结束时间仍由上下文退出时填写，从而保留后台任务的真实耗时。
        """

        self.status = _STATUS_ERROR
        self.error_type = error_type
        self.error_reason = sanitize_value("error_message", reason)[
            :_MAX_ATTR_VALUE_LEN
        ]

    def to_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_id": self.parent_id,
            "name": self.name[:_SPAN_NAME_MAX_LEN],
            "correlation_id": self.correlation_id,
            "start_time": round(self.start_time, 6),
            "end_time": round(self.end_time, 6) if self.end_time is not None else None,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "error_type": self.error_type,
            "error_reason": self.error_reason,
            "sampled": self.sampled,
            "attributes": self.attributes,
        }


class SpanExporter:
    """追踪片段导出器（file / console / none）。"""

    def __init__(self, export_kind: str, file_path: str) -> None:
        self.export_kind = export_kind
        self.file_path = file_path
        self._lock = threading.Lock()
        self._file_handle: Any = None
        if export_kind == "file":
            if not file_path:
                raise ValueError("file 导出方式必须提供非空的 trace 文件路径")
            directory = os.path.dirname(file_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            # 以 append 方式持续写入，进程重启后历史片段仍保留在同一文件
            self._file_handle = open(file_path, "a", encoding="utf-8")

    def export(self, spans: list[Span]) -> None:
        if not spans or self.export_kind == "none":
            return
        lines = [
            json.dumps(span.to_dict(), ensure_ascii=False, separators=(",", ":"))
            for span in spans
        ]
        with self._lock:
            if self.export_kind == "file" and self._file_handle is not None:
                for line in lines:
                    self._file_handle.write(line + "\n")
                self._file_handle.flush()
                os.fsync(self._file_handle.fileno())
            elif self.export_kind == "console":
                for line in lines:
                    print(f"[TRACE] {line}", flush=True)

    def shutdown(self) -> None:
        with self._lock:
            if self._file_handle is not None:
                self._file_handle.flush()
                os.fsync(self._file_handle.fileno())
                self._file_handle.close()
                self._file_handle = None


@dataclass
class Tracer:
    """追踪器：父子片段、采样决策、缓冲导出、关闭落盘。"""

    sample_rate: float
    exporter: SpanExporter
    _buffer: list[Span] = field(default_factory=list)
    _active: dict[int, Span] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _shut_down: bool = False

    def _sample_decision(self, is_root: bool) -> tuple[bool, str]:
        if is_root:
            decision = random.random() < self.sample_rate
            return decision, _new_id()
        sampled = _trace_sampled_var.get()
        trace_id = _trace_id_var.get() or _new_id()
        return bool(sampled), trace_id

    @contextmanager
    def span_context(self, name: str, attributes: dict[str, Any] | None = None) -> Iterator[Span]:
        stack = _span_stack_var.get()
        parent = stack[-1] if stack else None
        is_root = parent is None
        sampled, trace_id = self._sample_decision(is_root)

        span = Span(
            name=name[:_SPAN_NAME_MAX_LEN],
            trace_id=trace_id,
            parent_id=parent.span_id if parent else None,
            attributes=_safe_attributes(attributes),
            sampled=sampled,
            correlation_id=get_correlation_id_or_none(),
        )

        tokens = (
            _span_stack_var.set(stack + (span,)),
            _trace_sampled_var.set(sampled),
            _trace_id_var.set(trace_id),
        )
        with self._lock:
            self._active[id(span)] = span
        try:
            yield span
        except BaseException as exc:
            span.mark_error(exc)
            raise
        finally:
            if span.end_time is None:
                # 正常离开但未显式结束；保留此前通过 set_failure 标记的状态
                span.end(
                    status=span.status,
                    error_type=span.error_type,
                    error_reason=span.error_reason,
                )
            _span_stack_var.reset(tokens[0])
            _trace_sampled_var.reset(tokens[1])
            _trace_id_var.reset(tokens[2])
            self._finish_span(span)

    # 同步/异步同名上下文管理器的便捷封装
    def span(self, name: str, attributes: dict[str, Any] | None = None):
        return self.span_context(name, attributes)

    @asynccontextmanager
    async def async_span(
        self, name: str, attributes: dict[str, Any] | None = None
    ) -> AsyncIterator[Span]:
        with self.span_context(name, attributes) as span:
            yield span

    def _finish_span(self, span: Span) -> None:
        """结束片段入缓冲；被采样的片段批量导出，未采样的仅丢弃明细。"""

        with self._lock:
            self._active.pop(id(span), None)
            if span.sampled:
                self._buffer.append(span)
                ready = self._buffer[:]
                self._buffer.clear()
            else:
                ready = []
        if ready:
            try:
                self.exporter.export(ready)
            except Exception:  # noqa: BLE001 - 导出失败不能影响请求链路
                logging.getLogger("app").warning("trace_export_failed")

    def current_span(self) -> Span | None:
        stack = _span_stack_var.get()
        return stack[-1] if stack else None

    def shutdown(self) -> None:
        """关闭追踪器。

        幂等：重复调用安全。关闭时把仍在进行中的片段标记为
        ``interrupted``（并记录结束时间与原因），连同缓冲区内尚未导出的
        已结束片段一起落盘，保证进程退出/重启时追踪数据不静默丢弃。
        """

        with self._lock:
            if self._shut_down:
                return
            self._shut_down = True
            pending = self._buffer[:]
            self._buffer.clear()
            unfinished = list(self._active.values())
            self._active.clear()

        for span in unfinished:
            if span.end_time is None:
                span.end(
                    status=_STATUS_INTERRUPTED,
                    error_type="ProcessShutdown",
                    error_reason="追踪器关闭时片段尚未结束，按 interrupted 落盘",
                )
            if span.sampled:
                pending.append(span)
        if pending:
            try:
                self.exporter.export(pending)
            except Exception:  # noqa: BLE001
                logging.getLogger("app").warning("trace_export_failed")
        self.exporter.shutdown()


def configure_tracer(sample_rate: float, export_kind: str, file_path: str) -> Tracer:
    """构建进程级追踪器单例。"""

    global _tracer
    exporter = SpanExporter(export_kind=export_kind, file_path=file_path)
    tracer = Tracer(sample_rate=sample_rate, exporter=exporter)
    _tracer = tracer
    return tracer


_tracer: Tracer | None = None


def get_tracer() -> Tracer:
    if _tracer is None:
        raise RuntimeError("追踪器尚未初始化，请先调用 configure_tracer")
    return _tracer


def reset_tracer(tracer: Tracer | None = None) -> Tracer:
    """替换/重置单例追踪器（测试用）。"""

    global _tracer
    _tracer = tracer
    if tracer is None:
        exporter = SpanExporter(export_kind="none", file_path="")
        tracer = Tracer(sample_rate=1.0, exporter=exporter)
        _tracer = tracer
    return tracer
