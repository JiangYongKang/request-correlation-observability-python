"""滚动文件导出器：按大小/时间滚动、保留上限、后台异步写盘。

设计要点：
- **异步写盘**：``export`` 只把序列化后的行放入有界队列，由后台写线程
  落盘，请求主链路不被磁盘 I/O 拖慢；队列满时丢弃并计数（``dropped``），
  日志告警，绝不静默。
- **滚动**：当前文件达到 ``max_bytes`` 或打开时长超过 ``rotate_interval_s``
  （>0 时）即滚动：``spans.jsonl`` → ``spans.jsonl.1`` → … 序号越大越旧，
  总数（含当前文件）不超过 ``max_files``，超出部分删除，文件不无限增长。
- **周期刷写**：写线程按 ``flush_interval_s``（>0 时）定期 ``flush``+``fsync``
  已写入的数据，落盘开销与请求数解耦（不随请求数线性增长），数据可见性
  延迟以该间隔为上界；``0`` 表示不做周期刷写（仅显式 ``flush``/``shutdown``）。
- **不丢数据**：``flush`` 插入屏障并等待写线程排空队列后 ``fsync``；
  ``shutdown`` 排空队列、``fsync``、关闭并回收线程——进程正常退出时
  缓冲区数据全部落盘。文件以追加模式打开，重启后历史记录保留可读。
- **链路完整**：同一次请求的片段由上层按 trace 批量导出，同一批次写入
  同一文件；跨滚动文件的片段可用 ``trace_id`` 拼回完整链。
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
from typing import Any

from app.logging_setup import get_logger, log_event
from app.tracing import Span, SpanExporter

_logger = get_logger("app.exporter")

_KIND_LINES = "lines"
_KIND_FLUSH = "flush"
_KIND_STOP = "stop"


class RotatingFileSpanExporter(SpanExporter):
    """滚动 + 异步写盘的本地 JSONL 导出器。"""

    def __init__(
        self,
        path: str,
        *,
        max_bytes: int = 64 * 1024 * 1024,
        max_files: int = 5,
        rotate_interval_s: float = 0.0,
        queue_size: int = 10000,
        flush_interval_s: float = 1.0,
    ) -> None:
        if max_bytes < 1024:
            raise ValueError("max_bytes 至少 1024 字节")
        if max_files < 1:
            raise ValueError("max_files 至少为 1")
        if rotate_interval_s < 0:
            raise ValueError("rotate_interval_s 不能为负")
        if queue_size < 1:
            raise ValueError("queue_size 至少为 1")
        if flush_interval_s < 0:
            raise ValueError("flush_interval_s 不能为负")
        self.path = path
        self.max_bytes = max_bytes
        self.max_files = max_files
        self.rotate_interval_s = rotate_interval_s
        self.flush_interval_s = flush_interval_s
        self._queue: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=queue_size)
        self._closed = False
        self._state_lock = threading.Lock()  # 保护 _closed 与统计
        self._dropped_spans = 0
        self._written_spans = 0
        self._rotations = 0
        self._flushes = 0
        self._warned_full = 0
        self._fh: Any = None
        self._current_size = 0
        self._opened_at = 0.0
        self._dirty = False  # 仅写线程访问：有未刷盘数据
        self._writer = threading.Thread(
            target=self._run, name="span-exporter", daemon=True
        )
        self._started = False
        self._start_lock = threading.Lock()

    # ---- 写线程内部 ----

    def _ensure_open(self) -> None:
        if self._fh is not None or not self.path:
            return
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")
        self._current_size = os.path.getsize(self.path)
        self._opened_at = time.monotonic()

    def _flush_disk(self) -> None:
        """把已写入但未刷盘的数据 ``flush``+``fsync``（仅写线程调用）。"""
        if self._fh is not None and self._dirty:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._dirty = False
            with self._state_lock:
                self._flushes += 1

    def _rotate(self) -> None:
        if self._fh is not None:
            self._flush_disk()
            self._fh.close()
            self._fh = None
        # 依次后移：path.(n-1) → path.n，最旧的超出保留上限直接删除
        oldest = f"{self.path}.{self.max_files - 1}"
        if self.max_files > 1:
            if os.path.exists(oldest):
                os.remove(oldest)
            for index in range(self.max_files - 2, 0, -1):
                src = f"{self.path}.{index}"
                if os.path.exists(src):
                    os.replace(src, f"{self.path}.{index + 1}")
            if os.path.exists(self.path):
                os.replace(self.path, f"{self.path}.1")
        elif os.path.exists(self.path):
            os.remove(self.path)
        self._rotations += 1
        self._current_size = 0
        self._opened_at = time.monotonic()

    def _should_rotate(self, incoming_bytes: int) -> bool:
        if self._fh is None:
            return False
        if self._current_size > 0 and self._current_size + incoming_bytes > self.max_bytes:
            return True
        if (
            self.rotate_interval_s > 0
            and self._current_size > 0
            and time.monotonic() - self._opened_at >= self.rotate_interval_s
        ):
            return True
        return False

    def _write_lines(self, lines: list[str]) -> None:
        self._ensure_open()
        if self._fh is None:
            return
        incoming = sum(len(line.encode("utf-8")) for line in lines)
        if self._should_rotate(incoming):
            self._rotate()
            self._ensure_open()
        self._fh.writelines(lines)
        self._current_size += incoming
        self._dirty = True
        with self._state_lock:
            self._written_spans += len(lines)

    def _run(self) -> None:
        last_flush = time.monotonic()
        while True:
            timeout: float | None = None
            if self.flush_interval_s > 0:
                timeout = max(0.0, self.flush_interval_s - (time.monotonic() - last_flush))
            try:
                kind, payload = self._queue.get(timeout=timeout)
            except queue.Empty:
                # 空闲或到达周期：刷写已落文件但未 fsync 的数据
                self._flush_disk()
                last_flush = time.monotonic()
                continue
            try:
                if kind == _KIND_LINES:
                    self._write_lines(payload)
                elif kind == _KIND_FLUSH:
                    self._flush_disk()
                    payload.set()
                elif kind == _KIND_STOP:
                    self._flush_disk()
                    if self._fh is not None:
                        self._fh.close()
                        self._fh = None
                    payload.set()
                    return
            finally:
                self._queue.task_done()
            # 持续高压下队列不空也要保证周期刷写（数据可见性延迟有界）
            if (
                self._dirty
                and self.flush_interval_s > 0
                and time.monotonic() - last_flush >= self.flush_interval_s
            ):
                self._flush_disk()
                last_flush = time.monotonic()

    def _ensure_started(self) -> None:
        # 延迟启动：模块导入/应用构造不产生线程与文件副作用
        with self._start_lock:
            if not self._started:
                self._writer.start()
                self._started = True

    # ---- SpanExporter 接口 ----

    def export(self, spans: list[Span]) -> None:
        if not spans or not self.path:
            return
        with self._state_lock:
            if self._closed:
                self._dropped_spans += len(spans)
                return
        lines = [json.dumps(s.to_dict(), ensure_ascii=False, default=str) + "\n" for s in spans]
        self._ensure_started()
        try:
            self._queue.put_nowait((_KIND_LINES, lines))
        except queue.Full:
            with self._state_lock:
                self._dropped_spans += len(lines)
                self._warned_full += 1
                warned = self._warned_full
            if warned == 1 or warned % 100 == 0:
                log_event(
                    _logger,
                    30,
                    "span_export_queue_full",
                    dropped_total=self._dropped_spans,
                    queue_size=self._queue.maxsize,
                )

    def flush(self) -> None:
        if not self._started:
            return
        with self._state_lock:
            if self._closed:
                return
        event = threading.Event()
        self._queue.put((_KIND_FLUSH, event))
        event.wait()

    def shutdown(self) -> None:
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
        if not self._started:
            return
        event = threading.Event()
        self._queue.put((_KIND_STOP, event))
        event.wait()
        self._writer.join(timeout=5)

    def stats(self) -> dict[str, int]:
        """导出器计数快照（写入/丢弃/滚动/刷写次数）。"""
        with self._state_lock:
            return {
                "written_spans": self._written_spans,
                "dropped_spans": self._dropped_spans,
                "rotations": self._rotations,
                "flushes": self._flushes,
            }
