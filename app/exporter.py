"""滚动文件导出器：按大小/时间滚动、保留上限、后台批量异步写盘。

设计要点：
- **异步 + 批量写盘**：``export`` 只把序列化后的行放入有界队列（非阻塞
  ``put_nowait``）即返回，**请求主链路绝不等待任何磁盘 I/O**。写线程被
  唤醒后一次性 drain 队列中所有待写项，多批行合并为一次 ``writelines``，
  因此并发请求的写盘次数随"批次"增长而**不随请求数线性增长**。
- **刷写与请求解耦**：写线程每 ``autoflush_interval_s`` 把 OS 缓冲区
  flush 一次，每 ``fsync_interval_s`` 至多 ``fsync`` 一次——常态下每秒
  至多一次 fsync，而不是每个请求一次。显式 :meth:`flush`（屏障）才
  排空并 fsync，只在关停/测试等显式调用点使用。
- **并发安全**：所有文件写入只在唯一的写线程发生，``writelines`` 单次
  调用不会在行与行之间交错；跨请求的行互不串数据。
- **滚动**：当前文件达到 ``max_bytes`` 或打开时长超过 ``rotate_interval_s``
  （>0 时）即滚动：``spans.jsonl`` → ``spans.jsonl.1`` → … 序号越大越旧，
  总数（含当前文件）不超过 ``max_files``，超出部分删除，文件不无限增长。
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
        autoflush_interval_s: float = 0.2,
        fsync_interval_s: float = 1.0,
    ) -> None:
        if max_bytes < 1024:
            raise ValueError("max_bytes 至少 1024 字节")
        if max_files < 1:
            raise ValueError("max_files 至少为 1")
        if rotate_interval_s < 0:
            raise ValueError("rotate_interval_s 不能为负")
        if queue_size < 1:
            raise ValueError("queue_size 至少为 1")
        if autoflush_interval_s < 0:
            raise ValueError("autoflush_interval_s 不能为负")
        if fsync_interval_s < 0:
            raise ValueError("fsync_interval_s 不能为负")
        self.path = path
        self.max_bytes = max_bytes
        self.max_files = max_files
        self.rotate_interval_s = rotate_interval_s
        self.autoflush_interval_s = autoflush_interval_s
        self.fsync_interval_s = fsync_interval_s
        self._queue: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=queue_size)
        self._closed = False
        self._state_lock = threading.Lock()  # 保护 _closed 与统计
        self._dropped_spans = 0
        self._written_spans = 0
        self._rotations = 0
        self._write_batches = 0
        self._fsyncs = 0
        self._flush_barriers = 0
        self._warned_full = 0
        self._fh: Any = None
        self._current_size = 0
        self._opened_at = 0.0
        self._last_flush_at = 0.0
        self._last_fsync_at = 0.0
        self._dirty = False
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

    def _rotate(self) -> None:
        if self._fh is not None:
            self._fh.flush()
            os.fsync(self._fh.fileno())
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

    def _should_rotate(
        self, incoming_bytes: int, pending_bytes: int = 0
    ) -> bool:
        if self._fh is None:
            return False
        base = self._current_size + pending_bytes
        if base > 0 and base + incoming_bytes > self.max_bytes:
            return True
        if (
            self.rotate_interval_s > 0
            and self._current_size > 0
            and time.monotonic() - self._opened_at >= self.rotate_interval_s
        ):
            return True
        return False

    def _write_merged(self, groups: list[list[str]]) -> None:
        """把同一次 drain 中的多批行合并为尽量少的 ``writelines``。

        相邻批次只要合并后不越过滚动阈值就拼成一次物理写（一批行整体
        不拆开，保证同一次请求的行原子进入同一文件）；越界则先滚动再写，
        因此写盘系统调用次数随"合并批次"而非请求数增长。
        """
        if not groups:
            return
        self._ensure_open()
        if self._fh is None:
            return
        merged: list[str] = []
        merged_bytes = 0
        for lines in groups:
            incoming = sum(len(line.encode("utf-8")) for line in lines)
            if merged and self._should_rotate(incoming, pending_bytes=merged_bytes):
                # 当前块先落盘并滚动，新块进新文件
                self._fh.writelines(merged)
                self._current_size += merged_bytes
                self._dirty = True
                with self._state_lock:
                    self._written_spans += len(merged)
                    self._write_batches += 1
                merged, merged_bytes = [], 0
                self._rotate()
                self._ensure_open()
                if self._fh is None:
                    return
            elif not merged and self._should_rotate(incoming):
                self._rotate()
                self._ensure_open()
                if self._fh is None:
                    return
            merged.extend(lines)
            merged_bytes += incoming
        if merged:
            self._fh.writelines(merged)
            self._current_size += merged_bytes
            self._dirty = True
            with self._state_lock:
                self._written_spans += len(merged)
                self._write_batches += 1

    def _flush_buffer(self, *, fsync: bool, now: float | None = None) -> None:
        """把已写数据从 OS 缓冲刷入内核（必要时 fsync）。仅在写线程调用。"""
        if self._fh is None or not self._dirty:
            return
        now = time.monotonic() if now is None else now
        if (
            not fsync
            and self.autoflush_interval_s > 0
            and now - self._last_flush_at < self.autoflush_interval_s
        ):
            return
        if fsync or (
            self.fsync_interval_s > 0
            and self._last_fsync_at > 0
            and now - self._last_fsync_at >= self.fsync_interval_s
        ):
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._last_fsync_at = now
            self._last_flush_at = now
            with self._state_lock:
                self._fsyncs += 1
        else:
            self._fh.flush()
            self._last_flush_at = now
        self._dirty = False

    def _run(self) -> None:
        """写线程主循环：批量 drain、周期刷写、屏障与关停。"""
        timeout = (
            min(self.autoflush_interval_s, self.fsync_interval_s)
            if self.autoflush_interval_s > 0
            else self.fsync_interval_s
        )
        if timeout <= 0:
            timeout = None
        while True:
            try:
                kind, payload = self._queue.get(timeout=timeout)
            except queue.Empty:
                # 空闲节拍：按周期把缓冲区刷下去，不依赖任何请求触发
                self._flush_buffer(fsync=False)
                continue
            # 排空队列里当前已排队的全部项：多批 lines 合并成一次写盘，
            # flush/stop 屏障按入队顺序穿插处理。
            pending: list[tuple[str, Any]] = [(kind, payload)]
            while True:
                try:
                    pending.append(self._queue.get_nowait())
                except queue.Empty:
                    break
            stop = False
            line_groups: list[list[str]] = []
            for item_kind, item_payload in pending:
                if item_kind == _KIND_LINES:
                    line_groups.append(item_payload)
                elif item_kind == _KIND_FLUSH:
                    # 屏障：先把它之前（本批）的行全部合并落盘并 fsync，再放行
                    if line_groups:
                        self._write_merged(line_groups)
                        line_groups = []
                    self._flush_buffer(fsync=True)
                    with self._state_lock:
                        self._flush_barriers += 1
                    item_payload.set()
                elif item_kind == _KIND_STOP:
                    stop = True
            if line_groups:
                self._write_merged(line_groups)
            if stop:
                self._flush_buffer(fsync=True)
                if self._fh is not None:
                    self._fh.close()
                    self._fh = None
                # stop 之后不应再有屏障；若存在则放行，避免调用方永久等待
                for item_kind, item_payload in pending:
                    if item_kind == _KIND_FLUSH:
                        item_payload.set()
                payload.set()
                return
            # 写完一批即按周期尝试刷写（fsync 频率受 fsync_interval_s 限制）
            self._flush_buffer(fsync=False)

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
        """显式屏障：等待队列中排在前面的数据全部 fsync 完成。

        仅供关停/测试等显式调用点使用；请求主链路**不调用**本方法，
        避免逐请求等待磁盘 I/O。
        """
        if not self._started:
            return
        with self._state_lock:
            if self._closed:
                return
        event = threading.Event()
        self._queue.put((_KIND_FLUSH, event))
        if not event.wait(timeout=30):
            log_event(
                _logger,
                30,
                "span_export_flush_timeout",
                queue_size=self._queue.qsize(),
            )

    def shutdown(self) -> None:
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
        if not self._started:
            return
        event = threading.Event()
        self._queue.put((_KIND_STOP, event))
        event.wait(timeout=30)
        self._writer.join(timeout=5)

    def stats(self) -> dict[str, int]:
        """导出器计数快照（写入/丢弃/滚动/批次数/fsync 次数/屏障次数）。"""
        with self._state_lock:
            return {
                "written_spans": self._written_spans,
                "dropped_spans": self._dropped_spans,
                "rotations": self._rotations,
                "write_batches": self._write_batches,
                "fsyncs": self._fsyncs,
                "flush_barriers": self._flush_barriers,
            }
