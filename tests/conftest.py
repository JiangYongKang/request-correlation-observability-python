"""pytest 公共夹具。"""

from __future__ import annotations

import json
import logging

import pytest
from fastapi.testclient import TestClient

from app.config import ObservabilitySettings
from app.logging_setup import CorrelationFilter, JsonFormatter
from app.main import create_app
from app.metrics import get_metrics
from app.tracing import InMemorySpanExporter, Tracer, TracerConfig


class ListHandler(logging.Handler):
    """把结构化日志收集为记录列表。"""

    def __init__(self) -> None:
        super().__init__()
        self.setFormatter(JsonFormatter())
        self.addFilter(CorrelationFilter())
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def payloads(self) -> list[dict]:
        return [json.loads(self.format(r)) for r in self.records]


@pytest.fixture
def list_handler() -> ListHandler:
    handler = ListHandler()
    root = logging.getLogger()
    root.addHandler(handler)
    old_level = root.level
    root.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        root.removeHandler(handler)
        root.setLevel(old_level)


@pytest.fixture
def span_exporter() -> InMemorySpanExporter:
    return InMemorySpanExporter()


@pytest.fixture
def tracer(span_exporter: InMemorySpanExporter) -> Tracer:
    return Tracer(TracerConfig(sample_rate=1.0, exporter=span_exporter))


@pytest.fixture
def metrics():
    m = get_metrics()
    m.reset()
    return m


@pytest.fixture(autouse=True)
def _isolate_contextvars():
    """每个用例前后清空关联标识与追踪栈，杜绝用例间上下文污染。"""
    from app.correlation import _correlation_id_var
    from app.tracing import _stack_var

    _stack_var.set(())
    _correlation_id_var.set(None)
    yield
    _stack_var.set(())
    _correlation_id_var.set(None)


@pytest.fixture
def client(tracer: Tracer, metrics):
    # spans_export_path="" → 默认不写文件，全部进入内存导出器
    settings = ObservabilitySettings(spans_export_path="")
    application = create_app(settings=settings, tracer=tracer)
    with TestClient(application) as c:
        yield c
